#==============================================================================
# Copyright 2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#==============================================================================
from threading import Thread
from Queue import Queue
import logging
import botocore.session
import dnsprocessor
from dnsprocessor import DNSProcessor

try:
    import simplejson as json
except ImportError:
    import json

log = logging.getLogger("cr.dnsprocessor")
dnsprocessor.log = log


class AutoScalingNotificationRunner(object):
    def __init__(self, queue_url, region, table, num_threads=2):
        # The SQS queue to poll for events.
        self._queue_url = queue_url

        # Construct an unbounded Queue to hold our pending tasks
        self._task_queue = Queue()

        # Construct a task to poll the new queue
        self._task_queue.put(QueuePollTask(queue_url, region, table))

        # Determine the maximum number of threads to use
        self._num_threads = max(1, num_threads)

    def process_messages(self):
        for i in range(self._num_threads):
            worker = Thread(target=self.task_worker)
            worker.daemon = True
            worker.start()

    def task_worker(self):
        while True:
            task = self._task_queue.get()
            try:
                new_tasks = task.execute_task()
                if new_tasks:
                    for t in new_tasks:
                        self._task_queue.put(t)
            except:
                log.exception(u"Failed executing task")
            finally:
                self._task_queue.task_done()

                # Reschedule the polling tasks
                if isinstance(task, QueuePollTask):
                    self._task_queue.put(task)


class Message(object):
    def __init__(self, queue_url, region, message):
        self._queue_url = queue_url
        self._message = message
        self._region = region

    def parse_message(self):
        return json.loads(json.loads(self._message["Body"])["Message"])

    @property
    def region(self):
        return self._region

    def delete(self):
        sqs = botocore.session.get_session().get_service("sqs")
        delete = sqs.get_operation("DeleteMessage")
        http_response, response_data = delete.call(sqs.get_endpoint(self._region),
                                                   queue_url=self._queue_url,
                                                   receipt_handle=self._message.get("ReceiptHandle"))

        # Swallow up any errors/issues, logging them out
        if http_response.status_code != 200:
            log.error(u"Failed to delete message from queue %s with status_code %s: %s" %
                      (self._queue_url, http_response.status_code, response_data))

    def change_message_visibility(self, timeout):
        sqs = botocore.session.get_session().get_service("sqs")
        delete = sqs.get_operation("ChangeMessageVisibility")
        http_response, response_data = delete.call(sqs.get_endpoint(self._region),
                                                   queue_url=self._queue_url,
                                                   receipt_handle=self._message.get("ReceiptHandle"),
                                                   visibility_timeout=timeout)

        # Swallow up any errors/issues, logging them out
        if http_response.status_code != 200:
            log.error(u"Failed to change visibility of message from queue %s with status_code %s: %s" %
                      (self._queue_url, http_response.status_code, response_data))


class ScalingNotification(object):
    def _validate_property(self, property_name):
        if not property_name in self._notification:
            raise ValueError(u"ScalingNotification requires %s" % property_name)

    def __init__(self, message):
        self._message = message
        self._notification = self._message.parse_message()

        # Ensure the notification has some required fields.
        self._validate_property("AutoScalingGroupName")
        self._validate_property("EC2InstanceId")
        self._validate_property("Event")

        valid_events = ['autoscaling:EC2_INSTANCE_LAUNCH', 'autoscaling:EC2_INSTANCE_TERMINATE']
        event = self._notification["Event"]
        if event not in valid_events:
            raise ValueError(u"ScalingNotification requires Event to be %s", valid_events)

    @property
    def event(self):
        return self._notification["Event"]

    @property
    def instance_id(self):
        return self._notification["EC2InstanceId"]

    @property
    def auto_scaling_group(self):
        return self._notification["AutoScalingGroupName"]

    @property
    def region(self):
        return self._message.region

    def increase_timeout(self, timeout):
        """Attempts to increase the message visibility timeout."""
        self._message.change_message_visibility(timeout)

    def delete(self):
        self._message.delete()

    def __repr__(self):
        return str(self._notification)


class BaseTask(object):
    def execute_task(self):
        pass


class QueuePollTask(BaseTask):
    def __init__(self, queue_url, region, table):
        self._queue_url = queue_url
        self._region = region
        self._table = table

    def retrieve_notifications(self, max_notifications=1):
        """Attempts to retrieve notifications from the provided SQS queue"""
        session = botocore.session.get_session()
        sqs = session.get_service("sqs")
        receive = sqs.get_operation("ReceiveMessage")
        http_response, response_data = receive.call(sqs.get_endpoint(self._region),
                                                    queue_url=self._queue_url,
                                                    wait_time_seconds=20,
                                                    max_number_of_messages=max_notifications)

        # Swallow up any errors/issues, logging them out
        if http_response.status_code != 200 or not "Messages" in response_data:
            log.error(u"Failed to retrieve messages from queue %s with status_code %s: %s" %
                      (self._queue_url, http_response.status_code, response_data))
            return []

        notifications = []
        for msg in response_data.get("Messages", []):
            # Construct a message that we can parse into events.
            message = Message(self._queue_url, self._region, msg)
            try:
                notifications.append(ScalingNotification(message))
            except Exception:
                log.exception(u"Invalid message received; will delete from queue: %s", msg)
                message.delete()

        return notifications

    def execute_task(self):
        log.debug(u"Checking queue %s", self._queue_url)
        notifications = self.retrieve_notifications()

        tasks = []
        for notification in notifications:
            if notification.event == 'autoscaling:EC2_INSTANCE_LAUNCH':
                # Create a mapping
                tasks.append(CreateMappingTask(notification, self._table))
            else:
                # Delete a mapping
                tasks.append(DeleteMappingTask(notification, self._table))

        return tasks


class MappingTask(BaseTask):
    def __init__(self, notification, table):
        self._notification = notification
        self._table = table

    def get_processor_id(self):
        session = botocore.session.get_session()
        autoscaling = session.get_service("autoscaling")
        describe = autoscaling.get_operation("DescribeTags")
        response, data = describe.call(autoscaling.get_endpoint(self._notification.region),
                                       filters=[{
                                           "Name": "auto-scaling-group",
                                           "Values": [self._notification.auto_scaling_group]
                                       }])

        # Swallow up any error responses and return nothing.
        if response.status_code != 200 or 'Tags' not in data:
            log.error(u"Failed to retrieve tags for ASG %s with status_code %s: %s" %
                      (self._notification.auto_scaling_group, response.status_code, data))
            return None

        for tag in data.get('Tags', []):
            if tag.get('Key', '') == 'ProcessorId':
                return tag.get('Value')

        return None


class CreateMappingTask(MappingTask):
    def __init__(self, notification, table):
        super(CreateMappingTask, self).__init__(notification, table)

    def _get_instance_ip(self, instance_id):
        session = botocore.session.get_session()
        ec2 = session.get_service("ec2")
        describe = ec2.get_operation("DescribeInstances")
        response, data = describe.call(ec2.get_endpoint(self._notification.region),
                                       instance_ids=[instance_id])

        # Swallow up any error responses and return nothing.
        if response.status_code != 200 or 'Reservations' not in data:
            log.error(u"Failed to describe instance %s with status_code %s: %s" %
                      (instance_id, response.status_code, data))
            return None

        for reservation in data.get('Reservations', []):
            for instance in reservation.get('Instances', []):
                if instance.get('InstanceId', '') == instance_id:
                    return instance.get('PublicIpAddress')

        return None

    def execute_task(self):
        log.debug(u"CreateMapping for notification %s" % self._notification)
        # Determine the public ip address of our instance.
        ip_address = self._get_instance_ip(self._notification.instance_id)
        log.error("IP: %s", ip_address)
        if not ip_address:
            return []

        # Determine the processor_id that is handling our naming
        processor_id = self.get_processor_id()
        # If we couldn't find processor_id, keep message and we'll retry message later.
        log.error('Processor Id: %s', processor_id)
        if not processor_id:
            return []

        # Create the new route.
        processor = DNSProcessor(None, self._table, self._notification.region)
        try:
            delete_notification = processor.add_instance_mapping(processor_id,
                                                                 self._notification.instance_id,
                                                                 ip_address)
        except ValueError:
            log.exception("Invalid mapping to add")
            delete_notification = True

        # Delete our notification
        if delete_notification:
            self._notification.delete()


class DeleteMappingTask(MappingTask):
    def __init__(self, notification, table):
        super(DeleteMappingTask, self).__init__(notification, table)

    def execute_task(self):
        log.debug(u"DeleteMapping for notification %s" % self._notification)
        processor_id = self.get_processor_id()
        # If we couldn't find processor_id, keep message and we'll retry message later.
        if not processor_id:
            return []

        # Delete the route.
        processor = DNSProcessor(None, self._table, self._notification.region)
        try:
            delete_notification = processor.remove_instance_mapping(processor_id, self._notification.instance_id)
        except ValueError:
            log.exception("Invalid mapping to remove")
            delete_notification = True

        # Delete our notification
        if delete_notification:
            self._notification.delete()
