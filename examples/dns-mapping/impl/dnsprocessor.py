#!/usr/bin/env python
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
import logging
import hashlib
import sys
import pystache

import botocore.session

try:
    import simplejson as json
except ImportError:
    import json

handler = logging.StreamHandler(sys.stderr)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logging.getLogger().addHandler(handler)

log = logging.getLogger('dnsprocessor')
log.setLevel(logging.INFO)

_NAMES = sorted(['bart', 'lisa', 'homer', 'marge', 'maggie', 'patty', 'selma', 'abe', 'mona', 'moe', 'artie'])


class FatalError(SystemExit):
    def __init__(self, reason):
        super(FatalError, self).__init__(-1)
        log.error(u"Failing resource: %s", reason)
        print u'{ "Reason": "%s" }' % reason


class DNSProcessor(object):
    def __init__(self, topic, table_name, region):
        self._topic = topic
        self._table_name = table_name
        self._region = region
        self._ddb = botocore.session.get_session().get_service("dynamodb")
        self._r53 = botocore.session.get_session().get_service("route53")

    def _create_processor_id(self, stack_id, logical_id, hosted_zone_id, dns_pattern):
        """Constructs a repeated id to help with idempotency."""
        return str(hashlib.sha256(stack_id + "|" + logical_id + "|" + hosted_zone_id + "|" + dns_pattern).hexdigest())

    def _create_processor_record(self, processor_id, request_id, hosted_zone_id, dns_pattern):
        """Creates a record that can be stored in DynamoDB"""
        record = {
            "ProcessorId": {'S': processor_id},
            "HostedZoneId": {'S': hosted_zone_id},
            "DnsPattern": {'S': dns_pattern},
            "RequestId": {'S': request_id},
            "UpdateVersion": {'N': '1'}
        }

        # Write our names for this new resource into the DynamoDB table to track status.
        for name in _NAMES:
            record['name_' + name] = {'S': 'A'}

        return record

    def _get_processor_record(self, processor_id, attributes=None, default=None, error_type=FatalError):
        # Check if this is the same request.
        get = self._ddb.get_operation("GetItem")
        kwargs = {
            'consistent_read': True,
            'key': {
                "ProcessorId": {'S': processor_id}
            }
        }
        kwargs['table_name'] = self._table_name
        if attributes:
            kwargs['attributes_to_get'] = attributes

        get_response, get_data = get.call(self._ddb.get_endpoint(self._region), **kwargs)
        if get_response.status_code != 200:
            raise error_type(u"Failed reading entry (%s): %s: " % (get_response.status_code, get_data))

        try:
            return get_data.get('Item', default)
        except:
            return default

    def _delete_processor_record(self, processor_id):
        delete = self._ddb.get_operation("DeleteItem")

        response, data = delete.call(self._ddb.get_endpoint(self._region),
                                     table_name=self._table_name,
                                     key={
                                         "ProcessorId": {'S': processor_id}
                                     })

        if response.status_code != 200:
            raise FatalError(u"Failed deleting DynamoDB entry (%s): %s: " % (response.status_code, data))

    def _processor_create_success(self, processor_id):
        return u'{ "PhysicalResourceId" : "%s", "Data": { "Topic": "%s" } }' % (processor_id, self._topic)

    def _store_processor_record(self, processor_id, record, request_id):
        try:
            put = self._ddb.get_operation("PutItem")

            put_response, put_data = put.call(self._ddb.get_endpoint(self._region),
                                              table_name=self._table_name,
                                              item=record,
                                              expected={
                                                  "ProcessorId": {
                                                      "Exists": "false"
                                                  }
                                              })
            # If we couldn't store the row, check why
            if put_response.status_code != 200:
                valid_request = False
                for error in put_data.get('Errors', []):
                    # If the row already exists, handle duplicate requests as a success.
                    if error.get('Code', '') == 'ConditionalCheckFailedException':
                        existing_record = self._get_processor_record(processor_id, ['RequestId'])
                        # Determine if this is a duplicate request.
                        if existing_record and existing_record.get('RequestId', {}).get('S') != request_id:
                            raise FatalError(u"Resource already exists for the specified hosted zone.")

                        # This was a valid duplicate request.
                        valid_request = True
                        break

                if not valid_request:
                    raise FatalError(u"Failed creating DynamoDB entry (%s): %s: " % (put_response.status_code, put_data))

        except Exception, e:
            log.exception("Unexpected exception creating entry")
            raise FatalError(u"Unhandled exception creating entry: %s" % e)

    def _update_processor_record(self, record):
        try:
            expected_version = record.get('UpdateVersion', {}).get('N', '1')
            record['UpdateVersion'] = {'N': str(long(expected_version) + 1)}

            put = self._ddb.get_operation("PutItem")

            put_response, put_data = put.call(self._ddb.get_endpoint(self._region),
                                              table_name=self._table_name,
                                              item=record,
                                              expected={
                                                  "UpdateVersion": {
                                                      "Value": {"N": expected_version}
                                                  }
                                              })
            # If we couldn't update the row, fail and let it be retried later
            if put_response.status_code != 200:
                log.error(u"Failed creating DynamoDB entry (%s): %s: ", put_response.status_code, put_data)
                return False

        except Exception:
            log.exception(u"Unexpected exception updating entry")
            return False

        return True

    def create_processor(self, stack_id, logical_id, request_id, hosted_zone_id, dns_pattern):
        processor_id = self._create_processor_id(stack_id, logical_id, hosted_zone_id, dns_pattern)
        self._store_processor_record(processor_id,
                                     self._create_processor_record(processor_id, request_id,
                                                                   hosted_zone_id, dns_pattern),
                                     request_id)
        return self._processor_create_success(processor_id)

    def delete_processor(self, processor_id, hosted_zone_id):
        try:
            # Locate the resource we are trying to delete.
            existing_item = self._get_processor_record(processor_id, default={})

            #
            # Delete all of the route53 entries for this custom resource
            #
            changes = []
            for key, value in existing_item.iteritems():
                # Find names that are not available and add to r53 request to delete entry.
                if key.startswith('name_') and value.get('S', '') != 'A':
                    domain_data = value.get('S', '').split('|', 3)
                    if len(domain_data) == 3:
                        try:
                            log.info(u"Preparing to delete record set: %s", domain_data[2])
                            record_set = json.loads(domain_data[2])
                        except ValueError:
                            raise FatalError(u"Could not parse DNS record entry: %s: %s" % (key, value))

                        changes.append({
                            'Action':  'DELETE',
                            'ResourceRecordSet': record_set
                        })

            if len(changes) != 0:
                change_records = self._r53.get_operation("ChangeResourceRecordSets")
                change_response, change_data = change_records.call(self._r53.get_endpoint(self._region),
                                                                   hosted_zone_id=hosted_zone_id,
                                                                   change_batch={
                                                                       'Comment': u"Deleting DNSProcessor %s."
                                                                                  % processor_id,
                                                                       'Changes': changes
                                                                   })

                if change_response.status_code != 200:
                    valid_delete = True
                    for error in change_data.get('Errors', []):
                        # If the error isn't that the value doesn't match, we will say the update failed.
                        if (error.get('Code', '') != 'InvalidChangeBatch'
                                or (u"values provided do not match the current values" not in error.get('Message', '')
                                    and u"it was not found" not in error.get('Message', ''))):
                            valid_delete = False
                            break

                    if not valid_delete:
                        raise FatalError(u"Failed deleting DNS entries (%s): %s: " % (change_response.status_code,
                                                                                      change_data))

            # Lastly, delete our custom resource from our DynamoDB table
            self._delete_processor_record(processor_id)

        except Exception, e:
            log.exception(u"Unexpected exception deleting dns processor")
            raise FatalError(u"Unhandled exception deleting dns processor: %s" % e)

        return "{}"

    def _name_status(self, status, instance_id, record_set):
        try:
            if isinstance(record_set, basestring):
                record_set = json.loads(record_set)

            return "%s|%s|%s" % (status, instance_id, json.dumps(record_set))
        except ValueError:
            raise FatalError(u"Could not serialize status record set: %s" % record_set)

    def _get_hosted_zone_domain(self, hosted_zone_id, required=True, error_type=FatalError):
        """Retrieves the domain for a hosted zone"""
        try:
            get_zone = self._r53.get_operation("GetHostedZone")
            response, data = get_zone.call(self._r53.get_endpoint(self._region),
                                           id=hosted_zone_id)
            if response.status_code != 200:
                log.exception(u"Failed to retrieve hosted zone (%s) information" % hosted_zone_id)
                raise error_type(u"Failed to retrieve hosted zone (%s): %s: " % (response.status_code, data))

            domain = data.get('HostedZone', {}).get('Name')
            if not domain:
                if required:
                    raise error_type(u"HostedZone (%s) was not found." % hosted_zone_id)
                return None

            return domain
        except Exception, e:
            log.exception(u"Unexpected exception listing hosted zone")
            raise error_type(u"Unhandled exception listing hosted zone: %s" % e)

    def _generate_domain_name(self, dns_pattern, name, domain, error_type=FatalError):
        try:
            return pystache.render(dns_pattern, {'simpsons_name': name, 'hosted_zone_name': domain})
        except:
            raise error_type(u"Failed to generate domain name using pattern %s" % dns_pattern)

    def _find_instance_entry(self, processor, instance_id):
        """Locates the processor entry for an instance; returns parsed entry"""
        for key, value in processor.iteritems():
            if key.startswith('name_') and value.get('S', '') != 'A':
                    domain_data = value.get('S', '').split('|', 3)
                    if len(domain_data) != 3 or domain_data[1] != instance_id:
                        continue

                    return key.split("name_", 2)[1], domain_data

        return None

    def _create_r53_record(self, entry_name, ip_address):
        return {
            'Name': entry_name,
            'Type': 'A',
            'TTL': 300,
            'ResourceRecords': [{'Value': ip_address}]
        }

    def add_instance_mapping(self, processor_id, instance_id, ip_address):
        try:
            processor = self._get_processor_record(processor_id, error_type=ValueError)
        except ValueError:
            # Retry exceptions while trying to retrieve processor record
            return False

        # This should only happen if the processor is unregistered, so fail hard.
        if not processor:
            raise ValueError(u"Processor %s no longer exists." % processor_id)

        hosted_zone_id = processor['HostedZoneId'].get('S')
        dns_pattern = processor['DnsPattern'].get('S')

        # Check if instance is registered
        entry = self._find_instance_entry(processor, instance_id)

        if not entry:
            # Find a name to use, alphabetically so the same names keep being used.
            for key in sorted(processor):
                if key.startswith('name_') and processor[key].get('S', '') == 'A':
                    name = key.split('name_', 2)[1]

                    # Locate the root domain name
                    try:
                        domain = self._get_hosted_zone_domain(hosted_zone_id, False, error_type=ValueError)
                        if not domain:
                            raise ValueError(u"Hosted Zone %s does not exist" % hosted_zone_id)
                    except ValueError:
                        # Failures trying to get domain should be retried.
                        return False

                    dns_name = self._generate_domain_name(dns_pattern, name, domain, error_type=ValueError)

                    entry_str = self._name_status('A', instance_id, self._create_r53_record(dns_name, ip_address))
                    entry = (name, entry_str.split('|', 3))
                    break

            if not entry:
                # No names are currently available
                return False

        # If the name hasn't been reserved, update processor to mark the name as reserved.
        if entry[1][0] == 'A':
            # Set the name status to reserved.
            entry[1][0] = 'R'
            processor['name_' + entry[0]] = {'S': self._name_status(*(entry[1]))}

            # Attempt to reserve the entry.
            if not self._update_processor_record(processor):
                # Name couldn't be reserved, retry later
                return False

        # If the name is reserved, we need to create the r53 record.
        if entry[1][0] == 'R':

            try:
                record_set = json.loads(entry[1][2])
            except ValueError:
                log.exception("Failed to parse recordset %s" % entry[1][2])
                return False

            change_records = self._r53.get_operation("ChangeResourceRecordSets")
            change_response, change_data = change_records.call(self._r53.get_endpoint(self._region),
                                                               hosted_zone_id=hosted_zone_id,
                                                               change_batch={
                                                                   'Comment': u"Registering for DNSProcessor %s."
                                                                              % processor_id,
                                                                   'Changes': [
                                                                       {
                                                                           'Action': 'CREATE',
                                                                           'ResourceRecordSet': record_set
                                                                       }]
                                                               })
            if change_response.status_code != 200:
                valid_create = True
                for error in change_data.get('Errors', []):
                    if (error.get('Code', '') != 'InvalidChangeBatch'
                            or u"but it already exists" not in error.get('Message', '')):
                        valid_create = False
                        break

                if not valid_create:
                    log.error(u"Failed creating DNS entry (%s): %s: ", change_response.status_code, change_data)
                    return False

            # Set the name status to used.
            entry[1][0] = 'U'
            processor['name_' + entry[0]] = {'S': self._name_status(*(entry[1]))}

            # Attempt to reserve the entry.
            return self._update_processor_record(processor)

        return True

    def remove_instance_mapping(self, processor_id, instance_id):
        try:
            processor = self._get_processor_record(processor_id, error_type=ValueError)
        except ValueError:
            # Retry exceptions while trying to retrieve processor record
            return False

        # This should only happen if the processor is unregistered, so the record should be gone
        if not processor:
            return True

        hosted_zone_id = processor['HostedZoneId'].get('S')

        # Find the entry for our instance
        entry = self._find_instance_entry(processor, instance_id)
        if not entry:
            return True

        try:
            record_set = json.loads(entry[1][2])
        except ValueError:
            log.exception("Failed to parse recordset %s" % entry[1][2])
            return False

        # Delete r53 entry for instance
        change_records = self._r53.get_operation("ChangeResourceRecordSets")
        change_response, change_data = change_records.call(self._r53.get_endpoint(self._region),
                                                           hosted_zone_id=hosted_zone_id,
                                                           change_batch={
                                                               'Comment': u"Deleting DNSProcessor %s."
                                                                          % processor_id,
                                                               'Changes': [
                                                                   {
                                                                       'Action': 'DELETE',
                                                                       'ResourceRecordSet': record_set
                                                                   }]
                                                           })

        if change_response.status_code != 200:
            valid_delete = True
            for error in change_data.get('Errors', []):
                # If the error isn't that the value doesn't match, we will say the update failed.
                if (error.get('Code', '') != 'InvalidChangeBatch'
                        or (u"values provided do not match the current values" not in error.get('Message', '')
                            and u"it was not found" not in error.get('Message', ''))):
                    valid_delete = False
                    break

            if not valid_delete:
                log.error(u"Failed deleting DNS entries (%s): %s: ", change_response.status_code, change_data)
                return False

        # Update the processor to indicate name is available.
        processor['name_' + entry[0]] = {'S': 'A'}
        return self._update_processor_record(processor)

    def update_processor(self, old_processor_id, stack_id, logical_id, request_id, hosted_zone_id, dns_pattern):
        try:
            processor_id = self._create_processor_id(stack_id, logical_id, hosted_zone_id, dns_pattern)

            # If the processor id matches, no work needs to be done, so short-circuit.
            if old_processor_id == processor_id:
                return self._processor_create_success(processor_id)

            # Check if processor already exists
            record = self._get_processor_record(processor_id)
            if record:
                return self._processor_create_success(processor_id)

            # Get existing processor entries
            existing_record = self._get_processor_record(old_processor_id)

            # If there is no existing record, just create a default record.
            if not existing_record:
                return self.create_processor(stack_id, logical_id, request_id, hosted_zone_id, dns_pattern)

            # Construct a default entry for new processor
            new_record = self._create_processor_record(processor_id, request_id, hosted_zone_id, dns_pattern)

            domain = self._get_hosted_zone_domain(hosted_zone_id)

            # Copy over any existing entries and prepare to call route53
            changes = []
            unmatched = {}
            for key, value in existing_record.iteritems():
                # Find names that are not available and add to r53 request to delete entry.
                if key.startswith('name_') and value.get('S', '') != 'A':
                    current_name = key.split('name_', 2)[1]
                    domain_data = value.get('S', '').split('|', 3)
                    if len(domain_data) == 3:
                        try:
                            log.info(u"Preparing to copy record set: %s", domain_data[2])
                            record_set = json.loads(domain_data[2])
                        except ValueError:
                            raise FatalError(u"Could not parse DNS record entry: %s: %s" % (key, value))

                        # Store the unmatched entry to locate a new name later.
                        if key not in new_record:
                            unmatched[key] = (domain_data[1], record_set)
                            continue

                        record_set['Name'] = self._generate_domain_name(dns_pattern, current_name, domain)
                        new_record[key] = {'S': self._name_status('U', domain_data[1], record_set)}
                        changes.append({
                            'Action':  'CREATE',
                            'ResourceRecordSet': record_set
                        })

            for unmatched_key, unmatched_tuple in unmatched.iteritems():
                inserted = False
                for key in sorted(new_record):
                    if key.startswith('name_') and new_record[key].get('S', '') == 'A':
                        inserted = True
                        new_name = key.split('name_', 2)[1]

                        unmatched_record_set = unmatched_tuple[1]
                        unmatched_record_set['Name'] = self._generate_domain_name(dns_pattern, new_name, domain)
                        new_record[key] = {'S': self._name_status('U', unmatched_tuple[0], unmatched_record_set)}
                        changes.append({
                            'Action':  'CREATE',
                            'ResourceRecordSet': unmatched_record_set
                        })
                        break
                if not inserted:
                    raise FatalError(u"Unable to create DNS mapping for %s" % unmatched_key)

            # Write entries to route53
            if len(changes) != 0:
                change_records = self._r53.get_operation("ChangeResourceRecordSets")
                change_response, change_data = change_records.call(self._r53.get_endpoint(self._region),
                                                                   hosted_zone_id=hosted_zone_id,
                                                                   change_batch={
                                                                       'Comment': u"Updating DNSProcessor %s."
                                                                                  % processor_id,
                                                                       'Changes': changes
                                                                   })

                if change_response.status_code != 200:
                    valid_change = True
                    for error in change_data.get('Errors', []):
                        # If the error isn't that the value doesn't match, we will say the update failed.
                        if (error.get('Code', '') != 'InvalidChangeBatch'
                                or u"but it already exists" not in error.get('Message', '')):
                            valid_change = False
                            break
                    if not valid_change:
                        raise FatalError(u"Failed to create DNS entries (%s): %s: " % (change_response.status_code,
                                                                                       change_data))

            # Store the new processor record
            self._store_processor_record(processor_id, new_record, request_id)
            return self._processor_create_success(processor_id)
        except Exception, e:
            log.exception(u"Unexpected exception updating dns processor")
            raise FatalError(u"Unhandled exception updating dns processor: %s" % e)
