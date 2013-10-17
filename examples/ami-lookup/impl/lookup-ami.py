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
import os
import sys
import re
import time
import logging
from argparse import ArgumentParser

import requests
import botocore.session


handler = logging.StreamHandler(sys.stderr)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logging.getLogger().addHandler(handler)

log = logging.getLogger('lookup-ami')
log.setLevel(logging.INFO)

parser = ArgumentParser(prog='lookup-ami')
parser.add_argument("-s", "--source", help="The source url for the ami information", dest="source")
parser.add_argument("-r", "--region", help="The region the audit trail will be written to in DynamoDB", dest="region")
parser.add_argument("-t", "--audit-table", help="The DynamoDB table name to write the audit trail", dest="table_name")

options = parser.parse_args()


class FatalError(SystemExit):
    def __init__(self, reason):
        super(FatalError, self).__init__(-1)
        log.error('Failing resource: %s', reason)
        print u'{ "Reason": "%s" }' % reason


def required(dict_in, key, field):
    """Ensures the key is in the dictionary, otherwise fails the resource."""
    if key.lower() not in dict_in:
        raise FatalError(u"%s is not a recognized value for the '%s' property." % (key, field))
    return dict_in[key.lower()]

# Make sure we have a source to download from.
if not options.source:
    raise FatalError(u"Service not configured to handle requests.")

# Determine whether auditing should be enabled.
audit = True if options.region and options.table_name else False
if options.table_name and not options.region:
    raise FatalError(u"Region is a required parameter for auditing. Use -r/--region <region> to specify.")

# Get the request type for this
request_type = os.getenv('Event_RequestType')
if not request_type:
    raise FatalError(u"Event_RequestType was not valid.")

stack_id = os.getenv('Event_StackId')
if not stack_id:
    raise FatalError(u"Event_StackId is a required attribute.")

if request_type is not 'Delete':
    # Download the AMI manifest
    try:
        r = requests.get(options.source, verify=True)
        r.raise_for_status()
        os_bundle = r.json()
    except Exception, e:
        raise FatalError(u"Service not configured: %s" % (str(e)))

    # Pull in our options
    operating_system = os.getenv('Event_ResourceProperties_os')
    architecture = os.getenv('Event_ResourceProperties_arch', '64')
    version = os.getenv('Event_ResourceProperties_version')
    region = os.getenv('Event_ResourceProperties_region')

    # Validate we have the required fields.
    if not operating_system:
        raise FatalError(u"'os' is a required property")

    # Try to parse region from stackId if not provided by stack
    if not region:
        match = re.match(r"arn:aws:cloudformation:([^:]+):.*", stack_id, re.I)
        if not match:
            raise FatalError(u"Unable to determine region. Provide 'region' property for this resource.")
        region = match.group(1)

    # Locate our operating system/architecture.
    arch_map = required(required(os_bundle, operating_system, 'os'), architecture, 'arch')

    # Handle our version in a special manner, since they can specify 'latest'.
    if not version or version.lower() == 'latest':
        if 'latest' not in arch_map:
            raise FatalError(u"Latest version not defined for %s/%s. 'version' property is required.")
        version = arch_map['latest']

    # Locate our ami based on version/region.
    ami = required(required(arch_map, version, 'version'), region, 'region')
else:
    # When auditing for delete, we want the existing AMI, not a new one (if manifest has changed)
    ami = os.getenv('Event_PhysicalResourceId')

# Write our audit trail to DynamoDB to track ami usage
if audit:
    try:
        request_id = os.getenv('Event_RequestId')
        logical_id = os.getenv('Event_LogicalResourceId')
        if not (request_id and logical_id):
            raise FatalError(u"Event_RequestId and Event_LogicalResourceId are required for auditing.")

        ddb = botocore.session.get_session().get_service("dynamodb")
        put = ddb.get_operation("PutItem")
        http_response, response_data = put.call(ddb.get_endpoint(options.region),
                                                table_name=options.table_name,
                                                item={
                                                    "Ami": {"S": ami},
                                                    "RequestId": {"S": request_id},
                                                    "StackId": {"S": stack_id},
                                                    "LogicalResourceId": {"S": logical_id},
                                                    "Time": {"N": str(time.time())},
                                                    "RequestType": {"S": request_type}
                                                })

        # If we are auditing and can't log, fail the request.
        if http_response.status_code != 200:
            raise FatalError(u"Failed putting audit log (%s): %s: " % (http_response.status_code, response_data))
    except Exception, e:
        raise FatalError(u"Unhandled exception creating audit log: %s" % e)

# Write out our successful response!
print u'{ "PhysicalResourceId" : "%s", "Data": { "os": "%s", "arch": "%s", "version": "%s", "region": "%s", ' \
      u'"ami": "%s" } }' % (ami, operating_system.lower(), architecture.lower(), version.lower(), region.lower(), ami)
