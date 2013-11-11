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
import logging
from argparse import ArgumentParser
from dnsprocessor import DNSProcessor
from dnsprocessor import FatalError

try:
    import simplejson as json
except ImportError:
    import json

handler = logging.StreamHandler(sys.stderr)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logging.getLogger().addHandler(handler)

log = logging.getLogger('delete-dns-processor')
log.setLevel(logging.INFO)

parser = ArgumentParser(prog='delete-dns-processor')
parser.add_argument("-r", "--region", help="The region the DynamoDB table lives in", dest="region")
parser.add_argument("-t", "--table", help="The DynamoDB table name to write name states", dest="table_name")

options = parser.parse_args()

if not options.table_name:
    raise FatalError(u"Name table is a required parameter. Use -t/--table <table> to specify.")

if not options.region:
    raise FatalError(u"Region is a required parameter. Use -r/--region <region> to specify.")

try:
    event_obj = json.loads(os.environ.get('EventProperties'))
    log.info(u"Received delete event: %s", json.dumps(event_obj, indent=4))
except ValueError:
    raise FatalError(u"Could not parse properties as JSON")

resource_properties = event_obj.get('ResourceProperties')

if not resource_properties:
    raise FatalError(u"Resource Properties not found.")

physical_id = event_obj['PhysicalResourceId']

hosted_zone_id = resource_properties.get('HostedZoneId')

if not hosted_zone_id:
    raise FatalError(u"HostedZoneId is a required property.")

processor = DNSProcessor(None, options.table_name, options.region)

print processor.delete_processor(physical_id, hosted_zone_id)