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

import boto
from boto.dynamodb2.table import Table


handler = logging.StreamHandler(sys.stderr)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logging.getLogger().addHandler(handler)

log = logging.getLogger('lookup-eip')
log.setLevel(logging.INFO)

parser = ArgumentParser(prog='lookup-eip')
parser.add_argument("-r", "--region", help="The region the audit trail will be written to in DynamoDB", dest="region")
parser.add_argument("-t", "--eip-table", help="The DynamoDB table that lists available EIPs", dest="table_name")

options = parser.parse_args()


class FatalError(SystemExit):
    def __init__(self, reason):
        super(FatalError, self).__init__(-1)
        log.error('Failing resource: %s', reason)
        print u'{ "Reason": "%s" }' % reason

# Get Region
if not options.region or not options.table_name:
    raise FatalError(u"Service not configured to handle requests.")

# Get the request type for this
request_type = os.getenv('Event_RequestType')
if not request_type:
    raise FatalError(u"Event_RequestType was not valid.")

# Get Stack ID
stack_id = os.getenv('Event_StackId')
if not stack_id:
    raise FatalError(u"Event_StackId is a required attribute.")

# Get Logical ID of this resource
logical_id = os.getenv('Event_LogicalResourceId')
if not logical_id:
    raise FatalError(u"Event_LogicalResourceId is a required attribute.")

# Get pool indicating where to get EIP from
pool = os.getenv('Event_ResourceProperties_pool', 'default')


def get_address(pool):
    """Retrieve an EIP for the given pool from DynamoDB"""
    #Connect to ddb
    conn = boto.dynamodb2.connect_to_region(options.region)
    ddb = Table(options.table_name, connection=conn)

    # Get available EIPs from pool
    eips = ddb.query(
        pool__eq=pool,
        consistent=True
    )

    if not eips:
        raise FatalError(u"No EIPs found in pool %s" % pool)

    address = None
    for eip in eips:
        if not eip.get('stack_id', False):
            eip['stack_id'] = stack_id
            eip['logical_id'] = logical_id
            if eip.save():
                address = eip['address']
                break

    if not address:
        raise FatalError(u"All EIPs in pool %s are in use" % pool)

    return address


def delete_address(pool, address):
    """Mark an EIP as no longer in use"""
    #Connect to ddb
    conn = boto.dynamodb2.connect_to_region(options.region)
    ddb = Table(options.table_name, connection=conn)

    eip = ddb.get_item(pool=pool, address=address)
    del eip['stack_id']
    del eip['logical_id']
    eip.save()

if request_type == 'Create':
    physical_id = get_address(pool)

elif request_type == 'Update':
    old_pool = os.getenv('Event_OldResourceProperties_pool', 'default')
    old_address = os.getenv('Event_PhysicalResourceId')

    # If the updated resource wants an EIP from a different pool
    if not pool == old_pool:
        # Release the old address
        delete_address(old_pool, old_address)

        # And get a new one
        physical_id = get_address(pool)
    else:
        physical_id = old_address

elif request_type == 'Delete':
    address = os.getenv('Event_PhysicalResourceId')

    delete_address(pool, address)

# Write out our successful response!
if request_type != 'Delete':
    print u'{ "PhysicalResourceId" : "%s" }' % physical_id
else:
    print u"{}"
