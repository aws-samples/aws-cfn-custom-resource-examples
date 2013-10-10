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
import hashlib
import lockfile
import logging
import os
import subprocess
import sys
import tempfile
import re

try:
    import simplejson as json
except ImportError:
    import json

handler = logging.StreamHandler(sys.stderr)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logging.getLogger().addHandler(handler)

log = logging.getLogger('schema')
log.setLevel(logging.INFO)

_BOOLEANS = ["autoIncrement", "primaryKey", "nullable", "alwaysRun", "runOnChange", "failOnError", "runInTransaction",
             "defaultValueBoolean", "unique", "deleteCascade", "initiallyDeferred", "deferrable"]
_NUMBERS = ["defaultValueNumeric", "startWith", "incrementBy"]


def type_fixer(d):
    return_value = dict(d)
    for k, v in d.iteritems():
        if not isinstance(v, basestring):
            continue
        if k in _BOOLEANS:
            return_value[k] = (True if "true" == v.lower() else False)
        elif k in _NUMBERS:
            try:
                return_value[k] = int(v)
            except ValueError:
                return_value[k] = float(v)
    return return_value


class FatalError(SystemExit):
    def __init__(self, reason, code):
        super(FatalError, self).__init__(code)
        log.error('Failing resource: %s', reason)
        print json.dumps({'Reason': reason})


class Liquifier(object):
    def _get_property_or_fail(self, key, properties=None):
        if not properties:
            properties = self._resource_properties
        try:
            return properties[key]
        except KeyError:
            raise FatalError('Properties did not contain required field %s' % key, -1)

    def __init__(self, properties, old_properties, stackId, logicalId,
                 driver='com.mysql.jdbc.Driver', liquibase_home='/home/ec2-user/liquibase'):
        self._stack_id = stackId
        self._logical_id = logicalId
        self._resource_properties = properties
        self._url = self._get_property_or_fail('DatabaseURL')
        self._user = self._get_property_or_fail('DatabaseUsername')
        self._passwd = self._get_property_or_fail('DatabasePassword')
        self._old_properties = old_properties
        # TODO: fail if old url/username/password do not match (not allowed to update)
        self._driver = driver
        self._libjars = ':'.join(
            [os.path.join('%s/lib/' % liquibase_home, f) for f in os.listdir(liquibase_home + '/lib') if f.endswith('.jar')]
        )
        self._libjars += ':%s/liquibase.jar' % liquibase_home
        self._liquibase_home = liquibase_home

    def run_event(self, event_type):
        change_log = self._get_property_or_fail('databaseChangeLog')
        change_tag = hashlib.sha256(json.dumps(change_log)).hexdigest()
        old_change_log = self._get_property_or_fail('databaseChangeLog', self._old_properties)

        if event_type == 'Create':
            self._update_to_tag(change_log, change_tag)
        elif event_type == 'Update':
            # to roll back to a previous changelog, liquibase needs the "latest" changelog, but the previous tag.
            if not self._roll_back_to_tag(old_change_log, change_tag):
                self._update_to_tag(change_log, change_tag)
        elif event_type == 'Delete':
            if self._resource_properties.get('DropAllOnDelete', 'false').lower() == 'true':
                self._drop_all()

    def _get_command_base(self):
        return ['java',
                '-cp', self._libjars,
                'liquibase.integration.commandline.Main',
                '--logLevel=debug',
                '--classpath=%s' % self._libjars,
                '--driver=%s' % self._driver,
                '--url=%s' % self._url,
                '--username=%s' % self._user,
                '--password=%s' % self._passwd]

    def _run_cmd(self, cmdline, liquibase_cmd):
        log.info("Running command: %s", cmdline)
        proc = subprocess.Popen(cmdline, cwd=self._liquibase_home, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT)

        out = proc.communicate()[0]

        log.info('Liquibase %s output: %s', liquibase_cmd, out)

        return proc.returncode, out

    def _call_with_changelog(self, func, change_log):
        changelog_parent = os.path.join(tempfile.gettempdir(),
                                        re.sub('[^a-zA-Z0-9_-]', '_', self._stack_id),
                                        self._logical_id)

        if not os.path.isdir(changelog_parent):
            try:
                os.makedirs(changelog_parent)
            except OSError, e:
                raise FatalError(str(e), -2)

        lock = lockfile.FileLock(os.path.join(changelog_parent, 'changelog.json.lock'))

        with lock:
            changelog_path = os.path.join(changelog_parent, 'changelog.json')
            with file(changelog_path, 'w') as f:
                json.dump({'databaseChangeLog': change_log}, f, indent=4)

                f.flush()
                retval = func(changelog_path)

            os.remove(changelog_path)

        return retval

    def _update(self, changelog_file):
        cmd = self._get_command_base()
        cmd.append('--changeLogFile=%s' % changelog_file)
        cmd.append('update')

        retcode, output = self._run_cmd(cmd, 'update')

        if retcode:
            raise FatalError('Liquibase update failed with error %s' % retcode, retcode)

    def _update_to_tag(self, change_log, change_tag):
        self._call_with_changelog(lambda path: self._update(path), change_log)

        self._tag(change_tag)

    def _rollback(self, changelog_file, change_tag):
        cmd = self._get_command_base()
        cmd.append('--changeLogFile=%s' % changelog_file)
        cmd.append('rollback')
        cmd.append(change_tag)

        retcode, output = self._run_cmd(cmd, 'update_rollback')

        return False if retcode else True

    def _roll_back_to_tag(self, change_log, change_tag):
        return self._call_with_changelog(lambda path: self._rollback(path, change_tag), change_log)

    def _drop_all(self):
        cmd = self._get_command_base()
        cmd.append('dropAll')

        retcode, output = self._run_cmd(cmd, 'dropAll')

        if retcode:
            raise FatalError('Liquibase drop failed with error %s' % retcode, retcode)

    def _tag(self, tag):
        cmd = self._get_command_base()
        cmd.append('tag')
        cmd.append(tag)

        retcode, output = self._run_cmd(cmd, 'tag')

        if retcode:
            raise FatalError('Liquibase tag failed with error %s' % retcode, retcode)


try:
    event_obj = json.loads(os.environ.get('EventProperties'), object_hook=type_fixer)
except ValueError:
    raise FatalError('Could not parse properties as JSON', -1)

try:
    event_type = event_obj['RequestType']
except KeyError:
    raise FatalError('RequestType not found.', -1)

log.info('%s received event: %s', event_type, json.dumps(event_obj, indent=4))

resource_properties = event_obj.get('ResourceProperties')

if not resource_properties:
    raise FatalError('Resource Properties not found.', -1)

stack_id = event_obj.get('StackId')
if not stack_id:
    raise FatalError('StackId not found.', -1)

logical_id = event_obj.get('LogicalResourceId')
if not logical_id:
    raise FatalError('LogicalResourceId not found.', -1)

Liquifier(resource_properties, event_obj.get('OldResourceProperties', {}), stack_id, logical_id) .run_event(event_type)

print json.dumps({})