#!/bin/bash -x
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
exec 3>&1 # "save" stdout to fd 3
exec &>> /home/ec2-user/delete.log

function error_exit() {
    echo "{\"Reason\": \"$1\"}" >&3 3>&- # echo reason to stdout (instead of log) and then close fd 3
    exit $2
}

if [ -z "${Event_ResourceProperties_MountPoint}" ]
then
    error_exit "MountPoint is required." 64
fi

umount "${Event_ResourceProperties_MountPoint}"

umount_ret=$?
if [ $umount_ret -ne 0 ]
then
    error_exit "Unmount failed." $umount_ret
else
    echo "{}" >&3 3>&- # echo reason to stdout (instead of log) and then close fd 3
    exit 0
fi