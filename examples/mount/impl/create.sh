#!/bin/bash -x
#=======================================================================================================================
# Copyright 2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the
# License. A copy of the License is located at
#
#     http://aws.amazon.com/asl/
#
# or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and
# limitations under the License.
#=======================================================================================================================
exec 3>&1 # "save" stdout to fd 3
exec &>> /home/ec2-user/create.log

function error_exit() {
    echo "{\"Reason\": \"$1\"}" >&3 3>&- # echo reason to stdout (instead of log) and then close fd 3
    exit $2
}

if [ -z "${Event_ResourceProperties_Device}" ]
then
    error_exit "Device is required." 64
fi

if [ -z "${Event_ResourceProperties_MountPoint}" ]
then
    error_exit "MountPoint is required." 64
fi

if [ ! -e "${Event_ResourceProperties_MountPoint}" ]
then
    mkdir -p "${Event_ResourceProperties_MountPoint}"
    mkdir_ret=$?
    if [ $mkdir_ret -ne 0 ]
    then
        error_exit "Could not create ${Event_ResourceProperties_MountPoint}" $mkdir_ret
    fi
fi

if [ ! -z "${Event_ResourceProperties_Format}" ] && [ "true" = "${Event_ResourceProperties_Format}" ]
then
    if [ -z "${Event_ResourceProperties_FsType}" ]
    then
        error_exit "Cannot format without fstype." 64
    else
        mkfs -t "${Event_ResourceProperties_FsType}" "${Event_ResourceProperties_Device}"
        mkfs_ret=$?
        if [ $mkfs_ret -ne 0 ]
        then
            error_exit "Formatting failed." $mkfs_ret
        fi
    fi
fi

if [ ! -z "${Event_ResourceProperties_FsType}" ]
then
    mount -t "${Event_ResourceProperties_FsType}" "${Event_ResourceProperties_Device}" "${Event_ResourceProperties_MountPoint}"
else
    mount "${Event_ResourceProperties_Device}" "${Event_ResourceProperties_MountPoint}"
fi

mount_ret=$?
if [ $mount_ret -ne 0 ]
then
    error_exit "Mount failed." $mount_ret
else
    echo "{}" >&3 3>&-  # echo success to stdout (instead of log) and then close fd 3
    exit 0
fi