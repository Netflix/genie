#!/bin/bash

##
#
#  Copyright 2015 Netflix, Inc.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.
#
##

function executeWithRetry()
{
    echo "Trying to execute command: [$@]"
    n=0
    until [ $n -ge 3 ]
    do
        $@ && break
        n=$[$n+1]
        echo "retrying command"
    done
    if [ $n == 3 ]; then
        echo "Failed to execute command"
        exit 1 
    fi
}

function waitWithTimeout()
{
    local pid=$1
    local timeout=$((`date +%s` + 60))
    while true; do
        if ! $(kill -0 $pid > /dev/null 2>&1); then
            return
        elif [[ $(date +%s) -gt $timeout ]]; then
            echo "Timeout reached waiting for pid $pid"
            return
        fi
        echo "Waiting for pid $pid to exit gracefully..."
        sleep 3
    done
}

# basic error checking
if [[ $# != 1 ]]; then
    echo "Incorrect number of arguments"
    echo "Usage: jobkill.sh PARENT_PID"
    exit 1
fi

# the process id of the launcher
parent_pid=$1

# pause the process group so it doesn't trigger any retries
executeWithRetry "kill -STOP -$parent_pid"

# send a sigterm to all untrapped procs in the group
executeWithRetry "kill -- -$parent_pid"

# continue parent, this will kill it - and the trap will ensure that files are archived to S3
executeWithRetry "kill -CONT -$parent_pid"

waitWithTimeout "$parent_pid"

# Follow with a sigkill to the entire group if there are still processes
if pgrep -g "$parent_pid"; then
    executeWithRetry "kill -9 -$parent_pid"
fi

echo "Done"
exit 0
