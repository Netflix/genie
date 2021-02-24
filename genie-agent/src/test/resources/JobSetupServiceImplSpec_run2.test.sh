#!/usr/bin/env bash

#
# Generated by Genie for job: <JOB_ID_PLACEHOLDER>
#

# Error out if any command fails
set -o errexit
# Error out if any command in a pipeline fails
set -o pipefail
# Error out if unknown variable is used
set -o nounset
# Save original stdout and stderr in fd 6 and 7
exec 6>&1
exec 7>&2


# Trap exit signals to ensure children processes are dead before returning
function handle_kill_request {
    echo "Handling $1 signal" >&2
    # Update trap
    trap wait SIGTERM SIGINT SIGHUP
    # Send SIGTERM to all children
    pkill -P $$ || true
    for ((iteration=1; iteration < 30; iteration++))
    {
        if pkill -0 -P $$ &> /dev/null;
        then
            echo "Waiting for children to terminate" >&2
            sleep 1
        else
            echo "All children terminated" >&2
            exit 1
        fi
    }
    # Reaching this point means the children did not die. Kill with SIGKILL
    echo "Terminating all children with SIGKILL" >&2
    pkill -9 -P $$
}
trap 'handle_kill_request SIGTERM' SIGTERM
trap 'handle_kill_request SIGINT' SIGINT
trap 'handle_kill_request SIGHUP' SIGHUP

# Locally-generated environment variables

export GENIE_JOB_DIR="<JOB_DIR_PLACEHOLDER>"

export GENIE_APPLICATION_DIR="${GENIE_JOB_DIR}/genie/applications"

export GENIE_COMMAND_DIR="${GENIE_JOB_DIR}/genie/command/<COMMAND_ID_PLACEHOLDER>"

export GENIE_CLUSTER_DIR="${GENIE_JOB_DIR}/genie/cluster/<CLUSTER_ID_PLACEHOLDER>"

export __GENIE_SETUP_LOG_FILE="${GENIE_JOB_DIR}/genie/logs/setup.log"

export __GENIE_ENVIRONMENT_DUMP_FILE="${GENIE_JOB_DIR}/genie/logs/env.log"

export __GENIE_SETUP_ERROR_MARKER_FILE="${GENIE_JOB_DIR}/genie/setup_failed.txt"


# Mark the beginnig of the setup by creating a marker file
echo "The job script failed during setup. See ${__GENIE_SETUP_LOG_FILE} for details" > ${__GENIE_SETUP_ERROR_MARKER_FILE}

# During setup, redirect stdout and stderr of this script to a log file
exec > ${__GENIE_SETUP_LOG_FILE}
exec 2>&1

echo "Setup start: $(date '+%Y-%m-%d %H:%M:%S')"

# Server-provided environment variables

export SERVER_ENVIRONMENT_X="VALUE_X"

export SERVER_ENVIRONMENT_Y="VALUE_Y"

export SERVER_ENVIRONMENT_Z="VALUE_Z"


echo "No setup script for cluster <CLUSTER_ID_PLACEHOLDER>"

echo "No setup script for application <APPLICATION_1_PLACEHOLDER>"

echo "No setup script for application <APPLICATION_2_PLACEHOLDER>"

echo "No setup script for command <COMMAND_ID_PLACEHOLDER>"

echo "No setup script for job <JOB_ID_PLACEHOLDER>"


echo "Setup end: $(date '+%Y-%m-%d %H:%M:%S')"

# Setup completed successfully, delete marker file
rm ${__GENIE_SETUP_ERROR_MARKER_FILE}

# Restore the original stdout and stderr. Close fd 6 and 7
exec 1>&6 6>&-
exec 2>&7 7>&-

# Dump environment post-setup
env | grep -E --regex='.*' | sort > ${__GENIE_ENVIRONMENT_DUMP_FILE}

# Launch the command
presto -v --exec 'select * from table limit 10' <&0 &
pid=$!
ppid=$$
{ while kill -0 $ppid &> /dev/null; do sleep 10; done; kill -0 $pid &> /dev/null && kill -9 $pid; } &
wait %1
exit $?

