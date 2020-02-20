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


echo "Sourcing setup script for application <APPLICATION_1_PLACEHOLDER>"
source ${GENIE_JOB_DIR}/genie/applications/<APPLICATION_1_PLACEHOLDER>/genie_setup.sh

echo "Sourcing setup script for application <APPLICATION_2_PLACEHOLDER>"
source ${GENIE_JOB_DIR}/genie/applications/<APPLICATION_2_PLACEHOLDER>/genie_setup.sh

echo "Sourcing setup script for cluster <CLUSTER_ID_PLACEHOLDER>"
source ${GENIE_JOB_DIR}/genie/cluster/<CLUSTER_ID_PLACEHOLDER>/genie_setup.sh

echo "Sourcing setup script for command <COMMAND_ID_PLACEHOLDER>"
source ${GENIE_JOB_DIR}/genie/command/<COMMAND_ID_PLACEHOLDER>/genie_setup.sh

echo "Sourcing setup script for job <JOB_ID_PLACEHOLDER>"
source ${GENIE_JOB_DIR}/genie_setup.sh


echo "Setup end: $(date '+%Y-%m-%d %H:%M:%S')"

# Setup completed successfully, delete marker file
rm ${__GENIE_SETUP_ERROR_MARKER_FILE}

# Restore the original stdout and stderr. Close fd 6 and 7
exec 1>&6 6>&-
exec 2>&7 7>&-

# Dump environment post-setup
env | sort > ${__GENIE_ENVIRONMENT_DUMP_FILE}

# Launch the command
presto -v --exec 'select * from table limit 1'
