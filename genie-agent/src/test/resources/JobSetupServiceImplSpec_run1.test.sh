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


# Locally-generated environment variables

export GENIE_JOB_DIR="<JOB_DIR_PLACEHOLDER>"

export GENIE_APPLICATION_DIR="${GENIE_JOB_DIR}/genie/applications"

export GENIE_COMMAND_DIR="${GENIE_JOB_DIR}/genie/command/<COMMAND_ID_PLACEHOLDER>"

export GENIE_CLUSTER_DIR="${GENIE_JOB_DIR}/genie/cluster/<CLUSTER_ID_PLACEHOLDER>"

export __GENIE_SETUP_LOG_FILE="${GENIE_JOB_DIR}/genie/logs/setup.log"

export __GENIE_ENVIRONMENT_DUMP_FILE="${GENIE_JOB_DIR}/genie/logs/env.log"


# Server-provided environment variables

export SERVER_ENVIRONMENT_X="VALUE_X"

export SERVER_ENVIRONMENT_Y="VALUE_Y"

export SERVER_ENVIRONMENT_Z="VALUE_Z"


echo Setup begins: `date '+%Y-%m-%d %H:%M:%S'` >> ${__GENIE_SETUP_LOG_FILE}

echo "Sourcing setup script for application <APPLICATION_1_PLACEHOLDER>" >> ${__GENIE_SETUP_LOG_FILE}
source ${GENIE_JOB_DIR}/genie/applications/<APPLICATION_1_PLACEHOLDER>/genie_setup.sh 2>&1 >> ${__GENIE_SETUP_LOG_FILE}

echo "Sourcing setup script for application <APPLICATION_2_PLACEHOLDER>" >> ${__GENIE_SETUP_LOG_FILE}
source ${GENIE_JOB_DIR}/genie/applications/<APPLICATION_2_PLACEHOLDER>/genie_setup.sh 2>&1 >> ${__GENIE_SETUP_LOG_FILE}

echo "Sourcing setup script for cluster <CLUSTER_ID_PLACEHOLDER>" >> ${__GENIE_SETUP_LOG_FILE}
source ${GENIE_JOB_DIR}/genie/cluster/<CLUSTER_ID_PLACEHOLDER>/genie_setup.sh 2>&1 >> ${__GENIE_SETUP_LOG_FILE}

echo "Sourcing setup script for command <COMMAND_ID_PLACEHOLDER>" >> ${__GENIE_SETUP_LOG_FILE}
source ${GENIE_JOB_DIR}/genie/command/<COMMAND_ID_PLACEHOLDER>/genie_setup.sh 2>&1 >> ${__GENIE_SETUP_LOG_FILE}

echo "Sourcing setup script for job <JOB_ID_PLACEHOLDER>" >> ${__GENIE_SETUP_LOG_FILE}
source ${GENIE_JOB_DIR}/genie_setup.sh 2>&1 >> ${__GENIE_SETUP_LOG_FILE}

echo Setup end: `date '+%Y-%m-%d %H:%M:%S'` >> ${__GENIE_SETUP_LOG_FILE}

# Dump environment post-setup
env | sort > ${__GENIE_ENVIRONMENT_DUMP_FILE}

# Launch the command
presto -v --exec 'select * from table limit 1'
