#!/bin/bash

set -o errexit -o nounset -o pipefail

start_dir=`pwd`
cd `dirname ${BASH_SOURCE[0]}`
SPARK_BASE=`pwd`
cd $start_dir

export SPARK_DAEMON_JAVA_OPTS="-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps"

SPARK_DEPS=${SPARK_BASE}/dependencies

export SPARK_VERSION="1.6.3"

tar xzf ${SPARK_DEPS}/spark-${SPARK_VERSION}-bin-hadoop2.6.tgz -C ${SPARK_DEPS}

# Set the required environment variable.
export SPARK_HOME=${SPARK_DEPS}/spark-${SPARK_VERSION}-bin-hadoop2.6
export SPARK_CONF_DIR=${SPARK_HOME}/conf
export SPARK_LOG_DIR=${GENIE_JOB_DIR}
export SPARK_LOG_FILE=spark.log
export SPARK_LOG_FILE_PATH=${GENIE_JOB_DIR}/${SPARK_LOG_FILE}
export CURRENT_JOB_WORKING_DIR=${GENIE_JOB_DIR}
export CURRENT_JOB_TMP_DIR=${CURRENT_JOB_WORKING_DIR}/tmp

# Make Sure Script is on the Path
export PATH=$PATH:${SPARK_HOME}/bin

# Delete the tarball to save space
rm ${SPARK_DEPS}/spark-${SPARK_VERSION}-bin-hadoop2.6.tgz

