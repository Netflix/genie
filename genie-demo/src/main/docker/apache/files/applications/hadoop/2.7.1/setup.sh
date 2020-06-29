#!/bin/bash

set -o errexit -o nounset -o pipefail

START_DIR=`pwd`
cd `dirname ${BASH_SOURCE[0]}`
HADOOP_BASE=`pwd`
cd ${START_DIR}

HADOOP_VERSION=2.7.1
APP_NAME=hadoop-${HADOOP_VERSION}
export HADOOP_DEPENDENCIES_DIR=${HADOOP_BASE}/dependencies
export HADOOP_HOME=${HADOOP_DEPENDENCIES_DIR}/${APP_NAME}

tar -xf ${HADOOP_DEPENDENCIES_DIR}/${APP_NAME}.tar.gz -C ${HADOOP_DEPENDENCIES_DIR}

export HADOOP_CONF_DIR=${HADOOP_HOME}/conf
mkdir ${HADOOP_CONF_DIR}
export HADOOP_LIBEXEC_DIR=${HADOOP_HOME}/libexec

cp ${GENIE_CLUSTER_DIR}/config/* ${HADOOP_CONF_DIR}/

# Remove the zip to save space
rm ${HADOOP_DEPENDENCIES_DIR}/${APP_NAME}.tar.gz
