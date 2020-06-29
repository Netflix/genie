#!/bin/bash

set -o errexit -o nounset -o pipefail

START_DIR=`pwd`
cd `dirname ${BASH_SOURCE[0]}`
PRESTO_BASE=`pwd`
cd ${START_DIR}

chmod 755 ${PRESTO_BASE}/dependencies/presto-cli-337-executable.jar

# Set the cli path for the commands to use when they invoke presto using this Application
export PRESTO_CLI="${PRESTO_BASE}/dependencies/presto-cli-337-executable.jar"
