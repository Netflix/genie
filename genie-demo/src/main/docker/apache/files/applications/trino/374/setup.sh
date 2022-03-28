#!/bin/bash

set -o errexit -o nounset -o pipefail

START_DIR=`pwd`
cd `dirname ${BASH_SOURCE[0]}`
TRINO_BASE=`pwd`
cd "${START_DIR}"

chmod 755 "${TRINO_BASE}"/dependencies/trino-cli-374-executable.jar

# Set the cli path for the commands to use when they invoke presto using this Application
export TRINO_CLI="${TRINO_BASE}/dependencies/trino-cli-374-executable.jar"
