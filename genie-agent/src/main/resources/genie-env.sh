#!/usr/bin/env bash

#
#
#  Copyright 2018 Netflix, Inc.
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
#

# Usage: genie-env.sh <env_output_file> <log_file> [input_setup_script, ...]
env_output_file=$1
log_file=$2
input_setup_scripts=${@:3}

# Check inputs
if [[ -z "${env_output_file}" ]]; then
  echo "Invalid arguments: missing output file: ${env_output_file}"
  exit 1;
elif [[ -e ${env_output_file} ]]; then
  echo "Invalid arguments: output file exists: ${env_output_file}"
  exit 1;
elif [[ -z "${log_file}" ]]; then
  echo "Invalid arguments: missing log file: ${log_file}"
  exit 1;
elif [[ -e ${log_file} ]]; then
  echo "Invalid arguments: log file exists: ${log_file}"
  exit 1;
fi

for setup_file in ${input_setup_scripts}; do
  if [[ ! -f ${setup_file} ]]; then
    echo "Invalid input setup file: ${setup_file}"
    exit 1;
  fi
done

# Make sure output locations are writeable
touch ${env_output_file}
touch ${log_file}

# Source all input
for setup_file in ${input_setup_scripts}; do
  echo >> ${log_file}
  echo " *** Sourcing ${setup_file} *** " >> ${log_file}
  echo >> ${log_file}
  source ${setup_file} 2>&1 >> ${log_file}
done

# Write out resulting environment
echo " *** Dumping environment in ${env_output_file} *** " >> ${log_file}

unset IFS
for var in $(compgen -e); do
    printf "%s=\'%s\'\n" "$var" "${!var}" >> ${env_output_file}
done
