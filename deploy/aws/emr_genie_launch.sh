#!/bin/bash

##
#
#  Copyright 2013 Netflix, Inc.
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

if [ "$EMR_GENIE_BOOTSTRAP_LOC" == "" ]; then
    echo "Set EMR_GENIE_BOOTSTRAP_LOC to point to emr_genie_bootstrap.sh on S3"
    exit 1
fi

echo "Using EMR Genie bootstrap action from: $EMR_GENIE_BOOTSTRAP_LOC"
 
# Launching EMR
elastic-mapreduce --create --alive --instance-type m1.xlarge --instance-count 2  \
    --ssh --debug --trace --visible-to-all-users --name "Genie Testing" --ami-version "2.4.2" \
    --hive-interactive --hive-versions 0.11.0 --pig-interactive --pig-versions 0.11.1 \
    --bootstrap-action s3://elasticmapreduce/bootstrap-actions/run-if \
    --args "instance.isMaster=true,$EMR_GENIE_BOOTSTRAP_LOC"