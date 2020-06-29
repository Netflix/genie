#!/usr/bin/env python

# Copyright 2016-2020 Netflix, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

###################################################################################
# This script assumes init_demo.py has already been run to configure Genie.
###################################################################################

import logging

import pygenie

logging.basicConfig(level=logging.ERROR)

LOGGER = logging.getLogger(__name__)

pygenie.conf.DEFAULT_GENIE_URL = "http://genie:8080"

# Create a job instance and fill in the required parameters
# TODO: The Presto executable ends up executing in a different working directory and can't find the script file
#       too tired to fix it right now so just go back to using the --execute for now
# job = pygenie.jobs.PrestoJob() \
#     .job_name("Genie Demo Presto Job") \
#     .genie_username("root") \
#     .job_version("3.0.0") \
#     .script("select * from tpcds.sf1.item limit 100;")

job = pygenie.jobs.PrestoJob() \
    .job_name("Genie Demo Presto Job") \
    .genie_username("root") \
    .job_version("3.0.0") \
    .command_arguments("--execute \"select * from tpcds.sf1.item limit 100;\"")

# Set cluster criteria which determine the cluster to run the job on
job.cluster_tags(["sched:adhoc", "type:presto"])

# Set command criteria which will determine what command Genie executes for the job
job.command_tags(["type:presto"])

# Submit the job to Genie
running_job = job.execute()

print(f"Job {running_job.job_id} is {running_job.status}")
print(running_job.job_link)

# Block and wait until job is done
running_job.wait()

print(f"Job {running_job.job_id} finished with status {running_job.status}")
