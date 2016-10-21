#!/usr/bin/python2.7

# Copyright 2016 Netflix, Inc.
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

##################################################################################
# This script assumes setup.py has already been run to configure Genie and that
# this script is executed on the host where Genie is running. If it's executed on
# another host change the localhost line below.
##################################################################################

from __future__ import absolute_import, division, print_function, unicode_literals

import logging
import pygenie
import sys

logging.basicConfig(level=logging.ERROR)

LOGGER = logging.getLogger(__name__)

pygenie.conf.DEFAULT_GENIE_URL = "http://genie:8080"

# Create a job instance and fill in the required parameters
job = pygenie.jobs.HadoopJob() \
    .job_name('GenieDemoHDFSJob') \
    .genie_username('root') \
    .job_version('2.7.1')

# Set cluster criteria which determine the cluster to run the job on
job.cluster_tags(['sched:' + str(sys.argv[1]), 'type:yarn'])

# Set command criteria which will determine what command Genie executes for the job
job.command_tags(['type:hdfs'])

# Any command line arguments to run along with the command. In this case it holds
# the actual query but this could also be done via an attachment or file dependency.
job.command("dfs -ls input")

# Submit the job to Genie
running_job = job.execute()

print('Job {} is {}'.format(running_job.job_id, running_job.status))
print(running_job.job_link)

# Block and wait until job is done
running_job.wait()

print('Job {} finished with status {}'.format(running_job.job_id, running_job.status))
