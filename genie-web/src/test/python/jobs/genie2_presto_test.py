# #
#
#  Copyright 2014 Netflix, Inc.
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

import sys

sys.path.append('../utils')

import time
import eureka
import jobs
import json
import os

# the S3 prefix where the tests are located
GENIE_TEST_PREFIX = os.getenv("GENIE_TEST_PREFIX")

# get the serviceUrl from the eureka client
serviceUrl = eureka.EurekaClient().get_service_base_url() + '/genie/v2/jobs'
# works
clusterTags = json.dumps([{"tags": ['presto', 'prod']}])
cmdTags = json.dumps(['presto'])

print clusterTags


def test_presto_submit_job():
    print serviceUrl
    print "Running test_presto_submit_job"
    payload = '''
        {
            "name": "Genie2TestPrestoJob",
            "clusterCriterias" : ''' + clusterTags + ''',
            "user" : "tgianos",
            "version" : "1",
            "group" : "presto",
            "forwarded" : false,
            "commandArgs" : "--execute \\"select min(view_dateint) from dse.vhs_streaming_session_f;\\"",
            "commandCriteria" :''' + cmdTags + '''
        }
    '''
    print payload
    print "\n"
    return jobs.submit_job(serviceUrl, payload)

# driver method for all tests                
if __name__ == "__main__":
    print "Running presto tests:\n"
    job_id = test_presto_submit_job()
    print "\n"
    while True:
        print jobs.get_job_info(serviceUrl, job_id)
        print "\n"
        status = jobs.get_job_status(serviceUrl, job_id)
        print "Status =%s" % status
        print "\n"

        if (status != 'RUNNING') and (status != 'INIT'):
            print "Final status: ", status
            print "Job has terminated - exiting"
            break

        time.sleep(5)
