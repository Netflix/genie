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
import uuid
import urllib2

job_id = "job-" + str(uuid.uuid4())
# the S3 prefix where the tests are located
GENIE_TEST_PREFIX = os.getenv("GENIE_TEST_PREFIX")

# get the serviceUrl from the eureka client
service_url = eureka.EurekaClient().get_service_base_url() + '/genie/v2/jobs'


def test_conflict_job():
    print service_url
    print "Running test_conflict_job"
    cluster_tags = json.dumps([{"tags": ['adhoc']}])
    command_tags = json.dumps(['genie.id:pig11_mr2'])
    payload = '''
        {
            "id":"testblah",
            "name": "Genie2TestPigJob", 
            "clusterCriterias" : ''' + cluster_tags + ''',
            "user" : "genietest", 
            "group" : "hadoop", 
            "commandArgs" : "-f pig2.q", 
            "commandCriteria" :''' + command_tags + ''',
            "fileDependencies":"''' + GENIE_TEST_PREFIX + '''/pig2.q"
        }
    '''
    print payload
    print "\n"
    jobs.submit_job(service_url, payload)
    time.sleep(30)
    return jobs.submit_job(service_url, payload)


def test_no_cluster_job():
    print service_url
    print "Running test_no_cluster_job"
    cluster_tags = json.dumps([{"tags": ['adhoc', 'h2query1']}])
    command_tags = json.dumps(['genie.id:pig11_mr2'])
    payload = '''
        {
            "id":"''' + job_id + '''",
            "name": "Genie2TestPigJob", 
            "clusterCriterias" : ''' + cluster_tags + ''',
            "user" : "genietest", 
            "version" : "1",
            "group" : "hadoop", 
            "commandArgs" : "-f pig2.q", 
            "commandCriteria" :''' + command_tags + ''',
            "fileDependencies":"''' + GENIE_TEST_PREFIX + '''/pig2.q"
        }
    '''
    print payload
    print "\n"
    return jobs.submit_job(service_url, payload)


def test_good_job():
    print service_url
    print "Running test_good_job"
    cluster_tags = json.dumps([{"tags": ['adhoc']}])
    command_tags = json.dumps(['genie.id:pig11_mr2'])
    payload = '''
        {
            "id":"''' + job_id + '''",
            "name": "Genie2TestPigJob", 
            "clusterCriterias" : ''' + cluster_tags + ''',
            "user" : "genietest", 
            "group" : "hadoop", 
            "commandArgs" : "-f pig2.q", 
            "commandCriteria" :''' + command_tags + ''',
            "fileDependencies":"''' + GENIE_TEST_PREFIX + '''/pig2.q"
        }
    '''
    print payload
    print "\n"
    return jobs.submit_job(service_url, payload)

# driver method for all tests                
if __name__ == "__main__":
    print "Running unit tests:\n"
    try:
        #jobID = testConflictJob()
        #jobID = jobs.submitJob()
        job_id = test_good_job()
    except urllib2.HTTPError, e:
        print "Caught Exception"
        print "code = " + str(e.code)
        print "message =" + e.msg
        print "read =" + e.read()
        sys.exit(0)
    print "\n"
    while True:
        print jobs.get_job_info(service_url, job_id)
        print "\n"
        status = jobs.get_job_status(service_url, job_id)
        print "Status =%s" % status
        print "\n"

        if (status != 'RUNNING') and (status != 'INIT'):
            print "Final status: ", status
            print "Job has terminated - exiting"
            break

        time.sleep(5)
