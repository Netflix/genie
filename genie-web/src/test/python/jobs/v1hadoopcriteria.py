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

import sys
sys.path.append('../utils')

import time
import eureka
import jobs
import json
import os
import uuid

jobID = "job-" + str(uuid.uuid4())
# the S3 prefix where the tests are located
GENIE_TEST_PREFIX = os.getenv("GENIE_TEST_PREFIX")

# get the serviceUrl from the eureka client
serviceUrl = eureka.EurekaClient().getServiceBaseUrl() + '/genie/v1/jobs'
inner = json.dumps(['sla','adhoc'])
#lst = json.dumps([['sla','adhoc'],['prod','adhoc']])
#lst = json.dumps({"criteria" :{"id": "test"}})
#lst = json.dumps(cclist)
# works
#lst = json.dumps([{"id": "t1"},{"id":"t2"}])
lst = json.dumps([{"id": "t1","tagList" : ['a','b'] },{"id":"t2", "tagList": ['c','d']}])
print lst

def testJsonSubmitjob():
    print "Running testJsonSubmitjob "
    payload = '''
    {
        "jobInfo":
        {
            "jobID":"''' + jobID +'''",
            "jobName": "HADOOP-JOB-TEST", 
            "clusterCriteriaList" : ''' + lst + ''',
            "userName" : "genietest", 
            "groupName" : "hadoop", 
            "userAgent" : "laptop",
            "jobType": "hadoop", 
            "schedule": "adHoc",
            "cmdArgs":"jar hadoop-examples.jar sleep -m 1 -mt 1", 
            "fileDependencies":"''' + GENIE_TEST_PREFIX + '''/hadoop-examples.jar"
        }
    }
    '''
    print payload
    print "\n"
    return jobs.submitJob(serviceUrl, payload)

# driver method for all tests                
if __name__ == "__main__":
   print "Running unit tests:\n"
   jobID = testJsonSubmitjob()
   print "\n"
   while True:
       print jobs.getJobInfo(serviceUrl, jobID)
       print "\n"
       status = jobs.getJobStatus(serviceUrl, jobID)
       print status
       print "\n"

       if (status != 'RUNNING') and (status != 'INIT'):
           print "Final status: ", status
           print "Job has terminated - exiting"
           break
              
       time.sleep(5)
