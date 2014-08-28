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

import os
import tempfile
import base64

# the S3 prefix where the tests are located
GENIE_TEST_PREFIX = os.getenv("GENIE_TEST_PREFIX")

# get the serviceUrl from the eureka client
serviceUrl = eureka.EurekaClient().get_service_base_url() + '/genie/v0/jobs'

def testJsonSubmitjob():
    print "Running testJsonSubmitjob "
    # write out a temporary file with our query/dependencies
    query = tempfile.NamedTemporaryFile(delete=False)
    name = query.name
    queryFile = 'pig-' + str(time.time()) + '.q'
    query.write("fs -copyFromLocal %s %s; cmd = load '%s'; dump cmd;" % (queryFile, queryFile, queryFile))
    query.close()
    
    # read it back in as base64 encoded binary
    query = open(name, "rb")
    contents = base64.b64encode(query.read())
    print contents
    query.close()
    os.unlink(name)
    
    payload = '''
    {  
        "jobInfo":
        {
            "jobName": "PIG-JOB-TEST",
            "description": "This is a test", 
            "userName" : "genietest", 
            "groupName" : "hadoop", 
            "jobType": "pig", 
            "configuration": "prod",
            "schedule": "adHoc",
            "cmdArgs": "-f ''' + queryFile + '''",
            "attachments": {
                "data": "''' + contents + '''",
                "name": "''' + queryFile + '''"
            }
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

