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

import uuid
import time
import eureka
import jobs

# get the serviceUrl from the eureka client
serviceUrl = eureka.EurekaClient().get_service_base_url() + '/genie/v0/jobs'

jobID = str(uuid.uuid4())
   
def testXmlSubmitjob():
    print "Running testXmlSubmitjob"
    payload = '''
    <request>
      <jobInfo>
        <jobID>''' + jobID + '''</jobID>
        <jobName>HADOOP-FS-CLIENT-TEST</jobName>
        <userName>genietest</userName>
        <groupName>hadoop</groupName>
        <userAgent>laptop</userAgent>
        <jobType>hadoop</jobType>
        <schedule>adHoc</schedule>
        <cmdArgs>fs -ls /</cmdArgs>
      </jobInfo>
    </request>
    '''
    print payload
    return jobs.submitJob(serviceUrl, payload, 'application/xml')
            
# driver method for all tests                
if __name__ == "__main__":
   print "Running unit tests:\n"
   testXmlSubmitjob()
   print "\n"

   while True:
       print jobs.getJobInfo(serviceUrl, jobID)
       print "\n"
       status = jobs.getJobStatus(serviceUrl, jobID)
       print status
       print "\n"

       if (status != 'RUNNING') and (status != 'INIT'):
           print "Job has terminated - exiting"
           break
       
       time.sleep(5)



