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
import restclient

import os

# the S3 prefix where the tests are located
GENIE_TEST_PREFIX = os.getenv("GENIE_TEST_PREFIX")

# get the serviceUrl from the eureka client
serviceUrl = eureka.EurekaClient().get_service_base_url() + '/genie/v0/jobs'

def hiveSubmitJob():
    print "Running hiveSubmitJob"
    payload = '''
    {  
        "jobInfo":
        {
            "jobName": "HIVE-KILL-TEST", 
            "userName" : "genietest", 
            "groupName" : "hadoop", 
            "userAgent" : "laptop",
            "jobType": "hive", 
            "configuration": "prod",
            "schedule": "adHoc",
            "cmdArgs": "-f hive.q",
            "fileDependencies":"''' + GENIE_TEST_PREFIX + '''/hive.q"
        }
    }
    '''
    print payload
    print "\n"
    return jobs.submitJob(serviceUrl, payload)          

def pigSubmitJob():
    print "Running pigSubmitJob"
    payload = '''
    {
        "jobInfo":
        {
            "jobName": "PIG-KILL-TEST", 
            "userName" : "genietest", 
            "groupName" : "hadoop", 
            "userAgent" : "laptop",
            "jobType": "pig", 
            "configuration": "prod", 
            "schedule": "adHoc",
            "cmdArgs":"-f pig.q",
            "pigVersion":"0.11",
            "fileDependencies":"''' + GENIE_TEST_PREFIX + '''/pig.q"
        }
    }
    '''
    print payload
    print "\n"
    return jobs.submitJob(serviceUrl, payload)    

def testKillJob(jobID):
    print "Testing job kill"
    params = '/' + jobID
    response = restclient.delete(service_url=serviceUrl, params=params)
    print response.read()
        
# driver method for all tests                
if __name__ == "__main__":
    print "Running kill test for hive:"
    jobID = hiveSubmitJob()
    
    time.sleep(30)
    
    # refresh the base_url to maybe pick a different instance
    serviceUrl = eureka.EurekaClient().get_service_base_url() + '/genie/v0/jobs'
    
    testKillJob(jobID)
    print "Hive job kill successful\n"
    
    print "Running kill test for pig:"
    jobID = pigSubmitJob()
    
    time.sleep(30)
    
    # refresh the base_url to maybe pick a different instance
    serviceUrl = eureka.EurekaClient().get_service_base_url() + '/genie/v0/jobs'
      
    testKillJob(jobID)
    print "Pig job kill successful\n"

