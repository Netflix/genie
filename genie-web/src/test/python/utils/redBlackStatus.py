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

import os
import sys
import urllib2

import json
import urllib2
import pprint

from eureka import EurekaClient

if __name__ == "__main__":
    upHosts = []
    try:
        instancesUP = EurekaClient().getUPInstances()
        for i in instancesUP:
            upHosts.append(i['hostName'])
    except AssertionError, e:
        print e
        print
        
    # get list of running jobs
    jobInfo = []
    base_url = EurekaClient().getServiceBaseUrl() + '/genie/'
    service_url = base_url + 'v0/jobs?status=RUNNING'
    req = urllib2.Request(url=service_url)
    req.add_header('Accept', 'application/json')
    assert req.get_method() == 'GET'
    try:
        response = urllib2.urlopen(req)
        json_str = response.read()
        json_obj = json.loads(json_str)
        jobInfo = json_obj['jobs']['jobInfo']
    except urllib2.HTTPError, e:
        json_str = e.read()
        json_obj = json.loads(json_str)
        message = json_obj['errorMsg']
        print message
        print

    if not isinstance(jobInfo, list):
        jobInfo = [jobInfo]
    
    upJobs=0
    nonUpJobs=0
    env = os.getenv('GENIE_PRINT_ALL_JOBS')
    printAllJobs = True
    if env is not None and env.lower() == 'false':
        printAllJobs = False       
    for j in jobInfo:
        host = j['outputURI'].split("/")[2].split(":")[0]
        if host in upHosts:
            upJobs += 1
            if printAllJobs:
                print "Job on UP instance: ", j['killURI']
        else:
            nonUpJobs += 1
            print "Job on non-UP instance: ", j['killURI']
    print
            
    print "Number of jobs running on UP instances:", upJobs
    print "Number of jobs running on non-UP instances:", nonUpJobs
    

    
    
