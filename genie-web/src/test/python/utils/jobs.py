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

import restclient
import json

def submitJob(serviceUrl, payload, contentType='application/json'):
    response = restclient.post(serviceUrl=serviceUrl, payload=payload, contentType=contentType)
    results = response.read()
    json_obj = json.loads(results)
    jobID = json_obj[u'jobs'][u'jobInfo'][u'jobID']
    print "Job ID: ", jobID
    print "Output URI: ", json_obj[u'jobs'][u'jobInfo'][u'outputURI']
    print "Job submission successful"
    return jobID

def getJobInfo(serviceUrl, jobID):
    params = '/' + jobID
    response = restclient.get(serviceUrl=serviceUrl, params=params)
    return response.read()

def getJobStatus(serviceUrl, jobID):
    params = '/' + jobID + '/status'
    response = restclient.get(serviceUrl=serviceUrl, params=params)
    json_obj = json.loads(response.read())
    status = json_obj[u'status']
    return status
   
