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

import restclient
import json


def submit_job(service_url, payload, content_type='application/json'):
    response = restclient.post(service_url=service_url, payload=payload, content_type=content_type)
    results = response.read()
    json_obj = json.loads(results)
    job_id = json_obj[u'id']
    print "Job ID: ", job_id
    print "Output URI: ", json_obj[u'outputURI']
    print "Job submission successful"
    return job_id


def get_job_info(service_url, job_id):
    params = '/' + job_id
    response = restclient.get(service_url=service_url, params=params)
    return response.read()


def get_job_status(service_url, job_id):
    params = '/' + job_id + '/status'
    response = restclient.get(service_url=service_url, params=params)
    json_obj = json.loads(response.read())
    status = json_obj[u'status']
    return status
