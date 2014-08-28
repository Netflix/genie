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

import os

import json
import urllib2
import eureka

if __name__ == "__main__":
    up_hosts = []
    eureka_client = eureka.EurekaClient()
    try:
        instancesUP = eureka_client.get_up_instances()
        for i in instancesUP:
            up_hosts.append(i['hostName'])
    except AssertionError, e:
        print e
        print

    # get list of running jobs
    jobs = []
    base_url = eureka_client.get_service_base_url() + '/genie/v2/jobs?status=RUNNING'
    req = urllib2.Request(url=base_url)
    req.add_header('Accept', 'application/json')
    assert req.get_method() == 'GET'
    try:
        response = urllib2.urlopen(req)
        json_str = response.read()
        jobs = json.loads(json_str)
    except urllib2.HTTPError, e:
        print 'Error making request to Genie 2: ' + e.code + ' ' + e.read()
        print

    if not isinstance(jobs, list):
        jobs = [jobs]

    up_jobs = 0
    non_up_jobs = 0
    env = os.getenv('GENIE_PRINT_ALL_JOBS')
    print_all_jobs = True
    if env is not None and env.lower() == 'false':
        print_all_jobs = False
    for j in jobs:
        host = j['outputURI'].split("/")[2].split(":")[0]
        if host in up_hosts:
            up_jobs += 1
            if print_all_jobs:
                print "Job on UP instance:"
                print "     ID:", j['id']
                print "     Output URI: ", j['outputURI']
                print "     Kill URI: ", j['killURI']
        else:
            non_up_jobs += 1
            print "Job on non-UP instance:"
            print "     ID:", j['id']
            print "     Output URI: ", j['outputURI']
            print "     Kill URI: ", j['killURI']
    print

    print "Number of jobs running on UP instances:", up_jobs
    print "Number of jobs running on non-UP instances:", non_up_jobs
