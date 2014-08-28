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

import os
import urllib2
import eureka
import unittest
import restclient
import json

# get the serviceUrl from the eureka client
serviceUrl = eureka.EurekaClient().get_service_base_url() + '/genie/v0/jobs'

# use this user name for job submission and queries
user = os.getenv("USER")

class JobInfoGetTestCase(unittest.TestCase):

    def setUp(self):
        # if GENIE_SETUP_JOBS=false, don't submit a job
        env = os.getenv('GENIE_SETUP_JOBS')
        setUpJobs = True
        if env is not None and env.lower() == 'false':
            setUpJobs = False 
        if not setUpJobs:
            return
        
        # submit a job so the db is not empty, if needed
        payload = '''
        {
            "jobInfo":
            {
                "jobType": "hive",
                "jobName": "HIVE_TEST", 
                "schedule": "adHoc",
                "configuration": "test",
                "clusterName": "MY_CLUSTER_NAME",
                "clusterId": "MY_CLUSTER_ID",
                "cmdArgs": "-e THIS IS GOING TO FAIL",
                "userName": "''' + user + '''"
            }
        }
        '''
        try:
            restclient.post(service_url=serviceUrl, payload=payload)
        except urllib2.HTTPError, e:
            pass

    def testGetPage(self):
        print "HTTP GET - running GET for page 0: "
        params = '?page=0'
        response = restclient.get(service_url=serviceUrl, params=params)
        print response.code
        assert response.code == 200, "can't get elements by page"
    
    def testGetByJobID(self):
        print "HTTP GET - running GET for jobID like %: "
        params = '?jobID=%&limit=3'
        response = restclient.get(service_url=serviceUrl, params=params)
        print response.code
        assert response.code == 200, "can't get elements by jobID"
    
    def testGetByJobName(self):
        print "HTTP GET - running GET for jobName like HIVE%: "
        params = '?jobName=HIVE%&limit=3'
        response = restclient.get(service_url=serviceUrl, params=params)
        print response.code
        assert response.code == 200, "can't get elements by jobName"
        
    def testGetByUser(self):
        print "HTTP GET - running GET for user"
        params = '?userName=' + user + '&limit=3'
        response = restclient.get(service_url=serviceUrl, params=params)
        print response.code
        assert response.code == 200, "can't get elements by user"
    
    def testGetByJobType(self):
        print "HTTP GET - running GET for jobType hive: "
        params = '?jobType=hive&limit=3'
        response = restclient.get(service_url=serviceUrl, params=params)
        print response.code
        assert response.code == 200, "can't get elements by type"
    
    def testGetJobByStatus(self):
        print "HTTP GET - for status FAILED: "
        params = '?status=FAILED&limit=3'
        response = restclient.get(service_url=serviceUrl, params=params)
        print response.code
        assert response.code == 200, "can't get elements by type"

    def testGetJobByClusterName(self):
        print "HTTP GET - for clusterName MY_CLUSTER_NAME: "
        params = '?clusterName=MY_CLUSTER_NAME&limit=3'
        response = restclient.get(service_url=serviceUrl, params=params)
        print response.code
        assert response.code == 200, "can't get elements by type"

    def testGetJobByClusterId(self):
        print "HTTP GET - for clusterId MY_CLUSTER_ID: "
        params = '?clusterId=MY_CLUSTER_ID&limit=3'
        response = restclient.get(service_url=serviceUrl, params=params)
        print response.code
        assert response.code == 200, "can't get elements by type"

    def testGetForFakeUser(self):
        print "HTTP GET - test for user DUMMY: "
        params = '?userName=does_not_exist'
        try:
            response = restclient.get(service_url=serviceUrl, params=params)
            print response.read()
            assert False, "received unexpected successful response"
        except urllib2.HTTPError, e:
            print e.read()
            assert e.code == 404, "received unexpected unsuccessful response"

    def testGetForFakeType(self):        
        print "HTTP GET - test for jobType DUMMY: "
        params = '?jobType=DUMMY&limit=3'
        try:
            response = restclient.get(service_url=serviceUrl, params=params)
            print response.read()
            assert False, "received unexpected successful response"
        except urllib2.HTTPError, e:
            print e.read()
            assert e.code == 400, "received unexpected unsuccessful response"

    def testGetForFakeStatus(self):
        print "HTTP GET - test for status DUMMY: "
        params = '?status=DUMMY&limit=3'
        try:
            response = restclient.get(service_url=serviceUrl, params=params)
            print response.read()
            assert False, "received unexpected successful response"
        except urllib2.HTTPError, e:
            print e.read()
            assert e.code == 400, "received unexpected unsuccessful response"

    def testGetForFakeClusterName(self):
        print "HTTP GET - test for clusterName DUMMY: "
        params = '?clusterName=DUMMY&limit=3'
        try:
            response = restclient.get(service_url=serviceUrl, params=params)
            print response.read()
            assert False, "received unexpected successful response"
        except urllib2.HTTPError, e:
            print e.read()
            assert e.code == 404, "received unexpected unsuccessful response"

    def testGetForFakeClusterId(self):
        print "HTTP GET - test for clusterId DUMMY: "
        params = '?clusterId=DUMMY&limit=3'
        try:
            response = restclient.get(service_url=serviceUrl, params=params)
            print response.read()
            assert False, "received unexpected successful response"
        except urllib2.HTTPError, e:
            print e.read()
            assert e.code == 404, "received unexpected unsuccessful response"
           
# driver method for all tests                
if __name__ == "__main__":    
   print "Running unit tests:\n"
   suite = unittest.makeSuite(JobInfoGetTestCase, 'test')
   unittest.main()
