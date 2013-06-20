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
serviceUrl = eureka.EurekaClient().getServiceBaseUrl() + '/genie/v0/jobs'

# use this user name for job submission and queries
user = os.getenv("USER")

class JobSubmitErrorTestCase(unittest.TestCase):

    def testMissingUserName(self):    
        print "HTTP POST: missing user name: "
        payload = '''
        {
            "jobInfo":
            {
                "jobType": "hadoop", 
                "schedule": "adHoc",
                "cmdArgs":"jar hadoop-examples-0.20.205.1.jar sleep -m 1 -mt 1"
            }
        }
        '''
        try:
            response = restclient.post(serviceUrl=serviceUrl, payload=payload)
            print response.read()
            assert False, "received unexpected successful response"
        except urllib2.HTTPError, e:
            print e.read()
            assert e.code == 400, "received unexpected unsuccessful response"

    def testMissingJobType(self):     
        print "HTTP POST: missing job type: "
        payload = '''
        {
            "jobInfo":
            {
                "userName": "testuser",
                "schedule": "adHoc",
                "cmdArgs":"jar hadoop-examples-0.20.205.1.jar sleep -m 1 -mt 1" 
            }
        }
        '''
        try:
            response = restclient.post(serviceUrl=serviceUrl, payload=payload)
            print response.read()
            assert False, "received unexpected successful response"
        except urllib2.HTTPError, e:
            print e.read()
            assert e.code == 400, "received unexpected unsuccessful response"

    def testBadPigVersion(self):
        print "HTTP POST: bad pig version"
        payload = '''
        {
            "jobInfo":
            {
                "userName" : "genietest", 
                "jobType": "pig", 
                "configuration": "prod", 
                "schedule": "adHoc",
                "cmdArgs":"-f pig.q",
                "pigVersion": "0.0"
            }
        }
        '''
        try:
            response = restclient.post(serviceUrl=serviceUrl, payload=payload)
            print response.read()
            assert False, "received unexpected successful response"
        except urllib2.HTTPError, e:
            print e.read()
            assert e.code == 500 or e.code == 402, "received unexpected unsuccessful response"

    def testBadPigConfigId(self):
        print "HTTP POST: bad pig config"
        payload = '''
        {
            "jobInfo":
            {
                "userName" : "genietest", 
                "jobType": "pig", 
                "configuration": "prod", 
                "schedule": "adHoc",
                "cmdArgs":"-f pig.q",
                "pigConfigId": "DOES_NOT_EXIST"
            }
        }
        '''
        try:
            response = restclient.post(serviceUrl=serviceUrl, payload=payload)
            print response.read()
            assert False, "received unexpected successful response"
        except urllib2.HTTPError, e:
            print e.read()
            assert e.code == 500 or e.code == 402, "received unexpected unsuccessful response"
    
    def testBadHiveVersion(self):
        print "HTTP POST: bad hive version"
        payload = '''
        {
            "jobInfo":
            {
                "userName" : "genietest", 
                "jobType": "hive", 
                "configuration": "prod", 
                "schedule": "adHoc",
                "cmdArgs":"-f hive.q",
                "hiveVersion": "0.0"
            }
        }
        '''
        try:
            response = restclient.post(serviceUrl=serviceUrl, payload=payload)
            print response.read()
            assert False, "received unexpected successful response"
        except urllib2.HTTPError, e:
            print e.read()
            assert e.code == 500 or e.code == 402, "received unexpected unsuccessful response"

    def testBadHiveConfigId(self):
        print "HTTP POST: bad hive config"
        payload = '''
        {
            "jobInfo":
            {
                "userName" : "genietest", 
                "jobType": "hive", 
                "configuration": "prod", 
                "schedule": "adHoc",
                "cmdArgs": "-f hive.q",
                "hiveConfigId": "DOES_NOT_EXIST"
            }
        }
        '''
        try:
            response = restclient.post(serviceUrl=serviceUrl, payload=payload)
            print response.read()
            assert False, "received unexpected successful response"
        except urllib2.HTTPError, e:
            print e.read()
            assert e.code == 500 or e.code == 402, "received unexpected unsuccessful response"
           
    def testMissingSchedule(self):
        print "HTTP POST: missing schedule: "
        payload = '''
        {
            "jobInfo":
            {
                "userName": "testuser",
                "jobType": "hive",
                "cmdArgs":"jar hadoop-examples-0.20.205.1.jar sleep -m 1 -mt 1" 
            }
        }
        '''
        try:
            response = restclient.post(serviceUrl=serviceUrl, payload=payload)
            print response.read()
            assert False, "received unexpected successful response"
        except urllib2.HTTPError, e:
            print e.read()
            assert e.code == 400, "received unexpected unsuccessful response"

    def testMissingCmdArgs(self):    
        print "HTTP POST: missing command args: "
        payload = '''
        {
            "jobInfo":
            {
                "userName": "testuser",
                "jobType": "hive",
                "schedule": "adhoc"
            }
        }
        '''
        try:
            response = restclient.post(serviceUrl=serviceUrl, payload=payload)
            print response.read()
            assert False, "received unexpected successful response"
        except urllib2.HTTPError, e:
            print e.read()
            assert e.code == 400, "received unexpected unsuccessful response"
            
    def testMissingConfig(self):
        print "HTTP POST: missing configuration: "
        payload = '''
        {
            "jobInfo":
            {
                "userName": "testuser",
                "jobType": "hive",
                "schedule": "adhoc",
                "cmdArgs":"jar hadoop-examples-0.20.205.1.jar sleep -m 1 -mt 1" 
            }
        }
        '''
        try:
            response = restclient.post(serviceUrl=serviceUrl, payload=payload)
            print response.read()
            assert False, "received unexpected successful response"
        except urllib2.HTTPError, e:
            print e.read()
            assert e.code == 400, "received unexpected unsuccessful response"

    def testIncorrectCmdArgs(self):    
        print "HTTP POST: incorrect command-line args: "
        payload = '''
        {
            "jobInfo":
            {
                "userName": "testuser",
                "jobType": "hive",
                "schedule": "adhoc",
                "configuration": "prod",
                "cmdArgs":"foo bar" 
            }
        }
        '''
        try:
            response = restclient.post(serviceUrl=serviceUrl, payload=payload)
            print response.read()
            assert False, "received unexpected successful response"
        except urllib2.HTTPError, e:
            print e.read()
            assert e.code == 400, "received unexpected unsuccessful response"
            
    def testMissingClusterName(self):
        print "HTTP POST: cluster name doesn't exist: "
        payload = '''
        {
            "jobInfo":
            {
                "userName": "testuser",
                "jobType": "hive",
                "schedule": "adhoc",
                "configuration": "prod",
                "cmdArgs": "-f hive.q",
                "clusterName": "DOES_NOT_EXIST" 
            }
        }
        '''
        try:
            response = restclient.post(serviceUrl=serviceUrl, payload=payload)
            print response.read()
            assert False, "received unexpected successful response"
        except urllib2.HTTPError, e:
            print e.read()
            assert e.code == 402, "received unexpected unsuccessful response"
            
    def testMissingClusterId(self):
        print "HTTP POST: cluster id doesn't exist: "
        payload = '''
        {
            "jobInfo":
            {
                "userName": "testuser",
                "jobType": "hive",
                "schedule": "adhoc",
                "configuration": "prod",
                "cmdArgs": "-f hive.q",
                "clusterId": "DOES_NOT_EXIST" 
            }
        }
        '''
        try:
            response = restclient.post(serviceUrl=serviceUrl, payload=payload)
            print response.read()
            assert False, "received unexpected successful response"
        except urllib2.HTTPError, e:
            print e.read()
            assert e.code == 402, "received unexpected unsuccessful response"
                       
# driver method for all tests                
if __name__ == "__main__":
   print "Running unit tests:\n"
   suite = unittest.makeSuite(JobSubmitErrorTestCase, 'test')
   unittest.main()
