##
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

import sys
sys.path.append('../utils')

import time
import eureka
import json
import os
import uuid
import restclient

#ID = "app-" + str(uuid.uuid4())
# the S3 prefix where the tests are located
GENIE_TEST_PREFIX = os.getenv("GENIE_TEST_PREFIX")

# get the serviceUrl from the eureka client
serviceUrl = eureka.EurekaClient().getServiceBaseUrl() + '/genie/v1/config/applications'


def addApplicationConfigMr1():
    print "Creating App mr1 "
    ID = "mr1"
    configs = json.dumps(['s3://netflix-dataoven-prod/genie/cluster/bigdataplatform_query_20140518/mapred-site.xml'])
    jars = json.dumps(['s3://netflix-dataoven-test/genie2/application/mapreduce1/foo.jar'])
    payload = '''
    {
        "id":"''' + ID +'''",
        "name": "mapreduce1", 
        "status" : "ACTIVE",
        "user" : "amsharma", 
        "version" : "1.0",
        "configs": ''' + configs + ''', 
        "jars": ''' + jars + ''' 
    }
    '''
    print payload
    print "\n"
    print restclient.post(serviceUrl=serviceUrl, payload=payload, contentType='application/json')

def addApplicationConfigMr2():
    print "Creating App mr2 "
    
    ID = "mr2"
    configs = json.dumps(['s3://netflix-bdp-emr-clusters/users/bdp/hquery/20140505/185527/genie/mapred-site.xml'])
    jars = json.dumps(['s3://netflix-dataoven-test/genie2/application/mapreduce1/foo.jar'])
    
    payload = '''
    {
        "id":"''' + ID +'''",
        "name": "mapreduce2", 
        "status" : "ACTIVE",
        "user" : "amsharma", 
        "version" : "2.4.0",
        "configs": ''' + configs + ''', 
        "jars": ''' + jars + ''' 
    }
    '''
    print payload
    print "\n"
    print restclient.post(serviceUrl=serviceUrl, payload=payload, contentType='application/json')

def addApplicationConfigTz1():
    print "Creating App tz1"
    
    ID = "tz1"
    payload = '''
    {
        "id":"''' + ID +'''",
        "name": "tez", 
        "status" : "ACTIVE",
        "user" : "amsharma",
        "version" : "1.0"
    }
    '''

    print payload
    print "\n"
    print restclient.post(serviceUrl=serviceUrl, payload=payload, contentType='application/json')

def addApplicationConfigTz2():
    print "Creating App tz2"
    
    ID = "tz2"
    payload = '''
    {
        "id":"''' + ID +'''",
        "name": "tez", 
        "status" : "ACTIVE",
        "user" : "amsharma",
        "version" : "2.0"
    }
    '''

    print payload
    print "\n"
    print restclient.post(serviceUrl=serviceUrl, payload=payload, contentType='application/json')

# driver method for all tests                
if __name__ == "__main__":
   print "Creating Application Configs:\n"
   addApplicationConfigMr1()
   addApplicationConfigMr2()
   addApplicationConfigTz1()
   addApplicationConfigTz2()
