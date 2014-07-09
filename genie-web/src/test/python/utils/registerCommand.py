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

# get the serviceUrl from the eureka client
serviceUrl = eureka.EurekaClient().getServiceBaseUrl() + '/genie/v2/config/commands'

def addCommandConfigProdhive11Mr1():
    print "Adding Command prodhive11_mr1"
    ID = "prodhive11_mr1"
    configs = json.dumps(['s3://netflix-dataoven-prod/genie/config/hiveconf-prodhive-011/hive-site.xml','s3://netflix-dataoven-test/genie2/command/prodhive11_mr1/hive-default.xml'])
    tags = json.dumps(['tag1','tag2','tag3'])
    apps = json.dumps([{"id":"mr1"}])
    payload = '''
    {
        "id":"''' + ID +'''",
        "name": "prodhive", 
        "status" : "ACTIVE",
        "executable": "/apps/hive/0.11/bin/hive",
        "envPropFile": "s3://netflix-dataoven-test/genie2/command/prodhive11_mr1/envFile.sh",
        "user" : "amsharma", 
        "version" : "0.11",
        "tags": ''' + tags + ''',
        "configs": ''' + configs + '''
    }
    '''
    print payload
    print "\n"
    print restclient.post(serviceUrl=serviceUrl, payload=payload, contentType='application/json')

def addCommandConfigHadoop103():
    print "Adding Command hadoop103"
    ID = "hadoop103"
    tags = json.dumps(['tag1','tag2','tag3'])
    apps = json.dumps([{"id":"mr1"}])
    payload = '''
    {
        "id":"''' + ID +'''",
        "name": "hadoop", 
        "status" : "ACTIVE",
        "executable": "/apps/hadoop/1.0.3/bin/hadoop",
        "user" : "amsharma", 
        "tags": ''' + tags + ''',
        "version" : "1.0.3"
    }
    '''

    print payload
    print "\n"
    print restclient.post(serviceUrl=serviceUrl, payload=payload, contentType='application/json')

def addCommandConfigProdhive11Mr2():
    print "Adding Command prodhive11_mr2"
    ID = "prodhive11_mr2"
    configs = json.dumps(['s3://netflix-dataoven-prod/genie/config/hiveconf-prodhive-011/hive-site.xml','s3://netflix-dataoven-test/genie2/command/prodhive11_mr1/hive-default.xml'])
    tags = json.dumps(['tag1','tag2','tag3'])
    apps = json.dumps([{"id":"mr2"}])
    payload = '''
    {
        "id":"''' + ID +'''",
        "name": "prodhive", 
        "status" : "ACTIVE",
        "executable": "/apps/hive/0.11/bin/hive",
        "envPropFile": "s3://netflix-dataoven-test/genie2/command/prodhive11_mr2/envFile.sh",
        "user" : "amsharma", 
        "version" : "0.11",
        "tags": ''' + tags + ''',
        "configs": ''' + configs + '''
    }
    '''

    print payload
    print "\n"
    print restclient.post(serviceUrl=serviceUrl, payload=payload, contentType='application/json')

def addCommandConfigPig11Mr1():
    print "Adding Command pig11_mr1"
    ID = "pig11_mr1"
    tags = json.dumps(['tag1','tag2','tag3'])
    apps = json.dumps([{"id":"mr1"}])
    payload = '''
    {
        "id":"''' + ID +'''",
        "name": "pig", 
        "status" : "ACTIVE",
        "executable": "/apps/pig/0.11/bin/pig",
        "envPropFile": "s3://netflix-dataoven-test/genie2/command/pig11_mr1/envFile.sh",
        "user" : "amsharma", 
        "tags": ''' + tags + ''',
        "version" : "0.11"
    }
    '''
    print payload
    print "\n"
    print restclient.post(serviceUrl=serviceUrl, payload=payload, contentType='application/json')

def addCommandConfigPig11Mr2():
    print "Adding Command pig11_mr2"
    ID = "pig11_mr2"
    tags = json.dumps(['tag1','tag2','tag3'])
    apps = json.dumps([{"id":"mr2"}])
    payload = '''
    {
        "id":"''' + ID +'''",
        "name": "pig", 
        "status" : "ACTIVE",
        "executable": "/apps/pig/0.11-h2/bin/pig",
        "envPropFile": "s3://netflix-dataoven-test/genie2/command/pig11_mr2/envFile.sh",
        "user" : "amsharma", 
        "tags": ''' + tags + ''',
        "version" : "0.11-h2"
    }
    '''
    print payload
    print "\n"
    print restclient.post(serviceUrl=serviceUrl, payload=payload, contentType='application/json')

def addCommandConfigPig13Mr2():
    print "Adding Command pig13_mr2"
    ID = "pig13_mr2"
    tags = json.dumps(['tag1','tag2','tag3'])
    apps = json.dumps([{"id":"mr2"}])
    payload = '''
    {
        "id":"''' + ID +'''",
        "name": "pig", 
        "status" : "ACTIVE",
        "executable": "/apps/pig/0.13/bin/pig",
        "envPropFile": "s3://netflix-dataoven-test/genie2/command/pig13_mr2/envFile.sh",
        "user" : "amsharma", 
        "tags": ''' + tags + ''',
        "version" : "0.13"
    }
    '''
    print payload
    print "\n"
    print restclient.post(serviceUrl=serviceUrl, payload=payload, contentType='application/json')

# driver method for all tests                
if __name__ == "__main__":
   print "Adding Commands: \n"
   print "\n"

   addCommandConfigProdhive11Mr1()
   addCommandConfigProdhive11Mr2()
   addCommandConfigPig11Mr1()
   addCommandConfigPig11Mr2()
   addCommandConfigPig13Mr2()
   addCommandConfigHadoop103()
