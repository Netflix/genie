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
import json
import os
import uuid
import restclient

# get the serviceUrl from the eureka client
serviceUrl = eureka.EurekaClient().getServiceBaseUrl() + '/genie/v1/config/command'

def addCommandConfigProdhive11Mr1():
    print "Adding Command prodhive11_mr1"
    ID = "prodhive11_mr1"
    configs = json.dumps(['s3://netflix-dataoven-prod/genie/config/hiveconf-prodhive-011/hive-site.xml','s3://netflix-dataoven-test/genie2/command/prodhive11_mr1/hive-default.xml'])
    apps = json.dumps(['mr1'])
    payload = '''
    {
        "commandConfig":
        {
            "id":"''' + ID +'''",
            "name": "prodhive", 
            "status" : "active",
            "executable": "/apps/hive/0.11/bin/hive",
            "envPropFile": "s3://netflix-dataoven-test/genie2/command/prodhive11_mr1/envFile.sh",
            "user" : "amsharma", 
            "groupName" : "hadoop", 
            "version" : "0.11",
            "configs": ''' + configs + ''',
            "appIds": ''' + apps + ''' 
        }
    }
    '''
    print payload
    print "\n"
    print restclient.post(serviceUrl=serviceUrl, payload=payload, contentType='application/json')

def addCommandConfigProdhive11Mr2():
    print "Adding Command prodhive11_mr2"
    ID = "prodhive11_mr2"
    configs = json.dumps(['s3://netflix-dataoven-prod/genie/config/hiveconf-prodhive-011/hive-site.xml','s3://netflix-dataoven-test/genie2/command/prodhive11_mr1/hive-default.xml'])
    apps = json.dumps(['mr2'])
    payload = '''
    {
        "commandConfig":
        {
            "id":"''' + ID +'''",
            "name": "prodhive", 
            "status" : "active",
            "executable": "/apps/hive/0.11-h2/bin/hive",
            "envPropFile": "s3://netflix-dataoven-test/genie2/command/prodhive11_mr1/envFile.sh",
            "user" : "amsharma", 
            "groupName" : "hadoop", 
            "version" : "0.11",
            "configs": ''' + configs + ''',
            "appIds": ''' + apps + ''' 
        }
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
