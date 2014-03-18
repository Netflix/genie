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

ID = "app-" + str(uuid.uuid4())
# the S3 prefix where the tests are located
GENIE_TEST_PREFIX = os.getenv("GENIE_TEST_PREFIX")

# get the serviceUrl from the eureka client
serviceUrl = eureka.EurekaClient().getServiceBaseUrl() + '/genie/v1/config/command'

def addCommandConfig():
    print "Running testJsonSubmitjob "
    payload = '''
    <request>
        <command>
            <id>''' + ID + '''</id>
            <name>sample_app</name>
            <status>active</status>
            <user>genietest</user>
            <version>laptop</version>
            <appids>app-c5c027d7-5d64-45ec-ae50-3c786e51610e</appids>
        </command>
    </request>
    '''
    print payload
    print "\n"
    print restclient.post(serviceUrl=serviceUrl, payload=payload, contentType='application/xml')

# driver method for all tests                
if __name__ == "__main__":
   print "Running unit tests:\n"
   print "\n"
   addCommandConfig()
