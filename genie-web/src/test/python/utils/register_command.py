# #
#
# Copyright 2014 Netflix, Inc.
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

import eureka
import json
import restclient

# get the serviceUrl from the eureka client
serviceUrl = eureka.EurekaClient().get_service_base_url() + '/genie/v2/config/commands'


def add_command_hadoop240():
    print "Adding Command hadoop240"
    command_id = "hadoop240"
    tags = json.dumps(['hadoop240'])
    payload = '''
    {
        "id":"''' + command_id + '''",
        "name": "hadoop", 
        "status" : "ACTIVE",
        "executable": "/apps/hadoop/2.4.0/bin/hadoop",
        "user" : "amsharma", 
        "tags": ''' + tags + ''',
        "jobType" : "hadoop",
        "version" : "2.4.0"
    }
    '''

    print payload
    print "\n"
    print restclient.post(service_url=serviceUrl, payload=payload, content_type='application/json')


def add_command_prodhive11_mr2():
    print "Adding Command prodhive11_mr2"
    command_id = "prodhive11_mr2"
    configs = json.dumps(['s3://netflix-dataoven-prod/genie/config/hiveconf-prodhive-011-h24/hive-site.xml',
                          's3://netflix-dataoven-test/genie2/command/prodhive11_mr1/hive-default.xml'])
    tags = json.dumps(['hive', 'prod', '0.11-h24'])
    payload = '''
    {
        "id":"''' + command_id + '''",
        "name": "prodhive", 
        "status" : "ACTIVE",
        "executable": "/apps/hive/0.11-h2/bin/hive",
        "envPropFile": "s3://netflix-dataoven-test/genie2/command/prodhive11_mr2/envFile.sh",
        "user" : "amsharma", 
        "version" : "0.11-h2",
        "tags": ''' + tags + ''',
        "jobType" : "hive",
        "configs": ''' + configs + '''
    }
    '''

    print payload
    print "\n"
    print restclient.post(service_url=serviceUrl, payload=payload, content_type='application/json')


def add_command_pig11_mr2():
    print "Adding Command pig11_mr2"
    command_id = "pig11_mr2"
    tags = json.dumps(['tag1', 'tag2', 'tag3'])
    payload = '''
    {
        "id":"''' + command_id + '''",
        "name": "pig", 
        "status" : "ACTIVE",
        "executable": "/apps/pig/0.11-h2/bin/pig",
        "envPropFile": "s3://netflix-dataoven-test/genie2/command/pig11_mr2/envFile.sh",
        "user" : "amsharma", 
        "jobType" : "pig",
        "tags": ''' + tags + ''',
        "version" : "0.11-h2"
    }
    '''
    print payload
    print "\n"
    print restclient.post(service_url=serviceUrl, payload=payload, content_type='application/json')


def add_command_pig13_mr2():
    print "Adding Command pig13_mr2"
    command_id = "pig13_mr2"
    tags = json.dumps(['tag1', 'tag2', 'tag3'])
    payload = '''
    {
        "id":"''' + command_id + '''",
        "name": "pig", 
        "status" : "ACTIVE",
        "executable": "/apps/pig/0.13/bin/pig",
        "envPropFile": "s3://netflix-dataoven-test/genie2/command/pig13_mr2/envFile.sh",
        "user" : "amsharma", 
        "jobType" : "pig",
        "tags": ''' + tags + ''',
        "version" : "0.13"
    }
    '''
    print payload
    print "\n"
    print restclient.post(service_url=serviceUrl, payload=payload, content_type='application/json')


def add_command_ursula_hive():
    print "Adding Command ursula hive"
    command_id = "hiveconf-shadowtest_20120716-011-h24"
    configs = json.dumps(['s3://netflix-dataoven-prod/genie/config/hiveconf-shadowtest_20120716-011-h24/hive-site.xml',
                          's3://netflix-dataoven-test/genie2/command/prodhive11_mr1/hive-default.xml'])
    tags = json.dumps(['hive', 'shadowtest', 'ursula'])
    payload = '''
    {
        "id":"''' + command_id + '''",
        "name": "ursulahive", 
        "status" : "ACTIVE",
        "executable": "/apps/hive/0.11-h2/bin/hive",
        "envPropFile": "s3://netflix-dataoven-test/genie2/command/prodhive11_mr2/envFile.sh",
        "user" : "amsharma", 
        "version" : "0.11-h2",
        "jobType" : "hive",
        "tags": ''' + tags + ''',
        "configs": ''' + configs + '''
    }
    '''

    print payload
    print "\n"
    print restclient.post(service_url=serviceUrl, payload=payload, content_type='application/json')

# driver method for all tests
if __name__ == "__main__":
    print "Adding Commands: \n"
    print "\n"

    add_command_hadoop240()
    add_command_prodhive11_mr2()
    add_command_pig11_mr2()
    add_command_pig13_mr2()
    add_command_ursula_hive()
