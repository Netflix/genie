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

import sys

sys.path.append('../utils')

import eureka
import json
import os
import restclient

# the S3 prefix where the tests are located
GENIE_TEST_PREFIX = os.getenv("GENIE_TEST_PREFIX")

# get the serviceUrl from the eureka client
serviceUrl = eureka.EurekaClient().get_service_base_url() + '/genie/v2/config/applications'


def add_application_config_mr1():
    print "Creating App mr1 "
    application_id = "mr1"
    configs = json.dumps(['s3://netflix-dataoven-prod/genie/cluster/bigdataplatform_query_20140518/mapred-site.xml'])
    jars = json.dumps(['s3://netflix-dataoven-test/genie2/application/mapreduce1/foo.jar'])
    tags = json.dumps(['sla', 'cprod1', 'bdp_prod_20140101'])

    payload = '''
    {
        "id":"''' + application_id + '''",
        "name": "mapreduce1", 
        "status" : "ACTIVE",
        "user" : "amsharma", 
        "version" : "1.0.3",
        "configs": ''' + configs + ''',
        "tags": ''' + tags + ''',
        "jars": ''' + jars + ''' 
    }
    '''
    print payload
    print "\n"
    print restclient.post(service_url=serviceUrl, payload=payload, content_type='application/json')


def add_application_config_mr2():
    print "Creating App mr2 "

    application_id = "mr2"
    configs = json.dumps(['s3://netflix-bdp-emr-clusters/users/bdp/hquery/20140505/185527/genie/mapred-site.xml'])
    jars = json.dumps(['s3://netflix-dataoven-test/genie2/application/mapreduce1/foo.jar'])
    tags = json.dumps(['sla', 'h2prod', 'bdp_prod_20140101'])

    payload = '''
    {
        "id":"''' + application_id + '''",
        "name": "mapreduce2", 
        "status" : "ACTIVE",
        "user" : "amsharma", 
        "version" : "2.4.0",
        "configs": ''' + configs + ''', 
        "tags": ''' + tags + ''',
        "jars": ''' + jars + ''' 
    }
    '''
    print payload
    print "\n"
    print restclient.post(service_url=serviceUrl, payload=payload, content_type='application/json')


def add_application_config_tz1():
    print "Creating App tz1"
    tags = json.dumps(['sla', 'cprod1', 'bdp_prod_20140101'])

    application_id = "tz1"
    payload = '''
    {
        "id":"''' + application_id + '''",
        "name": "tez", 
        "status" : "ACTIVE",
        "user" : "amsharma",
        "tags": ''' + tags + ''',
        "version" : "1.0"
    }
    '''

    print payload
    print "\n"
    print restclient.post(service_url=serviceUrl, payload=payload, content_type='application/json')


def add_application_config_tz2():
    print "Creating App tz2"

    application_id = "tz2"
    payload = '''
    {
        "id":"''' + application_id + '''",
        "name": "tez", 
        "status" : "ACTIVE",
        "user" : "amsharma",
        "version" : "2.0"
    }
    '''

    print payload
    print "\n"
    print restclient.post(service_url=serviceUrl, payload=payload, content_type='application/json')

# driver method for all tests                
if __name__ == "__main__":
    print "Creating Application Configs:\n"
    add_application_config_mr1()
    add_application_config_mr2()
    add_application_config_tz1()
    add_application_config_tz2()
