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
serviceUrl = eureka.EurekaClient().getServiceBaseUrl() + '/genie/v2/config/clusters'

def addClusterGenie2():
    print "Registering genie2"
    ID="amsharma_genie2_20140716_204851"
    tags = json.dumps(['adhoc','h2query','amsharma_genie2_20140716_204851'])
    config = json.dumps(['s3://netflix-bdp-emr-clusters/users/amsharma/genie2/20140716/204851/genie/core-site.xml','s3://netflix-bdp-emr-clusters/users/amsharma/genie2/20140716/204851/genie/hdfs-site.xml','s3://netflix-bdp-emr-clusters/users/amsharma/genie2/20140716/204851/genie/mapred-site.xml','s3://netflix-bdp-emr-clusters/users/amsharma/genie2/20140716/204851/genie/yarn-site.xml'])
    payload = '''
    {
        "id":"''' + ID +'''",
        "name": "genie2", 
        "status" : "UP",
        "user" : "amsharma",
        "version" : "2.4.0",
        "configs": ''' + config + ''',
        "tags": ''' + tags + ''',
        "clusterType": "yarn"
    }
    '''
    print payload
    print "\n"
    print restclient.post(serviceUrl=serviceUrl, payload=payload, contentType='application/json')

def addUrsulaCluster():
    print "Registering genie2"
    ID="bdp_h2shadowtest_20140728_234936"
    tags = json.dumps(['bdp_h2shadowtest_20140728_234936','shadowtest_20120716'])
    config = json.dumps(['s3://netflix-bdp-emr-clusters/users/bdp/h2shadowtest/20140728/234936/genie/core-site.xml','s3://netflix-bdp-emr-clusters/users/bdp/h2shadowtest/20140728/234936/genie/hdfs-site.xml','s3://netflix-bdp-emr-clusters/users/bdp/h2shadowtest/20140728/234936/genie/mapred-site.xml','s3://netflix-bdp-emr-clusters/users/bdp/h2shadowtest/20140728/234936/genie/yarn-site.xml'])
    payload = '''
    {
        "id":"''' + ID +'''",
        "name": "shadowtest_20120716", 
        "status" : "UP",
        "user" : "amsharma",
        "version" : "2.4.0",
        "configs": ''' + config + ''',
        "tags": ''' + tags + ''',
        "clusterType": "yarn"
    }
    '''
    print payload
    print "\n"
    print restclient.post(serviceUrl=serviceUrl, payload=payload, contentType='application/json')
    
# driver method for all tests                
if __name__ == "__main__":
   print "Registering Clusters:\n"
   print "\n"
   addClusterGenie2()
   addUrsulaCluster()
