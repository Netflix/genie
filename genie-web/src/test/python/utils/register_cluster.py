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
import restclient
import urllib2

# get the serviceUrl from the eureka client
serviceUrl = eureka.EurekaClient().get_service_base_url() + '/genie/v2/config/clusters'


def add_cluster_cprod1():
    print "Registering Cprod1"
    cluster_id = "bdp_prod_20140101"

    tags = json.dumps(['sla', 'cprod1', 'bdp_prod_20140101'])
    config = json.dumps(['s3://netflix-dataoven-prod/genie/cluster/bigdataplatform_prod_20140415/hdfs-site.xml',
                         's3://netflix-dataoven-prod/genie/cluster/bigdataplatform_prod_20140415/core-site.xml',
                         's3://netflix-dataoven-prod/genie/cluster/bigdataplatform_prod_20140415/mapred-site.xml'])

    payload = '''
    {
        "id":"''' + cluster_id + '''",
        "name": "cprod1", 
        "status" : "UP",
        "user" : "amsharma", 
        "version" : "1.0.3",
        "configs": ''' + config + ''',
        "tags": ''' + tags + ''',
        "clusterType": "yarn"
    }
    '''
    print payload
    print "\n"
    print restclient.post(service_url=serviceUrl, payload=payload, content_type='application/json')


def add_cluster_cquery1():
    print "Registering cquery1"
    cluster_id = "bdp_query_20140101"
    tags = json.dumps(['adhoc'])
    config = json.dumps(['s3://netflix-dataoven-prod/genie/cluster/bigdataplatform_query_20140518/core-site.xml',
                         's3://netflix-dataoven-prod/genie/cluster/bigdataplatform_query_20140518/hdfs-site.xml'])
    payload = '''
    {
        "id":"''' + cluster_id + '''",
        "name": "cquery1", 
        "status" : "UP",
        "user" : "amsharma",
        "version" : "1.0.3",
        "configs": ''' + config + ''',
        "tags": ''' + tags + ''',
        "clusterType": "yarn"
    }
    '''
    print payload
    print "\n"
    print restclient.post(service_url=serviceUrl, payload=payload, content_type='application/json')


def add_cluster_h2query():
    print "Registering h2query"
    cluster_id = "bdp_hquery_20140505_185527"
    tags = json.dumps(['adhoc', 'h2query', 'bdp_hquery_20140505_185527'])
    config = json.dumps(['s3://netflix-bdp-emr-clusters/users/bdp/hquery/20140505/185527/genie/core-site.xml',
                         's3://netflix-bdp-emr-clusters/users/bdp/hquery/20140505/185527/genie/hdfs-site.xml',
                         's3://netflix-bdp-emr-clusters/users/bdp/hquery/20140505/185527/genie/yarn-site.xml'])
    payload = '''
    {
        "id":"''' + cluster_id + '''",
        "name": "h2query", 
        "status" : "UP",
        "user" : "amsharma",
        "version" : "1.0.3",
        "configs": ''' + config + ''',
        "tags": ''' + tags + ''',
        "clusterType": "yarn"
    }
    '''
    print payload
    print "\n"
    print restclient.post(service_url=serviceUrl, payload=payload, content_type='application/json')

# driver method for all tests                
if __name__ == "__main__":
    print "Registering Clusters:\n"
    print "\n"
    try:
        add_cluster_cprod1()
        add_cluster_cquery1()
        add_cluster_h2query()
    except urllib2.HTTPError, e:
        print "Caught Exception"
        print "code = " + str(e.code)
        print "message =" + e.msg
        print "read =" + e.read()
