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

import json
import urllib2
import eureka
import unittest
import restclient
import datetime

# get the serviceUrl from the eureka client
serviceUrl = eureka.EurekaClient().getServiceBaseUrl() + '/genie/v0/config/cluster'
hiveServiceUrl = eureka.EurekaClient().getServiceBaseUrl() + '/genie/v0/config/hive'

# use a global id for all tests
id='unittest_clusterconfig_id'
id_pattern='%clusterconfig%'
name='unittest_clusterconfig_name'
name_prefix='unittest%'

hive_id='unittest_hiveconfig_id'
hive_name='unittest_hiveconfig_name'
hive_type='UNITTEST'

class ClusterConfigTestCase(unittest.TestCase):

    def setUp(self):
        # create a new hive config, if needed
        # create a new one, if needed
        payload = '''
        <request>
          <hiveConfig>
            <id>''' + hive_id + '''</id>
            <name>''' + hive_name + '''</name>
            <type>''' + hive_type + '''</type>
            <s3HiveSiteXml>s3://BUCKET/PATH/TO/HIVE-SITE.XML</s3HiveSiteXml>
            <user>testuser</user>
          </hiveConfig>
        </request>
        '''
        try:
            response = restclient.post(serviceUrl=hiveServiceUrl, payload=payload, contentType='application/xml')
        except urllib2.HTTPError, e:
            if e.code == 409:
                pass
            else:
                raise

        # create a new cluster config, if needed
        payload = '''
        <request>
          <clusterConfig>
            <id>''' + id + '''</id>
            <name>''' + name + '''</name>
            <adHoc>false</adHoc>
            <unitTest>true</unitTest>
            <status>UP</status>
            <s3MapredSiteXml>s3://PATH/TO/MAPRED-SITE.XML</s3MapredSiteXml>
            <s3CoreSiteXml>s3://PATH/TO/CORE-SITE.XML</s3CoreSiteXml>
            <s3HdfsSiteXml>s3://PATH/TO/HDFS-SITE.XML</s3HdfsSiteXml>
            <unitTestHiveConfigId>unittest_hiveconfig_id</unitTestHiveConfigId>
            <user>testuser</user>
          </clusterConfig>
        </request>
        '''
        try:
            response = restclient.post(serviceUrl=serviceUrl, payload=payload, contentType='application/xml')
            obj = json.loads(response.read())
            assert obj['clusterConfigs']['clusterConfig']['createTime'] > 0, "Create time not set correctly for config"
            assert obj['clusterConfigs']['clusterConfig']['updateTime'] > 0, "Update time not set correctly for config"
        except urllib2.HTTPError, e:
            if e.code == 409:
                pass
            else:
                raise

    def tearDown(self):
        try:
            params = '/' + id
            response = restclient.delete(serviceUrl=serviceUrl, params=params)
        except:
            pass

        try:
            params = '/' + hive_id
            response = restclient.delete(serviceUrl=hiveServiceUrl, params=params)
        except:
            pass
        
    def testGetAll(self):
        print "HTTP GET: getting all elements"
        response = restclient.get(serviceUrl=serviceUrl)
        print response.code
        assert response.code == 200, "can't get all elements" 
    
    def testGetById(self):
        print "HTTP GET: getting elements by id"
        params = '?id=' + id
        response = restclient.get(serviceUrl=serviceUrl, params=params)
        print response.code
        assert response.code == 200, "can't get element by id: " + id

    def testGetByIdPattern(self):
        print "HTTP GET: getting elements by id"
        params = '?id=' + id_pattern
        response = restclient.get(serviceUrl=serviceUrl, params=params)
        print response.code
        assert response.code == 200, "can't get element by id: " + id
        
    def testGetByName(self):
        print "HTTP GET: getting elements by name"        
        params = '?name=' + name
        response = restclient.get(serviceUrl=serviceUrl, params=params)
        print response.code
        assert response.code == 200, "can't get element by name: " + name

    def testGetByClusterPrefix(self):
        print "HTTP GET: getting elements by name"        
        params = '?name=' + name_prefix
        response = restclient.get(serviceUrl=serviceUrl, params=params)
        print response.code
        assert response.code == 200, "can't get element by name: " + name
        
    def testGetByUpdateTime(self):
        print "HTTP GET: getting elements by update time"
        minUpdateTime = 0
        maxUpdateTime = int(datetime.datetime.utcnow().strftime('%s'))*1000        
        params = '?minUpdateTime=' + str(minUpdateTime) + "&maxUpdateTime=" + str(maxUpdateTime)
        response = restclient.get(serviceUrl=serviceUrl, params=params)
        print response.code
        assert response.code == 200, "can't get element by name: " + name
        
    def testGetByScheduleAndStatus(self):
        print "HTTP GET: getting elements by schedule and status"
        params = '?bonus=false&status=UP'
        response = restclient.get(serviceUrl=serviceUrl, params=params)
        print response.code
        assert response.code == 200, "can't get element by schedule and status"
 
    def testGetByStatus(self):
        print "HTTP GET: getting elements by multiple status"          
        params = '?status=&status=UP&status=OUT_OF_SERVICE'
        response = restclient.get(serviceUrl=serviceUrl, params=params)
        print response.code
        assert response.code == 200, "can't get element by multiple status"

    def testGetByConfiguration(self):
        print "HTTP GET: getting elements by configuration"
        params = '?unitTest=true'
        response = restclient.get(serviceUrl=serviceUrl, params=params)
        print response.code
        assert response.code == 200, "can't get element by configuration"

    def testGetBySchedule(self):
        print "HTTP GET: getting elements by schedule"
        params = '?adHoc=false'
        response = restclient.get(serviceUrl=serviceUrl, params=params)
        print response.code
        assert response.code == 200, "can't get element by configuration"

    def testGetByScheduleAndConfig(self):
        print "HTTP GET: getting elements by schedule and configuration"
        params = '?adHoc=false&unitTest=true'
        response = restclient.get(serviceUrl=serviceUrl, params=params)
        print response.code
        assert response.code == 200, "can't get element by configuration"

    def testGetByLimitAndPage(self):
        print "HTTP GET: getting elements by page and limit"    
        params = '?limit=1&page=0'
        response = restclient.get(serviceUrl=serviceUrl, params=params)
        print response.code
        assert response.code == 200, "can't get element by configuration"

    def testGetByJobTypeAndConfig(self):
        print "HTTP GET: getting cluster with unitTest=true and jobType=hive"    
        params = '?jobType=hive&unitTest=true'
        response = restclient.get(serviceUrl=serviceUrl, params=params)
        print response.code
        assert response.code == 200, "can't get element by configuration"
                
    def testNoSuchCluster(self):
        print "HTTP GET: no such cluster"
        params = '?id=does_not_exist'
        try:
            response = restclient.get(serviceUrl=serviceUrl, params=params)
            print response.read()
            assert False, "received unexpected successful response"
        except urllib2.HTTPError, e:
            print e.read()
            assert e.code == 404, "received unexpected unsuccessful response"

    def testNoSuchClusterByConfig(self):
        print "HTTP GET: no such cluster"
        params = '?name='+name+'&unitTest=false'
        try:
            response = restclient.get(serviceUrl=serviceUrl, params=params)
            print response.read()
            assert False, "received unexpected successful response"
        except urllib2.HTTPError, e:
            print e.read()
            assert e.code == 404, "received unexpected unsuccessful response"

    def testHadoopRequest(self):
        print "HTTP GET: test=false with jobType=hadoop"
        params = '?jobType=hadoop&test=false'
        response = restclient.get(serviceUrl=serviceUrl, params=params)
        print response.code
        assert response.code == 200, "can't get element by configuration"
    
    def testGetBadStatus(self):
        print "HTTP GET: bad status"
        params = '?status=FOO'
        try:
            response = restclient.get(serviceUrl=serviceUrl, params=params)
            print response.read()
            assert False, "received unexpected successful response"
        except urllib2.HTTPError, e:
            print e.read()
            assert e.code == 400, "received unexpected unsuccessful response"

    def testNoSuchClusterByUpdateTime(self):
        print "HTTP GET: no such cluster"
        params = '?maxUpdateTime=0'
        try:
            response = restclient.get(serviceUrl=serviceUrl, params=params)
            print response.read()
            assert False, "received unexpected successful response"
        except urllib2.HTTPError, e:
            print e.read()
            assert e.code == 404, "received unexpected unsuccessful response"

    def testNoSuchPage(self):           
        print "HTTP GET: no such page"
        params = '?page=1000'
        try:
            response = restclient.get(serviceUrl=serviceUrl, params=params)
            print response.read()
            assert False, "received unexpected successful response"
        except urllib2.HTTPError, e:
            print e.read()
            assert e.code == 404, "received unexpected unsuccessful response"
    
    def testPostMissingConfig(self):
        print "HTTP POST: missing config element"        
        payload = '''
        <request>
        </request>
        '''
        try:
            response = restclient.post(serviceUrl=serviceUrl, payload=payload, contentType='application/xml')
            print response.read()
            assert False, "received unexpected successful response"
        except urllib2.HTTPError, e:
            print e.read()
            assert e.code == 400, "received unexpected unsuccessful response"

    def testPostMissingUser(self):    
        print "HTTP POST: missing user"
        payload = '''
        <request>
          <clusterConfig>
            <name>prod</name>
            <test>true</test>
            <s3MapredSiteXml>s3://path/to/mapred-file</s3MapredSiteXml>
          </clusterConfig>
        </request>
        '''
        try:
            response = restclient.post(serviceUrl=serviceUrl, payload=payload, contentType='application/xml')
            print response.read()
            assert False, "received unexpected successful response"
        except urllib2.HTTPError, e:
            print e.read()
            assert e.code == 400, "received unexpected unsuccessful response"
           
    def testPut(self):
        print "HTTP PUT: updating entry"
        payload = '''
        {
            "clusterConfig": {
              "prod": true,
              "s3MapredSiteXml": "s3://PATH/TO/NEW/MAPRED",
              "s3CoreSiteXml": "s3://PATH/TO/NEW/CORE",
              "s3HdfsSiteXml": "s3://PATH/TO/NEW/HDFS",
              "user": "testuser",
              "prodHiveConfigId": "unittest_hiveconfig_id"
          }
        }
        '''
        params = '/' + id
        response = restclient.put(serviceUrl=serviceUrl, params=params, payload=payload)
        print response.code
        assert response.code == 200, "unable to put config for id" + id
        obj= json.loads(response.read())
        assert obj['clusterConfigs']['clusterConfig']['updateTime'] > obj['clusterConfigs']['clusterConfig']['createTime'], \
            "Update time not greater than create time"
                
    def testPutMissingParamsErr(self):
        print "HTTP PUT: missing required parameters for new entry"
        payload = '''
        {
            "clusterConfig": {
              "prod": true,
              "user": "testuser",
              "prodHiveConfigId": "unittest_hiveconfig_id"
          }
        }
        '''
        params = '/new-' + id
        try:
            response = restclient.put(serviceUrl=serviceUrl, params=params, payload=payload)
            print response.read()
            assert False, "received unexpected successful response"
        except urllib2.HTTPError, e:
            print e.read()
            assert e.code == 400, "received unexpected unsuccessful response"

    def testPutNoSuchConfigErr(self):
        print "HTTP PUT: updating entry"
        payload = '''
        {
            "clusterConfig": {
              "prod": true,
              "s3MapredSiteXml": "s3://PATH/TO/NEW/MAPRED",
              "s3CoreSiteXml": "s3://PATH/TO/NEW/CORE",
              "s3HdfsSiteXml": "s3://PATH/TO/NEW/HDFS",
              "user": "testuser",
              "prodHiveConfigId": "does_not_exist"
          }
        }
        '''
        params = '/' + id
        try:
            response = restclient.put(serviceUrl=serviceUrl, params=params, payload=payload)
            print response.read()
            assert False, "received unexpected successful response"
        except urllib2.HTTPError, e:
            print e.read()
            assert e.code == 400, "received unexpected unsuccessful response"
    
    def delTest(self):
        print "HTTP DELETE: deleting entry"
        params = '/' + id
        response = restclient.delete(serviceUrl=serviceUrl, params=params)
        print response.code
        assert response.code == 200, "can't delete entry"
    
    def testDelWrongParamsErr(self):
        print "HTTP DELETE: wrong params"
        params = '/does_not_exist'
        try:
            response = restclient.delete(serviceUrl=serviceUrl, params=params)
            print response.read()
            assert False, "received unexpected successful response"
        except urllib2.HTTPError, e:
            print e.read()
            assert e.code == 404, "received unexpected unsuccessful response"

    def testDelNoParamsErr(self):
        print "HTTP DELETE: no params"
        try:
            response = restclient.delete(serviceUrl=serviceUrl)
            print response.read()
            assert False, "received unexpected successful response"
        except urllib2.HTTPError, e:
            print e.read()
            assert e.code == 400, "received unexpected unsuccessful response"
        
# driver method for all tests                
if __name__ == "__main__":
   print "Running unit tests:\n"
   suite = unittest.makeSuite(ClusterConfigTestCase, 'test')
   unittest.installHandler()
   unittest.main()


   
   
