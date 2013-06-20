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

# get the serviceUrl from the eureka client
serviceUrl = eureka.EurekaClient().getServiceBaseUrl() + '/genie/v0/config/hive'

# use a global id for all tests
id='unittest_hiveconfig_id'
name='unittest_hiveconfig_name'
type='TEST'

class HiveConfigTestCase(unittest.TestCase):
            
    def setUp(self):
        # create a new one, if needed
        payload = '''
        <request>
          <hiveConfig>
            <id>''' + id + '''</id>
            <name>''' + name + '''</name>
            <type>''' + type + '''</type>
            <status>INACTIVE</status>
            <s3HiveSiteXml>s3://BUCKET/PATH/TO/HIVE-SITE.XML</s3HiveSiteXml>
            <user>testuser</user>
          </hiveConfig>
        </request>
        '''
        try:
            response = restclient.post(serviceUrl=serviceUrl, payload=payload, contentType='application/xml')
            obj = json.loads(response.read())
            assert obj['hiveConfigs']['hiveConfig']['createTime'] > 0, "Create time not set correctly for config"
            assert obj['hiveConfigs']['hiveConfig']['updateTime'] > 0, "Update time not set correctly for config"
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

    def testGetByName(self):
        print "HTTP GET: getting elements by name"        
        params = '?name=' + name
        response = restclient.get(serviceUrl=serviceUrl, params=params)
        print response.code
        assert response.code == 200, "can't get element by name: " + name

    def testGetByType(self):
        print "HTTP GET: getting elements by type"       
        params = '?type=' + type
        response = restclient.get(serviceUrl=serviceUrl, params=params)
        print response.code
        assert response.code == 200, "can't get element by type: " + type
        
    def testGetErr(self):
        print "HTTP GET: wrong params - no such id exists"
        params = '?id=does_not_exist'
        try:
            response = restclient.get(serviceUrl=serviceUrl, params=params)
            print response.read()
            assert False, "received unexpected successful response"
        except urllib2.HTTPError, e:
            print e.read()
            assert e.code == 404, "received unexpected unsuccessful response"
    
    def testPostMissingElement(self):
        print "HTTP GET: missing hiveConfig element"
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
        print "HTTP GET: missing user"
        payload = '''
        <request>
          <hiveConfig>
            <name>foo</name>
            <type>TEST</type>
            <s3HiveSiteXml>s3://bar</s3HiveSiteXml>
          </hiveConfig>
        </request>
        '''
        try:
            response = restclient.post(serviceUrl=serviceUrl, payload=payload, contentType='application/xml')
            print response.read()
            assert False, "received unexpected successful response"
        except urllib2.HTTPError, e:
            print e.read()
            assert e.code == 400, "received unexpected unsuccessful response"

    def testPostBadType(self):    
        print "HTTP GET: bad type"
        payload = '''
        <request>
          <hiveConfig>
            <name>''' + name + '''</name>
            <type>BAD_TYPE</type>
            <s3HiveSiteXml>s3://BUCKET/PATH/TO/HIVE-SITE.XML</s3HiveSiteXml>
            <user>testuser</user>
          </hiveConfig>
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
        print "HTTP PUT: changing fields"
        payload = '''
        {
            "hiveConfig": {
              "name": "NEW_NAME",
              "s3HiveSiteXml": "s3://NEW/PATH",
              "user": "testuser"
            }
        }
        '''
        params = '/' + id
        response = restclient.put(serviceUrl=serviceUrl, params=params, payload=payload)
        print response.code
        assert response.code == 200, "unable to put config for id" + id
        obj= json.loads(response.read())
        assert obj['hiveConfigs']['hiveConfig']['updateTime'] > obj['hiveConfigs']['hiveConfig']['createTime'], \
            "Update time not greater than create time"
                
    def testPutErr(self):
        print "HTTP PUT: missing all params for new entry"
        payload = '''
        {
            "hiveConfig": {
              "name": "prod",
              "type": "TEST",
              "user": "testuser"
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

    def testPutBadStatus(self):
        print "HTTP PUT: bad status"
        payload = '''
        {
            "hiveConfig": {
              "status": "BAD_STATUS",
              "user": "testuser"
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
        
    def testDel(self):
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
   suite = unittest.makeSuite(HiveConfigTestCase, 'test')
   unittest.main()
