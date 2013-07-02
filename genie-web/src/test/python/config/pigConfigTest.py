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
serviceUrl = eureka.EurekaClient().getServiceBaseUrl() + '/genie/v0/config/pig'

# use a global id for all tests
id='unittest_pigconfig_id'
name='unittest_pigconfig_name'
type='TEST'

class PigConfigTestCase(unittest.TestCase):
            
    def setUp(self):
        # create a new one, if needed
        payload = '''
        <request>
          <pigConfig>
            <id>''' + id + '''</id>
            <name>''' + name + '''</name>
            <type>''' + type + '''</type>
            <status>INACTIVE</status>
            <s3PigProperties>s3://BUCKET/PATH/TO/PIG.PROPERTIES</s3PigProperties>
            <user>testuser</user>
          </pigConfig>
        </request>
        '''
        try:
            response = restclient.post(serviceUrl=serviceUrl, payload=payload, contentType='application/xml')
            obj = json.loads(response.read())
            assert obj['pigConfigs']['pigConfig']['createTime'] > 0, "Create time not set correctly for config"
            assert obj['pigConfigs']['pigConfig']['updateTime'] > 0, "Update time not set correctly for config"
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
        print "HTTP GET: missing pigConfig element"
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
          <pigConfig>
            <name>foo</name>
            <type>TEST</type>
            <s3PigProperties>s3://bar</s3PigProperties>
          </pigConfig>
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
          <pigConfig>
            <name>''' + name + '''</name>
            <type>BAD_TYPE</type>
            <s3PigProperties>s3://BUCKET/PATH/TO/HIVE-SITE.XML</s3PigProperties>
            <user>testuser</user>
          </pigConfig>
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
            "pigConfig": {
              "name": "NEW_NAME",
              "s3PigProperties": "s3://NEW/PATH",
              "user": "testuser"
            }
        }
        '''
        params = '/' + id
        response = restclient.put(serviceUrl=serviceUrl, params=params, payload=payload)
        print response.code
        assert response.code == 200, "unable to put config for id" + id
        obj= json.loads(response.read())
        assert obj['pigConfigs']['pigConfig']['updateTime'] > obj['pigConfigs']['pigConfig']['createTime'], \
            "Update time not greater than create time"
    
    def testPutErr(self):
        print "HTTP PUT: missing all params for new entry"
        payload = '''
        {
            "pigConfig": {
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
            "pigConfig": {
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
   suite = unittest.makeSuite(PigConfigTestCase, 'test')
   unittest.installHandler()
   unittest.main()

