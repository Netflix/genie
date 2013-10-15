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
import json
import urllib2
import restclient
import eureka

baseUrl = eureka.EurekaClient().getServiceBaseUrl() + '/genie/'

def populateTestHive():
    serviceUrl = baseUrl + 'v0/config/hive/hiveconf-testhive-sample'
    payload = '''
    <request>
      <hiveConfig>
        <name>testhive</name>
        <type>TEST</type>
        <s3HiveSiteXml>file:///PATH/TO/TEST-HIVE-SITE.XML</s3HiveSiteXml>
        <user>testuser</user>
        <status>ACTIVE</status>
        <hiveVersion>0.11</hiveVersion>
      </hiveConfig>
    </request>
    '''
    restclient.put(serviceUrl=serviceUrl, payload=payload, contentType='application/xml')
        
def populateProdHive():
    serviceUrl = baseUrl + 'v0/config/hive/hiveconf-prodhive-sample'
    payload = '''
    <request>
      <hiveConfig>
        <name>prodhive</name>
        <type>PROD</type>
        <s3HiveSiteXml>file:///PATH/TO/PROD-HIVE-SITE.XML</s3HiveSiteXml>
        <user>testuser</user>
        <status>ACTIVE</status>
        <hiveVersion>0.11</hiveVersion>
      </hiveConfig>
    </request>
    '''
    restclient.put(serviceUrl=serviceUrl, payload=payload, contentType='application/xml')

def populateTestPig():
    serviceUrl = baseUrl + 'v0/config/pig/pigconf-testpig-sample'
    payload = '''
    <request>
      <pigConfig>
        <name>testpig</name>
        <type>TEST</type>
        <s3PigProperties>file:///PATH/TO/TEST-PIG.PROPERTIES</s3PigProperties>
        <user>testuser</user>
        <status>ACTIVE</status>
        <pigVersion>0.11</pigVersion>
      </pigConfig>
    </request>
    '''
    restclient.put(serviceUrl=serviceUrl, payload=payload, contentType='application/xml')
     
def populateProdPig():
    serviceUrl = baseUrl + 'v0/config/pig/pigconf-prodpig-sample'
    payload = '''
    <request>
      <pigConfig>
        <name>prodpig</name>
        <type>PROD</type>
        <s3PigProperties>file:///PATH/TO/PROD-PIG.PROPERTIES</s3PigProperties>
        <user>testuser</user>
        <status>ACTIVE</status>
        <pigVersion>0.11</pigVersion>
      </pigConfig>
    </request>
    '''
    restclient.put(serviceUrl=serviceUrl, payload=payload, contentType='application/xml')
     
def populateCluster():
    serviceUrl = baseUrl + 'v0/config/cluster/clusterconf-sample'
    payload = '''
    <request>
      <clusterConfig>
        <name>clusterconf-sample</name>
        <test>true</test>
        <prod>true</prod>
        <adHoc>true</adHoc>
        <bonus>true</bonus>
        <sla>true</sla>
        <s3MapredSiteXml>file:///PATH/TO/MAPRED-SITE.XML</s3MapredSiteXml>
        <s3CoreSiteXml>file:///PATH/TO/CORE-SITE.XML</s3CoreSiteXml>
        <s3HdfsSiteXml>file:///PATH/TO/HDFS-SITE.XML</s3HdfsSiteXml>
        <user>testuser</user>
        <testHiveConfigId>hiveconf-testhive-sample</testHiveConfigId>
        <prodHiveConfigId>hiveconf-prodhive-sample</prodHiveConfigId>
        <testPigConfigId>pigconf-testpig-sample</testPigConfigId>
        <prodPigConfigId>pigconf-prodpig-sample</prodPigConfigId>
        <hadoopVersion>1.0.3</hadoopVersion>
        <status>UP</status>
        <hasStats>false</hasStats>
        <jobFlowId>j-18EQGLSSWPOMG</jobFlowId>
        <gatewayJobFlowIds>j-31E6N85M87U3U</gatewayJobFlowIds>
      </clusterConfig>
    </request>
    '''
    restclient.put(serviceUrl=serviceUrl, payload=payload, contentType='application/xml')
       
# driver method for populating configs     
if __name__ == "__main__":
    
   print "################################"
   print "Adding config for testhive:\n"
   populateTestHive()
   print "################################"
   print "Adding config for prodhive:\n"
   populateProdHive()
   print "################################"
   print "Adding config for testpig:\n"
   populateTestPig()
   print "################################"
   print "Adding config for prodpig:\n"
   populateProdPig()
   print "################################"
   print "Adding config for cluster:\n"
   populateCluster()
   print "################################"
   
   
