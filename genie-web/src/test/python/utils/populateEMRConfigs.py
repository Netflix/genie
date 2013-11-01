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

def populateProdHive():
    serviceUrl = baseUrl + 'v0/config/hive/hiveconf-prodhive-emr'
    payload = '''
    <request>
      <hiveConfig>
        <name>prodhive</name>
        <type>PROD</type>
        <s3HiveSiteXml>file:///home/hadoop/.versions/hive-0.11.0/conf/hive-site.xml</s3HiveSiteXml>
        <user>produser</user>
        <status>ACTIVE</status>
        <hiveVersion>0.11</hiveVersion>
      </hiveConfig>
    </request>
    '''
    restclient.put(serviceUrl=serviceUrl, payload=payload, contentType='application/xml')

def populateProdPig():
    serviceUrl = baseUrl + 'v0/config/pig/pigconf-prodpig-emr'
    payload = '''
    <request>
      <pigConfig>
        <name>prodpig</name>
        <type>PROD</type>
        <s3PigProperties>file:///home/hadoop/.versions/pig-0.11.1/conf/pig.properties</s3PigProperties>
        <user>produser</user>
        <status>ACTIVE</status>
        <pigVersion>0.11</pigVersion>
      </pigConfig>
    </request>
    '''
    restclient.put(serviceUrl=serviceUrl, payload=payload, contentType='application/xml')
     
def populateCluster():
    serviceUrl = baseUrl + 'v0/config/cluster/clusterconf-emr'
    payload = '''
    <request>
      <clusterConfig>
        <name>clusterconf-emr</name>
        <prod>true</prod>
        <adHoc>true</adHoc>
        <s3MapredSiteXml>file:///home/hadoop/.versions/1.0.3/conf/mapred-site.xml</s3MapredSiteXml>
        <s3CoreSiteXml>file:///home/hadoop/.versions/1.0.3/conf/core-site.xml</s3CoreSiteXml>
        <s3HdfsSiteXml>file:///home/hadoop/.versions/1.0.3/conf/hdfs-site.xml</s3HdfsSiteXml>
        <user>produser</user>
        <prodHiveConfigId>hiveconf-prodhive-emr</prodHiveConfigId>
        <prodPigConfigId>pigconf-prodpig-emr</prodPigConfigId>
        <hadoopVersion>1.0.3</hadoopVersion>
        <status>UP</status>
      </clusterConfig>
    </request>
    '''
    restclient.put(serviceUrl=serviceUrl, payload=payload, contentType='application/xml')
       
# driver method for populating configs on master node of EMR   
if __name__ == "__main__":

   print "Registering EMR cluster\n"    
   print "################################"
   print "Adding config for prodhive:\n"
   populateProdHive()
   print "################################"
   print "Adding config for prodpig:\n"
   populateProdPig()
   print "################################"
   print "Adding config for cluster:\n"
   populateCluster()
   print "################################"
   print "\nDone"
   
