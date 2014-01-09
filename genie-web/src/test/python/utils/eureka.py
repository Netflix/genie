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
import os
import json
import urllib2
import random

class EurekaClient(object):

    # Users can either set the EUREKA_URL environment variable
    # or pass it as a parameter (which takes precedence)
    def __init__(self, eurekaUrl=None):
        self._eurekaUrl = eurekaUrl
        if self._eurekaUrl is None or len(self._eurekaUrl) == 0:
            self._eurekaUrl = os.getenv('EUREKA_URL')

    # If appName is not set, then use NETFLIX_APP env, and finally default to 'genie'
    def _getInstances(self, appName=None, status='UP'):
        # if appName is not set, try to infer it from the environment - else default to 'genie'
        if appName is None or len(appName) == 0:
            appName = os.getenv("NETFLIX_APP")
        if appName is None or len(appName) == 0:
            appName = 'genie'
        
        # ensure that we can find Eureka
        if self._eurekaUrl is None or len(self._eurekaUrl) == 0:
            raise RuntimeError("EUREKA_URL is not provided via env or constructor")
        
        # get the response from Eureka
        restUrl = self._eurekaUrl + '/' + appName     
        req = urllib2.Request(url=restUrl)
        req.add_header('Accept', 'application/json')
        assert req.get_method() == 'GET'
        response = urllib2.urlopen(req)
    
        # parse the json response
        json_str = response.read()
        json_obj = json.loads(json_str)
    
        # get a list of all instances
        instances = json_obj['application']['instance']
    
        # check to see if this is a list or a singleton
        isList = isinstance(instances, list)
        
        # filter out the instances as specified by status
        instancesList = []
        if (isList):
            for i in instances:
                if i['status'] == status:
                    instancesList.append(i)
        else:
            # singleton instance
            if instances['status'] == status:
                instancesList.append(instances)
        
        # ensure that we have at least 1 instance that is UP
        assert len(instancesList) > 0, \
            "No " + appName + " instances found that were " + status + " on " + self._eurekaUrl
        
        # return each one
        return instancesList
    
    # If the SERVICE_BASE_URL environment variable is set, return it - e.g. http://localhost:7001
    # Else use Eureka to find an instance that is UP   
    def getServiceBaseUrl(self, appName=None):
        service_url = os.getenv('SERVICE_BASE_URL')
        if service_url is not None and len(service_url) != 0:
            print "Returning SERVICE_BASE_URL provided by environment variable:", service_url
            print
            return service_url
        else:
            print "Getting UP instance from Eureka: " + self._eurekaUrl
            print
        
        instancesUp = self._getInstances(appName, status='UP')
 
        # pick a random one
        instance = instancesUp[random.randrange(0, len(instancesUp), 1)]
            
        service_url = 'http://' + instance['hostName'] + ':' + instance['port']['$']
        return service_url

    # Use Eureka to find instances that are OUT_OF_SERVICE
    def getOOSInstances(self, appName=None):
        return self._getInstances(appName, status='OUT_OF_SERVICE')

    # Use Eureka to find instances that are UP
    def getUPInstances(self, appName=None):
        return self._getInstances(appName, status='UP')

if __name__ == "__main__":
    
    client = EurekaClient()
    
    print "Getting base URL for Genie Service from Eureka:"
    print client.getServiceBaseUrl()
    print
    
    print "Getting list of all Genie OOS instances"
    print client.getOOSInstances()
