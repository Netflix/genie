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


def addCommandToCluster(clusterId,commandId):
    print "Adding Command [%s] to Cluster [%s] " % (clusterId, commandId) 

    # get the serviceUrl from the eureka client
    serviceUrl = eureka.EurekaClient().getServiceBaseUrl() + '/genie/v1/config/clusters/' + clusterId + '/commands'
    print "Service Url isi %s" % serviceUrl 
    
    #cmds = json.dumps([{'id':'prodhive11_mr1'},{'id':'pig11_mr1'},{'id':'hadoop103'}])
    cmds = json.dumps([{'id':commandId}])
    payload = cmds 
    print payload
    print "\n"
    
    print restclient.post(serviceUrl=serviceUrl, payload=payload, contentType='application/json')

# driver method for all tests                
if __name__ == "__main__":
    if(len(sys.argv) != 3):
        print "Usage: addCommandToCluster clusterId commandId"
        sys.exit(-1)

    addCommandToCluster(sys.argv[1],sys.argv[2])
