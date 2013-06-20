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

import urllib2

def _send(serviceUrl=None, params=None, payload=None, method='GET', 
           accept='application/json', contentType='application/json'):
    assert serviceUrl is not None, "serviceUrl must not be None"

    # construct the url from the params - assume client will provide /'s or ?'s
    url = serviceUrl
    if params is not None:
        url += params

    req = urllib2.Request(url=url, data=payload)
    req.add_header('Accept', accept)
    req.add_header('Content-Type', contentType)
    
    # construct the req method per request
    if method == 'GET' or method == 'POST':
        assert req.get_method() == method
    else:
        req.get_method = lambda: method
        
    response = urllib2.urlopen(req)
    return response
   
def get(serviceUrl=None, params=None, accept='application/json'):
    return _send(serviceUrl=serviceUrl, params=params, accept=accept, method='GET')

def post(serviceUrl=None, payload=None, accept='application/json', contentType='application/json'):
    return _send(serviceUrl=serviceUrl, payload=payload, 
                 accept=accept, contentType=contentType, method='POST')

def put(serviceUrl=None, params=None, payload=None, accept='application/json', contentType='application/json'):
    return _send(serviceUrl=serviceUrl, params=params, payload=payload, 
                 accept=accept, contentType=contentType, method='PUT')

def delete(serviceUrl=None, params=None, accept='application/json'):
    return _send(serviceUrl=serviceUrl, params=params, accept=accept, method='DELETE')


