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

import urllib2


def _send(service_url=None, params=None, payload=None, method='GET',
          accept='application/json', content_type='application/json'):
    assert service_url is not None, "serviceUrl must not be None"

    # construct the url from the params - assume client will provide /'s or ?'s
    url = service_url
    if params is not None:
        url += params

    req = urllib2.Request(url=url, data=payload)
    req.add_header('Accept', accept)
    req.add_header('Content-Type', content_type)

    # construct the req method per request
    if method == 'GET' or method == 'POST':
        assert req.get_method() == method
    else:
        req.get_method = lambda: method

    response = urllib2.urlopen(req)
    return response


def get(service_url=None, params=None, accept='application/json'):
    return _send(service_url=service_url, params=params, accept=accept, method='GET')


def post(service_url=None, payload=None, accept='application/json', content_type='application/json'):
    return _send(service_url=service_url, payload=payload,
                 accept=accept, content_type=content_type, method='POST')


def put(service_url=None, params=None, payload=None, accept='application/json', content_type='application/json'):
    return _send(service_url=service_url, params=params, payload=payload,
                 accept=accept, content_type=content_type, method='PUT')


def delete(service_url=None, params=None, accept='application/json'):
    return _send(service_url=service_url, params=params, accept=accept, method='DELETE')
