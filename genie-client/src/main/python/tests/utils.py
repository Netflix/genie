from __future__ import absolute_import, division, print_function, unicode_literals


import json
import requests


def fake_response(content, status_code=200):
    response = requests.Response()
    response.status_code = status_code
    response._content = json.dumps(content)
    return response
