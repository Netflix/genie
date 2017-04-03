from __future__ import absolute_import, division, print_function, unicode_literals


import json
import requests
import uuid


def fake_response(content, status_code=200, method='GET'):
    response = requests.Response()
    response.request = requests.Request()

    response.status_code = status_code
    response._content = json.dumps(content)
    response.request.method = method

    return response


class FakeRunningJob(object):
    def __init__(self, job_id=None, status='RUNNING', **kwargs):
        self.job_id = job_id or str(uuid.uuid1())
        self.status = status.upper()
        self.kwargs = kwargs

    def __getattr__(self, attr):
        return self.kwargs.get(attr)

    @property
    def is_done(self):
        return self.status != 'RUNNING'

    @property
    def is_successful(self):
        return self.status == 'SUCCEEDED'
