__author__ = 'tgianos'

import logging
from genie_client.apis import swagger
from genie_client.apis import V2Api
from genie_client.exception import genie_exception
import urllib2

DEBUG = logging.getLogger(__name__).debug
ERROR = logging.getLogger(__name__).error
INFO = logging.getLogger(__name__).info
WARN = logging.getLogger(__name__).warn
REQUEST_TIMEOUT = 60


class Job(object):
    """Class for dealing with Genie 2 Jobs."""

    def __init__(self, service_base_url):
        DEBUG("Application API initialized with Service Base URL [%s]" % service_base_url)
        self._service_base_url = service_base_url
        self._api_client = swagger.ApiClient("key", self._service_base_url)
        self._api = V2Api.V2Api(self._api_client)

    def find_jobs(self, **kwargs):
        """Get all running jobs for the given cluster"""

        DEBUG("Attempting to get jobs with args [%s]" % kwargs)

        try:
            return self._api.getJobs(**kwargs)
        except urllib2.HTTPError, http_error:
            raise genie_exception.GenieException('Unable to get jobs due to error code: [ '
                                                 + str(http_error.code)
                                                 + '] because ['
                                                 + http_error.read()
                                                 + ']', http_error.code)
        except urllib2.URLError, url_error:
            raise genie_exception.GenieException('Unable to get job. ' + url_error.reason)

    def get_job(self, job_id):
        """Get the job for the given id"""

        DEBUG("Attempting to get job with id [%s]" % job_id)

        try:
            return self._api.getJob(job_id)
        except urllib2.HTTPError, http_error:
            raise genie_exception.GenieException('Unable to get job due to error code: [ '
                                                 + str(http_error.code)
                                                 + '] because ['
                                                 + http_error.read()
                                                 + ']', http_error.code)
        except urllib2.URLError, url_error:
            raise genie_exception.GenieException('Unable to get job. ' + url_error.reason)

    def submit_job(self, job_obj):
        """Submit a job to genie"""

        DEBUG("Attempting to submit job with obj [%s]" % job_obj)

        try:
            return self._api.submitJob(job_obj)
        except urllib2.HTTPError, http_error:
            raise genie_exception.GenieException('Unable to submit job due to error code: [ '
                                                 + str(http_error.code)
                                                 + '] because ['
                                                 + http_error.read()
                                                 + ']', http_error.code)
        except urllib2.URLError, url_error:
            raise genie_exception.GenieException('Unable to get job. ' + url_error.reason)
