__author__ = 'amsharma'

import logging
import swagger
import V2Api
import genie_exception
import urllib2

DEBUG = logging.getLogger(__name__).debug
ERROR = logging.getLogger(__name__).error
INFO = logging.getLogger(__name__).info
WARN = logging.getLogger(__name__).warn
REQUEST_TIMEOUT = 60


class Application(object):
    """Class for dealing with Genie 2 Applications."""

    def __init__(self, service_base_url):
        DEBUG("Application API initialized with Service Base URL [%s]" % service_base_url)
        self._service_base_url = service_base_url
        self._api_client = swagger.ApiClient("key", self._service_base_url)
        self._api = V2Api.V2Api(self._api_client)

    def create_application(self, app_obj):
        DEBUG("Attempting to create application with obj [%s]" % app_obj)
        try:
            self._api.createApplication(app_obj)
        except urllib2.HTTPError, http_error:
            raise genie_exception.GenieException('Unable to create application due to error code: [ '
                                                 + str(http_error.code)
                                                 + '] because ['
                                                 + http_error.read()
                                                 + ']', http_error.code)
        except urllib2.URLError, url_error:
            raise genie_exception.GenieException('Unable to create application. ' + url_error.reason)

    def update_application(self, app_id, app_obj):
        DEBUG("Attempting to update application with id [%s] and obj [%s]" % (app_id, app_obj))
        try:
            self._api.updateApplication(app_id, app_obj)
        except urllib2.HTTPError, http_error:
            raise genie_exception.GenieException('Unable to update application due to error code: [ '
                                                 + str(http_error.code)
                                                 + '] because ['
                                                 + http_error.read()
                                                 + ']', http_error.code)
        except urllib2.URLError, url_error:
            raise genie_exception.GenieException('Unable to update application. ' + url_error.reason)

    def delete_application(self, app_id):
        DEBUG("Attempting to delete application with id [%s]" % app_id)
        try:
            self._api.deleteApplication(app_id)
        except urllib2.HTTPError, http_error:
            raise genie_exception.GenieException('Unable to delete application due to error code: [ '
                                                 + str(http_error.code)
                                                 + '] because ['
                                                 + http_error.read()
                                                 + ']', http_error.code)
        except urllib2.URLError, url_error:
            raise genie_exception.GenieException('Unable to delete application. ' + url_error.reason)

    def get_application(self, app_id):
        DEBUG("Attempting to get application with id [%s]" % app_id)
        try:
            return self._api.getApplication(app_id)
        except urllib2.HTTPError, http_error:
            raise genie_exception.GenieException('Unable to get application due to error code: [ '
                                                 + str(http_error.code)
                                                 + '] because ['
                                                 + http_error.read()
                                                 + ']', http_error.code)
        except urllib2.URLError, url_error:
            raise genie_exception.GenieException('Unable to get application. ' + url_error.reason)

    def get_applications(self):
        DEBUG("Attempting to get all applications from genie")
        try:
            return self._api.getApplications()
        except urllib2.HTTPError, http_error:
            raise genie_exception.GenieException('Unable to get all applications due to error code: [ '
                                                 + str(http_error.code)
                                                 + '] because ['
                                                 + http_error.read()
                                                 + ']', http_error.code)
        except urllib2.URLError, url_error:
            raise genie_exception.GenieException('Unable to get all applications. ' + url_error.reason)
