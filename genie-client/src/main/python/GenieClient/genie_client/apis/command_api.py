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


class Command(object):
    """Class for dealing with Genie 2 Commands."""

    def __init__(self, service_base_url):
        DEBUG("Command API initialized with Service Base URL [%s]" % service_base_url)
        self._service_base_url = service_base_url
        self._api_client = swagger.ApiClient("key", self._service_base_url)
        self._api = V2Api.V2Api(self._api_client)

    def create_command(self, cmd_obj):
        DEBUG("Attempting to create command with obj [%s]" % cmd_obj)
        try:
            self._api.createCommand(cmd_obj)
        except urllib2.HTTPError, http_error:
            raise genie_exception.GenieException('Unable to create command due to error code: [ '
                                                 + str(http_error.code)
                                                 + '] because ['
                                                 + http_error.read()
                                                 + ']', http_error.code)
        except urllib2.URLError, url_error:
            raise genie_exception.GenieException('Unable to create command. ' + url_error.reason)

    def update_command(self, cmd_id, cmd_obj):
        DEBUG("Attempting to update command with id [%s] and obj [%s]" % (cmd_id, cmd_obj))
        try:
            self._api.updateCommand(cmd_id, cmd_obj)
        except urllib2.HTTPError, http_error:
            raise genie_exception.GenieException('Unable to update command due to error code: [ '
                                                 + str(http_error.code)
                                                 + '] because ['
                                                 + http_error.read()
                                                 + ']', http_error.code)
        except urllib2.URLError, url_error:
            raise genie_exception.GenieException('Unable to update command. ' + url_error.reason)

    def delete_command(self, cmd_id):
        DEBUG("Attempting to delete command with id [%s]" % cmd_id)
        try:
            self._api.deleteCommand(cmd_id)
        except urllib2.HTTPError, http_error:
            raise genie_exception.GenieException('Unable to delete command due to error code: [ '
                                                 + str(http_error.code)
                                                 + '] because ['
                                                 + http_error.read()
                                                 + ']', http_error.code)
        except urllib2.URLError, url_error:
            raise genie_exception.GenieException('Unable to delete command. ' + url_error.reason)

    def get_command(self, cmd_id):
        DEBUG("Attempting to get command with id [%s]" % cmd_id)
        try:
            return self._api.getCommand(cmd_id)
        except urllib2.HTTPError, http_error:
            raise genie_exception.GenieException('Unable to get command due to error code: [ '
                                                 + str(http_error.code)
                                                 + '] because ['
                                                 + http_error.read()
                                                 + ']', http_error.code)
        except urllib2.URLError, url_error:
            raise genie_exception.GenieException('Unable to get command. ' + url_error.reason)

    def get_commands(self):
        DEBUG("Attempting to get all commands from genie")
        try:
            return self._api.getCommands()
        except urllib2.HTTPError, http_error:
            raise genie_exception.GenieException('Unable to get all commands due to error code: [ '
                                                 + str(http_error.code)
                                                 + '] because ['
                                                 + http_error.read()
                                                 + ']', http_error.code)
        except urllib2.URLError, url_error:
            raise genie_exception.GenieException('Unable to get all commands. ' + url_error.reason)
