__author__ = 'amsharma'

import logging
import collections
import urllib2
import genie2.client.api_client
import genie2.client.V2Api
import genie2.exception.genie_exception

DEBUG = logging.getLogger(__name__).debug
ERROR = logging.getLogger(__name__).error
INFO = logging.getLogger(__name__).info
WARN = logging.getLogger(__name__).warn
REQUEST_TIMEOUT = 60


class Cluster(object):
    """Class for dealing with Genie 2 Clusters."""

    def __init__(self, service_base_url):
        DEBUG("Cluster API initialized with Service Base URL [%s]" % service_base_url)
        self._service_base_url = service_base_url
        self._api_client = genie2.client.api_client.ApiClient(api_server=self._service_base_url)
        self._api = genie2.client.V2Api.V2Api(self._api_client)

    def create_cluster(self, cstr_obj):
        DEBUG("Attempting to create cluster with obj [%s]" % cstr_obj)
        try:
            return self._api.createCluster(cstr_obj)
        except urllib2.HTTPError, http_error:
            raise genie2.exception.genie_exception.GenieException(
                'Unable to create cluster due to error code: ['
                + str(http_error.code)
                + '] because ['
                + http_error.read()
                + ']', http_error.code)
        except urllib2.URLError, url_error:
            raise genie2.exception.genie_exception.GenieException(
                'Unable to create cluster. ' + url_error.reason)

    def update_cluster(self, cstr_id, cstr_obj):
        DEBUG("Attempting to update cluster with id [%s] and obj [%s]" % (cstr_id, cstr_obj))
        try:
            return self._api.updateCluster(cstr_id, cstr_obj)
        except urllib2.HTTPError, http_error:
            raise genie2.exception.genie_exception.GenieException(
                'Unable to update cluster due to error code: ['
                + str(http_error.code)
                + '] because ['
                + http_error.read()
                + ']', http_error.code)
        except urllib2.URLError, url_error:
            raise genie2.exception.genie_exception.GenieException(
                'Unable to update cluster. ' + url_error.reason)

    def delete_cluster(self, cstr_id):
        DEBUG("Attempting to delete cluster with id [%s]" % cstr_id)
        try:
            self._api.deleteCluster(cstr_id)
        except urllib2.HTTPError, http_error:
            raise genie2.exception.genie_exception.GenieException(
                'Unable to delete cluster due to error code: ['
                + str(http_error.code)
                + '] because ['
                + http_error.read()
                + ']', http_error.code)
        except urllib2.URLError, url_error:
            raise genie2.exception.genie_exception.GenieException(
                'Unable to delete cluster. ' + url_error.reason)

    def get_cluster(self, cstr_id):
        DEBUG("Attempting to get cluster with id [%s]" % cstr_id)
        try:
            return self._api.getCluster(cstr_id)
        except urllib2.HTTPError, http_error:
            raise genie2.exception.genie_exception.GenieException(
                'Unable to get cluster due to error code: ['
                + str(http_error.code)
                + '] because ['
                + http_error.read()
                + ']', http_error.code)
        except urllib2.URLError, url_error:
            raise genie2.exception.genie_exception.GenieException(
                'Unable to get cluster. ' + url_error.reason)

    def get_clusters(self, **params):
        DEBUG("Attempting to get all clusters from genie")
        try:
            return self._api.getClusters(**params)
        except urllib2.HTTPError, http_error:
            raise genie2.exception.genie_exception.GenieException(
                'Unable to get all clusters due to error code: ['
                + str(http_error.code)
                + '] because ['
                + http_error.read()
                + ']', http_error.code)
        except urllib2.URLError, url_error:
            raise genie2.exception.genie_exception.GenieException(
                'Unable to get all clusters. ' + url_error.reason)

    def cluster_is_configured(self, cluster_id):
        """Return true if cluster is configured in genie."""

        try:
            self._api.getCluster(cluster_id)
            return True
        except urllib2.HTTPError, http_error:
            if http_error.code == 404:
                return False
            else:
                raise genie2.exception.genie_exception.GenieException(http_error.read())
        except urllib2.URLError, url_error:
            raise genie2.exception.genie_exception.GenieException(url_error.reason)

    def add_commands_to_cluster(self, cluster_id, command_ids):
        """Add commands to a cluster"""

        try:
            if command_ids is None \
                    or not isinstance(command_ids, collections.Iterable) \
                    or not command_ids:
                raise genie2.exception.genie_exception.GenieException("Command ids is not valid. Unable to continue.")
            if not cluster_id:
                raise genie2.exception.genie_exception.GenieException("No cluster id entered. Unable to continue.")

            command_ids_set = set()
            commands = list()
            for command_id in command_ids:
                if command_id in command_ids_set:
                    raise genie2.exception.genie_exception.GenieException(command_id + "Entered twice. Must be unique.")
                else:
                    command_ids_set.add(command_id)
                commands.append(self._api.getCommand(command_id))
            return self._api.addCommandsForCluster(cluster_id, commands)
        except urllib2.HTTPError, http_error:
            raise genie2.exception.genie_exception.GenieException(http_error.read(), http_error.code)
        except urllib2.URLError, url_error:
            raise genie2.exception.genie_exception.GenieException(url_error.reason)

    def add_tags_to_cluster(self, cluster_id, tags):
        """Add commands to a cluster"""

        try:
            if tags is None \
                    or not isinstance(tags, collections.Iterable) \
                    or not tags:
                raise genie2.exception.genie_exception.GenieException("Tag is not a valid list. Unable to continue.")
            if not cluster_id:
                raise genie2.exception.genie_exception.GenieException("No cluster id entered. Unable to continue.")

            return self._api.addTagsForCluster(cluster_id, tags)
        except urllib2.HTTPError, http_error:
            raise genie2.exception.genie_exception.GenieException(http_error.read(), http_error.code)
        except urllib2.URLError, url_error:
            raise genie2.exception.genie_exception.GenieException(url_error.reason)

    def remove_tag_from_cluster(self, cluster_id, tag):
        """Add commands to a cluster"""

        try:
            if tag is None:
                raise genie2.exception.genie_exception.GenieException("Tag cannot be None. Unable to continue.")
            if not cluster_id:
                raise genie2.exception.genie_exception.GenieException("No cluster id entered. Unable to continue.")

            return self._api.removeTagForCluster(cluster_id, tag)
        except urllib2.HTTPError, http_error:
            raise genie2.exception.genie_exception.GenieException(http_error.read(), http_error.code)
        except urllib2.URLError, url_error:
            raise genie2.exception.genie_exception.GenieException(url_error.reason)

    def remove_tags_from_cluster(self, cluster_id, tags):
        """Remove tags for cluster"""
        if tags is None \
                or not isinstance(tags, collections.Iterable) \
                or not tags:
            raise genie2.exception.genie_exception.GenieException("Tag is not a valid list. Unable to continue.")
        if not cluster_id:
            raise genie2.exception.genie_exception.GenieException("No cluster id entered. Unable to continue.")

        for tag in tags:
            self.remove_tag_from_cluster(cluster_id, tag)
