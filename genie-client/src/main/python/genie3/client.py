"Genie python client library"

from __future__ import absolute_import, print_function, unicode_literals

import json
import logging

from genie3 import utils
from .exceptions import GenieHTTPError, GenieError

def _call(url, raise_not_status=None, *args, **kwargs):
    """
    Wrap HTTP request _calls to the Genie server.

    Args:
        raise_not_status: (int) if this status code is not returned by genie,
            then raise GenieHTTPError.
    """

    if kwargs.get('data'):
        kwargs['data'] = json.dumps(kwargs['data'])

    header = {'Content-Type': 'application/json'}

    resp = utils.call(url, headers=header, *args, **kwargs)

    if raise_not_status and resp.status_code != raise_not_status:
        logging.critical("Genie returned a bad status code. Expected %s. Got %s",
                         raise_not_status, resp.status_code)
        raise GenieHTTPError(resp)

    response = {}
    content = resp.content or '{}'
    content = json.loads(content)

    # Genie nests the responses differently depending on whether the type is a
    # list, dict, or string
    if isinstance(content, list):
        response['response'] = content
    elif content.get('_embedded'):
        response['response'] = content['_embedded']
    else:
        response['response'] = content

    response['headers'] = resp.headers

    return response

def _check_type(var, _type):
    """
    Genie returns generally unuseful type errors from the rest api. This
    function wraps an "isinstance() -> assert" call with a more helpful
    message.
    """
    if not isinstance(var, _type):
        raise GenieError('Invalid type. Expected %s but got %s: %s', _type,
                         type(var), var)

def _verify_files(filters, supported):
    """
    Helper function to verify user-supplied filter arguments. Raises a
    GenieError exception if the type is not supported
    """
    for _filter in filters.keys():
        if _filter not in supported:
            raise GenieError('Filter "%s" not supported. Supported: %s'
                             % (_filter, supported))


class Genie(object):
    "Genie client object"


    def __init__(self, host='http://127.0.0.1:8080'):
        self.host = host
        self.path_application = self.host + '/api/v3/applications'
        self.path_cluster = self.host + '/api/v3/clusters'
        self.path_command = self.host + '/api/v3/commands'
        self.path_job = self.host + '/api/v3/jobs'

    def get_applications(self, filters=None):
        "Get a list of applications."
        filters = filters or {}
        _verify_files(filters, ['name', 'user', 'status', 'tag', 'type', 'page'])
        resp = _call(self.path_application, params=filters)

        for app in resp['response'].get('applicationList', []):
            yield app

    def get_application(self, application):
        "Get an application."
        path = self.path_application + '/' + application
        resp = _call(path)

        return resp['response']

    def create_application(self, application):
        "Create an application."
        method = 'POST'
        resp = _call(self.path_application, data=application,
                                  method=method, raise_not_status=201)

        return resp['headers']['Location'].split('/')[-1]

    def update_application(self, application):
        "Update an existing application."
        method = 'PUT'
        path = self.path_application + '/' + application['id']
        _call(path, data=application, method=method,
                                  raise_not_status=204)

    def delete_application(self, application_id):
        "Delete an application."
        method = 'DELETE'
        path = self.path_application + '/' + application_id
        _call(path, method=method, raise_not_status=204)

    def delete_all_applications(self):
        "Delete all applications. Use with care."
        method = 'DELETE'
        _call(self.path_application, method=method)

    def get_application_commands(self, application_id):
        "Get a single application"

        path = self.path_application + '/' + application_id + '/commands'
        resp = _call(path)

        return resp['response']

    def remove_all_application_configs(self, application_id):
        "Remove all configs from an application"
        path = self.path_application + '/' + application_id + '/configs'
        method = 'DELETE'
        _call(path, method=method, raise_not_status=204)

    def get_application_configs(self, application_id):
        "Get configs for an application."
        path = self.path_application + '/' + application_id + '/configs'
        resp = _call(path)

        return resp['response']

    def add_application_configs(self, application_id, configs):
        "Add configs to an application."
        _check_type(configs, list)
        method = 'POST'
        path = self.path_application + '/' + application_id + '/configs'
        _call(path, method=method, data=configs, raise_not_status=204)

    def update_application_configs(self, application_id, configs):
        "Update configs for an application"
        _check_type(configs, list)
        method = 'PUT'
        path = self.path_application + '/' + application_id + '/configs'
        _call(path, method=method, data=configs, raise_not_status=204)

    def remove_all_application_dependencies(self, application_id):
        "Remove all dependencies in an application."
        method = 'DELETE'
        path = self.path_application + '/' + application_id + '/dependencies'
        _call(path, method=method, raise_not_status=204)

    def get_application_dependencies(self, application_id):
        "Get dependencies for an application"
        path = self.path_application + '/' + application_id + '/dependencies'
        resp = _call(path)

        return resp['response']

    def add_application_depencies(self, application_id, dependencies):
        "Add dependencies to an existing application"
        _check_type(dependencies, list)
        path = self.path_application + '/' + application_id + '/dependencies'
        method = 'POST'
        _call(path, method=method, raise_not_status=204)

    def update_application_depencies(self, application_id, dependenices):
        "Update dependencies for an application"
        _check_type(dependenices, list)
        path = self.path_application + '/' + application_id + '/dependencies'
        method = 'PUT'
        _call(path, method=method, raise_not_status=204)


    def remove_all_application_tags(self, application_id):
        "Remove all tags for an application"
        path = self.path_application + '/' + application_id + '/tags'
        method = 'DELETE'
        _call(path, method=method, raise_not_status=204)

    def get_application_tags(self, application_id):
        "Get tags for an application"
        path = self.path_application + '/' + application_id + '/tags'
        resp = _call(path)

        return resp['response']

    def add_application_tags(self, application_id, tags):
        "Add tags for an application"
        _check_type(tags, list)
        path = self.path_application + '/' + application_id + '/tags'
        method = 'POST'
        _call(path, data=tags, method=method, raise_not_status=204)

    def update_application_tags(self, application_id, tags):
        "Update tags for an application"
        _check_type(tags, list)
        path = self.path_application + '/' + application_id + '/tags'
        method = 'PUT'
        _call(path, method=method, data=tags, raise_not_status=204)

    def remove_application_tags(self, application_id, tags):
        "Remove tags for an application"
        _check_type(tags, list)
        path = self.path_application + '/' + application_id + '/tags'
        method = 'DELETE'
        _call(path, method=method, raise_not_status=204)

    def create_command(self, command):
        "Create a command."
        resp = _call(self.path_command, method='POST', data=command,
                      raise_not_status=201)
        command = resp['headers']['Location'].split('/')[-1]

        return command

    def delete_command(self, command_id):
        "Delete an existing command"
        path = self.path_command + '/' + command_id
        method = 'DELETE'
        _call(self.path_command, method=method, raise_not_status=204)

    def get_command(self, command_id, *args, **kwargs):
        "Get a command."
        path = self.path_command + '/' + command_id
        resp = _call(path, *args, **kwargs)

        return resp.get('response', [])

    def update_command(self, command):
        "Update an existing command."
        path = self.path_command + '/' + command['id']
        method = 'PUT'
        _call(path, method=method, data=command, raise_not_status=204)

    def delete_all_commands(self):
        "Delete all commands in a cluster. Use with caution. Returns None."
        method = 'DELETE'
        _call(self.path_command, method=method, raise_not_status=204)

    def get_commands(self, filters=None):
        "Get a command. Returns a generator that yields a command"
        filters = filters or {}
        _verify_files(filters, ['name', 'user', 'status', 'tag'])

        resp = _call(self.path_command, params=filters)
        for command in resp['response'].get('commandList', []):
            yield command

    def get_clusters(self, filters=None):
        "Get all clusters. Returns a generator that yields Cluster objects."
        filters = filters or {}
        _verify_files(filters, ['name', 'status', 'tag', 'page'])
        resp = _call(self.path_cluster, method='GET', params=filters)

        for cluster in resp['response'].get('clusterList', []):
            yield cluster

    def get_cluster(self, cluster_id, *args, **kwargs):
        "Get a cluster."
        path = self.path_cluster + '/%s' % cluster_id
        resp = _call(path)

        return resp['response']

    def create_cluster(self, cluster):
        "Create a new cluster"
        for req_var in ('name', 'version', 'user', 'status'):
            if not cluster.get(req_var):
                raise GenieError('Missing required kwarg: %s' % req_var)

        if cluster['status'].upper() not in ['TERMINATED', 'OUT_OF_SERVICE', 'UP']:
            raise GenieError('kwarg "status" must be in [TERMINATED, OUT_OF_SERVICE, UP]')

        cluster = {
            "name": cluster['name'],
            "version": cluster['version'],
            "user": cluster['user'],
            "status": cluster['status'],
        }

        resp = _call(self.path_cluster, method='POST', data=cluster,
                     raise_not_status=201)

        cluster_id = resp['headers'].get('Location').split('/')[-1]

        return cluster_id

    def update_cluster(self, cluster_id, cluster_settings):
        "Update an existing cluster"
        path = self.path_cluster + '/' + cluster_id
        method = 'PUT'
        _call(path, method=method, data=cluster_settings, raise_not_status=204)

    def delete_cluster(self, cluster_id):
        "Delete a cluster"
        path = self.path_cluster + '/' + cluster_id
        method = 'DELETE'
        _call(path, method=method, raise_not_status=204)

    def delete_all_clusters(self):
        "Remove all clusters. Use with caution."
        path = self.path_cluster
        method = 'DELETE'
        _call(path, method=method, raise_not_status=204)

    def remove_all_cluster_commands(self, cluster_id):
        "Remove all commands for a cluster"
        path = self.path_cluster + '/' + cluster_id + '/commands'
        method = 'DELETE'
        _call(path, method=method, raise_not_status=204)

    def remove_cluster_command(self, cluster_id, command_id):
        "Remove commands for a cluster"
        path = self.path_cluster + '/' + cluster_id + '/commands/' + command_id
        method = 'DELETE'
        _call(path, method=method, raise_not_status=204)

    def get_cluster_commands(self, cluster_id, status=None):
        "Get commands for a cluster"
        path = self.path_cluster + '/' + cluster_id + '/commands'
        status = status or None
        if status:
            status = {'status': status}
        resp = _call(path, params=status)

        return resp['response']

    def add_cluster_commands(self, cluster_id, commands):
        "Add commands to a cluster"
        path = self.path_cluster + '/' + cluster_id + '/commands'
        commands = commands or []
        method = 'POST'
        _call(path, method=method, data=commands, raise_not_status=204)

    def remove_all_cluster_configs(self, cluster_id):
        "Remove all configs for a cluster"
        path = self.path_cluster + '/' + cluster_id + '/configs'
        method = 'DELETE'
        _call(path, method=method, raise_not_status=204)

    def get_cluster_configs(self, cluster_id):
        path = self.path_cluster + '/' + cluster_id + '/configs'
        resp = _call(path)

        return resp['response']

    def add_cluster_configs(self, cluster_id, configs):
        "Add configs to a cluster"
        path = self.path_cluster + '/' + cluster_id + '/configs'
        _check_type(configs, list)
        method = 'POST'
        _call(path, method=method, data=configs, raise_not_status=204)

    def update_cluster_configs(self, cluster_id, configs):
        "Update configs for a cluster"
        path = self.path_cluster + '/' + cluster_id + '/configs'
        method = 'PUT'
        _call(path, method=method, data=configs, raise_not_status=204)

    def remove_all_cluster_tags(self, cluster_id):
        "Remove all tags for a cluster"
        path = self.path_cluster + '/' + cluster_id + '/tags'
        method = 'DELETE'
        _call(path, method=method, raise_not_status=204)

    def remove_cluster_tag(self, cluster_id, tag):
        "Remove a tag for a cluster"
        path = self.path_cluster + '/' + cluster_id + '/tags/' + tag
        method = 'DELETE'
        _call(path, method=method, raise_not_status=204)

    def get_cluster_tags(self, cluster_id):
        "Get tags for a cluster"
        path = self.path_cluster + '/' + cluster_id + '/tags'
        resp = _call(path)

        return resp['response']

    def add_cluster_tags(self, cluster_id, tags):
        "Add tags to a cluster"
        path = self.path_cluster + '/' + cluster_id + '/tags'
        method = 'POST'
        _call(path, method=method, data=tags, raise_not_status=204)

    def update_cluster_tags(self, cluster_id, tags):
        "Update tags for a cluster"
        path = self.path_cluster + '/' + cluster_id + '/tags'
        method = 'PUT'
        _call(path, method=method, data=tags, raise_not_status=204)

    def delete_cluster_tag(self, cluster_id, tag):
        "Delete tags for a cluster"
        path = self.path_cluster + '/' + cluster_id + '/tags/' + tag
        _check_type(tag, str)
        method = 'DELETE'
        _call(path, method=method, raise_not_status=204)

    def remove_all_command_applications(self, command_id):
        "Remove applications for a command"
        path = self.path_command + '/' + command_id + '/applications'
        method = 'DELETE'
        _call(path, method=method, raise_not_status=204)

    def get_command_applications(self, command_id):
        "Get applications for a command"
        path = self.path_command + '/' + command_id + '/applications'
        resp = _call(path)

        return resp['response']

    def add_command_applications(self, command_id, application_ids):
        "Add applications for a command"
        path = self.path_command + '/' + command_id + '/applications'
        _check_type(application_ids, list)
        method = 'POST'
        _call(path, method=method, data=application_ids, raise_not_status=204)

    def update_command_applications(self, command_id, application_ids):
        "Update applications for a command"
        path = self.path_command + '/' + command_id + '/applications'
        _check_type(application_ids, list)
        method = 'PUT'
        _call(path, method=method, data=application_ids, raise_not_status=204)

    def remove_command_applications(self, command_id, application_id):
        "Remove applications for a command"
        path = self.path_command + '/' + command_id + '/applications' + '/' + application_id
        method = 'DELETE'
        _call(path, method=method, raise_not_status=204)

    def get_command_clusters(self, command_id, status=None):
        "Get commands for a cluster"
        _verify_files(status, ['UP', 'OUT_OF_SERVICE', 'TERMINATED'])
        path = self.path_command + '/' + command_id + '/clusters'
        params = {'status': status}
        resp = _call(path, params=params)

        return resp['response']

    def remove_all_command_configs(self, command_id):
        "Remove all configs for a command"
        path = self.path_command + '/' + command_id
        method = 'DELETE'
        _call(path, method=method, raise_not_status=204)

    def get_command_configs(self, command_id):
        "Get configs for a command"
        path = self.path_command + '/' + command_id + '/configs'
        resp = _call(path)

        return resp['response']

    def add_command_configs(self, command_id, configs):
        "Add configs for a command"
        _check_type(configs, list)
        path = self.path_command + '/' + command_id
        method = 'POST'
        _call(path, method=method, data=configs, raise_not_status=204)

    def update_command_configs(self, command_id, configs):
        "Update configs for a command"
        _check_type(configs, list)
        path = self.path_command + '/' + command_id
        method = 'PUT'
        _call(path, method=method, data=configs, raise_not_status=204)

    def remove_all_command_tags(self, command_id):
        "Remove all tags for a command"
        path = self.path_command + '/' + command_id + '/tags'
        method = 'DELETE'
        _call(path, method=method, raise_not_status=204)

    def get_command_tags(self, command_id):
        "Get tags for a command"
        path = self.path_command + '/' + command_id + '/tags'
        resp = _call(path)

        return resp['response']

    def add_command_tags(self, command_id, tags):
        "Add tags to a command"
        _check_type(tags, list)
        path = self.path_command + '/' + command_id
        method = 'POST'
        _call(path, method=method, data=tags, raise_not_status=204)

    def update_command_tags(self, command_id, tags):
        "Update tags for a command"
        _check_type(tags, list)
        path = self.path_command + '/' + command_id
        method = 'PUT'
        _call(path, method=method, data=tags, raise_not_status=204)

    def remove_command_tag(self, command_id, tag):
        "Remove tag for command"
        path = self.path_command + '/' + command_id + '/' + tag
        method = 'DELETE'
        _call(path, method=method, raise_not_status=204)

    def get_jobs(self, filters=None):
        "Get jobs"
        filters = filters or {}
        _verify_files(filters, ['id', 'name', 'user', 'status', 'tag'])
        resp = _call(self.path_job)

        yield resp['response']

    def get_job(self, job_id):
        "Get a job"
        path = self.path_job % '/' + job_id
        resp = _call(path)

        return resp['response']
