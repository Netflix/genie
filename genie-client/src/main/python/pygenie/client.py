"Genie python client library"

from __future__ import absolute_import, print_function, unicode_literals

import json
import logging
import os

from .conf import GenieConf
from .exceptions import GenieHTTPError, GenieError
from .utils import call, DotDict


logger = logging.getLogger('com.netflix.pygenie.client')

def _check_patch_operation(operation):
    "Helper function for checking HTTP patch methods"
    valid_ops = ['add', 'remove', 'replace', 'move', 'copy', 'test']
    if operation not in valid_ops:
        raise GenieError('Invalid operation [%s]. Must be in: %s' % (
            operation, valid_ops))


def _call(url, *args, **kwargs):
    """
    Wrap HTTP request _calls to the Genie server.

    """

    if kwargs.get('data'):
        kwargs['data'] = json.dumps(kwargs['data'])

    header = {'Content-Type': 'application/json'}

    resp = call(url, headers=header, *args, **kwargs)

    if not hasattr(resp, 'content'):
        return None

    response = {}
    content = resp.content or '{}'
    content = json.loads(content)

    # Genie nests the responses differently depending on whether the type is a
    # list, dict, or string response type
    if isinstance(content, list):
        response['response'] = content
    elif content.get('_embedded'):
        response['response'] = content['_embedded']
    else:
        response['response'] = content

    response['headers'] = resp.headers
    if isinstance(content, dict):
        response['page'] = content.get('page')

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


def _verify_filters(filters, supported):
    """
    Helper function to verify user-supplied filter arguments. Raises a
    GenieError exception if the type is not supported
    """
    if filters is None:
        return

    for _filter in filters.keys():
        if _filter not in supported:
            raise GenieError('Filter "%s" not supported. Supported: %s'
                             % (_filter, supported))


class Genie(object):
    """
    Genie client object.

    Args:
        conf (optional[object]): custom GenieConf object

    Returns:
        object: a genie object

    Example:
        >>> genie = Genie()
        >>> print genie.get_job('job1')

    """


    def __init__(self, conf=None):
        self.conf = conf or GenieConf()
        self.host = self.conf.genie.url
        self.version = self.conf.genie.version

        self.path_application = self.host \
            + self.conf.genie.get('application_url', '/api/v3/applications')
        self.path_cluster = self.host \
            + self.conf.genie.get('cluster_url', '/api/v3/clusters')
        self.path_command = self.host \
            + self.conf.genie.get('command_url', '/api/v3/commands')
        self.path_job = self.host \
            + self.conf.genie.get('job_url', '/api/v3/jobs')

    def get_applications(self, filters=None, req_size=1000):
        """
        Get a list of applications.

        Args:
            filters (dict): a dictionary of filters to use in the query.
            req_size (int): the number of items to return per request.

        Yields:
           dict: an application

        Example:
            >>> print([i for i in get_applications()])
            >>> [{'id':'test3'}, {'id':'test2'}]

        """
        params = filters or {}
        _verify_filters(params, ['name', 'user', 'size', 'status', 'tag',
                                 'type', 'page'])

        # Iterate through any responses until we get to the end
        params['page'] = 0
        params['size'] = req_size
        while True:
            resp = _call(self.path_application, method='GET', params=params)

            if resp:
                for app in resp['response'].get('applicationList', []):
                    yield DotDict(app)
            else:
                yield None

            # Break if we're at the end
            if (resp['page']['totalPages']) <= (resp['page']['number'] + 1):
                break

            # Get the next result
            params['page'] += 1
            logger.info('Fetching additional applications from genie [%s/%s]',
                         params['page'], resp['page']['totalPages'] - 1)

    def get_application(self, application_id):
        """
        Get an application. Returns None if the application is not found.

        Args:
            application_id (str): the application id to get

        Returns:
            dict: an application (None if the application doesn't exist)

        Example:
            >>> print(get_application('test'))
            >>> {'id': 'test'}

            >>> print(get_application('test2')
            >>> None

        """
        path = self.path_application + '/' + application_id
        resp = _call(path, none_on_404=True) or {}

        return resp.get('response')

    def create_application(self, application):
        """
        Create an application.

        Args:
            application (dict): an application to create

        Returns:
            str: the newly created application id.

        Example:
            >>> create_application({'id': 'test'})

        """
        resp = _call(self.path_application, data=application, method='POST',
                     raise_not_status=201)

        return resp['headers']['Location'].split('/')[-1]

    # TODO: We should add the id to the application
    def update_application(self, application):
        """
        Update an existing application. The application is identified based on
        the "id" key within the application dictionary.

        Args:
            application (dict): an application to update

        Returns: None

        Example:
            >>> update_application({'id': 'test'})

        """
        path = self.path_application + '/' + application['id']
        _call(path, data=application, method='PUT', raise_not_status=204)

    def patch_application(self, application_id, patches):
        """
        Patch an application.

        Note:
            A patch is a dictionary that has the following three required keys:
                - op (str): the operation
                - path (str): the path to apply the patch. E.g. "/version"
                - value (str): the value to apply. E.g. "1.1"

        Args:
            application_id (str): the application id
            patches (list): a list of patches to apply.

        Returns: None

        Example:
            >>> patch1 = {'op': 'replace', 'path': '/version', 'value': '1.1'}
            >>> patch_application('app1', [patch1])

        """
        _check_type(patches, list)
        for patch in patches:
            _check_patch_operation(patch.get('op', 'NOTSET'))

        path = self.path_application + '/' + application_id
        _call(path, data=patches, method='PATCH')

    def delete_application(self, application_id):
        """
        Delete an application.

        Args:
            application_id (str): the id of the application to delete

        Returns: None

        Example:
            >>> delete_application('test')

        """
        path = self.path_application + '/' + application_id
        _call(path, method='DELETE', raise_not_status=204)

    def delete_all_applications(self):
        """
        Delete all applications. Use with care.

        Returns: None

        Example:
            >>> delete_all_applications()

        """
        _call(self.path_application, method='DELETE')

    def get_commands_for_application(self, application_id, status=None):
        """
        Get a list of commands assigned to an application

        Args:
            application_id (str): the application id
            status (optional[str]): only return commands in this status

        Returns:
            list: a list of command ids

        Example:
            >>> get_commands_for_application('test')
            >>> ['cmd1', 'cmd2', 'cmd3']

        """
        status = status or []
        params = {'status': status}


        path = self.path_application + '/' + application_id + '/commands'
        resp = _call(path, none_on_404=True, params=params) or {}

        return resp.get('response', [])

    def remove_all_application_configs(self, application_id):
        """
        Remove all configs from an application

        Args:
            application_id (str): the application id to remove commands

        Returns: None

        Example:
            >>> remove_all_application_configs('application2')

        """
        path = self.path_application + '/' + application_id + '/configs'
        _call(path, method='DELETE', raise_not_status=204)

    def get_configs_for_application(self, application_id):
        """
        Get configs for an application.

        Args:
            application_id (str): the application id to get commands

        Returns:
            list: configs attached to this application

        Example:
            >>> get_application_configs('app1')
            >>> ['config1', 'config2', 'config3']

        """
        path = self.path_application + '/' + application_id + '/configs'
        resp = _call(path, none_on_404=True) or {}

        return resp.get('response', [])

    def add_configs_to_application(self, application_id, configs):
        """
        Add configs to an application.

        Note:
            This command simply appends configs to an existing application. It
            will not remove them like ``update_configs_for_application`` does.

        Args:
            application_id (str): the application id to add configs to
            configs (list): the configs to add to the application

        Returns: None

        Example:
            >>> add_configs_to_application('app1', ['config1', 'config2'])

        """
        _check_type(configs, list)
        path = self.path_application + '/' + application_id + '/configs'
        _call(path, method='POST', data=configs, raise_not_status=204)

    def update_configs_for_application(self, application_id, configs):
        """
        Update configs for an application

        Args:
            application_id (str): the application id
            configs (list): the configs to update

        Returns: None

        Example:
            >>> update_configs_for_application('app1', ['config1'])

        """
        _check_type(configs, list)
        path = self.path_application + '/' + application_id + '/configs'
        _call(path, method='PUT', data=configs, raise_not_status=204)

    def remove_all_dependencies_for_application(self, application_id):
        """
        Remove all dependencies in an application.

        Args:
            application_id (str): the application id

        Returns: None

        Example:
            >>> remove_all_dependencies_for_application('app1')

        """
        path = self.path_application + '/' + application_id + '/dependencies'
        _call(path, method='DELETE', raise_not_status=204)

    def get_dependencies_for_application(self, application_id):
        """
        Get dependencies for an application

        Args:
            application_id (str): the application id

        Returns:
            list: dependencies for the application

        Example:
            >>> get_dependencies_for_application('app1')

        """
        path = self.path_application + '/' + application_id + '/dependencies'
        resp = _call(path, none_on_404=True) or {}

        # Return a dotdict if the response is not empty
        return resp.get('response', [])

    def add_dependencies_for_application(self, application_id, dependencies):
        """
        Add dependencies to an existing application.

        Note:
            This command simply appends dependencies. It does not overwrite any
            existing dependencies like ``update_dependencies_for_application``
            does.

        Args:
            application_id (str): the application id
            dependencies (list): the dependencies to add

        Returns: None

        Example:
            >>> add_dependencies_for_application('app1', ['test1', 'test2'])

        """
        _check_type(dependencies, list)
        path = self.path_application + '/' + application_id + '/dependencies'
        _call(path, method='POST', raise_not_status=204)

    def update_dependencies_for_application(self, application_id, dependenices):
        """
        Update dependencies for an application.

        Args:
            application_id (str): the application id
            dependencies (list): the dependencies to update

        Returns: None

        Example:
            >>> add_dependencies_for_application('app1', ['test2'])

        """
        _check_type(dependenices, list)
        path = self.path_application + '/' + application_id + '/dependencies'
        _call(path, method='PUT', raise_not_status=204)


    def remove_all_tags_for_application(self, application_id):
        """
        Remove all tags for an application.

        Args:
            application_id (str): the application id

        Returns: None

        Example:
            >>> remove_all_tags_for_application('app1')

        """
        path = self.path_application + '/' + application_id + '/tags'
        _call(path, method='DELETE', raise_not_status=204)

    def get_tags_for_application(self, application_id):
        """
        Get tags for an application.

        Args:
            application_id (str): the application id

        Returns:
            list: the tags for an application

        Example:
            >>> get_tags_for_application('test1')
            >>> ['tag1', 'tag2', 'tag3']

        """
        path = self.path_application + '/' + application_id + '/tags'
        resp = _call(path, none_on_404=True) or {}

        return resp.get('response', [])

    def add_tags_for_application(self, application_id, tags):
        """
        Add tags to an application. This function appends tags to an existing
        application.

        Args:
            application_id (str): the application id
            tags (list): the tags to add

        Returns: None

        Example:
            >>> add_tags_for_application('app1', ['tag1', 'tag2'])

        """
        _check_type(tags, list)
        path = self.path_application + '/' + application_id + '/tags'
        _call(path, data=tags, method='POST', raise_not_status=204)

    def update_tags_for_application(self, application_id, tags):
        """
        Update tags for an application. Unlike ```add_tags_for_application```
        this function will overwrite any existing tags.

        Args:
            application_id (str): the application id
            tags (list): the tags to update

        Returns: None

        Example:
            >>> update_tags_for_application('app1', ['tag4', 'tag5'])

        """
        _check_type(tags, list)
        path = self.path_application + '/' + application_id + '/tags'
        _call(path, method='PUT', data=tags, raise_not_status=204)

    def remove_tag_for_application(self, application_id, tag):
        """
        Remove a tag for an application.

        Args:
            application_id (str): the application id
            tags (list): the tags to remove from the application

        Returns: None

        Example:
            >>> remove_tag_for_application('app1', ['tag1', 'tag2'])

        """
        path = self.path_application + '/' + application_id + '/tags'
        _call(path, method='DELETE', raise_not_status=204)

    def create_command(self, command):
        """
        Create a command.

        Args:
            command (dict): the command to create

        Returns:
            str: the newly create command id

        Example: create_command({'id': 'test_command'})

        """
        resp = _call(self.path_command, method='POST', data=command,
                     raise_not_status=201)
        command = resp['headers']['Location'].split('/')[-1]

        return command

    def delete_command(self, command_id):
        """
        Delete an existing command.

        Args:
            command_id (str): the command id to delete

        Returns: None

        Example:
            >>> delete_command('cmd1')

        """
        path = self.path_command + '/' + command_id
        _call(path, method='DELETE', raise_not_status=204)

    def get_command(self, command_id):
        """
        Get a command.

        Args:
            command_id (str): the command id to get

        Returns:
            list: the command configuration

        Example:
            >>> get_command('cmd1')
            >>> {'id':'cmd1', 'name':'testcmd'}

        """
        path = self.path_command + '/' + command_id
        resp = _call(path, none_on_404=True) or {}

        if resp.get('response'):
            return DotDict(resp.get('response'))

    def update_command(self, command):
        """
        Update an existing command.

        Note:
            Genie uses the "id" key in the command's configuration to determine
            which command to update.

        Args:
            command (dict): the command to update

        Returns: None

        Example:
            >>> update_command({'id':'test', 'name':'new_name'})

        """
        if not command.get('id'):
            raise GenieError('"id" key missing from command configuration.')

        path = self.path_command + '/' + command['id']
        _call(path, method='PUT', data=command, raise_not_status=204)

    def update_commands_for_cluster(self, cluster_id, commands):
        """
        This method is not an official method, but added for backwards compat-
        ibility for genie3.
        """
        cmd_ids = [i['id'] for i in commands]
        self.set_commands_for_cluster(cluster_id, cmd_ids)

    def patch_command(self, command_id, patches):
        """
        Patch an command.

        Note:
            A patch is a dictionary that has the following three required keys:
                - op (str): the operation
                - path (str): the path to apply the patch. E.g. "/version"
                - value (str): the value to apply. E.g. "1.1"

        Args:
            command_id (str): the command id
            patches (list): a list of patches to apply.

        Returns: None

        Example:
            >>> patch1 = {'op': 'replace', 'path': '/version', 'value': '1.1'}
            >>> patch_command('cmd11', [patch1])

        """
        _check_type(patches, list)
        for patch in patches:
            _check_patch_operation(patch.get('op', 'NOTSET'))

        path = self.path_command + '/' + command_id
        _call(path, data=patches, method='PATCH')

    def delete_all_commands(self):
        """
        Delete all commands in a cluster.

        Returns: None

        Example:
            >>> delete_all_commands()

        """
        _call(self.path_command, method='DELETE', raise_not_status=204)

    # TODO: Page and size should be separate arguments
    def get_commands(self, filters=None, req_size=1000):
        """
        Get all of the commands in a cluster.

        Note:
            This function takes advantage of Genie's paging process. It will
            make all of the additional calls required until it retrieves all
            of the commands in the Genie cluster.

        Args:
            filters (dict): a dictionary of filters to use. Valid key parameters
                are: name, user, status, tag
            req_size (int): the number of items to return per request.

        Yields:
            dict: a command dictionary

        Examples:
            >>> [i for i in get_commands()]
            >>> [{...}, {...}, {...}]

            >>> [i for i in get_commands(filters={'status':'OUT_OF_SERVICE'})]
            >>> [{...}, {...}, {...}]

        """
        filters = filters or {}
        _check_type(filters, dict)

        params = filters or {}
        _verify_filters(params, ['name', 'user', 'size', 'status', 'tag'])

        # Iterate through any responses until we get to the end
        params['page'] = 0
        params['size'] = req_size
        while True:
            resp = _call(self.path_command, method='GET', params=params)

            # TODO: Check to see if _imbedded key is missing, then break
            if resp:
                for command in resp['response'].get('commandList', []):
                    yield DotDict(command)
            else:
                yield None

            # Break if we're at the end
            if (resp['page']['totalPages']) <= (resp['page']['number'] + 1):
                break

            # On to the next iteration
            params['page'] += 1
            logger.info('Fetching additional commands from genie [%s/%s]',
                         params['page'], resp['page']['totalPages'])

    def get_clusters(self, filters=None, req_size=1000):
        """
        Get all of the clusters.

        Note:
            This function takes advantage of Genie's paging process. It will
            make all of the additional calls required until it retrieves all
            of the commands in the Genie cluster.

        Args:
            filters (optional[dict]): a dictionary of filters to use. Valid key parameters
                are: name, status, tag
            req_size (int): the number of items to return per request.

        Yields:
            dict: a cluster configuration

        Examples:
            >>> [i for i in get_clusters()]
            >>> [{...}, {...}, {...}]

            >>> [i for i in get_clusters(filters={'status':'UP'})]
            >>> [{...}, {...}, {...}]

        """
        params = filters or {}
        _verify_filters(params, ['name', 'size', 'status', 'tag', 'page'])

        # Iterate through any responses until we get to the end
        params['page'] = 0
        params['size'] = req_size
        while True:
            resp = _call(self.path_cluster, method='GET', params=params)

            if resp:
                for cluster in resp['response'].get('clusterList', []):
                    yield DotDict(cluster)
            else:
                yield None


            # Break if we're at the end
            if (resp['page']['totalPages']) <= (resp['page']['number'] + 1):
                break

            # On to the next iteration
            params['page'] += 1
            logger.info('Fetching additional clusters from genie [%s/%s]',
                         params['page'], resp['page']['totalPages'])

    def get_cluster(self, cluster_id):
        """
        Get a cluster.

        Args:
            cluster_id (str): the cluster id to retrieve

        Returns:
            dict: a cluster configuration

        Example:
            >>> get_cluster('test1')
            >>> {'id':'test1', 'name':'testcluster'}

        """
        path = self.path_cluster + '/%s' % cluster_id
        resp = _call(path, none_on_404=True) or {}

        if resp.get('response'):
            return DotDict(resp.get('response'))

    def create_cluster(self, cluster):
        """
        Create a new cluster. If an "id" is not specified in the configuration
        then Genie will automatically assign the cluster an UUID.

        Args:
            cluster (dict): a cluster configuration

        Returns:
            str: the newly created cluster id"

        Example:
            >>> cluster = {'name':'testcluster'}
            >>> create_cluster(cluster)
            >>> 00355f22-a8f2-4c64-8ff4-be9bba0c7b44

        """
        req_var = ('name', 'version', 'user', 'status')

        # TODO: genie2 -> 3 compatibility
        # Unpack old genie2 Cluster objects into a a dictionary. This adds some
        # backwards compatibility with genie2 clients
        if cluster.__class__.__name__ == "Cluster":
            new_cluster = {}
            for key, val in cluster.__dict__.iteritems():
                if key in ['updated', 'created']:
                    val = cluster.updated.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                new_cluster[key] = val
            cluster = new_cluster

        for var in req_var:
            if not cluster.get(var):
                raise GenieError('Missing required kwarg: %s' % var)

        if cluster['status'].upper() not in ['TERMINATED', 'OUT_OF_SERVICE', 'UP']:
            raise GenieError('kwarg "status" must be in [TERMINATED, OUT_OF_SERVICE, UP]')

        resp = _call(self.path_cluster, method='POST', data=cluster,
                     raise_not_status=201)

        cluster_id = resp['headers'].get('Location').split('/')[-1]

        return cluster_id

    def update_cluster(self, cluster_id, cluster):
        """
        Update an existing cluster.

        Args:
            cluster_id (str): the cluster id to update
            cluster (dict): the updated cluster configuration

        Returns: None

        Example:
            >>> cluster = {'name':'newname'}
            >>> update_cluster('testcluster', cluster)

        """
        path = self.path_cluster + '/' + cluster_id
        _call(path, method='PUT', data=cluster, raise_not_status=204)

    def patch_cluster(self, cluster_id, patches):
        """
        Patch an cluster.

        Note:
            A patch is a dictionary that has the following three required keys:
                - op (str): the operation
                - path (str): the path to apply the patch. E.g. "/version"
                - value (str): the value to apply. E.g. "1.1"

        Args:
            cluster_id (str): the cluster id
            patches (list): a list of patches to apply.

        Returns: None

        Example:
            >>> patch1 = {'op': 'replace', 'path': '/version', 'value': '1.1'}
            >>> patch_cluster('cluster1', [patch1])

        """
        _check_type(patches, list)
        for patch in patches:
            _check_patch_operation(patch.get('op', 'NOTSET'))

        path = self.path_cluster + '/' + cluster_id
        _call(path, data=patches, method='PATCH')

    def delete_cluster(self, cluster_id):
        """
        Delete a cluster

        Args:
            cluster_id (str): the cluster id to delete

        Returns: None

        Example:
            >>> delete_cluster('cluster1')

        """
        path = self.path_cluster + '/' + cluster_id
        _call(path, method='DELETE', raise_not_status=204)

    def delete_all_clusters(self):
        """
        Remove all clusters. Use with caution.

        Returns: None

        Example:
            >>> delete_all_clusters()

        """
        path = self.path_cluster
        _call(path, method='DELETE', raise_not_status=204)

    def remove_all_commands_for_cluster(self, cluster_id):
        """
        Remove all commands for a cluster.

        Args:
            cluster_id (str): the cluster id

        Returns: None

        Examples:
            >>> remove_all_cluster_commands('cluster1')

        """
        path = self.path_cluster + '/' + cluster_id + '/commands'
        _call(path, method='DELETE', raise_not_status=204)

    def remove_commands_for_cluster(self, cluster_id, command_id):
        """
        Remove a command for a cluster

        Args:
            cluster_id (str): the cluster id
            command_id (str): the command id to remove

        Returns: None

        Example:
            >>> remove_all_cluster_commands('cluster1', 'cmd1')

        """
        path = self.path_cluster + '/' + cluster_id + '/commands/' + command_id
        _call(path, method='DELETE', raise_not_status=204)

    def get_commands_for_cluster(self, cluster_id, status=None):
        """
        Get commands for a cluster

        Args:
            cluster_id (str): the cluster id
            status (optional[str]): filter by this status

        Returns:
            dict: a dictionary of commands

        Examples:
            >>> get_commands_for_cluster('cluster1')
            >>> ['cmd1', 'cmd2', 'cmd3']

            >>> get_commands_for_cluster('cluster1', 'UP')
            >>> ['cmd2']

        """
        path = self.path_cluster + '/' + cluster_id + '/commands'
        status = status or None
        if status:
            status = {'status': status}
        resp = _call(path, none_on_404=True, params=status) or {}

        # Return dotdict for any commands
        if resp.get('response'):
            return [DotDict(i) for i in resp['response']]
        else:
            return []

    def add_commands_for_cluster(self, cluster_id, commands):
        """
        Add commands to a cluster.

        Args:
            cluster_id (str): the cluster id
            commands (list): the commands to add to the cluster

        Returns: None

        Example:
            >>> add_commands_for_cluster('cluster1', ['cmd1', 'cmd2'])

        """
        path = self.path_cluster + '/' + cluster_id + '/commands'
        commands = commands or []
        _call(path, method='POST', data=commands, raise_not_status=204)

    def set_commands_for_cluster(self, cluster_id, commands):
        """
        Set commands for a cluster. Unlike "add" this will overwrite.

        Args:
            cluster_id (str): the cluster id
            commands (list): the commands to set to the cluster

        Returns: None

        Example:
            >>> set_commands_for_cluster('cluster1', ['cmd1', 'cmd2'])

        """
        path = self.path_cluster + '/' + cluster_id + '/commands'
        commands = commands or []
        _call(path, method='PUT', data=commands, raise_not_status=204)

    def remove_all_configs_for_cluster(self, cluster_id):
        """
        Remove all configs for a cluster.

        Args:
            cluster_id (str): the cluster id

        Returns: None

        Example:
            >>> remove_all_configs_for_cluster('cluster1')

        """
        path = self.path_cluster + '/' + cluster_id + '/configs'
        _call(path, method='DELETE', raise_not_status=204)

    def get_all_configs_for_cluster(self, cluster_id):
        """
        Get configs for a cluster.

        Args:
            cluster_id (str): the cluster id to get

        Returns:
            list: the configurations for a cluster

        Example:
            >>> get_all_configs_for_cluster('cluster1')
            >>> ['config1', 'config2']
        """

        path = self.path_cluster + '/' + cluster_id + '/configs'
        resp = _call(path, none_on_404=True) or {}

        return resp.get('response', [])

    def add_configs_for_cluster(self, cluster_id, configs):
        """
        Add configs to a cluster.

        Args:
            cluster_id (str): the cluster id
            configs (list): the configs to add to the cluster

        Returns: None

        Examples:
            >>> add_configs_for_cluster('cluster1', ['config1', 'config2'])

        """
        path = self.path_cluster + '/' + cluster_id + '/configs'
        _check_type(configs, list)
        _call(path, method='POST', data=configs, raise_not_status=204)

    def update_configs_for_cluster(self, cluster_id, configs):
        """
        Update configs for a cluster.

        Args:
            cluster_id (str): the cluster id
            configs (list): the configs to update

        Returns: None

        Example:
            >>> update_configs_for_cluster('cluster1', ['config1', 'config2']

        """
        path = self.path_cluster + '/' + cluster_id + '/configs'
        _call(path, method='PUT', data=configs, raise_not_status=204)

    def remove_all_tags_for_cluster(self, cluster_id):
        """
        Remove all tags for a cluster.

        Args:
            cluster_id (str): the cluster id

        Returns: None

        Example:
            >>> remove_all_tags_for_cluster('cluster1')

        """
        path = self.path_cluster + '/' + cluster_id + '/tags'
        _call(path, method='DELETE', raise_not_status=204)

    def get_tags_for_cluster(self, cluster_id):
        """
        Get tags for a cluster.

        Args:
            cluster_id (str): the cluster id

        Returns:
            list: the tags for a cluster

        Example:
            >>> get_tags_for_cluster('cluster1')
            >>> ['test:true', 'prod:false', 'dev:false']

        """
        path = self.path_cluster + '/' + cluster_id + '/tags'
        resp = _call(path, none_on_404=True) or {}

        return resp.get('response', [])

    def add_tags_for_cluster(self, cluster_id, tags):
        """
        Add tags to a cluster.

        Args:
            cluster_id (str): the cluster id
            tags (list): the tags to add"

        Returns: None

        Example:
            >>> add_tags_for_cluster('cluster1', ['test:true'])

        """
        _check_type(tags, list)
        path = self.path_cluster + '/' + cluster_id + '/tags'
        _call(path, method='POST', data=tags, raise_not_status=204)

    def update_tags_for_cluster(self, cluster_id, tags):
        """
        Update tags for a cluster.

        Args:
            cluster_id (str): the cluster id
            tags (list): the tags to add"

        Example:
            >>> update_tags_for_cluster('cluster1', ['test:true'])

        """
        _check_type(tags, list)
        path = self.path_cluster + '/' + cluster_id + '/tags'
        _call(path, method='PUT', data=tags, raise_not_status=204)

    def remove_tag_for_cluster(self, cluster_id, tag):
        """
        Delete a tag for a cluster.

        Args:
            cluster_id (str): the cluster id
            tag (str): the tag to remove

        Returns: None

        Example:
            >>> remove_tag_for_cluster('cluster1', 'test:true')

        """
        _check_type(tag, str)
        path = self.path_cluster + '/' + cluster_id + '/tags/' + tag
        _call(path, method='DELETE', raise_not_status=204)

    def remove_all_applications_for_command(self, command_id):
        """
        Remove applications for a command.

        Args:
            command_id (str): the command id

        Returns: None

        Example:
            >>> remove_all_command_applications('command1')

        """
        path = self.path_command + '/' + command_id + '/applications'
        _call(path, method='DELETE', raise_not_status=204)

    def get_applications_for_command(self, command_id):
        """
        Get applications for a command

        Args:
            command_id (str): the command id

        Returns:
            list: the applications for the command

        Example:
            >>> get_applications_for_command('command1')
            >>> ['app1', 'app2', 'app3']

        """
        path = self.path_command + '/' + command_id + '/applications'
        resp = _call(path, none_on_404=True) or {}

        return resp.get('response', [])

    def add_applications_for_command(self, command_id, application_ids):
        """
        Add applications for a command.

        Args:
            command_id (str): the command id
            application_ids (list): the application ids to add

        Returns: None

        Example:
            >>> add_applications_for_command('cmd1', ['app1', 'app2'])

        """
        _check_type(application_ids, list)
        path = self.path_command + '/' + command_id + '/applications'
        _call(path, method='POST', data=application_ids, raise_not_status=204)

    def set_application_for_command(self, command_id, application_ids):
        """
        Set applications for a command.

        Args:
            command_id (str): the command id
            application_ids (list): the application ids to set

        Returns: None

        Example:
            >>> set_application_for_command('cmd1', ['app1', 'app2', 'app3'])

        """
        _check_type(application_ids, list)
        path = self.path_command + '/' + command_id + '/applications'
        _call(path, method='PUT', data=application_ids, raise_not_status=204)

    def remove_application_for_command(self, command_id, application_id):
        """
        Remove applications for a command.

        Args:
            command_id (str): the command id
            application_id (str): the application to remove

        Returns: None

        Example:
            >>> remove_application_for_command('cmd1', 'app1')

        """
        path = self.path_command + '/' + command_id + '/applications' + '/' \
            + application_id
        _call(path, method='DELETE', raise_not_status=204)

    def get_clusters_for_command(self, command_id, status=None):
        """
        Get commands for a cluster.

        Args:
            command_id (str): the command id
            status (optional[str]): filter by this cluster status. Valid
                parameters are UP, OUT_OF_SERVICE, and TERMINATED

        Returns:
            list: the clusters

        Examples:
            >>> get_clusters_for_command('cmd1')
            >>> [{'name':'cluster1'}, {'name':'cluster2'}]

            >>> get_clusters_for_command('cmd1', status='UP')
            >>> [{'name':'cluster2'}]

        """
        _verify_filters(status, ['UP', 'OUT_OF_SERVICE', 'TERMINATED'])
        path = self.path_command + '/' + command_id + '/clusters'
        params = {'status': status}
        resp = _call(path, none_on_404=True, params=params) or {}

        return resp.get('response')

    def remove_all_configs_for_command(self, command_id):
        """
        Remove all configs for a command.

        Args:
            command_id (str): the command id

        Returns: None

        Example:
            >>> remove_all_command_configs('cmd1')

        """
        path = self.path_command + '/' + command_id
        _call(path, method='DELETE', raise_not_status=204)

    def get_configs_for_command(self, command_id):
        """
        Get configs for a command.

        Args:
            command_id (str): the command id

        Returns:
            list: the configs for a command

        Example:
            >>> get_configs_for_command('cmd1')
            >>> [{'id':'test'}, {'id':'test2'}]
        """

        path = self.path_command + '/' + command_id + '/configs'
        resp = _call(path, none_on_404=True) or {}

        return resp.get('response')

    def add_configs_for_command(self, command_id, configs):
        """
        Add configs for a command.

        Args:
            command_id (str): the command id
            configs (list): the configs to add

        Returns: None

        Example:
            >>> add_configs_for_command('cmd1', ['conf1', 'conf2'])

        """
        _check_type(configs, list)
        path = self.path_command + '/' + command_id
        _call(path, method='POST', data=configs, raise_not_status=204)

    def update_configs_for_command(self, command_id, configs):
        """
        Update configs for a command.

        Args:
            command_id (str): the command id
            configs (list): the configs to update

        Returns: None

        Example:
            >>> update_configs_for_command('cmd1', ['conf1', 'conf2'])

        """
        _check_type(configs, list)
        path = self.path_command + '/' + command_id
        _call(path, method='PUT', data=configs, raise_not_status=204)

    def remove_all_tags_for_command(self, command_id):
        """
        Remove all tags for a command.

        Args:
            command_id (str): the command id

        Returns: None

        Example:
            >>> remove_all_tags_for_command('cmd1')

        """
        path = self.path_command + '/' + command_id + '/tags'
        _call(path, method='DELETE', raise_not_status=204)

    def get_tags_for_command(self, command_id):
        """
        Get tags for a command.

        Args:
            command_id (str): the command id

        Returns:
            list: the tags for the command

        Example:
            >>> get_tags_for_command('cmd1')
            >>> ['test:true', 'prod:false']

        """
        path = self.path_command + '/' + command_id + '/tags'
        resp = _call(path, none_on_404=True) or {}

        return resp.get('response', [])

    def add_tags_for_command(self, command_id, tags):
        """
        Add tags to a command.

        Args:
            command_id (str); the command i

        Returns: None

        Example:
            >>> add_tags_for_command('cmd1', ['test:true', 'prod:false'])

        """
        _check_type(tags, list)
        path = self.path_command + '/' + command_id
        _call(path, method='POST', data=tags, raise_not_status=204)

    def update_tags_for_command(self, command_id, tags):
        """
        Update tags for a command.

        Args:
            command_id (str): the command id
            tags (list): the tags to update

        Returns: None

        Example:
            >>> add_tags_for_command('cmd1', ['prod:true'])

        """
        _check_type(tags, list)
        path = self.path_command + '/' + command_id
        _call(path, method='PUT', data=tags, raise_not_status=204)

    def remove_tags_for_command(self, command_id, tag):
        """
        Remove tag for command.

        Args:
            command_id (str): the command id
            tag (str): the tag to remove

        Returns: None

        Example:
            >>> remove_tags_for_command('cmd1', ['test:true'])

        """
        path = self.path_command + '/' + command_id + '/' + tag
        _call(path, method='DELETE', raise_not_status=204)

    def get_jobs(self, filters=None, req_size=1000):
        """
        Get jobs. This command makes pages through results from Genie and will
        make multiple API calls until all of the results have been returned.

        Args:
            filters (dict): filter the jobs by these value(s). Valid parameters
                are id, clusterName, user, status, and tag.
            req_size (int): the number of items to return per request.

        Yields:
            dict: a job

        Examples:
            >>> [i for i in get_jobs()]
            >>> [{'id':'testjob'}, {'id':'testjob2'}]

            >>> [i for i in get_jobs(filters={'user': 'testuser'})]
            >>> [{'id': 'testcluster'}]

        """
        params = filters or {}

        # Iterate through any responses until we get to the end
        params['page'] = 0
        params['size'] = req_size
        while True:
            resp = _call(self.path_job, method='GET', params=params)

            if resp:
                for job in resp['response'].get('jobSearchResultList', []):
                    yield DotDict(job)
            else:
                yield None


            # Break if we're at the end
            if (resp['page']['totalPages']) == (resp['page']['number'] + 1):
                break

            # On to the next iteration
            params['page'] += 1
            logger.info('Fetching additional jobs from genie [%s/%s]',
                         params['page'], resp['page']['totalPages'])

    def get_job(self, job_id):
        """
        Get a job.

        Args:
            job_id (str): the job id

        Returns:
            dict: the job

        Example:
            >>> get_job('testjob1')
            >>> {'id': 'testjob', 'name': 'mytestjob'}

        """
        path = self.path_job + '/' + job_id
        resp = _call(path, none_on_404=True) or {}

        if resp.get('response'):
            return DotDict(resp.get('response'))

    def patch_job(self, job_id, patches):
        """
        Patch an job.

        Note:
            A patch is a dictionary that has the following three required keys:
                - op (str): the operation
                - path (str): the path to apply the patch. E.g. "/version"
                - value (str): the value to apply. E.g. "1.1"

        Args:
            job_id (str): the job id
            patches (list): a list of patches to apply.

        Returns: None

        Example:
            >>> patch1 = {'op': 'replace', 'path': '/version', 'value': '1.1'}
            >>> patch_job('job1', [patch1])

        """
        _check_type(patches, list)
        for patch in patches:
            _check_patch_operation(patch.get('op', 'NOTSET'))

        path = self.path_job + '/' + job_id
        _call(path, data=patches, method='PATCH')

    def get_job_applications(self, job_id):
        """
        Get job applications.

        Args:
            job_id (str): the job id to get applicationss

        Returns:
            list: the job applications

        Example:
            >>> get_job_applications('job1')
            >>> ['app1', 'app2', 'app3']

        """
        path = self.path_job + '/' + job_id + '/applications'
        resp = _call(path, none_on_404=True) or {}

        return resp.get('response', [])

    def get_job_cluster(self, job_id):
        """
        Get job cluster.

        Args:
            job_id (str): the job id to get clusters

        Returns:
            dict: the job cluster

        Example:
            >>> get_job_cluster('job1')
            >>> {'id': 'cluster1'}

        """
        path = self.path_job + '/' + job_id + '/cluster'
        resp = _call(path, none_on_404=True)

        if resp:
            return DotDict(resp['response'])

    def get_job_command(self, job_id):
        """
        Get job command.

        Args:
            job_id (str): the job id

        Returns:
            dict: the job command

        Example:
            >>> get_job_command('job1')
            >>> {'id': 'cmd1'}

        """
        path = self.path_job + '/' + job_id + '/command'
        resp = _call(path, none_on_404=True)

        if resp:
            return DotDict(resp['response'])

    def get_job_execution(self, job_id):
        """
        Get job execution.

        Args:
            job_id (str): the job id

        Returns:
            dict: the job execution

        Example:
            >>> get_job_execution('job1')
            >>> {'id': 'job1'}

        """
        path = self.path_job + '/' + job_id + '/execution'
        resp = _call(path, none_on_404=True)

        if resp:
            return DotDict(resp['response'])

    def get_job_output(self, job_id):
        """
        Get job output.

        Args:
            job_id (str): the job id

        Returns:
            dict: the job output

        Example:
            >>> get_job_output('job1')
            >>> "running"

        """
        path = self.path_job + '/' + job_id + '/output'
        resp = _call(path, none_on_404=True)

        if resp:
            return DotDict(resp['response'])

    def get_job_request(self, job_id):
        """
        Get job request.

        Args:
            job_id (str): the job

        Returns:
            str: the job request

        Example:
            >>> get_job_request('job1')
            >>> "running"

        """
        path = self.path_job + '/' + job_id + '/request'
        resp = _call(path, none_on_404=True)

        if resp:
            return resp.get('response')


    def get_job_status(self, job_id):
        """
        Get job status.

        Args:
            job_id (str): the job id

        Returns:
            str: the job status

        Example:
            >>> get_job_status('job1')
            >>> "running"

        """
        path = self.path_job + '/' + job_id + '/status'
        resp = _call(path, none_on_404=True)

        if resp:
            return resp.get('response')

    def submit_job(self, job_parameters):
        """
        Submit a job to genie.

        Args:
            job_parameters (dict): a dictionary with job parameters

        Returns:
            str: the job id

        Example:
            >>> get_job_status({'id': 'testjob'})

        """
        _check_type(job_parameters, dict)
        path = self.path_job
        resp = _call(path, method='POST', data=job_parameters)

        return resp['headers']['Location'].split('/')[-1]
