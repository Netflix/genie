"""
  Copyright 2015 Netflix, Inc.

     Licensed under the Apache License, Version 2.0 (the "License");
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

     Unless required by applicable law or agreed to in writing, software
     distributed under the License is distributed on an "AS IS" BASIS,
     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     See the License for the specific language governing permissions and
     limitations under the License.
"""


class V2Api(object):
    def __init__(self, apiClient):
        self.apiClient = apiClient

    def createCluster(self, body, **kwargs):
        """Create a cluster

        Args:
            body, Cluster: The cluster to create. (required)

            

        Returns: Cluster
        """

        allParams = ['body']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method createCluster" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/clusters'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'POST'

        queryParams = {}
        headerParams = {}

        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Cluster')
        return responseObject

    def getClusters(self, **kwargs):
        """Find clusters

        Args:
            name, str: Name of the cluster. (optional)

            status, list[str]: Status of the cluster. (optional)

            tag, list[str]: Tags for the cluster. (optional)

            minUpdateTime, long: Minimum time threshold for cluster update (optional)

            maxUpdateTime, long: Maximum time threshold for cluster update (optional)

            page, int: The page to start on. (optional)

            limit, int: Max number of results per page. (optional)

            descending, bool: Whether results should be sorted in descending. Defaults to true. (optional)

            orderBy, str: The fields to order the results by, must not be collection fields. (optional)

            

        Returns: Array[Cluster]
        """

        allParams = ['name', 'status', 'tag', 'minUpdateTime', 'maxUpdateTime', 'page', 'limit']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method getClusters" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/clusters'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'GET'

        queryParams = {}
        headerParams = {}

        if ('name' in params):
            queryParams['name'] = self.apiClient.toPathValue(params['name'])
        if ('status' in params):
            queryParams['status'] = self.apiClient.toPathValue(params['status'])
        if ('tag' in params):
            queryParams['tag'] = self.apiClient.toPathValue(params['tag'])
        if ('minUpdateTime' in params):
            queryParams['minUpdateTime'] = self.apiClient.toPathValue(params['minUpdateTime'])
        if ('maxUpdateTime' in params):
            queryParams['maxUpdateTime'] = self.apiClient.toPathValue(params['maxUpdateTime'])
        if ('page' in params):
            queryParams['page'] = self.apiClient.toPathValue(params['page'])
        if ('limit' in params):
            queryParams['limit'] = self.apiClient.toPathValue(params['limit'])
        if ('descending' in params):
            queryParams['descending'] = self.apiClient.toPathValue(params['descending'])
        if ('orderBy' in params):
            queryParams['orderBy'] = self.apiClient.toPathValue(params['orderBy'])
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[Cluster]')
        return responseObject

    def deleteAllClusters(self, **kwargs):
        """Delete all clusters

        Args:
            

        Returns: Array[Cluster]
        """

        allParams = []

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method deleteAllClusters" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/clusters'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'DELETE'

        queryParams = {}
        headerParams = {}

        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[Cluster]')
        return responseObject

    def updateCluster(self, id, body, **kwargs):
        """Update a cluster

        Args:
            id, str: Id of the cluster to update. (required)

            body, Cluster: The cluster information to update with. (required)

            

        Returns: Cluster
        """

        allParams = ['id', 'body']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method updateCluster" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/clusters/{id}'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'PUT'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Cluster')
        return responseObject

    def deleteCluster(self, id, **kwargs):
        """Delete a cluster

        Args:
            id, str: Id of the cluster to delete. (required)

            

        Returns: Cluster
        """

        allParams = ['id']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method deleteCluster" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/clusters/{id}'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'DELETE'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Cluster')
        return responseObject

    def getCluster(self, id, **kwargs):
        """Find a cluster by id

        Args:
            id, str: Id of the cluster to get. (required)

            

        Returns: Cluster
        """

        allParams = ['id']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method getCluster" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/clusters/{id}'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'GET'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Cluster')
        return responseObject

    def addConfigsForCluster(self, id, body, **kwargs):
        """Add new configuration files to a cluster

        Args:
            id, str: Id of the cluster to add configuration to. (required)

            body, list[str]: The configuration files to add. (required)

            

        Returns: Array[str]
        """

        allParams = ['id', 'body']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method addConfigsForCluster" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/clusters/{id}/configs'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'POST'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[str]')
        return responseObject

    def getConfigsForCluster(self, id, **kwargs):
        """Get the configuration files for a cluster

        Args:
            id, str: Id of the cluster to get configurations for. (required)

            

        Returns: Array[str]
        """

        allParams = ['id']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method getConfigsForCluster" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/clusters/{id}/configs'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'GET'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[str]')
        return responseObject

    def updateConfigsForCluster(self, id, body, **kwargs):
        """Update configuration files for an cluster

        Args:
            id, str: Id of the cluster to update configurations for. (required)

            body, list[str]: The configuration files to replace existing with. (required)

            

        Returns: Array[str]
        """

        allParams = ['id', 'body']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method updateConfigsForCluster" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/clusters/{id}/configs'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'PUT'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[str]')
        return responseObject

    def addCommandsForCluster(self, id, body, **kwargs):
        """Add new commands to a cluster

        Args:
            id, str: Id of the cluster to add commands to. (required)

            body, list[Command]: The commands to add. (required)

            

        Returns: Array[Command]
        """

        allParams = ['id', 'body']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method addCommandsForCluster" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/clusters/{id}/commands'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'POST'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[Command]')
        return responseObject

    def getCommandsForCluster(self, id, **kwargs):
        """Get the commands for a cluster

        Args:
            id, str: Id of the cluster to get commands for. (required)

            

        Returns: Array[Command]
        """

        allParams = ['id']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method getCommandsForCluster" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/clusters/{id}/commands'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'GET'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[Command]')
        return responseObject

    def updateCommandsForCluster(self, id, body, **kwargs):
        """Update the commands for an cluster

        Args:
            id, str: Id of the cluster to update commands for. (required)

            body, list[Command]: The commands to replace existing with. Should already be created (required)

            

        Returns: Array[Command]
        """

        allParams = ['id', 'body']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method updateCommandsForCluster" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/clusters/{id}/commands'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'PUT'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[Command]')
        return responseObject

    def removeAllCommandsForCluster(self, id, **kwargs):
        """Remove all commands from an cluster

        Args:
            id, str: Id of the cluster to delete from. (required)

            

        Returns: Array[Command]
        """

        allParams = ['id']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method removeAllCommandsForCluster" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/clusters/{id}/commands'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'DELETE'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[Command]')
        return responseObject

    def removeCommandForCluster(self, id, cmdId, **kwargs):
        """Remove a command from a cluster

        Args:
            id, str: Id of the cluster to delete from. (required)

            cmdId, str: The id of the command to remove. (required)

            

        Returns: Array[Command]
        """

        allParams = ['id', 'cmdId']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method removeCommandForCluster" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/clusters/{id}/commands/{cmdId}'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'DELETE'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        if ('cmdId' in params):
            replacement = str(self.apiClient.toPathValue(params['cmdId']))
            resourcePath = resourcePath.replace('{' + 'cmdId' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[Command]')
        return responseObject

    def addTagsForCluster(self, id, body, **kwargs):
        """Add new tags to a cluster

        Args:
            id, str: Id of the cluster to add configuration to. (required)

            body, list[str]: The tags to add. (required)

            

        Returns: Array[str]
        """

        allParams = ['id', 'body']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method addTagsForCluster" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/clusters/{id}/tags'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'POST'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[str]')
        return responseObject

    def getTagsForCluster(self, id, **kwargs):
        """Get the tags for a cluster

        Args:
            id, str: Id of the cluster to get tags for. (required)

            

        Returns: Array[str]
        """

        allParams = ['id']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method getTagsForCluster" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/clusters/{id}/tags'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'GET'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[str]')
        return responseObject

    def updateTagsForCluster(self, id, body, **kwargs):
        """Update tags for a cluster

        Args:
            id, str: Id of the cluster to update tags for. (required)

            body, list[str]: The tags to replace existing with. (required)

            

        Returns: Array[str]
        """

        allParams = ['id', 'body']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method updateTagsForCluster" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/clusters/{id}/tags'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'PUT'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[str]')
        return responseObject

    def removeAllTagsForCluster(self, id, **kwargs):
        """Remove all tags from a cluster

        Args:
            id, str: Id of the cluster to delete from. (required)

            

        Returns: Array[str]
        """

        allParams = ['id']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method removeAllTagsForCluster" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/clusters/{id}/tags'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'DELETE'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[str]')
        return responseObject

    def removeTagForCluster(self, id, tag, **kwargs):
        """Remove a tag from a cluster

        Args:
            id, str: Id of the cluster to delete from. (required)

            tag, str: The tag to remove. (required)

            

        Returns: Array[str]
        """

        allParams = ['id', 'tag']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method removeTagForCluster" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/clusters/{id}/tags/{tag}'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'DELETE'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        if ('tag' in params):
            replacement = str(self.apiClient.toPathValue(params['tag']))
            resourcePath = resourcePath.replace('{' + 'tag' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[str]')
        return responseObject

    def createCommand(self, body, **kwargs):
        """Create a command

        Args:
            body, Command: The command to create. (required)

            

        Returns: Command
        """

        allParams = ['body']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method createCommand" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/commands'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'POST'

        queryParams = {}
        headerParams = {}

        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Command')
        return responseObject

    def getCommands(self, **kwargs):
        """Find commands

        Args:
            name, str: Name of the command. (optional)

            userName, str: User who created the command. (optional)

            status, list[str]: The statuses of the commands to find. (optional)

            tag, list[str]: Tags for the cluster. (optional)

            page, int: The page to start on. (optional)

            limit, int: Max number of results per page. (optional)

            descending, bool: Whether results should be sorted in descending. Defaults to true. (optional)

            orderBy, str: The fields to order the results by, must not be collection fields. (optional)

            

        Returns: Array[Command]
        """

        allParams = ['name', 'userName', 'status', 'tag', 'page', 'limit']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method getCommands" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/commands'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'GET'

        queryParams = {}
        headerParams = {}

        if ('name' in params):
            queryParams['name'] = self.apiClient.toPathValue(params['name'])
        if ('userName' in params):
            queryParams['userName'] = self.apiClient.toPathValue(params['userName'])
        if ('status' in params):
            queryParams['status'] = self.apiClient.toPathValue(params['status'])
        if ('tag' in params):
            queryParams['tag'] = self.apiClient.toPathValue(params['tag'])
        if ('page' in params):
            queryParams['page'] = self.apiClient.toPathValue(params['page'])
        if ('limit' in params):
            queryParams['limit'] = self.apiClient.toPathValue(params['limit'])
        if ('descending' in params):
            queryParams['descending'] = self.apiClient.toPathValue(params['descending'])
        if ('orderBy' in params):
            queryParams['orderBy'] = self.apiClient.toPathValue(params['orderBy'])
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[Command]')
        return responseObject

    def deleteAllCommands(self, **kwargs):
        """Delete all commands

        Args:
            

        Returns: Array[Command]
        """

        allParams = []

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method deleteAllCommands" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/commands'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'DELETE'

        queryParams = {}
        headerParams = {}

        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[Command]')
        return responseObject

    def getCommand(self, id, **kwargs):
        """Find a command by id

        Args:
            id, str: Id of the command to get. (required)

            

        Returns: Command
        """

        allParams = ['id']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method getCommand" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/commands/{id}'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'GET'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Command')
        return responseObject

    def updateCommand(self, id, body, **kwargs):
        """Update a command

        Args:
            id, str: Id of the command to update. (required)

            body, Command: The command information to update. (required)

            

        Returns: Command
        """

        allParams = ['id', 'body']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method updateCommand" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/commands/{id}'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'PUT'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Command')
        return responseObject

    def deleteCommand(self, id, **kwargs):
        """Delete an comamnd

        Args:
            id, str: Id of the command to delete. (required)

            

        Returns: Command
        """

        allParams = ['id']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method deleteCommand" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/commands/{id}'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'DELETE'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Command')
        return responseObject

    def addConfigsForCommand(self, id, body, **kwargs):
        """Add new configuration files to a command

        Args:
            id, str: Id of the command to add configuration to. (required)

            body, list[str]: The configuration files to add. (required)

            

        Returns: Array[str]
        """

        allParams = ['id', 'body']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method addConfigsForCommand" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/commands/{id}/configs'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'POST'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[str]')
        return responseObject

    def getConfigsForCommand(self, id, **kwargs):
        """Get the configuration files for a command

        Args:
            id, str: Id of the command to get configurations for. (required)

            

        Returns: Array[str]
        """

        allParams = ['id']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method getConfigsForCommand" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/commands/{id}/configs'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'GET'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[str]')
        return responseObject

    def updateConfigsForCommand(self, id, body, **kwargs):
        """Update configuration files for an command

        Args:
            id, str: Id of the command to update configurations for. (required)

            body, list[str]: The configuration files to replace existing with. (required)

            

        Returns: Array[str]
        """

        allParams = ['id', 'body']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method updateConfigsForCommand" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/commands/{id}/configs'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'PUT'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[str]')
        return responseObject

    def removeAllConfigsForCommand(self, id, **kwargs):
        """Remove all configuration files from an command

        Args:
            id, str: Id of the command to delete from. (required)

            

        Returns: Array[str]
        """

        allParams = ['id']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method removeAllConfigsForCommand" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/commands/{id}/configs'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'DELETE'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[str]')
        return responseObject

    def addTagsForCommand(self, id, body, **kwargs):
        """Add new tags to a command

        Args:
            id, str: Id of the command to add configuration to. (required)

            body, list[str]: The tags to add. (required)

            

        Returns: Array[str]
        """

        allParams = ['id', 'body']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method addTagsForCommand" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/commands/{id}/tags'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'POST'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[str]')
        return responseObject

    def getTagsForCommand(self, id, **kwargs):
        """Get the tags for a command

        Args:
            id, str: Id of the command to get tags for. (required)

            

        Returns: Array[str]
        """

        allParams = ['id']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method getTagsForCommand" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/commands/{id}/tags'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'GET'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[str]')
        return responseObject

    def updateTagsForCommand(self, id, body, **kwargs):
        """Update tags for a command

        Args:
            id, str: Id of the command to update tags for. (required)

            body, list[str]: The tags to replace existing with. (required)

            

        Returns: Array[str]
        """

        allParams = ['id', 'body']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method updateTagsForCommand" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/commands/{id}/tags'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'PUT'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[str]')
        return responseObject

    def removeAllTagsForCommand(self, id, **kwargs):
        """Remove all tags from a command

        Args:
            id, str: Id of the command to delete from. (required)

            

        Returns: Array[str]
        """

        allParams = ['id']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method removeAllTagsForCommand" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/commands/{id}/tags'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'DELETE'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[str]')
        return responseObject

    def setApplicationForCommand(self, id, body, **kwargs):
        """Set the application for a command

        Args:
            id, str: Id of the command to set application for. (required)

            body, Application: The application to add. (required)

            

        Returns: Application
        """

        allParams = ['id', 'body']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method setApplicationForCommand" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/commands/{id}/application'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'POST'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Application')
        return responseObject

    def getApplicationForCommand(self, id, **kwargs):
        """Get the application for a command

        Args:
            id, str: Id of the command to get the application for. (required)

            

        Returns: Application
        """

        allParams = ['id']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method getApplicationForCommand" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/commands/{id}/application'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'GET'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Application')
        return responseObject

    def removeApplicationForCommand(self, id, **kwargs):
        """Remove an application from a command

        Args:
            id, str: Id of the command to delete from. (required)

            

        Returns: Application
        """

        allParams = ['id']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method removeApplicationForCommand" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/commands/{id}/application'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'DELETE'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Application')
        return responseObject

    def getClustersForCommand(self, id, **kwargs):
        """Get the clusters this command is associated with

        Args:
            id, str: Id of the command to get the clusters for. (required)

            

        Returns: Array[Cluster]
        """

        allParams = ['id']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method getClustersForCommand" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/commands/{id}/clusters'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'GET'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[Cluster]')
        return responseObject

    def removeTagForCommand(self, id, tag, **kwargs):
        """Remove a tag from a command

        Args:
            id, str: Id of the command to delete from. (required)

            tag, str: The tag to remove. (required)

            

        Returns: Array[str]
        """

        allParams = ['id', 'tag']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method removeTagForCommand" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/commands/{id}/tags/{tag}'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'DELETE'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        if ('tag' in params):
            replacement = str(self.apiClient.toPathValue(params['tag']))
            resourcePath = resourcePath.replace('{' + 'tag' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[str]')
        return responseObject

    def getJob(self, id, **kwargs):
        """Find a job by id

        Args:
            id, str: Id of the job to get. (required)

            

        Returns: Job
        """

        allParams = ['id']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method getJob" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/jobs/{id}'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'GET'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Job')
        return responseObject

    def killJob(self, id, **kwargs):
        """Delete a job

        Args:
            id, str: Id of the job. (required)

            

        Returns: Job
        """

        allParams = ['id']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method killJob" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/jobs/{id}'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'DELETE'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Job')
        return responseObject

    def getJobStatus(self, id, **kwargs):
        """Get the status of the job 

        Args:
            id, str: Id of the job. (required)

            

        Returns: str
        """

        allParams = ['id']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method getJobStatus" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/jobs/{id}/status'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'GET'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'str')
        return responseObject

    def getJobs(self, **kwargs):
        """Find jobs

        Args:
            id, str: Id of the job. (optional)

            name, str: Name of the job. (optional)

            userName, str: Name of the user who submitted the job. (optional)

            status, list[str]: Statuses of the jobs to fetch. (optional)

            tag, list[str]: Tags for the job. (optional)

            executionClusterName, str: Name of the cluster on which the job ran. (optional)

            executionClusterId, str: Id of the cluster on which the job ran. (optional)

            page, int: The page to start on. (optional)

            limit, int: Max number of results per page. (optional)

            descending, bool: Whether results should be sorted in descending. Defaults to true. (optional)

            orderBy, str: The fields to order the results by, must not be collection fields. (optional)

            

        Returns: Array[Job]
        """

        allParams = ['id', 'name', 'userName', 'status', 'tag', 'executionClusterName', 'executionClusterId', 'page',
                     'limit']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method getJobs" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/jobs'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'GET'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            queryParams['id'] = self.apiClient.toPathValue(params['id'])
        if ('name' in params):
            queryParams['name'] = self.apiClient.toPathValue(params['name'])
        if ('userName' in params):
            queryParams['userName'] = self.apiClient.toPathValue(params['userName'])
        if ('status' in params):
            queryParams['status'] = self.apiClient.toPathValue(params['status'])
        if ('tag' in params):
            queryParams['tag'] = self.apiClient.toPathValue(params['tag'])
        if ('executionClusterName' in params):
            queryParams['executionClusterName'] = self.apiClient.toPathValue(params['executionClusterName'])
        if ('executionClusterId' in params):
            queryParams['executionClusterId'] = self.apiClient.toPathValue(params['executionClusterId'])
        if ('page' in params):
            queryParams['page'] = self.apiClient.toPathValue(params['page'])
        if ('limit' in params):
            queryParams['limit'] = self.apiClient.toPathValue(params['limit'])
        if ('descending' in params):
            queryParams['descending'] = self.apiClient.toPathValue(params['descending'])
        if ('orderBy' in params):
            queryParams['orderBy'] = self.apiClient.toPathValue(params['orderBy'])
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[Job]')
        return responseObject

    def submitJob(self, body, **kwargs):
        """Submit a job

        Args:
            body, Job: Job object to run. (required)

            

        Returns: Job
        """

        allParams = ['body']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method submitJob" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/jobs'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'POST'

        queryParams = {}
        headerParams = {}

        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Job')
        return responseObject

    def addTagsForJob(self, id, body, **kwargs):
        """Add new tags to a job

        Args:
            id, str: Id of the job to add configuration to. (required)

            body, list[str]: The tags to add. (required)

            

        Returns: Array[str]
        """

        allParams = ['id', 'body']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method addTagsForJob" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/jobs/{id}/tags'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'POST'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[str]')
        return responseObject

    def getTagsForJob(self, id, **kwargs):
        """Get the tags for a job

        Args:
            id, str: Id of the job to get tags for. (required)

            

        Returns: Array[str]
        """

        allParams = ['id']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method getTagsForJob" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/jobs/{id}/tags'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'GET'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[str]')
        return responseObject

    def updateTagsForJob(self, id, body, **kwargs):
        """Update tags for a job

        Args:
            id, str: Id of the job to update tags for. (required)

            body, list[str]: The tags to replace existing with. (required)

            

        Returns: Array[str]
        """

        allParams = ['id', 'body']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method updateTagsForJob" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/jobs/{id}/tags'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'PUT'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[str]')
        return responseObject

    def removeAllTagsForJob(self, id, **kwargs):
        """Remove all tags from a job

        Args:
            id, str: Id of the job to delete from. (required)

            

        Returns: Array[str]
        """

        allParams = ['id']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method removeAllTagsForJob" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/jobs/{id}/tags'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'DELETE'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[str]')
        return responseObject

    def removeTagForJob(self, id, tag, **kwargs):
        """Remove a tag from a job

        Args:
            id, str: Id of the job to delete from. (required)

            tag, str: The tag to remove. (required)

            

        Returns: Array[str]
        """

        allParams = ['id', 'tag']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method removeTagForJob" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/jobs/{id}/tags/{tag}'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'DELETE'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        if ('tag' in params):
            replacement = str(self.apiClient.toPathValue(params['tag']))
            resourcePath = resourcePath.replace('{' + 'tag' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[str]')
        return responseObject

    def getApplication(self, id, **kwargs):
        """Find an application by id

        Args:
            id, str: Id of the application to get. (required)

            

        Returns: Application
        """

        allParams = ['id']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method getApplication" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/applications/{id}'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'GET'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Application')
        return responseObject

    def updateApplication(self, id, body, **kwargs):
        """Update an application

        Args:
            id, str: Id of the application to update. (required)

            body, Application: The application information to update. (required)

            

        Returns: Application
        """

        allParams = ['id', 'body']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method updateApplication" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/applications/{id}'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'PUT'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Application')
        return responseObject

    def deleteApplication(self, id, **kwargs):
        """Delete an application

        Args:
            id, str: Id of the application to delete. (required)

            

        Returns: Application
        """

        allParams = ['id']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method deleteApplication" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/applications/{id}'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'DELETE'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Application')
        return responseObject

    def getApplications(self, **kwargs):
        """Find applications

        Args:
            name, str: Name of the application. (optional)

            userName, str: User who created the application. (optional)

            status, list[str]: The status of the applications to get. (optional)

            tag, list[str]: Tags for the cluster. (optional)

            page, int: The page to start on. (optional)

            limit, int: Max number of results per page. (optional)

            descending, bool: Whether results should be sorted in descending. Defaults to true. (optional)

            orderBy, str: The fields to order the results by, must not be collection fields. (optional)

            

        Returns: Array[Application]
        """

        allParams = ['name', 'userName', 'status', 'tag', 'page', 'limit']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method getApplications" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/applications'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'GET'

        queryParams = {}
        headerParams = {}

        if ('name' in params):
            queryParams['name'] = self.apiClient.toPathValue(params['name'])
        if ('userName' in params):
            queryParams['userName'] = self.apiClient.toPathValue(params['userName'])
        if ('status' in params):
            queryParams['status'] = self.apiClient.toPathValue(params['status'])
        if ('tag' in params):
            queryParams['tag'] = self.apiClient.toPathValue(params['tag'])
        if ('page' in params):
            queryParams['page'] = self.apiClient.toPathValue(params['page'])
        if ('limit' in params):
            queryParams['limit'] = self.apiClient.toPathValue(params['limit'])
        if ('descending' in params):
            queryParams['descending'] = self.apiClient.toPathValue(params['descending'])
        if ('orderBy' in params):
            queryParams['orderBy'] = self.apiClient.toPathValue(params['orderBy'])
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[Application]')
        return responseObject

    def createApplication(self, body, **kwargs):
        """Create an application

        Args:
            body, Application: The application to create. (required)

            

        Returns: Application
        """

        allParams = ['body']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method createApplication" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/applications'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'POST'

        queryParams = {}
        headerParams = {}

        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Application')
        return responseObject

    def deleteAllApplications(self, **kwargs):
        """Delete all applications

        Args:
            

        Returns: Array[Application]
        """

        allParams = []

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method deleteAllApplications" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/applications'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'DELETE'

        queryParams = {}
        headerParams = {}

        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[Application]')
        return responseObject

    def addConfigsToApplication(self, id, body, **kwargs):
        """Add new configuration files to an application

        Args:
            id, str: Id of the application to add configuration to. (required)

            body, list[str]: The configuration files to add. (required)

            

        Returns: Array[str]
        """

        allParams = ['id', 'body']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method addConfigsToApplication" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/applications/{id}/configs'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'POST'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[str]')
        return responseObject

    def getConfigsForApplication(self, id, **kwargs):
        """Get the configuration files for an application

        Args:
            id, str: Id of the application to get configurations for. (required)

            

        Returns: Array[str]
        """

        allParams = ['id']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method getConfigsForApplication" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/applications/{id}/configs'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'GET'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[str]')
        return responseObject

    def updateConfigsForApplication(self, id, body, **kwargs):
        """Update configuration files for an application

        Args:
            id, str: Id of the application to update configurations for. (required)

            body, list[str]: The configuration files to replace existing with. (required)

            

        Returns: Array[str]
        """

        allParams = ['id', 'body']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method updateConfigsForApplication" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/applications/{id}/configs'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'PUT'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[str]')
        return responseObject

    def removeAllConfigsForApplication(self, id, **kwargs):
        """Remove all configuration files from an application

        Args:
            id, str: Id of the application to delete from. (required)

            

        Returns: Array[str]
        """

        allParams = ['id']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError(
                    "Got an unexpected keyword argument '%s' to method removeAllConfigsForApplication" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/applications/{id}/configs'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'DELETE'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[str]')
        return responseObject

    def addJarsForApplication(self, id, body, **kwargs):
        """Add new jar files to an application

        Args:
            id, str: Id of the application to add jar to. (required)

            body, list[str]: The jar files to add. (required)

            

        Returns: Array[str]
        """

        allParams = ['id', 'body']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method addJarsForApplication" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/applications/{id}/jars'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'POST'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[str]')
        return responseObject

    def getJarsForApplication(self, id, **kwargs):
        """Get the jars for an application

        Args:
            id, str: Id of the application to get the jars for. (required)

            

        Returns: Array[str]
        """

        allParams = ['id']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method getJarsForApplication" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/applications/{id}/jars'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'GET'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[str]')
        return responseObject

    def updateJarsForApplication(self, id, body, **kwargs):
        """Update jar files for an application

        Args:
            id, str: Id of the application to update configurations for. (required)

            body, list[str]: The jar files to replace existing with. (required)

            

        Returns: Array[str]
        """

        allParams = ['id', 'body']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method updateJarsForApplication" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/applications/{id}/jars'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'PUT'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[str]')
        return responseObject

    def removeAllJarsForApplication(self, id, **kwargs):
        """Remove all jar files from an application

        Args:
            id, str: Id of the application to delete from. (required)

            

        Returns: Array[str]
        """

        allParams = ['id']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method removeAllJarsForApplication" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/applications/{id}/jars'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'DELETE'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[str]')
        return responseObject

    def addTagsForApplication(self, id, body, **kwargs):
        """Add new tags to a application

        Args:
            id, str: Id of the application to add configuration to. (required)

            body, list[str]: The tags to add. (required)

            

        Returns: Array[str]
        """

        allParams = ['id', 'body']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method addTagsForApplication" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/applications/{id}/tags'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'POST'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[str]')
        return responseObject

    def getTagsForApplication(self, id, **kwargs):
        """Get the tags for a application

        Args:
            id, str: Id of the application to get tags for. (required)

            

        Returns: Array[str]
        """

        allParams = ['id']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method getTagsForApplication" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/applications/{id}/tags'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'GET'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[str]')
        return responseObject

    def updateTagsForApplication(self, id, body, **kwargs):
        """Update tags for a application

        Args:
            id, str: Id of the application to update tags for. (required)

            body, list[str]: The tags to replace existing with. (required)

            

        Returns: Array[str]
        """

        allParams = ['id', 'body']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method updateTagsForApplication" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/applications/{id}/tags'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'PUT'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[str]')
        return responseObject

    def removeAllTagsForApplication(self, id, **kwargs):
        """Remove all tags from a application

        Args:
            id, str: Id of the application to delete from. (required)

            

        Returns: Array[str]
        """

        allParams = ['id']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method removeAllTagsForApplication" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/applications/{id}/tags'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'DELETE'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[str]')
        return responseObject

    def getCommandsForApplication(self, id, **kwargs):
        """Get the commands this application is associated with

        Args:
            id, str: Id of the application to get the commands for. (required)

            

        Returns: Array[Command]
        """

        allParams = ['id']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method getCommandsForApplication" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/applications/{id}/commands'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'GET'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[Command]')
        return responseObject

    def removeTagForApplication(self, id, tag, **kwargs):
        """Remove a tag from a application

        Args:
            id, str: Id of the application to delete from. (required)

            tag, str: The tag to remove. (required)

            

        Returns: Array[str]
        """

        allParams = ['id', 'tag']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' to method removeTagForApplication" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/v2/config/applications/{id}/tags/{tag}'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'DELETE'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        if ('tag' in params):
            replacement = str(self.apiClient.toPathValue(params['tag']))
            resourcePath = resourcePath.replace('{' + 'tag' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if not response:
            return None

        responseObject = self.apiClient.deserialize(response, 'Array[str]')
        return responseObject