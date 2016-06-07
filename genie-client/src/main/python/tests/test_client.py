"""
Netflix test for pygenie.client.py
"""

import types
import unittest

import json
import os
import re
import responses

from mock import patch
from nose.tools import assert_equals
from pygenie.client import Genie
from pygenie.conf import GenieConf

GENIE_INI = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'genie.ini')
GENIE_CONF = GenieConf().load_config_file(GENIE_INI)
GENIE_URL = GENIE_CONF.genie.url

# Used to generate basic responses from genie
@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestGenie(unittest.TestCase):
    "Test related to the bdp EMR library"

    def setUp(self):
        self.genie = Genie(GENIE_CONF)
        self.cluster_name = 'test'
        self.path = re.compile(GENIE_URL + '/api/v3/.*')

    def test_properties(self):
        "Testing that relative paths exists as methods"
        assert self.genie.path_application
        assert self.genie.path_command
        assert self.genie.path_job
        assert self.genie.path_cluster

    @responses.activate
    def test_get_clusters(self):
        "Testing that get_clusters returns a generator"
        responses.add(responses.GET, self.path)
        clusters = self.genie.get_clusters()
        assert isinstance(clusters, types.GeneratorType), "Did not return a generator"

    @responses.activate
    def test_get_applications(self):
        "Testing that get_applications returns a generator"
        responses.add(responses.GET, self.path)
        applications = self.genie.get_applications()
        assert isinstance(applications, types.GeneratorType), "Did not return a generator"

    @responses.activate
    def test_get_jobs(self):
        "Testing that get_jobs returns a generator"
        responses.add(responses.GET, self.path)
        jobs = self.genie.get_jobs()
        assert isinstance(jobs, types.GeneratorType), "Did not return a generator"

    @responses.activate
    def test_get_commands(self):
        "Testing that get_commands returns a generator"
        responses.add(responses.GET, self.path)
        commands = self.genie.get_commands()
        assert isinstance(commands, types.GeneratorType), "Did not return a generator"


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestCommand(unittest.TestCase):

    def setUp(self):
        self.genie = Genie(GENIE_CONF)
        self.path = re.compile(GENIE_URL + '/api/v3/commands.*')
        self.command = {
            "id": "test_command",
            "created": "2016-05-09T20:50:56.000Z",
            "updated": "2016-05-11T00:18:26.000Z",
            "tags": ["genie.id:test_command", "type:test", "ver:1", "genie.name:test"],
            "version": "1",
            "user": "testuser",
            "name": "presto",
            "description": None,
            "configs": [],
            "setupFile": None,
            "status": "ACTIVE",
            "executable": "sleep 1",
            "checkDelay": 5000,
            "page": {
                "totalPages": 1,
                "number": 1,
            },
        }

    @responses.activate
    def test_create_command(self):
        location = GENIE_URL + '/' + self.command['id']
        responses.add(responses.POST, self.path, status=201,
                      adding_headers={'Location': location})
        new_command_name = self.genie.create_command(self.command)
        assert_equals(new_command_name, self.command['id'])

    @responses.activate
    def test_patch_command(self):
        patch1 = {'op': 'replace', 'path': '/version', 'value': '1.1'}
        patch2 = {'op': 'replace', 'path': '/user', 'value': 'test'}
        responses.add(responses.PATCH, self.path)
        self.genie.patch_command(self.command['id'], [patch1, patch2])

    @responses.activate
    def test_get_command(self):
        "Testing that get_commands returns all required keys"
        responses.add(responses.GET, self.path, body=json.dumps(self.command))
        command = self.genie.get_command(self.command['id'])
        assert_equals(command, self.command)

    @responses.activate
    def test_get_command_returns_none(self):
        responses.add(responses.GET, self.path, status=404)
        cluster = self.genie.get_command('does_not_exist')
        assert_equals(cluster, None)

    @responses.activate
    def test_get_command_with_params(self):
        resp = {
            "_embedded": {"commandList": [self.command]},
            "page": {"totalPages": 0, "number": 0}
        }
        resp = json.dumps(resp)
        responses.add(responses.GET, self.path, body=resp)
        commands = [i for i in self.genie.get_commands(
            filters={'name': self.command['name']})]
        assert len(commands) == 1, "Didn't return the correct amount of commands"
        assert_equals(commands[0], self.command)

    @responses.activate
    def test_update_command(self):
        responses.add(responses.PUT, self.path, status=204)
        self.genie.update_command(self.command)

    @responses.activate
    def test_delete_command(self):
        responses.add(responses.DELETE, self.path, status=204)
        self.genie.delete_command(self.command['id'])

    @responses.activate
    def test_create_and_delete_all_commands(self):
        responses.add(responses.DELETE, self.path, status=204)
        self.genie.delete_all_commands()

    @responses.activate
    def test_add_applications_for_command(self):
        responses.add(responses.POST, self.path, status=204)
        self.genie.add_applications_for_command(self.command['id'], ['test'])

    @responses.activate
    def test_add_configs_for_command(self):
        responses.add(responses.POST, self.path, status=204)
        self.genie.add_configs_for_command(self.command['id'], ['test'])

    @responses.activate
    def test_add_tags_for_command(self):
        responses.add(responses.POST, self.path, status=204)
        self.genie.add_tags_for_command(self.command['id'], ['tag:test'])

    @responses.activate
    def test_get_applications_for_command(self):
        responses.add(responses.GET, self.path, body='["app1", "app2"]')
        apps = self.genie.get_applications_for_command('test')
        assert len(apps) == 2, "Did not return enough apps for command"

    @responses.activate
    def test_get_clusters_for_command(self):
        responses.add(responses.GET, self.path, body='["test1", "test2"]')
        clusters = self.genie.get_clusters_for_command(self.command['id'])
        assert len(clusters) == 2, "Did not return enough commands"
        assert "test1" in clusters, "Did not return the right clusters"

    @responses.activate
    def test_get_configs_for_command(self):
        responses.add(responses.GET, self.path, body='["conf1", "conf2"]')
        configs = self.genie.get_configs_for_command(self.command['id'])
        assert len(configs) == 2, "Did not return enough configs"
        assert "conf1" in configs, "did not return the right configs"

    @responses.activate
    def test_get_tags_for_command(self):
        responses.add(responses.GET, self.path, body='["tag:1", "tag:2"]')
        tags = self.genie.get_tags_for_command(self.command['id'])
        assert len(tags) == 2, "Did not return enough tags"
        assert "tag:1" in tags, "Did not return the right tags"

    @responses.activate
    def test_remove_all_applications_for_command(self):
        responses.add(responses.DELETE, self.path, status=204)
        self.genie.remove_all_applications_for_command(self.command['id'])

    @responses.activate
    def test_remove_all_configs_for_command(self):
        responses.add(responses.DELETE, self.path, status=204)
        self.genie.remove_all_configs_for_command(self.command['id'])

    @responses.activate
    def test_remove_all_tags_for_command(self):
        responses.add(responses.DELETE, self.path, status=204)
        self.genie.remove_all_tags_for_command(self.command['id'])

    @responses.activate
    def test_remove_application_for_command(self):
        responses.add(responses.DELETE, self.path, status=204)
        self.genie.remove_application_for_command(self.command['id'], 'test')

    @responses.activate
    def test_remove_tags_for_command(self):
        responses.add(responses.DELETE, self.path, status=204)
        self.genie.remove_tags_for_command(self.command['id'], 'test')

    @responses.activate
    def test_set_application_for_command(self):
        responses.add(responses.PUT, self.path, status=204)
        self.genie.set_application_for_command('test', ['app1', 'app2'])

    @responses.activate
    def test_update_configs_for_command(self):
        responses.add(responses.PUT, self.path, status=204)
        self.genie.update_configs_for_command('test', ['conf1', 'conf2'])

    @responses.activate
    def test_update_tags_for_command(self):
        responses.add(responses.PUT, self.path, status=204)
        self.genie.update_tags_for_command('test', ['tag:test', 'tag2:test2'])


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestApplication(unittest.TestCase):

    def setUp(self):
        self.genie = Genie(GENIE_CONF)
        self.path = re.compile(GENIE_URL + '/api/v3/applications.*')
        self.application = {
            "id": "test",
            "created": "2016-05-10T23:37:24.000Z",
            "updated": "2016-05-11T00:18:23.000Z",
            "tags": ["type:hadoop", "genie.id:test", "genie.name:test"],
            "version": "1",
            "user": "testuser",
            "name": "test",
            "description": "Test command",
            "configs": [],
            "setupFile": "s3://test-bucket/setup.sh",
            "dependencies": ["s3://test-bucket/test.tgz"],
            "status": "ACTIVE",
            "page": {
                "totalPages": 1,
                "number": 1,
            },
        }

    @responses.activate
    def test_create_application(self):
        location = GENIE_URL + '/' + self.application['id']
        responses.add(responses.POST, self.path, status=201,
                      adding_headers={'Location': location})
        app_id = self.genie.create_application(self.application)
        assert_equals(app_id, self.application['id'])

    @responses.activate
    def test_delete_application(self):
        responses.add(responses.DELETE, self.path, status=204)
        self.genie.delete_application(self.application['id'])

    @responses.activate
    def test_remove_all_application_configs(self):
        responses.add(responses.DELETE, self.path, status=204)
        self.genie.remove_all_application_configs(self.application['id'])

    @responses.activate
    def test_update_application(self):
        responses.add(responses.PUT, self.path, status=204)
        self.genie.delete_application(self.application['id'], self.application)

    @responses.activate
    def test_patch_application(self):
        patch1 = {'op': 'replace', 'path': '/version', 'value': '1.1'}
        patch2 = {'op': 'replace', 'path': '/user', 'value': 'test'}
        responses.add(responses.PATCH, self.path)
        self.genie.patch_application(self.application['id'], [patch1, patch2])

    @responses.activate
    def test_get_application(self):
        responses.add(responses.GET, self.path, body=json.dumps(self.application))
        app = self.genie.get_application(self.application['id'])
        assert_equals(app, self.application)

    @responses.activate
    def test_get_application_returns_none(self):
        responses.add(responses.GET, self.path, status=404)
        cluster = self.genie.get_application('does_not_exist')
        assert_equals(cluster, None)

    @responses.activate
    def test_tags_for_application(self):
        tags = ['test:true', 'region:us-east-1', 'tag:very-long-tag-name']
        responses.add(responses.GET, self.path, body=json.dumps(tags))
        new_tags = self.genie.get_application(self.application['id'])
        assert_equals(tags, new_tags)

    @responses.activate
    def test_update_application(self):
        responses.add(responses.PUT, self.path, status=204)
        self.genie.update_application(self.application)


    @responses.activate
    def test_update_configs_for_application(self):
        responses.add(responses.PUT, self.path, status=204)
        self.genie.update_configs_for_application(self.application['id'],
                                                  ['conf1', 'conf2'])

    @responses.activate
    def test_update_dependencies_for_application(self):
        responses.add(responses.PUT, self.path, status=204)
        self.genie.update_dependencies_for_application(self.application['id'],
                                                       ['dep1', 'dep2'])

    @responses.activate
    def test_update_tags_for_application(self):
        responses.add(responses.PUT, self.path, status=204)
        self.genie.update_tags_for_application(self.application['id'],
                                               ['tag:test', 'genie:true'])

    @responses.activate
    def test_add_dependencies_for_application(self):
        responses.add(responses.POST, self.path, status=204)
        self.genie.add_dependencies_for_application(self.application['id'],
                                                    ['conf1', 'conf2'])

    @responses.activate
    def test_add_tags_for_application(self):
        responses.add(responses.POST, self.path, status=204)
        self.genie.add_tags_for_application(self.application['id'],
                                            ['tag:test', 'genie:true'])

    @responses.activate
    def test_remove_all_dependencies_for_application(self):
        responses.add(responses.DELETE, self.path, status=204)
        self.genie.remove_all_dependencies_for_application(self.application['id'])

    @responses.activate
    def test_remove_all_tags_for_application(self):
        responses.add(responses.DELETE, self.path, status=204)
        self.genie.remove_all_tags_for_application(self.application['id'])

    @responses.activate
    def test_remove_tag_for_application(self):
        responses.add(responses.DELETE, self.path, status=204)
        self.genie.remove_tag_for_application(self.application['id'], ['tag:true'])

    @responses.activate
    def test_get_commands_for_application(self):
        cmds = [{'id': 'test1'}, {'id':'test2', 'name':'test'}]
        responses.add(responses.GET, self.path, body=json.dumps(cmds))
        commands = self.genie.get_commands_for_application(self.application['id'])
        assert_equals(cmds, commands)

    @responses.activate
    def test_get_configs_for_application(self):
        confs = [{'id': 'test1'}, {'id':'test2', 'name':'test'}]
        responses.add(responses.GET, self.path, body=json.dumps(confs))
        configs = self.genie.get_configs_for_application(self.application['id'])
        assert_equals(confs, configs)

    @responses.activate
    def test_get_dependencies_for_application(self):
        deps = [{'id': 'test1'}, {'id':'test2', 'name':'test'}]
        responses.add(responses.GET, self.path, body=json.dumps(deps))
        dependencies = self.genie.get_dependencies_for_application(self.application['id'])
        assert_equals(deps, dependencies)


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestCluster(unittest.TestCase):

    def setUp(self):
        self.genie = Genie(GENIE_CONF)
        self.path = re.compile(GENIE_URL + '/api/v3/clusters.*')
        self.cluster = {
            "id": "presto_test_20160318",
            "created": "2016-04-12T19:52:10.000Z",
            "updated": "2016-05-09T20:52:53.000Z",
            "tags": ["genie.name:test", "ver:1", "type:test"],
            "version": "1",
            "user": "testuser",
            "name": "testcluster",
            "description": "Test Cluster",
            "configs": [],
            "setupFile": "s3://test-bucket/cluster.sh",
            "status": "UP",
            "page": {
                "totalPages": 1,
                "number": 1,
            },
        }

    @responses.activate
    def test_get_cluster(self):
        responses.add(responses.GET, self.path, body=json.dumps(self.cluster))
        cluster = self.genie.get_cluster(self.cluster['id'])
        assert_equals(cluster, self.cluster)

    @responses.activate
    def test_get_cluster_returns_none(self):
        responses.add(responses.GET, self.path, status=404)
        cluster = self.genie.get_cluster(self.cluster['id'])
        assert_equals(cluster, None)

    @responses.activate
    def test_get_cluster_returns_none(self):
        responses.add(responses.GET, self.path, status=404)
        cluster = self.genie.get_cluster(self.cluster['id'])
        assert_equals(cluster, None)

    @responses.activate
    def test_get_commands_for_cluster(self):
        cmds = [{'id': 'test1'}, {'id':'test2', 'name':'test'}]
        responses.add(responses.GET, self.path, body=json.dumps(cmds))
        commands = self.genie.get_commands_for_cluster(self.cluster['id'])
        assert_equals(cmds, commands)

    @responses.activate
    def test_get_tags_for_cluster(self):
        responses.add(responses.GET, self.path, body='["tag:true", "test:false"]')
        tags = self.genie.get_tags_for_cluster(self.cluster['id'])
        assert len(tags) == 2, "Did not return all of the tags for cluster"
        assert "tag:true" in tags, "Did not return the correct tags for cluster"

    @responses.activate
    def test_add_commands_for_cluster(self):
        responses.add(responses.POST, self.path, status=204)
        self.genie.add_commands_for_cluster(self.cluster['id'], ['cmd1', 'cmd2'])

    @responses.activate
    def test_add_configs_for_cluster(self):
        responses.add(responses.POST, self.path, status=204)
        self.genie.add_configs_for_cluster(self.cluster['id'], ['c1', 'c2'])


    @responses.activate
    def test_add_tags_for_cluster(self):
        responses.add(responses.POST, self.path, status=204)
        self.genie.add_tags_for_cluster(self.cluster['id'], ['t:1', 't:2'])

    @responses.activate
    def test_create_cluster(self):
        location = GENIE_URL + '/' + self.cluster['id']
        responses.add(responses.POST, self.path, status=201,
                      adding_headers={'Location': location})
        cluster_id = self.genie.create_cluster(self.cluster)
        assert_equals(cluster_id, self.cluster['id'])

    @responses.activate
    def test_delete_all_clusters(self):
        responses.add(responses.DELETE, self.path, status=204)
        self.genie.delete_all_clusters()

    @responses.activate
    def test_delete_cluster(self):
        responses.add(responses.DELETE, self.path, status=204)
        self.genie.delete_cluster(self.cluster['id'])

    @responses.activate
    def test_patch_cluster(self):
        patch1 = {'op': 'replace', 'path': '/version', 'value': '1.1'}
        patch2 = {'op': 'replace', 'path': '/user', 'value': 'test'}
        responses.add(responses.PATCH, self.path)
        self.genie.patch_cluster(self.cluster['id'], [patch1, patch2])

    @responses.activate
    def test_remove_all_commands_for_cluster(self):
        responses.add(responses.DELETE, self.path, status=204)
        self.genie.remove_all_commands_for_cluster(self.cluster['id'])

    @responses.activate
    def test_remove_all_configs_for_cluster(self):
        responses.add(responses.DELETE, self.path, status=204)
        self.genie.remove_all_configs_for_cluster(self.cluster['id'])

    @responses.activate
    def test_remove_all_tags_for_cluster(self):
        responses.add(responses.DELETE, self.path, status=204)
        self.genie.remove_all_tags_for_cluster(self.cluster['id'])

    @responses.activate
    def test_remove_commands_for_cluster(self):
        responses.add(responses.DELETE, self.path, status=204)
        self.genie.remove_all_commands_for_cluster(self.cluster['id'])

    @responses.activate
    def test_remove_tag_for_cluster(self):
        responses.add(responses.DELETE, self.path, status=204)
        self.genie.remove_tag_for_cluster(self.cluster['id'], 'test:true')

    @responses.activate
    def test_set_commands_for_cluster(self):
        responses.add(responses.PUT, self.path, status=204)
        self.genie.set_commands_for_cluster(self.cluster['id'],
                                            ['cmd1', 'cmd2'])

    @responses.activate
    def test_update_cluster(self):
        responses.add(responses.PUT, self.path, status=204)
        self.genie.update_cluster(self.cluster['id'], self.cluster)

    @responses.activate
    def test_update_commands_for_cluster(self):
        responses.add(responses.PUT, self.path, status=204)
        cmds = [{'id': 'test'}, {'id': 'test2'}]
        self.genie.update_commands_for_cluster(self.cluster['id'], cmds)

    @responses.activate
    def test_update_configs_for_cluster(self):
        responses.add(responses.PUT, self.path, status=204)
        confs = [{'id': 'test'}, {'id': 'test2'}]
        self.genie.update_configs_for_cluster(self.cluster['id'], confs)

    @responses.activate
    def test_update_tags_for_cluster(self):
        responses.add(responses.PUT, self.path, status=204)
        tags = ['test:true', 'version:1.0', 'prod:false']
        self.genie.update_tags_for_cluster(self.cluster['id'], tags)


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestJob(unittest.TestCase):

    def setUp(self):
        self.genie = Genie(GENIE_CONF)
        self.path = re.compile(GENIE_URL + '/api/v3/jobs.*')
        self.job = {
            "id": "5afdcab5-16df-11e6-b78d-6003088f363e",
            "created": "2016-05-10T18:45:29.000Z",
            "updated": "2016-05-10T19:06:18.000Z",
            "tags": [],
            "version": "1.0",
            "user": "tst",
            "name": "test_command_name",
            "description": "Test command",
            "commandArgs": "-f presto.q",
            "status": "SUCCEEDED",
            "statusMsg": "Job finished successfully.",
            "started": "2016-05-10T18:45:30.000Z",
            "finished": "2016-05-10T19:06:18.000Z",
            "archiveLocation": "s3://test-bucket/test.tar.gz",
            "clusterName": "test_cluster",
            "commandName": "test_command",
            "runtime": "TEST005",
        }

    @responses.activate
    def test_get_job(self):
        responses.add(responses.GET, self.path, body=json.dumps(self.job))
        job = self.genie.get_job(self.job['id'])
        assert_equals(job, self.job)

    @responses.activate
    def test_get_job_returns_none(self):
        responses.add(responses.GET, self.path, status=404)
        cluster = self.genie.get_job('does_not_exist')
        assert_equals(cluster, None)

    @responses.activate
    def test_get_job_applications(self):
        apps = ['app1', 'app2']
        responses.add(responses.GET, self.path, body=json.dumps(apps))
        new_apps = self.genie.get_job_applications(self.job['id'])
        assert_equals(apps, new_apps)

    @responses.activate
    def test_get_job_cluster(self):
        cluster = {'id': 'test'}
        responses.add(responses.GET, self.path, body=json.dumps(cluster))
        new_cluster = self.genie.get_job_cluster(self.job['id'])
        assert_equals(cluster, new_cluster)

    @responses.activate
    def test_get_job_command(self):
        command = {'id': 'test'}
        responses.add(responses.GET, self.path, body=json.dumps(command))
        new_command = self.genie.get_job_command(self.job['id'])
        assert_equals(command, new_command)

    @responses.activate
    def test_get_job_execution(self):
        execution = {'id': 'test'}
        responses.add(responses.GET, self.path, body=json.dumps(execution))
        new_execution = self.genie.get_job_execution(self.job['id'])
        assert_equals(execution, new_execution)

    @responses.activate
    def test_get_job_output(self):
        output = {'id': 'test'}
        responses.add(responses.GET, self.path, body=json.dumps(output))
        new_output = self.genie.get_job_output(self.job['id'])
        assert_equals(output, new_output)

    @responses.activate
    def test_get_job_request(self):
        request = {'id': 'test'}
        responses.add(responses.GET, self.path, body=json.dumps(request))
        new_request = self.genie.get_job_request(self.job['id'])
        assert_equals(request, new_request)

    @responses.activate
    def test_get_job_status(self):
        status = {'id': 'test'}
        responses.add(responses.GET, self.path, body=json.dumps(status))
        new_status = self.genie.get_job_status(self.job['id'])
        assert_equals(status, new_status)

    @responses.activate
    def test_patch_job(self):
        patch1 = {'op': 'replace', 'path': '/version', 'value': '1.1'}
        patch2 = {'op': 'replace', 'path': '/user', 'value': 'test'}
        responses.add(responses.PATCH, self.path)
        self.genie.patch_job(self.job['id'], [patch1, patch2])


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestGeniepyAPI(unittest.TestCase):
    "Test that all required APIs are available through geniepy"

    def setUp(self):
        self.genie = Genie(GENIE_CONF)
        self.required_apis = [
            'add_applications_for_command',
            'add_commands_for_cluster',
            'add_configs_for_cluster',
            'add_configs_for_command',
            'add_configs_to_application',
            'add_dependencies_for_application',
            'add_tags_for_application',
            'add_tags_for_cluster',
            'add_tags_for_command',
            'create_application',
            'create_cluster',
            'create_command',
            'delete_all_applications',
            'delete_all_clusters',
            'delete_all_commands',
            'delete_application',
            'delete_cluster',
            'delete_command',
            'get_all_configs_for_cluster',
            'get_application',
            'get_applications',
            'get_applications_for_command',
            'get_cluster',
            'get_clusters',
            'get_clusters_for_command',
            'get_command',
            'get_commands',
            'get_commands_for_cluster',
            'get_configs_for_application',
            'get_configs_for_command',
            'get_dependencies_for_application',
            'get_job_applications',
            'get_job_cluster',
            'get_job_command',
            'get_job_execution',
            'get_job_output',
            'get_job_request',
            'get_job_status',
            'get_tags_for_application',
            'get_tags_for_cluster',
            'get_tags_for_command',
            'patch_application',
            'patch_cluster',
            'patch_command',
            'remove_all_application_configs',
            'remove_all_applications_for_command',
            'remove_all_commands_for_cluster',
            'remove_all_configs_for_cluster',
            'remove_all_configs_for_command',
            'remove_all_dependencies_for_application',
            'remove_all_tags_for_application',
            'remove_all_tags_for_cluster',
            'remove_all_tags_for_command',
            'remove_application_for_command',
            'remove_commands_for_cluster',
            'remove_tag_for_application',
            'remove_tag_for_cluster',
            'remove_tag_for_cluster',
            'remove_tags_for_command',
            'set_application_for_command',
            'set_commands_for_cluster',
            'submit_job',
            'update_application',
            'update_cluster',
            'update_command',
            'update_commands_for_cluster',
            'update_configs_for_application',
            'update_configs_for_cluster',
            'update_configs_for_command',
            'update_dependencies_for_application',
            'update_tags_for_application',
            'update_tags_for_cluster',
            'update_tags_for_command',
        ]

    def test_available_apis(self):
        "Test that all required APIs are available by the library"
        missing = []
        for api in self.required_apis:
            if not hasattr(self.genie, api):
                missing.append(api)
        if missing:
            self.fail('Missing required apis: %s' % missing)

def main():
    "Run the BdpMaint unit tests"
    unittest.main()

if __name__ == '__main__':
    main()
