from __future__ import absolute_import, division, print_function, unicode_literals

import os
import unittest

from mock import patch
from nose.tools import assert_equals, assert_raises


assert_equals.__self__.maxDiff = None


import pygenie


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestingGenieJob(unittest.TestCase):
    """Test GenieJob."""

    def test_default_command_tag(self):
        """Test GenieJob default command tags."""

        job = pygenie.jobs.GenieJob()

        assert_equals(
            job.get('default_command_tags'),
            [u'type:genie']
        )

    def test_cmd_args_explicit(self):
        """Test GenieJob explicit cmd args."""

        job = pygenie.jobs.GenieJob() \
            .command_arguments('explicitly stating command args')

        assert_equals(
            job.cmd_args,
            u'explicitly stating command args'
        )

    def test_cmd_args_constructed(self):
        """Test GenieJob constructed cmd args."""

        with assert_raises(pygenie.exceptions.GenieJobError) as cm:
            pygenie.jobs.GenieJob().cmd_args


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestingGenieJobRepr(unittest.TestCase):
    """Test GenieJob repr."""

    @patch('pygenie.jobs.core.is_file')
    def test_repr(self, is_file):
        """Test GenieJob repr."""

        is_file.return_value = True

        job = pygenie.jobs.GenieJob() \
            .applications('app1') \
            .applications('app2') \
            .archive(False) \
            .cluster_tags('cluster1') \
            .cluster_tags('cluster2') \
            .command_arguments('genie job repr args') \
            .command_tags('cmd1') \
            .command_tags('cmd2') \
            .dependencies('/dep1') \
            .dependencies('/dep2') \
            .description('description') \
            .disable_archive() \
            .email('jsmith@email.com') \
            .genie_url('http://asdfasdf') \
            .group('group1') \
            .job_id('geniejob_repr') \
            .job_name('geniejob_repr') \
            .job_version('1.1.1') \
            .setup_file('/setup.sh') \
            .tags('tag1') \
            .tags('tag2') \
            .timeout(999) \
            .username('jsmith')

        assert_equals(
            str(job),
            u'GenieJob()'
            u'.applications("app1")'
            u'.applications("app2")'
            u'.archive(False)'
            u'.cluster_tags("cluster1")'
            u'.cluster_tags("cluster2")'
            u'.command_arguments("genie job repr args")'
            u'.command_tags("cmd1")'
            u'.command_tags("cmd2")'
            u'.dependencies("/dep1")'
            u'.dependencies("/dep2")'
            u'.description("description")'
            u'.email("jsmith@email.com")'
            u'.genie_url("http://asdfasdf")'
            u'.group("group1")'
            u'.job_id("geniejob_repr")'
            u'.job_name("geniejob_repr")'
            u'.job_version("1.1.1")'
            u'.setup_file("/setup.sh")'
            u'.tags("tag1")'
            u'.tags("tag2")'
            u'.timeout(999)'
            u'.username("jsmith")'
        )


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestingGenieJobAdapters(unittest.TestCase):
    """Test adapting GenieJob to different clients."""

    def setUp(self):
        self.dirname = os.path.dirname(os.path.realpath(__file__))

    def test_genie3_payload(self):
        """Test GenieJob payload for Genie 3."""

        with patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'}):
            genie3_conf = pygenie.conf.GenieConf() \
                .load_config_file(os.path.join(self.dirname, 'genie3.ini'))

        job = pygenie.jobs.GenieJob(genie3_conf) \
            .applications(['applicationid1']) \
            .cluster_tags('type:cluster1') \
            .command_arguments('command args for geniejob') \
            .command_tags('type:geniecmd') \
            .dependencies(['/file1', '/file2']) \
            .description('this job is to test geniejob adapter') \
            .archive(False) \
            .email('jdoe@email.com') \
            .genie_url('http://fdsafdsa') \
            .group('geniegroup1') \
            .job_id('geniejob1') \
            .job_name('testing_adapting_geniejob') \
            .tags('tag1, tag2') \
            .timeout(100) \
            .username('jdoe') \
            .job_version('0.0.1alpha')

        assert_equals(
            pygenie.adapter.genie_3.get_payload(job),
            {
                u'applications': [u'applicationid1'],
                u'attachments': [],
                u'clusterCriterias': [{u'tags': [u'type:cluster1']}],
                u'commandArgs': u'command args for geniejob',
                u'commandCriteria': [u'type:geniecmd'],
                u'dependencies': [u'/file1', u'/file2'],
                u'description': u'this job is to test geniejob adapter',
                u'disableLogArchival': True,
                u'email': u'jdoe@email.com',
                u'group': u'geniegroup1',
                u'id': u'geniejob1',
                u'name': u'testing_adapting_geniejob',
                u'setupFile': None,
                u'tags': [u'tag1', u'tag2'],
                u'timeout': 100,
                u'user': u'jdoe',
                u'version': u'0.0.1alpha'
            }
        )
