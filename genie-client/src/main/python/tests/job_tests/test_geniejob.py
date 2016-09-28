from __future__ import absolute_import, division, print_function, unicode_literals

import os
import unittest

from mock import patch
from nose.tools import assert_equals, assert_raises

import pygenie

from ..utils import FakeRunningJob


assert_equals.__self__.maxDiff = None


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
            .genie_email('jsmith@email.com') \
            .genie_setup_file('/setup.sh') \
            .genie_timeout(999) \
            .genie_url('http://asdfasdf') \
            .genie_username('jsmith') \
            .group('group1') \
            .job_id('geniejob_repr') \
            .job_name('geniejob_repr') \
            .job_version('1.1.1') \
            .parameter('param1', 'pval1') \
            .parameter('param2', 'pval2') \
            .parameters(param3='pval3', param4='pval4') \
            .tags('tag1') \
            .tags('tag2')

        assert_equals(
            str(job),
            '.'.join([
                u'GenieJob()',
                u'applications("app1")',
                u'applications("app2")',
                u'archive(False)',
                u'cluster_tags("cluster1")',
                u'cluster_tags("cluster2")',
                u'command_arguments("genie job repr args")',
                u'command_tags("cmd1")',
                u'command_tags("cmd2")',
                u'dependencies("/dep1")',
                u'dependencies("/dep2")',
                u'description("description")',
                u'genie_email("jsmith@email.com")',
                u'genie_setup_file("/setup.sh")',
                u'genie_timeout(999)',
                u'genie_url("http://asdfasdf")',
                u'genie_username("jsmith")',
                u'group("group1")',
                u'job_id("geniejob_repr")',
                u'job_name("geniejob_repr")',
                u'job_version("1.1.1")',
                u'parameter("param1", "pval1")',
                u'parameter("param2", "pval2")',
                u'parameter("param3", "pval3")',
                u'parameter("param4", "pval4")',
                u'tags("tag1")',
                u'tags("tag2")'
            ])
        )

    def test_genie_cpu(self):
        """Test GenieJob repr (genie_cpu)."""

        job = pygenie.jobs.GenieJob() \
            .job_id('123') \
            .genie_username('user') \
            .genie_cpu(12)

        assert_equals(
            '.'.join([
                'GenieJob()',
                'genie_cpu(12)',
                'genie_username("user")',
                'job_id("123")'
            ]),
            str(job)
        )

    def test_genie_memory(self):
        """Test GenieJob repr (genie_memory)."""

        job = pygenie.jobs.GenieJob() \
            .job_id('123') \
            .genie_username('user') \
            .genie_memory(7000)

        assert_equals(
            '.'.join([
                'GenieJob()',
                'genie_memory(7000)',
                'genie_username("user")',
                'job_id("123")'
            ]),
            str(job)
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
            .genie_cpu(3) \
            .genie_email('jdoe@email.com') \
            .genie_memory(999) \
            .genie_timeout(100) \
            .genie_url('http://fdsafdsa') \
            .genie_username('jdoe') \
            .group('geniegroup1') \
            .job_id('geniejob1') \
            .job_name('testing_adapting_geniejob') \
            .tags('tag1, tag2') \
            .job_version('0.0.1alpha')

        assert_equals(
            pygenie.adapter.genie_3.get_payload(job),
            {
                'applications': ['applicationid1'],
                'attachments': [],
                'clusterCriterias': [
                    {'tags': ['type:cluster1']},
                    {'tags': ['type:genie']},
                ],
                'commandArgs': 'command args for geniejob',
                'commandCriteria': ['type:geniecmd'],
                'cpu': 3,
                'dependencies': ['/file1', '/file2'],
                'description': 'this job is to test geniejob adapter',
                'disableLogArchival': True,
                'email': 'jdoe@email.com',
                'group': 'geniegroup1',
                'id': 'geniejob1',
                'memory': 999,
                'name': 'testing_adapting_geniejob',
                'setupFile': None,
                'tags': ['tag1', 'tag2'],
                'timeout': 100,
                'user': 'jdoe',
                'version': '0.0.1alpha'
            }
        )


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestingJobExecute(unittest.TestCase):
    """Test executing job."""

    @patch('pygenie.jobs.core.reattach_job')
    @patch('pygenie.jobs.core.generate_job_id')
    @patch('pygenie.jobs.core.execute_job')
    def test_job_execute(self, exec_job, gen_job_id, reattach_job):
        """Testing job execution."""

        job = pygenie.jobs.HiveJob() \
            .job_id('exec') \
            .genie_username('exectester') \
            .script('select * from db.table')

        job.execute()

        gen_job_id.assert_not_called()
        reattach_job.assert_not_called()
        exec_job.assert_called_once_with(job)

    @patch('pygenie.jobs.core.reattach_job')
    @patch('pygenie.jobs.core.generate_job_id')
    @patch('pygenie.jobs.core.execute_job')
    def test_job_execute_retry(self, exec_job, gen_job_id, reattach_job):
        """Testing job execution with retry."""

        job_id = 'exec-retry'
        new_job_id = '{}-5'.format(job_id)

        gen_job_id.return_value = new_job_id
        reattach_job.side_effect = pygenie.exceptions.GenieJobNotFoundError

        job = pygenie.jobs.HiveJob() \
            .job_id(job_id) \
            .genie_username('exectester') \
            .script('select * from db.table')

        job.execute(retry=True)

        gen_job_id.assert_called_once_with(job_id,
                                           return_success=True,
                                           conf=job._conf)
        reattach_job.assert_called_once_with(new_job_id, conf=job._conf)
        exec_job.assert_called_once_with(job)
        assert_equals(new_job_id, job._job_id)

    @patch('pygenie.jobs.core.reattach_job')
    @patch('pygenie.jobs.core.generate_job_id')
    @patch('pygenie.jobs.core.execute_job')
    def test_job_execute_retry_force(self, exec_job, gen_job_id, reattach_job):
        """Testing job execution with force retry."""

        job_id = 'exec-retry-force'
        new_job_id = '{}-8'.format(job_id)

        gen_job_id.return_value = new_job_id
        reattach_job.side_effect = pygenie.exceptions.GenieJobNotFoundError

        job = pygenie.jobs.HiveJob() \
            .job_id(job_id) \
            .genie_username('exectester') \
            .script('select * from db.table')

        job.execute(retry=True, force=True)

        gen_job_id.assert_called_once_with(job_id,
                                           return_success=False,
                                           conf=job._conf)
        reattach_job.assert_called_once_with(new_job_id, conf=job._conf)
        exec_job.assert_called_once_with(job)
        assert_equals(new_job_id, job._job_id)
