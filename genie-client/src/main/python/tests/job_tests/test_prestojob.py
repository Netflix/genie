from __future__ import absolute_import, division, print_function, unicode_literals

import os
import unittest

from mock import patch
from nose.tools import assert_equals, assert_raises

import pygenie


assert_equals.__self__.maxDiff = None


def mock_to_attachment(att):
    if isinstance(att, dict):
        return {u'name': att['name'], u'data': att['data']}
    else:
        return {u'name': os.path.basename(att), u'data': u'file contents'}


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestingPrestoJob(unittest.TestCase):
    """Test PrestoJob."""

    def test_default_command_tag(self):
        """Test PrestoJob default command tags."""

        job = pygenie.jobs.PrestoJob()

        assert_equals(
            job.get('default_command_tags'),
            [u'type:presto']
        )

    def test_cmd_args_explicit(self):
        """Test PrestoJob explicit cmd args."""

        job = pygenie.jobs.PrestoJob() \
            .command_arguments('explicitly stating command args') \
            .script('select * from something') \
            .option('source', 'tester')

        assert_equals(
            job.cmd_args,
            u'explicitly stating command args'
        )

    def test_cmd_args_constructed_script_code(self):
        """Test PrestoJob constructed cmd args for adhoc script."""

        job = pygenie.jobs.PrestoJob() \
            .script('select * from something') \
            .option('debug') \
            .option('opt1', 'val1') \
            .option('source', 'tester') \
            .session('s1', 'v1') \
            .session('s2', 'v2')

        assert_equals(
            job.cmd_args,
            u'--session s1=v1 --session s2=v2 --debug --opt1 val1 --source tester -f script.presto'
        )

    @patch('pygenie.jobs.presto.is_file')
    def test_cmd_args_constructed_script_file(self, is_file):
        """Test PrestoJob constructed cmd args for file script."""

        is_file.return_value = True

        job = pygenie.jobs.PrestoJob() \
            .script('/Users/presto/test.presto') \
            .session('s1', 'v1') \
            .session('s2', 'v2') \
            .option('debug') \
            .option('opt1', 'val1') \
            .option('source', 'tester')

        assert_equals(
            job.cmd_args,
            u'--session s1=v1 --session s2=v2 --debug --opt1 val1 --source tester -f test.presto'
        )

    @patch('pygenie.jobs.presto.is_file')
    def test_cmd_args_post_cmd_args(self, is_file):
        """Test PrestoJob constructed cmd args with post cmd args."""

        is_file.return_value = True

        job = pygenie.jobs.PrestoJob() \
            .script('/Users/presto/test.presto') \
            .option('debug') \
            .session('s1', 'v1') \
            .session('s2', 'v2') \
            .option('opt1', 'val1') \
            .option('source', 'tester') \
            .post_cmd_args('a') \
            .post_cmd_args(['a', 'b', 'c']) \
            .post_cmd_args('d e f')

        assert_equals(
            '--session s1=v1 --session s2=v2 --debug --opt1 val1 --source tester -f test.presto a b c d e f',
            job.cmd_args
        )


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestingPrestoJobRepr(unittest.TestCase):
    """Test PrestoJob repr."""

    def test_attachment_repr(self):
        job = pygenie.jobs.PrestoJob() \
            .dependencies([{"data": "blah", "name": "blah"}]) \
            .job_id('prestojob_repr') \
            .genie_username('testuser')

        assert_equals(
            str(job),
            '.'.join([
                'PrestoJob()',
                'dependencies([{"data": "blah", "name": "blah"}])',
                'genie_username("testuser")',
                'job_id("prestojob_repr")'
                ])
        )

    @patch('pygenie.jobs.core.is_file')
    def test_repr(self, is_file):
        """Test PrestoJob repr."""

        is_file.return_value = True

        job = pygenie.jobs.PrestoJob() \
            .applications('prestoapp1') \
            .applications('prestoapp2') \
            .archive(False) \
            .cluster_tags('prestocluster1') \
            .cluster_tags('prestocluster2') \
            .command_tags('prestocmd1') \
            .command_tags('prestocmd2') \
            .dependencies('/prestodep1') \
            .dependencies('/prestodep2') \
            .description('presto description') \
            .disable_archive() \
            .genie_email('jpresto@email.com') \
            .genie_setup_file('/prestosetup.sh') \
            .genie_timeout(1) \
            .genie_username('jpresto') \
            .group('prestogroup') \
            .headers() \
            .job_id('prestojob_repr') \
            .job_name('prestojob_repr') \
            .job_version('1.1.1') \
            .option('debug') \
            .option('source', 'testrepr') \
            .script("""
            SELECT * FROM TEST
            """) \
            .session('session1', 'value1') \
            .session('session2', 'value2') \
            .tags('prestotag1') \
            .tags('prestotag2')

        assert_equals(
            str(job),
            '.'.join([
                'PrestoJob()',
                'applications("prestoapp1")',
                'applications("prestoapp2")',
                'archive(False)',
                'cluster_tags("prestocluster1")',
                'cluster_tags("prestocluster2")',
                'command_tags("prestocmd1")',
                'command_tags("prestocmd2")',
                'dependencies("/prestodep1")',
                'dependencies("/prestodep2")',
                'description("presto description")',
                'genie_email("jpresto@email.com")',
                'genie_setup_file("/prestosetup.sh")',
                'genie_timeout(1)',
                'genie_username("jpresto")',
                'group("prestogroup")',
                'job_id("prestojob_repr")',
                'job_name("prestojob_repr")',
                'job_version("1.1.1")',
                'option("debug", None)',
                'option("output-format", "CSV_HEADER")',
                'option("source", "testrepr")',
                'script("""\n            SELECT * FROM TEST\n            """)',
                'session("session1", "value1")',
                'session("session2", "value2")',
                'tags("headers")',
                'tags("prestotag1")',
                'tags("prestotag2")'
            ])
        )


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestingPrestoJobAdapters(unittest.TestCase):
    """Test adapting PrestoJob to different clients."""

    def setUp(self):
        self.dirname = os.path.dirname(os.path.realpath(__file__))
        with patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'}):
            self.genie_2_conf = pygenie.conf.GenieConf() \
                .load_config_file(os.path.join(self.dirname, 'genie2.ini'))
            self.genie_3_conf = pygenie.conf.GenieConf() \
                .load_config_file(os.path.join(self.dirname, 'genie3.ini'))


    @patch('pygenie.adapter.genie_2.to_attachment')
    @patch('os.path.isfile')
    def test_genie2_payload_adhoc_script(self, os_isfile, to_att):
        """Test PrestoJob payload for Genie 2 (adhoc script)."""

        os_isfile.side_effect = lambda f: f.startswith('/')
        to_att.side_effect = mock_to_attachment

        job = pygenie.jobs.PrestoJob(self.genie_2_conf) \
            .applications(['prestoapplicationid1']) \
            .cluster_tags('type:prestocluster1') \
            .command_tags('type:presto') \
            .dependencies(['/prestofile1', '/prestofile2']) \
            .description('this job is to test prestojob adapter') \
            .archive(False) \
            .genie_email('jpresto@email.com') \
            .genie_timeout(7) \
            .genie_username('jpresto') \
            .group('prestogroup') \
            .job_id('prestojob1') \
            .job_name('testing_adapting_prestojob') \
            .script('SELECT * FROM DUAL') \
            .tags('prestotag1, prestotag2') \
            .job_version('0.0.1presto')

        assert_equals(
            pygenie.adapter.genie_2.get_payload(job),
            {
                u'attachments': [
                    {u'name': u'prestofile1', u'data': u'file contents'},
                    {u'name': u'prestofile2', u'data': u'file contents'},
                    {u'name': u'script.presto', u'data': u'SELECT * FROM DUAL;'}
                ],
                u'clusterCriterias': [
                    {u'tags': [u'type:prestocluster1']},
                    {u'tags': [u'type:presto']}
                ],
                u'commandArgs': u'-f script.presto',
                u'commandCriteria': [u'type:presto'],
                u'description': u'this job is to test prestojob adapter',
                u'disableLogArchival': True,
                u'email': u'jpresto@email.com',
                u'envPropFile': None,
                u'fileDependencies': [],
                u'group': u'prestogroup',
                u'id': u'prestojob1',
                u'name': u'testing_adapting_prestojob',
                u'tags': [u'prestotag1', u'prestotag2'],
                u'user': u'jpresto',
                u'version': u'0.0.1presto'
            }
        )

    @patch('pygenie.adapter.genie_2.to_attachment')
    @patch('os.path.isfile')
    @patch('pygenie.jobs.presto.is_file')
    def test_genie2_payload_file_script(self, presto_is_file, os_isfile, to_att):
        """Test PrestoJob payload for Genie 2 (file script)."""

        os_isfile.return_value = True
        presto_is_file.return_value = True
        to_att.side_effect = mock_to_attachment

        job = pygenie.jobs.PrestoJob(self.genie_2_conf) \
            .applications(['prestoapplicationid1']) \
            .cluster_tags('type:prestocluster1') \
            .command_tags('type:presto') \
            .dependencies(['/prestofile1', '/prestofile2']) \
            .description('this job is to test prestojob adapter') \
            .archive(False) \
            .genie_email('jpresto@email.com') \
            .genie_timeout(7) \
            .genie_username('jpresto') \
            .group('prestogroup') \
            .job_id('prestojob1') \
            .job_name('testing_adapting_prestojob') \
            .script('/path/to/test/file.presto') \
            .tags('prestotag1, prestotag2') \
            .job_version('0.0.1presto')

        assert_equals(
            pygenie.adapter.genie_2.get_payload(job),
            {
                u'attachments': [
                    {u'name': u'prestofile1', u'data': u'file contents'},
                    {u'name': u'prestofile2', u'data': u'file contents'},
                    {u'name': u'file.presto', u'data': u'file contents'}
                ],
                u'clusterCriterias': [
                    {u'tags': [u'type:prestocluster1']},
                    {u'tags': [u'type:presto']}
                ],
                u'commandArgs': u'-f file.presto',
                u'commandCriteria': [u'type:presto'],
                u'description': u'this job is to test prestojob adapter',
                u'disableLogArchival': True,
                u'email': u'jpresto@email.com',
                u'envPropFile': None,
                u'fileDependencies': [],
                u'group': u'prestogroup',
                u'id': u'prestojob1',
                u'name': u'testing_adapting_prestojob',
                u'tags': [u'prestotag1', u'prestotag2'],
                u'user': u'jpresto',
                u'version': u'0.0.1presto'
            }
        )

    @patch('pygenie.adapter.genie_3.open')
    @patch('os.path.isfile')
    def test_genie3_payload_adhoc_script(self, os_isfile, file_open):
        """Test PrestoJob payload for Genie 3 (adhoc script)."""

        os_isfile.side_effect = lambda f: f.startswith('/')
        file_open.side_effect = lambda f, m: u"open file '{}'".format(f)

        job = pygenie.jobs.PrestoJob(self.genie_3_conf) \
            .applications(['prestoapplicationid1']) \
            .cluster_tags('type:prestocluster1') \
            .command_tags('type:presto') \
            .dependencies(['/prestofile1', '/prestofile2']) \
            .description('this job is to test prestojob adapter') \
            .archive(False) \
            .genie_email('jpresto@email.com') \
            .genie_timeout(7) \
            .genie_username('jpresto') \
            .group('prestogroup') \
            .job_id('prestojob1') \
            .job_name('testing_adapting_prestojob') \
            .script('SELECT * FROM DUAL') \
            .tags('prestotag1, prestotag2') \
            .job_version('0.0.1presto')

        assert_equals(
            pygenie.adapter.genie_3.get_payload(job),
            {
                u'applications': [u'prestoapplicationid1'],
                u'attachments': [
                    (u'prestofile1', u"open file '/prestofile1'"),
                    (u'prestofile2', u"open file '/prestofile2'"),
                    (u'script.presto', u'SELECT * FROM DUAL;')
                ],
                u'clusterCriterias': [
                    {u'tags': [u'type:prestocluster1']},
                    {u'tags': [u'type:presto']}
                ],
                u'commandArgs': u'-f script.presto',
                u'commandCriteria': [u'type:presto'],
                u'dependencies': [],
                u'description': u'this job is to test prestojob adapter',
                u'disableLogArchival': True,
                u'email': u'jpresto@email.com',
                u'group': u'prestogroup',
                u'id': u'prestojob1',
                u'name': u'testing_adapting_prestojob',
                u'setupFile': None,
                u'tags': [u'prestotag1', u'prestotag2'],
                u'timeout': 7,
                u'user': u'jpresto',
                u'version': u'0.0.1presto'
            }
        )

    @patch('pygenie.adapter.genie_3.open')
    @patch('os.path.isfile')
    @patch('pygenie.jobs.presto.is_file')
    def test_genie3_payload_file_script(self, presto_is_file, os_isfile, file_open):
        """Test PrestoJob payload for Genie 3 (file script)."""

        os_isfile.return_value = True
        presto_is_file.return_value = True
        file_open.side_effect = lambda f, m: u"open file '{}'".format(f)

        job = pygenie.jobs.PrestoJob(self.genie_3_conf) \
            .applications(['prestoapplicationid1']) \
            .cluster_tags('type:prestocluster1') \
            .command_tags('type:presto') \
            .dependencies(['/prestofile1', '/prestofile2']) \
            .description('this job is to test prestojob adapter') \
            .archive(False) \
            .genie_email('jpresto@email.com') \
            .genie_timeout(7) \
            .genie_username('jpresto') \
            .group('prestogroup') \
            .job_id('prestojob1') \
            .job_name('testing_adapting_prestojob') \
            .script('/path/to/test/file.presto') \
            .tags('prestotag1, prestotag2') \
            .job_version('0.0.1presto')

        assert_equals(
            pygenie.adapter.genie_3.get_payload(job),
            {
                u'applications': [u'prestoapplicationid1'],
                u'attachments': [
                    (u'prestofile1', u"open file '/prestofile1'"),
                    (u'prestofile2', u"open file '/prestofile2'"),
                    (u'file.presto', u"open file '/path/to/test/file.presto'")
                ],
                u'clusterCriterias': [
                    {u'tags': [u'type:prestocluster1']},
                    {u'tags': [u'type:presto']}
                ],
                u'commandArgs': u'-f file.presto',
                u'commandCriteria': [u'type:presto'],
                u'dependencies': [],
                u'description': u'this job is to test prestojob adapter',
                u'disableLogArchival': True,
                u'email': u'jpresto@email.com',
                u'group': u'prestogroup',
                u'id': u'prestojob1',
                u'name': u'testing_adapting_prestojob',
                u'setupFile': None,
                u'tags': [u'prestotag1', u'prestotag2'],
                u'timeout': 7,
                u'user': u'jpresto',
                u'version': u'0.0.1presto'
            }
        )
