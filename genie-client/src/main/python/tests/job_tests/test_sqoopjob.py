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
class TestingSqoopJob(unittest.TestCase):
    """Test SqoopJob."""

    def test_default_command_tag(self):
        """Test SqoopJob default command tags."""

        job = pygenie.jobs.SqoopJob()

        assert_equals(
            job.get('default_command_tags'),
            [u'type:sqoop']
        )

    def test_cmd_args_explicit(self):
        """Test SqoopJob explicit cmd args."""

        job = pygenie.jobs.SqoopJob() \
            .command_arguments('sqoop job explicitly stating command args') \
            .cmd('cmd_test') \
            .option('should_not', 'be_used')

        assert_equals(
            job.cmd_args,
            u'sqoop job explicitly stating command args'
        )

    def test_cmd_args_constructed_script_code(self):
        """Test SqoopJob constructed cmd args for adhoc script."""

        job = pygenie.jobs.SqoopJob() \
            .cmd('export') \
            .option('option1', 'value1') \
            .option('option2', 'value2') \
            .option('option3', 'value3') \
            .option('verbose') \
            .property('prop1', 'value1') \
            .property('prop2', 'value2', flag='-X') \
            .connect('jdbc://test') \
            .password('passw0rd') \
            .split_by('col1') \
            .table('mytable') \
            .target_dir('/path/to/output')

        assert_equals(
            job.cmd_args,
            ' '.join([
                'export',
                '-Dprop1=value1',
                '--options-file _sqoop_options.txt'
            ])
        )

    def test_cmd_args_post_cmd_args(self):
        """Test SqoopJob constructed cmd args with post cmd args."""

        job = pygenie.jobs.SqoopJob() \
            .cmd('export') \
            .option('option1', 'value1') \
            .option('option2', 'value2') \
            .option('option3', 'value3') \
            .option('verbose') \
            .property('prop1', 'value1') \
            .property('prop2', 'value2', flag='-X') \
            .connect('jdbc://test') \
            .password('passw0rd') \
            .split_by('col1') \
            .table('mytable') \
            .target_dir('/path/to/output') \
            .post_cmd_args('a') \
            .post_cmd_args(['a', 'b', 'c']) \
            .post_cmd_args('d e f')

        assert_equals(
            ' '.join([
                'export',
                '-Dprop1=value1',
                '--options-file _sqoop_options.txt',
                'a b c d e f'
            ]),
            job.cmd_args
        )


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestingSqoopJobRepr(unittest.TestCase):
    """Test SqoopJob repr."""

    def test_repr(self):
        """Test SqoopJob repr."""

        job = pygenie.jobs.SqoopJob() \
            .job_id('1234-abcd') \
            .cmd('importtest') \
            .option('bar', 'buzz') \
            .option('debug') \
            .option('foo', 'fizz') \
            .option('hello', 'world') \
            .option('verbose') \
            .property('p1', 'v1') \
            .property('p2', 'v2', flag='-f') \
            .connect('jdbc://') \
            .password('1234') \
            .split_by('col_a') \
            .table('test_table') \
            .target_dir('/path/to/test/output')

        assert_equals(
            str(job),
            '.'.join([
                'SqoopJob()',
                'cmd("importtest")',
                'genie_username("user_ini")',
                'job_id("1234-abcd")',
                'option("bar", "buzz")',
                'option("connect", "jdbc://")',
                'option("debug", None)',
                'option("foo", "fizz")',
                'option("hello", "world")',
                'option("password", "1234")',
                'option("split-by", "col_a")',
                'option("table", "test_table")',
                'option("target-dir", "/path/to/test/output")',
                'option("verbose", None)',
                'property("p1", "v1", "-D")',
                'property("p2", "v2", "-f")'
            ])
        )


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestingSqoopOptionsFile(unittest.TestCase):
    """Test SqoopJob options file construction."""

    def test_basic(self):
        """Test SqoopJob options file construction"""

        job = pygenie.jobs.SqoopJob() \
            .option('connect', 'jdbc:oracle@ZZZZZ') \
            .option('username', 'testsqoop') \
            .option('password', 'testsqooppw') \
            .option('target-dir', 's3://xyz/1') \
            .option('num-mappers', 1) \
            .option('fields-terminated-by', '\\001') \
            .option('query', 'select * from table') \
            .option('null-string', '\\\\N') \
            .option('null-non-string', '') \
            .option('verbose') \
            .option('direct')

        assert_equals(
            "\n".join([
                "--connect",
                "'jdbc:oracle@ZZZZZ'",
                "--username",
                "'testsqoop'",
                "--password",
                "'testsqooppw'",
                "--target-dir",
                "'s3://xyz/1'",
                "--num-mappers",
                "'1'",
                "--fields-terminated-by",
                "'\\001'",
                "--query",
                "'select * from table'",
                "--null-string",
                "'\\\\N'",
                "--null-non-string",
                "''",
                "--verbose",
                "--direct",
            ]) + "\n",
            job._options_file
        )

    def test_newlines(self):
        """Test SqoopJob options file construction with new lines"""

        query = """
SELECT
    col1,
    col2,
FROM
    table
WHERE
    col1 > 0
"""

        job = pygenie.jobs.SqoopJob() \
            .option('query', query)

        assert_equals(
            u"--query\n' SELECT     col1,     col2, FROM     table WHERE     col1 > 0 '\n",
            job._options_file
        )


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestingSqoopJobAdapters(unittest.TestCase):
    """Test adapting SqoopJob to different clients."""

    def setUp(self):
        self.dirname = os.path.dirname(os.path.realpath(__file__))
        with patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'}):
            self.genie_2_conf = pygenie.conf.GenieConf() \
                .load_config_file(os.path.join(self.dirname, 'genie2.ini'))
            self.genie_3_conf = pygenie.conf.GenieConf() \
                .load_config_file(os.path.join(self.dirname, 'genie3.ini'))


    @patch('pygenie.adapter.genie_2.to_attachment')
    @patch('os.path.isfile')
    def test_genie2_payload(self, os_isfile, to_att):
        """Test SqoopJob payload for Genie 2."""

        os_isfile.side_effect = lambda f: f.startswith('/')
        to_att.side_effect = mock_to_attachment

        job = pygenie.jobs.SqoopJob(self.genie_2_conf) \
            .applications(['sqoopapplicationid1']) \
            .cluster_tags('type:sqoopcluster1') \
            .command_tags('type:sqoop-test') \
            .dependencies(['/sqoopfile1', '/sqoopfile2']) \
            .description('this job is to test sqoopjob adapter') \
            .archive(False) \
            .genie_email('jsqoop@email.com') \
            .genie_timeout(58) \
            .genie_username('jsqoop-genie') \
            .group('sqoopgroup') \
            .job_id('sqoopjob1') \
            .job_name('testing_adapting_sqoopjob') \
            .job_version('0.0.1sqoop') \
            .setup_file('/path/to/testsetup.sh') \
            .tags('sqooptag1, sqooptag2') \
            .username('jsqoop') \
            .cmd('test-export') \
            .option('username', 'user') \
            .password('t3st') \
            .option('opt1', 'val1') \
            .property('prop1', 'pval1') \
            .connect('jdbc://test')

        assert_equals(
            pygenie.adapter.genie_2.get_payload(job),
            {
                u'attachments': [
                    {u'name': u'sqoopfile1', u'data': u'file contents'},
                    {u'name': u'sqoopfile2', u'data': u'file contents'},
                    {u'name': u'_sqoop_options.txt', u'data': u"--username\n'user'\n--password\n't3st'\n--opt1\n'val1'\n--connect\n'jdbc://test'\n"}
                ],
                u'clusterCriterias': [
                    {'tags': [u'type:sqoopcluster1']},
                    {'tags': [u'type:sqoop']}
                ],
                u'commandArgs': u' '.join([
                    u'test-export',
                    u'-Dprop1=pval1',
                    u'--options-file _sqoop_options.txt']),
                u'commandCriteria': [u'type:sqoop-test'],
                u'description': u'this job is to test sqoopjob adapter',
                u'disableLogArchival': True,
                u'email': u'jsqoop@email.com',
                u'envPropFile': '/path/to/testsetup.sh',
                u'fileDependencies': [],
                u'group': u'sqoopgroup',
                u'id': u'sqoopjob1',
                u'name': u'testing_adapting_sqoopjob',
                u'tags': [u'sqooptag1', u'sqooptag2'],
                u'user': u'jsqoop-genie',
                u'version': u'0.0.1sqoop'
            }
        )

    @patch('pygenie.adapter.genie_3.open')
    @patch('os.path.isfile')
    def test_genie3_payload(self, os_isfile, file_open):
        """Test SqoopJob payload for Genie 3."""

        os_isfile.side_effect = lambda f: f.startswith('/')
        file_open.side_effect = lambda f, m: u"open file '{}'".format(f)

        job = pygenie.jobs.SqoopJob(self.genie_3_conf) \
            .applications(['sqoop-app-id1']) \
            .cluster_tags('type:sqoop-cluster-1') \
            .command_tags('type:sqoop-test-1') \
            .dependencies(['/sqoopfile1a', '/sqoopfile2b']) \
            .description('this job is to test sqoopjob adapter for genie 3') \
            .archive(False) \
            .genie_email('jsqoop-g3@email.com') \
            .genie_timeout(5) \
            .genie_username('jsqoop-genie3') \
            .group('sqoopgroup-g3') \
            .job_id('sqoopjob1-g3') \
            .job_name('testing_adapting_sqoopjob-g3') \
            .job_version('0.0.1sqoop-g3') \
            .setup_file('/path/to/testsetup.sh-g3') \
            .tags('sqooptag1-g3, sqooptag2-g3') \
            .username('jsqoop-g3') \
            .cmd('test-export-g3') \
            .option('username', 'user-g3') \
            .password('t3st-g3') \
            .option('opt1-g3', 'val1-g3') \
            .property('prop1-g3', 'pval1-g3') \
            .connect('jdbc://test-g3')

        assert_equals(
            pygenie.adapter.genie_3.get_payload(job),
            {
                u'applications': [u'sqoop-app-id1'],
                u'attachments': [
                    (u'sqoopfile1a', u"open file '/sqoopfile1a'"),
                    (u'sqoopfile2b', u"open file '/sqoopfile2b'"),
                    (u'_sqoop_options.txt', u"--username\n'user-g3'\n--password\n't3st-g3'\n--opt1-g3\n'val1-g3'\n--connect\n'jdbc://test-g3'\n")
                ],
                u'clusterCriterias': [
                    {'tags': [u'type:sqoop-cluster-1']},
                    {'tags': [u'type:sqoop']}
                ],
                u'commandArgs': u'test-export-g3 -Dprop1-g3=pval1-g3 --options-file _sqoop_options.txt',
                u'commandCriteria': [u'type:sqoop-test-1'],
                u'dependencies': [],
                u'description': u'this job is to test sqoopjob adapter for genie 3',
                u'disableLogArchival': True,
                u'email': u'jsqoop-g3@email.com',
                u'group': u'sqoopgroup-g3',
                u'id': u'sqoopjob1-g3',
                u'name': u'testing_adapting_sqoopjob-g3',
                u'setupFile': u'/path/to/testsetup.sh-g3',
                u'tags': [u'sqooptag1-g3', u'sqooptag2-g3'],
                u'timeout': 5,
                u'user': u'jsqoop-genie3',
                u'version': u'0.0.1sqoop-g3'
            }
        )
