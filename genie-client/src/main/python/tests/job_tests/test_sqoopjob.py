from __future__ import absolute_import, division, print_function, unicode_literals

import os
import unittest

from mock import patch
from nose.tools import assert_equals, assert_raises

import pygenie


assert_equals.__self__.maxDiff = None


def mock_to_attachment(att):
    if isinstance(att, dict):
        return {u'name': unicode(att['name']), u'data': unicode(att['data'])}
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
            u' '.join([
                u'export',
                u'-Dprop1=value1',
                u'--verbose',
                u'--target-dir /path/to/output',
                u'--split-by col1',
                u'--connect jdbc://test',
                u'--table mytable',
                u'--password passw0rd',
                u'--option2 value2',
                u'--option3 value3',
                u'--option1 value1',
                u'-Xprop2 value2'
            ])
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
            u'SqoopJob()'
            u'.cmd("importtest")'
            u'.job_id("1234-abcd")'
            u'.option("bar", "buzz")'
            u'.option("connect", "jdbc://")'
            u'.option("debug", None)'
            u'.option("foo", "fizz")'
            u'.option("hello", "world")'
            u'.option("password", "1234")'
            u'.option("split-by", "col_a")'
            u'.option("table", "test_table")'
            u'.option("target-dir", "/path/to/test/output")'
            u'.option("verbose", None)'
            u'.property("p1", "v1", "-D")'
            u'.property("p2", "v2", "-f")'
            u'.username("user_ini")'
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
            .email('jsqoop@email.com') \
            .group('sqoopgroup') \
            .job_id('sqoopjob1') \
            .job_name('testing_adapting_sqoopjob') \
            .job_version('0.0.1sqoop') \
            .setup_file('/path/to/testsetup.sh') \
            .tags('sqooptag1, sqooptag2') \
            .timeout(58) \
            .username('jsqoop') \
            .cmd('test-export') \
            .option('opt1', 'val1') \
            .option('username', 'user') \
            .property('prop1', 'pval1') \
            .connect('jdbc://test') \
            .password('t3st')

        assert_equals(
            pygenie.adapter.genie_2.get_payload(job),
            {
                u'attachments': [
                    {u'name': u'sqoopfile1', u'data': u'file contents'},
                    {u'name': u'sqoopfile2', u'data': u'file contents'}
                ],
                u'clusterCriterias': [
                    {'tags': [u'type:sqoopcluster1']},
                    {'tags': [u'type:sqoop']}
                ],
                u'commandArgs': u' '.join([
                    u'test-export',
                    u'-Dprop1=pval1',
                    u'--username user',
                    u'--password t3st',
                    u'--opt1 val1',
                    u'--connect jdbc://test']),
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
                u'user': u'jsqoop',
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
            .email('jsqoop-g3@email.com') \
            .group('sqoopgroup-g3') \
            .job_id('sqoopjob1-g3') \
            .job_name('testing_adapting_sqoopjob-g3') \
            .job_version('0.0.1sqoop-g3') \
            .setup_file('/path/to/testsetup.sh-g3') \
            .tags('sqooptag1-g3, sqooptag2-g3') \
            .timeout(5) \
            .username('jsqoop-g3') \
            .cmd('test-export-g3') \
            .option('opt1-g3', 'val1-g3') \
            .option('username', 'user-g3') \
            .property('prop1-g3', 'pval1-g3') \
            .connect('jdbc://test-g3') \
            .password('t3st-g3')

        assert_equals(
            pygenie.adapter.genie_3.get_payload(job),
            {
                u'applications': [u'sqoop-app-id1'],
                u'attachments': [
                    (u'sqoopfile1a', u"open file '/sqoopfile1a'"),
                    (u'sqoopfile2b', u"open file '/sqoopfile2b'")
                ],
                u'clusterCriterias': [
                    {'tags': [u'type:sqoop-cluster-1']},
                    {'tags': [u'type:sqoop']}
                ],
                u'commandArgs': u'test-export-g3 -Dprop1-g3=pval1-g3 --username user-g3 --password t3st-g3 --opt1-g3 val1-g3 --connect jdbc://test-g3',
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
                u'user': u'jsqoop-g3',
                u'version': u'0.0.1sqoop-g3'
            }
        )
