from __future__ import absolute_import, division, print_function, unicode_literals

import os
import unittest

from mock import patch
from nose.tools import assert_equals, assert_raises


assert_equals.__self__.maxDiff = None


import pygenie


def mock_to_attachment(att):
    if isinstance(att, dict):
        return {u'name': unicode(att['name']), u'data': unicode(att['data'])}
    else:
        return {u'name': os.path.basename(att), u'data': u'file contents'}


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestingHadoopJob(unittest.TestCase):
    """Test HadoopJob."""

    def test_default_command_tag(self):
        """Test HadoopJob default command tags."""

        job = pygenie.jobs.HadoopJob()

        assert_equals(
            job.get('default_command_tags'),
            [u'type:hadoop']
        )

    def test_cmd_args_explicit(self):
        """Test HadoopJob explicit cmd args."""

        job = pygenie.jobs.HadoopJob() \
            .command_arguments('explicitly stating command args') \
            .script("""this should not be taken""") \
            .post_cmd_args('should not be used')

        assert_equals(
            job.cmd_args,
            'explicitly stating command args'
        )

    def test_cmd_args_constructed_script_code(self):
        """Test HadoopJob constructed cmd args for adhoc script."""

        job = pygenie.jobs.HadoopJob() \
            .command("fs -ls /test") \
            .property('prop1', 'v1') \
            .property('prop2', 'v2') \
            .property_file('/Users/hadoop/props.conf')

        assert_equals(
            'fs -conf props.conf -Dprop1=v1 -Dprop2=v2 -ls /test',
            job.cmd_args
        )

    def test_cmd_args_post_cmd_args(self):
        """Test HadoopJob constructed cmd args with post cmd args."""

        job = pygenie.jobs.HadoopJob() \
            .command("fs -ls /test") \
            .property('prop1', 'v1') \
            .property('prop2', 'v2') \
            .property_file('/Users/hadoop/props.conf') \
            .post_cmd_args('a') \
            .post_cmd_args(['a', 'b', 'c']) \
            .post_cmd_args('d e f')

        assert_equals(
            'fs -conf props.conf -Dprop1=v1 -Dprop2=v2 -ls /test a b c d e f',
            job.cmd_args
        )


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestingHadoopJobRepr(unittest.TestCase):
    """Test HadoopJob repr."""

    @patch('pygenie.jobs.core.is_file')
    def test_repr(self, is_file):
        """Test HadoopJob repr."""

        is_file.return_value = True

        job = pygenie.jobs.HadoopJob() \
            .applications('hadoop_app_1') \
            .applications('hadoop_app_2') \
            .archive(False) \
            .cluster_tags('hadoop_cluster_1') \
            .cluster_tags('hadoop_cluster_2') \
            .command_tags('hadoop_cmd_1') \
            .command_tags('hadoop_cmd_2') \
            .dependencies('/Users/hadoop/dep1') \
            .dependencies('/Users/hadoop/dep2') \
            .description('"test hadoop desc"') \
            .disable_archive() \
            .genie_email('hadoop@email.com') \
            .genie_setup_file('/hadoop_setup.sh') \
            .genie_timeout(5) \
            .genie_username('jhadoop') \
            .group('hadoopgroup') \
            .job_id('hadoopjob_repr_id') \
            .job_name('hadoopjob_repr') \
            .job_version('0.0.0') \
            .tags('hadoop_tag_1') \
            .tags('hadoop_tag_2')
        job \
            .property('mapred.1', 'p1') \
            .property('mapred.2', 'p2') \
            .property_file('/Users/hadoop/test.conf') \
            .script('jar testing.jar')

        assert_equals(
            str(job),
            '.'.join([
                'HadoopJob()',
                'applications("hadoop_app_1")',
                'applications("hadoop_app_2")',
                'archive(False)',
                'cluster_tags("hadoop_cluster_1")',
                'cluster_tags("hadoop_cluster_2")',
                'command_tags("hadoop_cmd_1")',
                'command_tags("hadoop_cmd_2")',
                'dependencies("/Users/hadoop/dep1")',
                'dependencies("/Users/hadoop/dep2")',
                "description('\"test hadoop desc\"')",
                'genie_email("hadoop@email.com")',
                'genie_setup_file("/hadoop_setup.sh")',
                'genie_timeout(5)',
                'genie_username("jhadoop")',
                'group("hadoopgroup")',
                'job_id("hadoopjob_repr_id")',
                'job_name("hadoopjob_repr")',
                'job_version("0.0.0")',
                'property("mapred.1", "p1")',
                'property("mapred.2", "p2")',
                'property_file("/Users/hadoop/test.conf")',
                'script("jar testing.jar")',
                'tags("hadoop_tag_1")',
                'tags("hadoop_tag_2")'
            ])
        )


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestingHadoopJobAdapters(unittest.TestCase):
    """Test adapting HadoopJob to different clients."""

    def setUp(self):
        self.dirname = os.path.dirname(os.path.realpath(__file__))
        with patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'}):
            self.genie_2_conf = pygenie.conf.GenieConf() \
                .load_config_file(os.path.join(self.dirname, 'genie2.ini'))
            self.genie_3_conf = pygenie.conf.GenieConf() \
                .load_config_file(os.path.join(self.dirname, 'genie3.ini'))

    @patch('pygenie.adapter.genie_2.to_attachment')
    @patch('os.path.isfile')
    @patch('pygenie.jobs.pig.is_file')
    def test_genie2_payload_file_script(self, is_file, os_isfile, to_att):
        """Test HadoopJob payload for Genie 2 (file script)."""

        os_isfile.return_value = True
        is_file.return_value = True
        to_att.side_effect = mock_to_attachment

        job = pygenie.jobs.HadoopJob(self.genie_2_conf) \
            .applications(['hadoopapp1']) \
            .cluster_tags('type:hadoopcluster1') \
            .command_tags('type:hadoopcmd') \
            .dependencies(['/hadoopfile1']) \
            .description('this job is to test hadoopjob adapter') \
            .archive(False) \
            .genie_email('jhadoop@email.com') \
            .genie_timeout(7) \
            .genie_username('jhadoop') \
            .group('hadoopgroup') \
            .job_id('hadoopjob1') \
            .job_name('testing_adapting_hadoopjob') \
            .script('fs -ls /testing/adapter') \
            .tags('hadooptag') \
            .job_version('0.0.hadoop-alpha')

        assert_equals(
            pygenie.adapter.genie_2.get_payload(job),
            {
                'attachments': [
                    {'name': 'hadoopfile1', 'data': 'file contents'}
                ],
                'clusterCriterias': [
                    {'tags': ['type:hadoopcluster1']},
                    {'tags': ['type:hadoop']}
                ],
                'commandArgs': 'fs   -ls /testing/adapter',
                'commandCriteria': ['type:hadoopcmd'],
                'description': 'this job is to test hadoopjob adapter',
                'disableLogArchival': True,
                'email': 'jhadoop@email.com',
                'envPropFile': None,
                'fileDependencies': [],
                'group': 'hadoopgroup',
                'id': 'hadoopjob1',
                'name': 'testing_adapting_hadoopjob',
                'tags': ['hadooptag'],
                'user': 'jhadoop',
                'version': '0.0.hadoop-alpha'
            }
        )

    @patch('pygenie.adapter.genie_3.open')
    @patch('os.path.isfile')
    @patch('pygenie.jobs.pig.is_file')
    def test_genie3_payload_file_script(self, is_file, os_isfile, file_open):
        """Test HadoopJob payload for Genie 3 (file script)."""

        os_isfile.return_value = True
        is_file.return_value = True
        file_open.side_effect = lambda f, m: u"open file '{}'".format(f)

        job = pygenie.jobs.HadoopJob(self.genie_3_conf) \
            .applications(['hadoop.app1']) \
            .cluster_tags('type:hadoop.cluster1') \
            .command_tags('type:hadoop.cmd') \
            .dependencies(['/hadoop.file1']) \
            .description('this job is to test hadoopjob adapter') \
            .archive(False) \
            .genie_email('jhadoop@email.com') \
            .genie_timeout(7) \
            .genie_username('jhadoop') \
            .group('hadoop-group') \
            .job_id('hadoop-job1') \
            .job_name('testing_adapting_hadoopjob') \
            .script('jar jar jar') \
            .tags('hadoop.tag') \
            .job_version('0.0.hadoop')

        assert_equals(
            pygenie.adapter.genie_3.get_payload(job),
            {
                u'applications': [u'hadoop.app1'],
                u'attachments': [
                    (u'hadoop.file1', u"open file '/hadoop.file1'")
                ],
                u'clusterCriterias': [
                    {u'tags': [u'type:hadoop.cluster1']},
                    {u'tags': [u'type:hadoop']}
                ],
                u'commandArgs': u'jar jar jar',
                u'commandCriteria': [u'type:hadoop.cmd'],
                u'dependencies': [],
                u'description': u'this job is to test hadoopjob adapter',
                u'disableLogArchival': True,
                u'email': u'jhadoop@email.com',
                u'group': u'hadoop-group',
                u'id': u'hadoop-job1',
                u'name': u'testing_adapting_hadoopjob',
                u'setupFile': None,
                u'tags': [u'hadoop.tag'],
                u'timeout': 7,
                u'user': u'jhadoop',
                u'version': u'0.0.hadoop'
            }
        )
