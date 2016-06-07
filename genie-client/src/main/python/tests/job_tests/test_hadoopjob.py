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
            .script("""this should not be taken""")

        assert_equals(
            job.cmd_args,
            u'explicitly stating command args'
        )

    def test_cmd_args_constructed_script_code(self):
        """Test HadoopJob constructed cmd args for adhoc script."""

        job = pygenie.jobs.HadoopJob() \
            .command("fs -ls /test") \
            .property('prop1', 'v1') \
            .property('prop2', 'v2') \
            .property_file('/Users/hadoop/props.conf')

        assert_equals(
            job.cmd_args,
            u'-conf props.conf ' \
            u'-Dprop1=v1 -Dprop2=v2 ' \
            u'fs -ls /test'
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
            .email('hadoop@email.com') \
            .group('hadoopgroup') \
            .job_id('hadoopjob_repr_id') \
            .job_name('hadoopjob_repr') \
            .job_version('0.0.0') \
            .setup_file('/hadoop_setup.sh') \
            .tags('hadoop_tag_1') \
            .tags('hadoop_tag_2') \
            .timeout(5) \
            .username('jhadoop')
        job \
            .property('mapred.1', 'p1') \
            .property('mapred.2', 'p2') \
            .property_file('/Users/hadoop/test.conf') \
            .script('jar testing.jar') \

        assert_equals(
            str(job),
            u'HadoopJob()' \
            u'.applications("hadoop_app_1")' \
            u'.applications("hadoop_app_2")' \
            u'.archive(False)' \
            u'.cluster_tags("hadoop_cluster_1")' \
            u'.cluster_tags("hadoop_cluster_2")' \
            u'.command_tags("hadoop_cmd_1")' \
            u'.command_tags("hadoop_cmd_2")' \
            u'.dependencies("/Users/hadoop/dep1")' \
            u'.dependencies("/Users/hadoop/dep2")' \
            u".description('\"test hadoop desc\"')" \
            u'.email("hadoop@email.com")' \
            u'.group("hadoopgroup")' \
            u'.job_id("hadoopjob_repr_id")' \
            u'.job_name("hadoopjob_repr")' \
            u'.job_version("0.0.0")' \
            u'.property("mapred.1", "p1")' \
            u'.property("mapred.2", "p2")' \
            u'.property_file("/Users/hadoop/test.conf")' \
            u'.script("jar testing.jar")' \
            u'.setup_file("/hadoop_setup.sh")' \
            u'.tags("hadoop_tag_1")' \
            u'.tags("hadoop_tag_2")' \
            u'.timeout(5)' \
            u'.username("jhadoop")'
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
            .email('jhadoop@email.com') \
            .group('hadoopgroup') \
            .job_id('hadoopjob1') \
            .job_name('testing_adapting_hadoopjob') \
            .script('fs -ls /testing/adapter') \
            .tags('hadooptag') \
            .timeout(7) \
            .username('jhadoop') \
            .job_version('0.0.hadoop-alpha')

        assert_equals(
            pygenie.adapter.genie_2.get_payload(job),
            {
                u'attachments': [
                    {u'name': u'hadoopfile1', u'data': u'file contents'}
                ],
                u'clusterCriterias': [{u'tags': [u'type:hadoopcluster1']}],
                u'commandArgs': u'fs -ls /testing/adapter',
                u'commandCriteria': [u'type:hadoopcmd'],
                u'description': u'this job is to test hadoopjob adapter',
                u'disableLogArchival': True,
                u'email': u'jhadoop@email.com',
                u'envPropFile': None,
                u'fileDependencies': [],
                u'group': u'hadoopgroup',
                u'id': u'hadoopjob1',
                u'name': u'testing_adapting_hadoopjob',
                u'tags': [u'hadooptag'],
                u'user': u'jhadoop',
                u'version': u'0.0.hadoop-alpha'
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
            .email('jhadoop@email.com') \
            .group('hadoop-group') \
            .job_id('hadoop-job1') \
            .job_name('testing_adapting_hadoopjob') \
            .script('jar jar jar') \
            .tags('hadoop.tag') \
            .timeout(7) \
            .username('jhadoop') \
            .job_version('0.0.hadoop')

        assert_equals(
            pygenie.adapter.genie_3.get_payload(job),
            {
                u'applications': [u'hadoop.app1'],
                u'attachments': [
                    (u'hadoop.file1', u"open file '/hadoop.file1'")
                ],
                u'clusterCriterias': [{u'tags': [u'type:hadoop.cluster1']}],
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
