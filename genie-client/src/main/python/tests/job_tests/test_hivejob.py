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
class TestingHiveJob(unittest.TestCase):
    """Test HiveJob."""

    def test_default_command_tag(self):
        """Test HiveJob default command tags."""

        job = pygenie.jobs.HiveJob()

        assert_equals(
            job.get('default_command_tags'),
            [u'type:hive']
        )

    def test_cmd_args_explicit(self):
        """Test HiveJob explicit cmd args."""

        job = pygenie.jobs.HiveJob() \
            .command_arguments('explicitly stating command args') \
            .script('select * from something') \
            .property('source', 'tester')

        assert_equals(
            job.cmd_args,
            u'explicitly stating command args'
        )

    def test_cmd_args_constructed_script_code(self):
        """Test HiveJob constructed cmd args for adhoc script."""

        job = pygenie.jobs.HiveJob() \
            .script('select * from something') \
            .parameter('foo', 'fizz') \
            .parameter('bar', 'buzz') \
            .hiveconf('hconf1', 'h1') \
            .property('prop1', 'p1') \
            .property('prop2', 'p2')

        assert_equals(
            job.cmd_args,
            u'--hiveconf hconf1=h1 --hiveconf prop1=p1 --hiveconf prop2=p2 ' \
            u'-d foo=fizz -d bar=buzz ' \
            u'-f script.hive'
        )

    @patch('pygenie.jobs.hive.is_file')
    def test_cmd_args_constructed_script_file(self, is_file):
        """Test HiveJob constructed cmd args for file script."""

        is_file.return_value = True

        job = pygenie.jobs.HiveJob() \
            .script('/Users/hive/test.hql') \
            .parameter('hello', 'hi') \
            .parameter('goodbye', 'bye') \
            .property('p1', 'v1') \
            .property('p2', 'v2')

        assert_equals(
            job.cmd_args,
            u'--hiveconf p2=v2 --hiveconf p1=v1 ' \
            u'-d hello=hi -d goodbye=bye ' \
            u'-f test.hql'
        )


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestingHiveJobRepr(unittest.TestCase):
    """Test HiveJob repr."""

    @patch('pygenie.jobs.core.is_file')
    def test_repr(self, is_file):
        """Test HiveJob repr."""

        is_file.return_value = True

        job = pygenie.jobs.HiveJob() \
            .applications('hive.app.1') \
            .applications('hive.app.2') \
            .archive(False) \
            .cluster_tags('hive.cluster1') \
            .cluster_tags('hive.cluster2') \
            .command_tags('hive.cmd1') \
            .command_tags('hive.cmd2') \
            .dependencies('/hive/dep1') \
            .dependencies('/hive/dep2') \
            .description('hive description') \
            .disable_archive() \
            .email('jhive@email.com') \
            .group('hive-group') \
            .job_id('hivejob_repr') \
            .job_name('hivejob_repr') \
            .job_version('1.1.5') \
            .parameter('param1', 'pval1') \
            .parameter('param2', 'pval2') \
            .setup_file('/hive.setup.sh') \
            .tags('hive.tag.1') \
            .tags('hive.tag.2') \
            .timeout(1) \
            .username('jhive')

        job \
            .hiveconf('hconf1', '1') \
            .hiveconf('hconf2', '2') \
            .property('prop1', '1') \
            .property('prop2', '2') \
            .property_file('/hive/conf.prop') \
            .script("SELECT * FROM TEST") \

        assert_equals(
            str(job),
            u'HiveJob()'
            u'.applications("hive.app.1")'
            u'.applications("hive.app.2")'
            u'.archive(False)'
            u'.cluster_tags("hive.cluster1")'
            u'.cluster_tags("hive.cluster2")'
            u'.command_tags("hive.cmd1")'
            u'.command_tags("hive.cmd2")'
            u'.dependencies("/hive/dep1")'
            u'.dependencies("/hive/dep2")'
            u'.description("hive description")'
            u'.email("jhive@email.com")'
            u'.group("hive-group")'
            u'.job_id("hivejob_repr")'
            u'.job_name("hivejob_repr")'
            u'.job_version("1.1.5")'
            u'.parameter("param1", "pval1")'
            u'.parameter("param2", "pval2")'
            u'.property("hconf1", "1")'
            u'.property("hconf2", "2")'
            u'.property("prop1", "1")'
            u'.property("prop2", "2")'
            u'.property_file("/hive/conf.prop")'
            u'.script("SELECT * FROM TEST")'
            u'.setup_file("/hive.setup.sh")'
            u'.tags("hive.tag.1")'
            u'.tags("hive.tag.2")'
            u'.timeout(1)'
            u'.username("jhive")'
        )


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestingHiveJobAdapters(unittest.TestCase):
    """Test adapting HiveJob to different clients."""

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
        """Test HiveJob payload for Genie 2 (adhoc script)."""

        os_isfile.side_effect = lambda f: f.startswith('/')
        to_att.side_effect = mock_to_attachment

        job = pygenie.jobs.HiveJob(self.genie_2_conf) \
            .applications(['hive.applicationid1']) \
            .archive(False) \
            .cluster_tags('type:hive.cluster1') \
            .command_tags('type:hive.cmd') \
            .dependencies(['/hive.file1', '/hive.file2']) \
            .description('this job is to test hivejob adapter') \
            .email('jhive@email.com') \
            .group('hive-group') \
            .job_id('hive-job1') \
            .job_name('testing_adapting_hivejob') \
            .job_version('0.0.hive') \
            .parameter('a', 'b') \
            .setup_file('/hive/setup.sh') \
            .tags('hive.tag1, hive.tag2') \
            .timeout(7) \
            .username('jhive') \
            .script('SELECT * FROM DUAL')

        assert_equals(
            pygenie.adapter.genie_2.get_payload(job),
            {
                u'attachments': [
                    {u'name': u'hive.file1', u'data': u'file contents'},
                    {u'name': u'hive.file2', u'data': u'file contents'},
                    {u'name': u'script.hive', u'data': u'SELECT * FROM DUAL'}
                ],
                u'clusterCriterias': [{u'tags': [u'type:hive.cluster1']}],
                u'commandArgs': u'-d a=b -f script.hive',
                u'commandCriteria': [u'type:hive.cmd'],
                u'description': u'this job is to test hivejob adapter',
                u'disableLogArchival': True,
                u'email': u'jhive@email.com',
                u'envPropFile': '/hive/setup.sh',
                u'fileDependencies': [],
                u'group': u'hive-group',
                u'id': u'hive-job1',
                u'name': u'testing_adapting_hivejob',
                u'tags': [u'hive.tag1', u'hive.tag2'],
                u'user': u'jhive',
                u'version': u'0.0.hive'
            }
        )

    @patch('pygenie.adapter.genie_2.to_attachment')
    @patch('os.path.isfile')
    @patch('pygenie.jobs.hive.is_file')
    def test_genie2_payload_file_script(self, presto_is_file, os_isfile, to_att):
        """Test HiveJob payload for Genie 2 (file script)."""

        os_isfile.return_value = True
        presto_is_file.return_value = True
        to_att.side_effect = mock_to_attachment

        job = pygenie.jobs.HiveJob(self.genie_2_conf) \
            .applications(['hive.app2']) \
            .archive(False) \
            .cluster_tags('type:hive.cluster2') \
            .command_tags('type:hive.cmd.2') \
            .dependencies(['/hive.file1', '/hive.file2']) \
            .description('this job is to test hivejob adapter') \
            .email('hive@email.com') \
            .group('hive-group') \
            .job_id('hive-job-2') \
            .job_name('testing_adapting_hivejob') \
            .job_version('0.0.hive-alpha') \
            .parameter('a', '1') \
            .parameter('b', '2') \
            .setup_file('/hive/setup.sh') \
            .tags('hive.tag1, hive.tag2') \
            .timeout(9) \
            .username('hive') \
            .script('/hive/script.hql')

        assert_equals(
            pygenie.adapter.genie_2.get_payload(job),
            {
                u'attachments': [
                    {u'name': u'hive.file1', u'data': u'file contents'},
                    {u'name': u'hive.file2', u'data': u'file contents'},
                    {u'name': u'script.hql', u'data': u'file contents'}
                ],
                u'clusterCriterias': [{u'tags': [u'type:hive.cluster2']}],
                u'commandArgs': u'-d a=1 -d b=2 -f script.hql',
                u'commandCriteria': [u'type:hive.cmd.2'],
                u'description': u'this job is to test hivejob adapter',
                u'disableLogArchival': True,
                u'email': u'hive@email.com',
                u'envPropFile': u'/hive/setup.sh',
                u'fileDependencies': [],
                u'group': u'hive-group',
                u'id': u'hive-job-2',
                u'name': u'testing_adapting_hivejob',
                u'tags': [u'hive.tag1', u'hive.tag2'],
                u'user': u'hive',
                u'version': u'0.0.hive-alpha'
            }
        )

    @patch('pygenie.adapter.genie_3.open')
    @patch('os.path.isfile')
    def test_genie3_payload_adhoc_script(self, os_isfile, file_open):
        """Test HiveJob payload for Genie 3 (adhoc script)."""

        os_isfile.side_effect = lambda f: f.startswith('/')
        file_open.side_effect = lambda f, m: u"open file '{}'".format(f)

        job = pygenie.jobs.HiveJob(self.genie_2_conf) \
            .applications(['hive.app']) \
            .archive(False) \
            .cluster_tags('type:hive.cluster-1') \
            .cluster_tags('type:hive.cluster-2') \
            .command_tags('type:hive.cmd.1') \
            .command_tags('type:hive.cmd.2') \
            .dependencies(['/hive.file1', '/hive.file2']) \
            .description('this job is to test hivejob adapter') \
            .email('hive@email.com') \
            .group('hive-group') \
            .job_id('hive-job-1') \
            .job_name('testing_adapting_hivejob') \
            .job_version('0.0.-0') \
            .parameter('a', 'a') \
            .parameter('b', 'b') \
            .setup_file('/hive/setup.sh') \
            .tags('hive.tag.1, hive.tag.2') \
            .timeout(9) \
            .username('hive') \
            .property_file('x://properties.conf') \
            .script('SELECT * FROM DUAL')

        assert_equals(
            pygenie.adapter.genie_3.get_payload(job),
            {
                u'applications': [u'hive.app'],
                u'attachments': [
                    (u'hive.file1', u"open file '/hive.file1'"),
                    (u'hive.file2', u"open file '/hive.file2'"),
                    (u'script.hive', u'SELECT * FROM DUAL')
                ],
                u'clusterCriterias': [
                    {u'tags': [u'type:hive.cluster-1', u'type:hive.cluster-2']}
                ],
                u'commandArgs': u'-i properties.conf  -d a=a -d b=b -f script.hive',
                u'commandCriteria': [u'type:hive.cmd.1', u'type:hive.cmd.2'],
                u'dependencies': [u'x://properties.conf'],
                u'description': u'this job is to test hivejob adapter',
                u'disableLogArchival': True,
                u'email': u'hive@email.com',
                u'group': u'hive-group',
                u'id': u'hive-job-1',
                u'name': u'testing_adapting_hivejob',
                u'setupFile': u'/hive/setup.sh',
                u'tags': [u'hive.tag.1', u'hive.tag.2'],
                u'timeout': 9,
                u'user': u'hive',
                u'version': u'0.0.-0'
            }
        )

    @patch('pygenie.adapter.genie_3.open')
    @patch('os.path.isfile')
    @patch('pygenie.jobs.hive.is_file')
    def test_genie3_payload_file_script(self, presto_is_file, os_isfile, file_open):
        """Test PrestoJob payload for Genie 3 (file script)."""

        os_isfile.return_value = True
        presto_is_file.return_value = True
        file_open.side_effect = lambda f, m: u"open file '{}'".format(f)

        job = pygenie.jobs.HiveJob(self.genie_2_conf) \
            .applications(['hive.app']) \
            .archive(False) \
            .cluster_tags('type:hive.cluster-1') \
            .cluster_tags('type:hive.cluster-2') \
            .command_tags('type:hive.cmd.1') \
            .command_tags('type:hive.cmd.2') \
            .dependencies(['/hive.file1', '/hive.file2']) \
            .description('this job is to test hivejob adapter') \
            .email('hive@email.com') \
            .group('hive-group') \
            .job_id('hive-job-1') \
            .job_name('testing_adapting_hivejob') \
            .job_version('0.0.-0') \
            .parameter('a', 'a') \
            .parameter('b', 'b') \
            .setup_file('/hive/setup.sh') \
            .tags('hive.tag.1, hive.tag.2') \
            .timeout(9) \
            .username('hive') \
            .property_file('/properties.conf') \
            .script('/script.hql')

        assert_equals(
            pygenie.adapter.genie_3.get_payload(job),
            {
                u'applications': [u'hive.app'],
                u'attachments': [
                    (u'hive.file1', u"open file '/hive.file1'"),
                    (u'hive.file2', u"open file '/hive.file2'"),
                    (u'properties.conf', u"open file '/properties.conf'"),
                    (u'script.hql', u"open file '/script.hql'")
                ],
                u'clusterCriterias': [
                    {u'tags': [u'type:hive.cluster-1', u'type:hive.cluster-2']}
                ],
                u'commandArgs': u'-i properties.conf  -d a=a -d b=b -f script.hql',
                u'commandCriteria': [u'type:hive.cmd.1', u'type:hive.cmd.2'],
                u'dependencies': [],
                u'description': u'this job is to test hivejob adapter',
                u'disableLogArchival': True,
                u'email': u'hive@email.com',
                u'group': u'hive-group',
                u'id': u'hive-job-1',
                u'name': u'testing_adapting_hivejob',
                u'setupFile': u'/hive/setup.sh',
                u'tags': [u'hive.tag.1', u'hive.tag.2'],
                u'timeout': 9,
                u'user': u'hive',
                u'version': u'0.0.-0'
            }
        )
