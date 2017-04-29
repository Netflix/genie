from __future__ import absolute_import, division, print_function, unicode_literals

import os
import unittest

from mock import patch
from nose.tools import assert_equals, assert_raises


assert_equals.__self__.maxDiff = None


import pygenie


def mock_to_attachment(att):
    if isinstance(att, dict):
        return {u'name': att['name'], u'data': att['data']}
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
            .property('source', 'tester') \
            .property_file('properties.hive')

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
            .property('prop2', 'p2') \
            .property_file('properties_1.hive') \
            .property_file('properties_2.hive')

        assert_equals(
            job.cmd_args,
            " ".join([
                "-i properties_1.hive -i properties_2.hive",
                "--hiveconf hconf1=h1 --hiveconf prop1=p1 --hiveconf prop2=p2",
                "-i _hive_parameters.txt",
                "-f script.hive"
            ])
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
            .property('p2', 'v2') \
            .property_file('props_1.hive') \
            .property_file('props_2.hive')

        assert_equals(
            " ".join([
                "-i props_1.hive -i props_2.hive",
                "--hiveconf p1=v1 --hiveconf p2=v2",
                "-i _hive_parameters.txt",
                "-f test.hql"
            ]),
            job.cmd_args
        )

    @patch('pygenie.jobs.hive.is_file')
    def test_cmd_args_post_cmd_args(self, is_file):
        """Test HiveJob constructed cmd args with post cmd args."""

        is_file.return_value = True

        job = pygenie.jobs.HiveJob() \
            .script('/Users/hive/test.hql') \
            .parameter('hello', 'hi') \
            .parameter('goodbye', 'bye') \
            .property('p1', 'v1') \
            .property('p2', 'v2') \
            .post_cmd_args('a') \
            .post_cmd_args(['a', 'b', 'c']) \
            .post_cmd_args('d e f')

        assert_equals(
            " ".join([
                "--hiveconf p1=v1 --hiveconf p2=v2",
                "-i _hive_parameters.txt",
                "-f test.hql",
                "a b c d e f"
            ]),
            job.cmd_args
        )


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestingHiveJobParameters(unittest.TestCase):
    """Test HiveJob parameters."""

    def test_parameter_file(self):
        """Test HiveJob parameters into file."""

        job = pygenie.jobs.HiveJob() \
            .parameter("spaces", "this has spaces") \
            .parameter("single_quotes", "test' test'") \
            .parameter("escaped_single_quotes", "Barney\\\'s Adventure") \
            .parameter("unicode", "\xf3\xf3\xf3") \
            .parameter("number", 8)

        assert_equals(
            '\n'.join([
                "SET hivevar:spaces=this has spaces;",
                "SET hivevar:single_quotes=test' test';",
                "SET hivevar:escaped_single_quotes=Barney\\\'s Adventure;",
                "SET hivevar:unicode=\xf3\xf3\xf3;",
                "SET hivevar:number=8;"
            ]),
            job._parameter_file
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
            .genie_email('jhive@email.com') \
            .genie_setup_file('/hive.setup.sh') \
            .genie_timeout(1) \
            .genie_username('jhive') \
            .group('hive-group') \
            .job_id('hivejob_repr') \
            .job_name('hivejob_repr') \
            .job_version('1.1.5') \
            .parameter('param1', 'pval1') \
            .parameter('param2', 'pval2') \
            .tags('hive.tag.1') \
            .tags('hive.tag.2')

        job \
            .hiveconf('hconf1', '1') \
            .hiveconf('hconf2', '2') \
            .property('prop1', '1') \
            .property('prop2', '2') \
            .property_file('/hive/conf1.prop') \
            .property_file('/hive/conf2.prop') \
            .script("SELECT * FROM TEST") \

        assert_equals(
            '.'.join([
                'HiveJob()',
                'applications("hive.app.1")',
                'applications("hive.app.2")',
                'archive(False)',
                'cluster_tags("hive.cluster1")',
                'cluster_tags("hive.cluster2")',
                'command_tags("hive.cmd1")',
                'command_tags("hive.cmd2")',
                'dependencies("/hive/dep1")',
                'dependencies("/hive/dep2")',
                'description("hive description")',
                'genie_email("jhive@email.com")',
                'genie_setup_file("/hive.setup.sh")',
                'genie_timeout(1)',
                'genie_username("jhive")',
                'group("hive-group")',
                'hiveconf("hconf1", "1")',
                'hiveconf("hconf2", "2")',
                'hiveconf("prop1", "1")',
                'hiveconf("prop2", "2")',
                'job_id("hivejob_repr")',
                'job_name("hivejob_repr")',
                'job_version("1.1.5")',
                'parameter("param1", "pval1")',
                'parameter("param2", "pval2")',
                'property_file("/hive/conf1.prop")',
                'property_file("/hive/conf2.prop")',
                'script("SELECT * FROM TEST")',
                'tags("hive.tag.1")',
                'tags("hive.tag.2")'
            ]),
            str(job)
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
            .genie_email('jhive@email.com') \
            .genie_setup_file('/hive/setup.sh') \
            .genie_timeout(7) \
            .genie_username('jhive') \
            .group('hive-group') \
            .job_id('hive-job1') \
            .job_name('testing_adapting_hivejob') \
            .job_version('0.0.hive') \
            .parameter('a', 'b') \
            .tags('hive.tag1, hive.tag2') \
            .script('SELECT * FROM DUAL')

        assert_equals(
            pygenie.adapter.genie_2.get_payload(job),
            {
                u'attachments': [
                    {u'name': u'hive.file1', u'data': u'file contents'},
                    {u'name': u'hive.file2', u'data': u'file contents'},
                    {u'name': u'script.hive', u'data': u'SELECT * FROM DUAL'},
                    {u'name': u'_hive_parameters.txt', u'data': u'SET hivevar:a=b;'}
                ],
                u'clusterCriterias': [
                    {u'tags': [u'type:hive.cluster1']},
                    {u'tags': [u'type:hive']}
                ],
                u'commandArgs': u'-i _hive_parameters.txt -f script.hive',
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
            .genie_email('hive@email.com') \
            .genie_setup_file('/hive/setup.sh') \
            .genie_timeout(9) \
            .genie_username('hive') \
            .group('hive-group') \
            .job_id('hive-job-2') \
            .job_name('testing_adapting_hivejob') \
            .job_version('0.0.hive-alpha') \
            .parameter('a', '1') \
            .parameter('b', '2') \
            .tags('hive.tag1, hive.tag2') \
            .script('/hive/script.hql')

        assert_equals(
            pygenie.adapter.genie_2.get_payload(job),
            {
                u'attachments': [
                    {u'name': u'hive.file1', u'data': u'file contents'},
                    {u'name': u'hive.file2', u'data': u'file contents'},
                    {u'name': u'script.hql', u'data': u'file contents'},
                    {u'name': u'_hive_parameters.txt',
                     u'data': u'SET hivevar:a=1;\nSET hivevar:b=2;'}
                ],
                u'clusterCriterias': [
                    {u'tags': [u'type:hive.cluster2']},
                    {u'tags': [u'type:hive']}
                ],
                u'commandArgs': u'-i _hive_parameters.txt -f script.hql',
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
            .genie_email('hive@email.com') \
            .genie_setup_file('/hive/setup.sh') \
            .genie_timeout(9) \
            .genie_username('hive') \
            .group('hive-group') \
            .job_id('hive-job-1') \
            .job_name('testing_adapting_hivejob') \
            .job_version('0.0.-0') \
            .parameter('a', 'a') \
            .parameter('b', 'b') \
            .tags('hive.tag.1, hive.tag.2') \
            .property_file('x://properties.conf') \
            .property_file('/properties_local.conf') \
            .script('SELECT * FROM DUAL')

        assert_equals(
            {
                'applications': ['hive.app'],
                'attachments': [
                    ('hive.file1', "open file '/hive.file1'"),
                    ('hive.file2', "open file '/hive.file2'"),
                    ('properties_local.conf', "open file '/properties_local.conf'"),
                    ('script.hive', 'SELECT * FROM DUAL'),
                    ('_hive_parameters.txt', 'SET hivevar:a=a;\nSET hivevar:b=b;')
                ],
                'clusterCriterias': [
                    {'tags': ['type:hive.cluster-1', 'type:hive.cluster-2']},
                    {'tags': ['type:hive']}
                ],
                'commandArgs': '-i properties_local.conf -i properties.conf  -i _hive_parameters.txt -f script.hive',
                'commandCriteria': ['type:hive.cmd.1', 'type:hive.cmd.2'],
                'dependencies': ['x://properties.conf'],
                'description': 'this job is to test hivejob adapter',
                'disableLogArchival': True,
                'email': 'hive@email.com',
                'group': 'hive-group',
                'id': 'hive-job-1',
                'name': 'testing_adapting_hivejob',
                'setupFile': '/hive/setup.sh',
                'tags': ['hive.tag.1', 'hive.tag.2'],
                'timeout': 9,
                'user': 'hive',
                'version': '0.0.-0'
            },
            pygenie.adapter.genie_3.get_payload(job)
        )

    @patch('pygenie.adapter.genie_3.open')
    @patch('os.path.isfile')
    @patch('pygenie.jobs.hive.is_file')
    def test_genie3_payload_file_script(self, presto_is_file, os_isfile, file_open):
        """Test HiveJob payload for Genie 3 (file script)."""

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
            .genie_email('hive@email.com') \
            .genie_setup_file('/hive/setup.sh') \
            .genie_timeout(9) \
            .genie_username('hive') \
            .group('hive-group') \
            .job_id('hive-job-1') \
            .job_name('testing_adapting_hivejob') \
            .job_version('0.0.-0') \
            .parameter('a', 'a') \
            .parameter('b', 'b') \
            .tags('hive.tag.1, hive.tag.2') \
            .property_file('/properties1.conf') \
            .property_file('/properties2.conf') \
            .script('/script.hql')

        assert_equals(
            {
                'applications': ['hive.app'],
                'attachments': [
                    ('hive.file1', "open file '/hive.file1'"),
                    ('hive.file2', "open file '/hive.file2'"),
                    ('properties1.conf', "open file '/properties1.conf'"),
                    ('properties2.conf', "open file '/properties2.conf'"),
                    ('script.hql', "open file '/script.hql'"),
                    ('_hive_parameters.txt', 'SET hivevar:a=a;\nSET hivevar:b=b;')
                ],
                'clusterCriterias': [
                    {'tags': ['type:hive.cluster-1', 'type:hive.cluster-2']},
                    {'tags': ['type:hive']}
                ],
                'commandArgs': '-i properties1.conf -i properties2.conf  -i _hive_parameters.txt -f script.hql',
                'commandCriteria': ['type:hive.cmd.1', 'type:hive.cmd.2'],
                'dependencies': [],
                'description': 'this job is to test hivejob adapter',
                'disableLogArchival': True,
                'email': 'hive@email.com',
                'group': 'hive-group',
                'id': 'hive-job-1',
                'name': 'testing_adapting_hivejob',
                'setupFile': '/hive/setup.sh',
                'tags': ['hive.tag.1', 'hive.tag.2'],
                'timeout': 9,
                'user': 'hive',
                'version': '0.0.-0'
            },
            pygenie.adapter.genie_3.get_payload(job)
        )
