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
class TestingPigJob(unittest.TestCase):
    """Test PigJob."""

    def test_default_command_tag(self):
        """Test PigJob default command tags."""

        job = pygenie.jobs.PigJob()

        assert_equals(
            job.get('default_command_tags'),
            [u'type:pig']
        )

    def test_cmd_args_explicit(self):
        """Test PigJob explicit cmd args."""

        job = pygenie.jobs.PigJob() \
            .command_arguments('explicitly stating command args') \
            .script("""A = LOAD SOMETHING;""") \
            .parameter('foo', 'fizz')

        assert_equals(
            job.cmd_args,
            u'explicitly stating command args'
        )

    def test_cmd_args_constructed_script_code(self):
        """Test PigJob constructed cmd args for adhoc script."""

        job = pygenie.jobs.PigJob() \
            .script("""B = LOAD B;""") \
            .parameter('p1', 'v1') \
            .parameter('p2', 'v2') \
            .parameter('p3', 'with space') \
            .parameter_file('/Users/pig/params1.param') \
            .parameter_file('/Users/pig/params2.param') \
            .property('prop1', 'v1') \
            .property('prop2', 'v2') \
            .property_file('/Users/pig/props1.conf') \
            .property_file('/Users/pig/props2.conf')

        assert_equals(
            " ".join([
                "-Dprop1=v1 -Dprop2=v2",
                "-P props1.conf -P props2.conf",
                "-param_file params1.param",
                "-param_file params2.param",
                "-param_file _pig_parameters.txt",
                "-f script.pig"
            ]),
            job.cmd_args
        )

    @patch('pygenie.jobs.pig.is_file')
    def test_cmd_args_constructed_script_file(self, is_file):
        """Test PigJob constructed cmd args for file script."""

        is_file.return_value = True

        job = pygenie.jobs.PigJob() \
            .script('/Users/pig/test.pig') \
            .parameter('p', 'v') \
            .parameter_file('/Users/pig/p.params') \
            .property('p1', 'v1') \
            .property('p2', 'v2') \
            .property_file('/Users/pig/p1.conf') \
            .property_file('/Users/pig/p2.conf')

        assert_equals(
            " ".join([
                "-Dp1=v1 -Dp2=v2",
                "-P p1.conf -P p2.conf",
                "-param_file p.params",
                "-param_file _pig_parameters.txt",
                "-f test.pig"
            ]),
            job.cmd_args
        )

    @patch('pygenie.jobs.pig.is_file')
    def test_cmd_args_post_cmd_args(self, is_file):
        """Test PigJob constructed cmd args with post cmd args."""

        is_file.return_value = True

        job = pygenie.jobs.PigJob() \
            .script('/Users/pig/test.pig') \
            .parameter('p', 'v') \
            .parameter_file('/Users/pig/p.params') \
            .property('p1', 'v1') \
            .property('p2', 'v2') \
            .property_file('/Users/pig/p1.conf') \
            .property_file('/Users/pig/p2.conf') \
            .post_cmd_args('a') \
            .post_cmd_args(['a', 'b', 'c']) \
            .post_cmd_args('d e f')

        assert_equals(
            " ".join([
                "-Dp1=v1 -Dp2=v2",
                "-P p1.conf -P p2.conf",
                "-param_file p.params",
                "-param_file _pig_parameters.txt",
                "-f test.pig",
                "a b c d e f"
            ]),
            job.cmd_args
        )


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestingPigParameterFile(unittest.TestCase):
    """Test PigJob parameter file consturction."""

    def test_basic(self):
        """Test PigJob parameter file construction."""

        job = pygenie.jobs.PigJob() \
            .parameter("spaces", "this has spaces") \
            .parameter("single_quotes", "test' test'") \
            .parameter("double_quotes", 'Something: "Episode 189"') \
            .parameter("escaped_single_quotes", "Barney\\'s Adventure") \
            .parameter("number", 8) \
            .parameter("unicode", "\u0147\u0147\u0147") \
            .parameter("filter_ts", "ts >= '2000-01-01 00:00:00' AND ts < '2000-12-31 00:00:00'")

        param_file = \
"""\
spaces = "this has spaces"
single_quotes = "test' test'"
double_quotes = "Something: \\"Episode 189\\""
escaped_single_quotes = "Barney\\'s Adventure"
number = "8"
unicode = "\u0147\u0147\u0147"
filter_ts = "ts >= '2000-01-01 00:00:00' AND ts < '2000-12-31 00:00:00'"\
"""

        assert_equals(
            param_file,
            job._parameter_file
        )


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestingPigJobRepr(unittest.TestCase):
    """Test PigJob repr."""

    @patch('pygenie.jobs.core.is_file')
    def test_repr(self, is_file):
        """Test PigJob repr."""

        is_file.return_value = True

        job = pygenie.jobs.PigJob() \
            .applications('pig_app_1') \
            .applications('pig_app_2') \
            .archive(False) \
            .cluster_tags('pig_cluster_1') \
            .cluster_tags('pig_cluster_2') \
            .command_tags('pig_cmd_1') \
            .command_tags('pig_cmd_2') \
            .dependencies('/Users/pig/dep1') \
            .dependencies('/Users/pig/dep2') \
            .description('"test pig desc"') \
            .disable_archive() \
            .genie_email('pig@email.com') \
            .genie_setup_file('/pig_setup.sh') \
            .genie_timeout(5) \
            .genie_username('jpig') \
            .group('piggroup') \
            .job_id('pigjob_repr_id') \
            .job_name('pigjob_repr') \
            .job_version('0.0.0') \
            .parameter('pig_param_1', '1') \
            .parameter('pig_param_2', '2') \
            .parameter_file('/Users/pig/param_1.params') \
            .parameter_file('/Users/pig/param_2.params') \
            .property('mapred.pig.1', 'p1') \
            .property('mapred.pig.2', 'p2') \
            .property_file('/Users/pig/test1.conf') \
            .property_file('/Users/pig/test2.conf') \
            .script('/Users/pig/testscript.pig') \
            .tags('pig_tag_1') \
            .tags('pig_tag_2')

        assert_equals(
            '.'.join([
                'PigJob()',
                'applications("pig_app_1")',
                'applications("pig_app_2")',
                'archive(False)',
                'cluster_tags("pig_cluster_1")',
                'cluster_tags("pig_cluster_2")',
                'command_tags("pig_cmd_1")',
                'command_tags("pig_cmd_2")',
                'dependencies("/Users/pig/dep1")',
                'dependencies("/Users/pig/dep2")',
                "description('\"test pig desc\"')",
                'genie_email("pig@email.com")',
                'genie_setup_file("/pig_setup.sh")',
                'genie_timeout(5)',
                'genie_username("jpig")',
                'group("piggroup")',
                'job_id("pigjob_repr_id")',
                'job_name("pigjob_repr")',
                'job_version("0.0.0")',
                'parameter("pig_param_1", "1")',
                'parameter("pig_param_2", "2")',
                'parameter_file("/Users/pig/param_1.params")',
                'parameter_file("/Users/pig/param_2.params")',
                'property("mapred.pig.1", "p1")',
                'property("mapred.pig.2", "p2")',
                'property_file("/Users/pig/test1.conf")',
                'property_file("/Users/pig/test2.conf")',
                'script("/Users/pig/testscript.pig")',
                'tags("pig_tag_1")',
                'tags("pig_tag_2")'
            ]),
            str(job)
        )


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestingPigJobAdapters(unittest.TestCase):
    """Test adapting PigJob to different clients."""

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
        """Test PigJob payload for Genie 2 (adhoc script)."""

        os_isfile.side_effect = lambda f: f.startswith('/')
        to_att.side_effect = mock_to_attachment

        job = pygenie.jobs.PigJob(self.genie_2_conf) \
            .applications(['pig_app_1']) \
            .cluster_tags('type:pig_cluster_1') \
            .command_tags('type:pig') \
            .dependencies(['/pigfile1', '/pigfile2']) \
            .dependencies('x://externalfile') \
            .description('this job is to test pigjob adapter') \
            .archive(False) \
            .genie_email('pig@email.com') \
            .genie_timeout(1) \
            .genie_username('jpig') \
            .group('piggroup') \
            .job_id('pig_job_id_1') \
            .job_name('testing_adapting_pigjob') \
            .parameter('param1', '1') \
            .parameter('param2', '2') \
            .parameter_file('/pig_param1.params') \
            .parameter_file('/pig_param2.params') \
            .property('mr.p1', 'a') \
            .property('mr.p2', 'b') \
            .property_file('x://external/my_properties.conf') \
            .script("""A = LOAD;""") \
            .tags('pigtag1, pigtag2') \
            .job_version('0.0.1pig')

        assert_equals(
            pygenie.adapter.genie_2.get_payload(job),
            {
                u'attachments': [
                    {u'data': u'file contents', u'name': u'pigfile1'},
                    {u'data': u'file contents', u'name': u'pigfile2'},
                    {u'data': u'file contents', u'name': u'pig_param1.params'},
                    {u'data': u'file contents', u'name': u'pig_param2.params'},
                    {u'data': u'A = LOAD;', u'name': u'script.pig'},
                    {u'data': u'param1 = "1"\nparam2 = "2"', u'name': u'_pig_parameters.txt'}
                ],
                u'clusterCriterias': [
                    {u'tags': [u'type:pig_cluster_1']},
                    {u'tags': [u'type:pig']}
                ],
                u'commandArgs': u' '.join([
                    u'-Dmr.p1=a -Dmr.p2=b',
                    u'-P my_properties.conf',
                    u'-param_file pig_param1.params -param_file pig_param2.params',
                    u'-param_file _pig_parameters.txt',
                    u'-f script.pig'
                ]),
                u'commandCriteria': [u'type:pig'],
                u'description': u'this job is to test pigjob adapter',
                u'disableLogArchival': True,
                u'email': u'pig@email.com',
                u'envPropFile': None,
                u'fileDependencies': [
                    u'x://externalfile',
                    u'x://external/my_properties.conf'
                ],
                u'group': u'piggroup',
                u'id': u'pig_job_id_1',
                u'name': u'testing_adapting_pigjob',
                u'tags': [u'pigtag1', u'pigtag2'],
                u'user': u'jpig',
                u'version': u'0.0.1pig'
            }
        )

    @patch('pygenie.adapter.genie_2.to_attachment')
    @patch('os.path.isfile')
    @patch('pygenie.jobs.pig.is_file')
    def test_genie2_payload_file_script(self, is_file, os_isfile, to_att):
        """Test PigJob payload for Genie 2 (file script)."""

        os_isfile.return_value = True
        is_file.return_value = True
        to_att.side_effect = mock_to_attachment

        job = pygenie.jobs.PigJob(self.genie_2_conf) \
            .applications(['pigapp1']) \
            .cluster_tags('type:pigcluster1') \
            .command_tags('type:pigcmd') \
            .dependencies(['/pigfile1']) \
            .description('this job is to test pigjob adapter') \
            .archive(False) \
            .genie_email('jpig@email.com') \
            .genie_timeout(7) \
            .genie_username('jpig') \
            .group('piggroup') \
            .job_id('pigjob1') \
            .job_name('testing_adapting_pigjob') \
            .script('/path/to/test/script.pig') \
            .tags('pigtag') \
            .job_version('0.0.pig')

        assert_equals(
            pygenie.adapter.genie_2.get_payload(job),
            {
                u'attachments': [
                    {u'name': u'pigfile1', u'data': u'file contents'},
                    {u'name': u'script.pig', u'data': u'file contents'}
                ],
                u'clusterCriterias': [
                    {u'tags': [u'type:pigcluster1']},
                    {u'tags': [u'type:pig']}
                ],
                u'commandArgs': u'-f script.pig',
                u'commandCriteria': [u'type:pigcmd'],
                u'description': u'this job is to test pigjob adapter',
                u'disableLogArchival': True,
                u'email': u'jpig@email.com',
                u'envPropFile': None,
                u'fileDependencies': [],
                u'group': u'piggroup',
                u'id': u'pigjob1',
                u'name': u'testing_adapting_pigjob',
                u'tags': [u'pigtag'],
                u'user': u'jpig',
                u'version': u'0.0.pig'
            }
        )

    @patch('pygenie.adapter.genie_3.open')
    @patch('os.path.isfile')
    def test_genie3_payload_adhoc_script(self, os_isfile, file_open):
        """Test PigJob payload for Genie 3 (adhoc script)."""

        os_isfile.side_effect = lambda f: f.startswith('/')
        file_open.side_effect = lambda f, m: u"open file '{}'".format(f)

        job = pygenie.jobs.PigJob(self.genie_3_conf) \
            .applications(['pig_app_1']) \
            .cluster_tags('type:pig_cluster_1') \
            .command_tags('type:pig') \
            .dependencies(['/pigfile1', '/pigfile2']) \
            .dependencies('x://externalfile') \
            .description('this job is to test pigjob adapter') \
            .archive(False) \
            .genie_email('pig@email.com') \
            .genie_timeout(1) \
            .genie_username('jpig') \
            .group('piggroup') \
            .job_id('pig_job_id_1') \
            .job_name('testing_adapting_pigjob') \
            .parameter('param1', '1') \
            .parameter('param2', '2') \
            .parameter_file('/pig_param1.params') \
            .parameter_file('/pig_param2.params') \
            .property('mr.p1', 'a') \
            .property('mr.p2', 'b') \
            .property_file('x://external/my_properties.conf') \
            .property_file('/my_properties_local.conf') \
            .script("""A = LOAD;""") \
            .tags('pigtag1, pigtag2') \
            .job_version('0.0.1pig')

        assert_equals(
            {
                'applications': ['pig_app_1'],
                'attachments': [
                    ('pigfile1', "open file '/pigfile1'"),
                    ('pigfile2', "open file '/pigfile2'"),
                    ('pig_param1.params', "open file '/pig_param1.params'"),
                    ('pig_param2.params', "open file '/pig_param2.params'"),
                    ('my_properties_local.conf', "open file '/my_properties_local.conf'"),
                    ('script.pig', 'A = LOAD;'),
                    ('_pig_parameters.txt', 'param1 = "1"\nparam2 = "2"')
                ],
                'clusterCriterias': [
                    {'tags': ['type:pig_cluster_1']},
                    {'tags': ['type:pig']}
                ],
                'commandArgs': ' '.join([
                    '-Dmr.p1=a -Dmr.p2=b',
                    '-P my_properties_local.conf -P my_properties.conf',
                    '-param_file pig_param1.params',
                    '-param_file pig_param2.params',
                    '-param_file _pig_parameters.txt',
                    '-f script.pig'
                ]),
                'commandCriteria': ['type:pig'],
                'dependencies': [
                    'x://externalfile',
                    'x://external/my_properties.conf'
                ],
                'description': 'this job is to test pigjob adapter',
                'disableLogArchival': True,
                'email': 'pig@email.com',
                'group': 'piggroup',
                'id': 'pig_job_id_1',
                'name': 'testing_adapting_pigjob',
                'setupFile': None,
                'tags': ['pigtag1', 'pigtag2'],
                'timeout': 1,
                'user': 'jpig',
                'version': '0.0.1pig'
            },
            pygenie.adapter.genie_3.get_payload(job)
        )

    @patch('pygenie.adapter.genie_3.open')
    @patch('os.path.isfile')
    @patch('pygenie.jobs.pig.is_file')
    def test_genie3_payload_file_script(self, is_file, os_isfile, file_open):
        """Test PigJob payload for Genie 3 (file script)."""

        os_isfile.return_value = True
        is_file.return_value = True
        file_open.side_effect = lambda f, m: u"open file '{}'".format(f)

        job = pygenie.jobs.PigJob(self.genie_3_conf) \
            .applications(['pigapp1']) \
            .cluster_tags('type:pigcluster1') \
            .command_tags('type:pigcmd') \
            .dependencies(['/pigfile1']) \
            .description('this job is to test pigjob adapter') \
            .archive(False) \
            .genie_email('jpig@email.com') \
            .genie_timeout(7) \
            .genie_username('jpig') \
            .group('piggroup') \
            .job_id('pigjob1') \
            .job_name('testing_adapting_pigjob') \
            .script('/path/to/test/script.pig') \
            .tags('pigtag') \
            .job_version('0.0.pig')

        assert_equals(
            {
                'applications': ['pigapp1'],
                'attachments': [
                    ('pigfile1', "open file '/pigfile1'"),
                    ('script.pig', "open file '/path/to/test/script.pig'")
                ],
                'clusterCriterias': [
                    {'tags': ['type:pigcluster1']},
                    {'tags': ['type:pig']}
                ],
                'commandArgs': '-f script.pig',
                'commandCriteria': ['type:pigcmd'],
                'dependencies': [],
                'description': 'this job is to test pigjob adapter',
                'disableLogArchival': True,
                'email': 'jpig@email.com',
                'group': 'piggroup',
                'id': 'pigjob1',
                'name': 'testing_adapting_pigjob',
                'setupFile': None,
                'tags': ['pigtag'],
                'timeout': 7,
                'user': 'jpig',
                'version': '0.0.pig'
            },
            pygenie.adapter.genie_3.get_payload(job)
        )
