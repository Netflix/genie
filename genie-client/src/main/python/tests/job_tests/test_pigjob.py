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
            .property_file('/Users/pig/props.conf')

        assert_equals(
            job.cmd_args,
            u'-P props.conf -Dprop1=v1 -Dprop2=v2 ' \
            u'-param_file params1.param ' \
            u'-param_file params2.param ' \
            u'-p p2=v2 -p "p3=with space" -p p1=v1 ' \
            u'-f script.pig'
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
            .property_file('/Users/pig/p.conf')

        assert_equals(
            job.cmd_args,
            u'-P p.conf -Dp2=v2 -Dp1=v1 ' \
            u'-param_file p.params -p p=v -f test.pig'
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
            .email('pig@email.com') \
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
            .property_file('/Users/pig/test.conf') \
            .script('/Users/pig/testscript.pig') \
            .setup_file('/pig_setup.sh') \
            .tags('pig_tag_1') \
            .tags('pig_tag_2') \
            .timeout(5) \
            .username('jpig')

        assert_equals(
            str(job),
            u'PigJob()' \
            u'.applications("pig_app_1")' \
            u'.applications("pig_app_2")' \
            u'.archive(False)' \
            u'.cluster_tags("pig_cluster_1")' \
            u'.cluster_tags("pig_cluster_2")' \
            u'.command_tags("pig_cmd_1")' \
            u'.command_tags("pig_cmd_2")' \
            u'.dependencies("/Users/pig/dep1")' \
            u'.dependencies("/Users/pig/dep2")' \
            u".description('\"test pig desc\"')" \
            u'.email("pig@email.com")' \
            u'.group("piggroup")' \
            u'.job_id("pigjob_repr_id")' \
            u'.job_name("pigjob_repr")' \
            u'.job_version("0.0.0")' \
            u'.parameter("pig_param_1", "1")' \
            u'.parameter("pig_param_2", "2")' \
            u'.parameter_file("/Users/pig/param_1.params")' \
            u'.parameter_file("/Users/pig/param_2.params")' \
            u'.property("mapred.pig.1", "p1")' \
            u'.property("mapred.pig.2", "p2")' \
            u'.property_file("/Users/pig/test.conf")' \
            u'.script("/Users/pig/testscript.pig")' \
            u'.setup_file("/pig_setup.sh")' \
            u'.tags("pig_tag_1")' \
            u'.tags("pig_tag_2")' \
            u'.timeout(5)' \
            u'.username("jpig")'
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
            .email('pig@email.com') \
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
            .timeout(1) \
            .username('jpig') \
            .job_version('0.0.1pig')

        assert_equals(
            pygenie.adapter.genie_2.get_payload(job),
            {
                u'attachments': [
                    {u'data': u'file contents', u'name': u'pigfile1'},
                    {u'data': u'file contents', u'name': u'pigfile2'},
                    {u'data': u'file contents', u'name': u'pig_param1.params'},
                    {u'data': u'file contents', u'name': u'pig_param2.params'},
                    {u'data': u'A = LOAD;', u'name': u'script.pig'}
                ],
                u'clusterCriterias': [{u'tags': [u'type:pig_cluster_1']}],
                u'commandArgs': u'-P my_properties.conf ' \
                                u'-Dmr.p1=a -Dmr.p2=b ' \
                                u'-param_file pig_param1.params -param_file pig_param2.params '\
                                u'-p param2=2 -p param1=1 '\
                                u'-f script.pig',
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
            .email('jpig@email.com') \
            .group('piggroup') \
            .job_id('pigjob1') \
            .job_name('testing_adapting_pigjob') \
            .script('/path/to/test/script.pig') \
            .tags('pigtag') \
            .timeout(7) \
            .username('jpig') \
            .job_version('0.0.pig')

        assert_equals(
            pygenie.adapter.genie_2.get_payload(job),
            {
                u'attachments': [
                    {u'name': u'pigfile1', u'data': u'file contents'},
                    {u'name': u'script.pig', u'data': u'file contents'}
                ],
                u'clusterCriterias': [{u'tags': [u'type:pigcluster1']}],
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
            .email('pig@email.com') \
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
            .timeout(1) \
            .username('jpig') \
            .job_version('0.0.1pig')

        assert_equals(
            pygenie.adapter.genie_3.get_payload(job),
            {
                u'applications': [u'pig_app_1'],
                u'attachments': [
                    (u'pigfile1', u"open file '/pigfile1'"),
                    (u'pigfile2', u"open file '/pigfile2'"),
                    (u'pig_param1.params', u"open file '/pig_param1.params'"),
                    (u'pig_param2.params', u"open file '/pig_param2.params'"),
                    (u'script.pig', u'A = LOAD;')
                ],
                u'clusterCriterias': [{u'tags': [u'type:pig_cluster_1']}],
                u'commandArgs': u'-P my_properties.conf -Dmr.p1=a -Dmr.p2=b ' \
                                u'-param_file pig_param1.params ' \
                                u'-param_file pig_param2.params ' \
                                u'-p param2=2 -p param1=1 ' \
                                u'-f script.pig',
                u'commandCriteria': [u'type:pig'],
                u'dependencies': [
                    u'x://externalfile',
                    u'x://external/my_properties.conf'
                ],
                u'description': u'this job is to test pigjob adapter',
                u'disableLogArchival': True,
                u'email': u'pig@email.com',
                u'group': u'piggroup',
                u'id': u'pig_job_id_1',
                u'name': u'testing_adapting_pigjob',
                u'setupFile': None,
                u'tags': [u'pigtag1', u'pigtag2'],
                u'timeout': 1,
                u'user': u'jpig',
                u'version': u'0.0.1pig'
            }
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
            .email('jpig@email.com') \
            .group('piggroup') \
            .job_id('pigjob1') \
            .job_name('testing_adapting_pigjob') \
            .script('/path/to/test/script.pig') \
            .tags('pigtag') \
            .timeout(7) \
            .username('jpig') \
            .job_version('0.0.pig')

        assert_equals(
            pygenie.adapter.genie_3.get_payload(job),
            {
                u'applications': [u'pigapp1'],
                u'attachments': [
                    (u'pigfile1', u"open file '/pigfile1'"),
                    (u'script.pig', u"open file '/path/to/test/script.pig'")
                ],
                u'clusterCriterias': [{u'tags': [u'type:pigcluster1']}],
                u'commandArgs': u'-f script.pig',
                u'commandCriteria': [u'type:pigcmd'],
                u'dependencies': [],
                u'description': u'this job is to test pigjob adapter',
                u'disableLogArchival': True,
                u'email': u'jpig@email.com',
                u'group': u'piggroup',
                u'id': u'pigjob1',
                u'name': u'testing_adapting_pigjob',
                u'setupFile': None,
                u'tags': [u'pigtag'],
                u'timeout': 7,
                u'user': u'jpig',
                u'version': u'0.0.pig'
            }
        )
