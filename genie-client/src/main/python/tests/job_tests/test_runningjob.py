from __future__ import absolute_import, division, print_function, unicode_literals

import unittest

from mock import call, patch
from nose.tools import assert_equals, assert_raises

import pygenie

from ..utils import fake_response

assert_equals.__self__.maxDiff = None


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestingRunningJobIsDone(unittest.TestCase):
    """Test RunningJob().is_done."""

    @patch('pygenie.adapter.genie_3.Genie3Adapter.get_status')
    def test_init_status(self, get_status):
        """Test RunningJob().is_done with 'INIT' status."""

        get_status.return_value = 'INIT'

        running_job = pygenie.jobs.RunningJob('1234-init',
                                              info={'status': 'INIT'})

        assert_equals(
            running_job.is_done,
            False
        )

    @patch('pygenie.adapter.genie_3.Genie3Adapter.get_status')
    def test_running_status(self, get_status):
        """Test RunningJob().is_done with 'RUNNING' status."""

        get_status.return_value = 'RUNNING'

        running_job = pygenie.jobs.RunningJob('1234-running',
                                              info={'status': 'RUNNING'})

        assert_equals(
            running_job.is_done,
            False
        )

    def test_killed_status(self):
        """Test RunningJob().is_done with 'KILLED' status."""

        running_job = pygenie.jobs.RunningJob('1234-killed',
                                              info={'status': 'KILLED'})

        assert_equals(
            running_job.is_done,
            True
        )

    def test_succeeded_status(self):
        """Test RunningJob().is_done with 'SUCCEEDED' status."""

        running_job = pygenie.jobs.RunningJob('1234-succeeded',
                                              info={'status': 'SUCCEEDED'})

        assert_equals(
            running_job.is_done,
            True
        )

    def test_failed_status(self):
        """Test RunningJob().is_done with 'FAILED' status."""

        running_job = pygenie.jobs.RunningJob('1234-failed',
                                              info={'status': 'FAILED'})

        assert_equals(
            running_job.is_done,
            True
        )


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestingRunningJobUpdate(unittest.TestCase):
    """Test updating RunningJob information."""

    @patch('pygenie.adapter.genie_3.Genie3Adapter.get_info_for_rj')
    def test_init_with_info(self, get_info):
        """Test init RunningJob with info."""

        info = {
            'job_id': '1234',
            'status': 'SUCCEEDED'
        }

        running_job = pygenie.jobs.RunningJob('1234', info=info)

        get_info.assert_not_called()

    @patch('pygenie.adapter.genie_3.Genie3Adapter.get_info_for_rj')
    def test_init_without_info(self, get_info):
        """Test init RunningJob without info."""

        running_job = pygenie.jobs.RunningJob('1234-no-info')

        get_info.assert_not_called()

    @patch('pygenie.adapter.genie_3.Genie3Adapter.get_info_for_rj')
    def test_update(self, get_info):
        """Test calling update for RunningJob."""

        running_job = pygenie.jobs.RunningJob('1234-update')
        running_job.update()

        assert_equals(
            get_info.call_args_list,
            [call(u'1234-update')]
        )

    @patch('pygenie.adapter.genie_3.Genie3Adapter.get')
    def test_update_timeout(self, get):
        """Test calling update for RunningJob (with timeout)."""

        get.side_effect = [{'_links':{'self':{'href':'http://example.com'}}},{},[],{},{},{}]

        running_job = pygenie.jobs.RunningJob('1234-update-timeout')
        running_job.update(timeout=3)

        assert_equals(
            [
                call('1234-update-timeout', timeout=3),
                call('1234-update-timeout', path='request', timeout=3),
                call('1234-update-timeout', if_not_found=[], path='applications', timeout=3),
                call('1234-update-timeout', if_not_found={}, path='cluster', timeout=3),
                call('1234-update-timeout', if_not_found={}, path='command', timeout=3),
                call('1234-update-timeout', if_not_found={}, path='execution', timeout=3)
            ],
            get.call_args_list
        )

    @patch('pygenie.adapter.genie_3.Genie3Adapter.get')
    def test_update_section(self, get):
        """Test calling update section for RunningJob."""

        get.side_effect = [{'_links':{'self':{'href':'http://example.com'}}}]

        running_job = pygenie.jobs.RunningJob('1234-update-section')
        running_job.update(info_section='job')

        assert_equals(
            [call(u'1234-update-section', timeout=30)],
            get.call_args_list
        )

    @patch('pygenie.adapter.genie_3.Genie3Adapter.get')
    def test_update_section_timeout(self, get):
        """Test calling update section for RunningJob (with timeout)."""

        running_job = pygenie.jobs.RunningJob('1234-update-section-timeout')
        running_job.update(info_section='request', timeout=1)

        assert_equals(
            [call('1234-update-section-timeout', path='request', timeout=1)],
            get.call_args_list
        )


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestingRunningJobProperties(unittest.TestCase):
    """Test accessing RunningJob properties."""

    @patch('pygenie.adapter.genie_3.Genie3Adapter.get_status')
    @patch('pygenie.adapter.genie_3.Genie3Adapter.get_info_for_rj')
    def test_get_runningjob_cluster_name(self, get_info, get_status):
        """Test getting RunningJob.cluster_name."""

        cluster_name = 'test_cluster'

        get_status.return_value = 'RUNNING'
        get_info.return_value = {'cluster_name': cluster_name}

        running_job = pygenie.jobs.RunningJob('rj-cluster_name')

        values = [
            running_job.cluster_name,
            running_job.cluster_name
        ]

        get_info.assert_called_once_with(u'rj-cluster_name', cluster=True)

        assert_equals(
            [cluster_name, cluster_name],
            values
        )

    @patch('pygenie.adapter.genie_3.Genie3Adapter.get_status')
    @patch('pygenie.adapter.genie_3.Genie3Adapter.get_info_for_rj')
    def test_get_runningjob_command_args(self, get_info, get_status):
        """Test getting RunningJob.command_args."""

        value = 'this is the command args'

        get_status.return_value = 'RUNNING'
        get_info.return_value = {'command_args': value}

        running_job = pygenie.jobs.RunningJob('rj-command_args')

        values = [
            running_job.command_args,
            running_job.command_args
        ]

        get_info.assert_called_once_with(u'rj-command_args', job=True)

        assert_equals(
            [value, value],
            values
        )

    @patch('pygenie.adapter.genie_3.Genie3Adapter.get_status')
    @patch('pygenie.adapter.genie_3.Genie3Adapter.get_info_for_rj')
    def test_get_runningjob_cpu(self, get_info, get_status):
        """Test getting RunningJob.cpu."""

        value = 9

        get_status.return_value = 'RUNNING'
        get_info.return_value = {'request_data': {'cpu': value}}

        running_job = pygenie.jobs.RunningJob('rj-cpu')

        # access property multiple times and make sure GET is only called once
        values = [
            running_job.cpu,
            running_job.cpu,
            running_job.cpu
        ]

        get_info.assert_called_once_with('rj-cpu', request=True)

        assert_equals(
            [
                value,
                value,
                value
            ],
            values
        )

    @patch('pygenie.adapter.genie_3.Genie3Adapter.get_status')
    @patch('pygenie.adapter.genie_3.Genie3Adapter.get_info_for_rj')
    def test_get_runningjob_description(self, get_info, get_status):
        """Test getting RunningJob.description."""

        value = 'job description'

        get_status.return_value = 'RUNNING'
        get_info.return_value = {'description': value}

        running_job = pygenie.jobs.RunningJob('rj-description')

        values = [
            running_job.description,
            running_job.description
        ]

        get_info.assert_called_once_with(u'rj-description', job=True)

        assert_equals(
            [value, value],
            values
        )

    @patch('pygenie.adapter.genie_3.Genie3Adapter.get_status')
    @patch('pygenie.adapter.genie_3.Genie3Adapter.get_info_for_rj')
    def test_get_runningjob_memory(self, get_info, get_status):
        """Test getting RunningJob.memory."""

        value = 111

        get_status.return_value = 'RUNNING'
        get_info.return_value = {'request_data': {'memory': value}}

        running_job = pygenie.jobs.RunningJob('rj-memory')

        # access property multiple times and make sure GET is only called once
        values = [
            running_job.memory,
            running_job.memory,
            running_job.memory
        ]

        get_info.assert_called_once_with('rj-memory', request=True)

        assert_equals(
            [
                value,
                value,
                value
            ],
            values
        )

    @patch('pygenie.adapter.genie_3.Genie3Adapter.get_status')
    @patch('pygenie.adapter.genie_3.Genie3Adapter.get_info_for_rj')
    def test_get_runningjob_request_data(self, get_info, get_status):
        """Test getting RunningJob.request_data."""

        value = 'request data'

        get_status.return_value = 'RUNNING'
        get_info.return_value = {'request_data': value}

        running_job = pygenie.jobs.RunningJob('rj-request_data')

        values = [
            running_job.request_data,
            running_job.request_data
        ]

        get_info.assert_called_once_with(u'rj-request_data', request=True)

        assert_equals(
            [value, value],
            values
        )

    @patch('pygenie.adapter.genie_3.Genie3Adapter.get_status')
    def test_get_runningjob_status(self, get_status):
        """Test getting RunningJob.status."""

        get_status.side_effect = ['RUNNING', 'SUCCEEDED']

        running_job = pygenie.jobs.RunningJob('rj-status')

        values = [
            running_job.status,
            running_job.status,
            running_job.status
        ]

        assert_equals(
            get_status.call_args_list,
            [
                call(u'rj-status'),
                call(u'rj-status')
            ]
        )

        assert_equals(
            [
                'RUNNING',
                'SUCCEEDED',
                'SUCCEEDED'
            ],
            values
        )

    @patch('pygenie.adapter.genie_3.Genie3Adapter.get_status')
    @patch('pygenie.adapter.genie_3.Genie3Adapter.get_info_for_rj')
    def test_get_runningjob_status_msg(self, get_info, get_status):
        """Test getting RunningJob.status_msg."""

        get_status.side_effect = [
            'RUNNING',
            'SUCCEEDED'
        ]
        get_info.side_effect = [
            {'status_msg': 'job is running'},
            {'status_msg': 'job finished successfully'}
        ]

        running_job = pygenie.jobs.RunningJob('rj-status_msg')

        values = [
            running_job.status_msg,
            running_job.status_msg,
            running_job.status_msg
        ]

        assert_equals(
            get_info.call_args_list,
            [
                call(u'rj-status_msg', job=True),
                call(u'rj-status_msg', job=True)
            ]
        )

        assert_equals(
            [
                'job is running',
                'job finished successfully',
                'job finished successfully'
            ],
            values
        )


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestingRunningStderr(unittest.TestCase):
    """Test RunningJob stderr log."""

    @patch('pygenie.adapter.genie_3.Genie3Adapter.get_stderr')
    @patch('pygenie.adapter.genie_3.Genie3Adapter.get_status')
    def test_update_stderr(self, get_status, get_stderr):
        """Test RunningJob() updating stderr."""

        get_status.side_effect = [
            'RUNNING',
            'RUNNING',
            'SUCCEEDED',
            'SUCCEEDED'
        ]
        get_stderr.side_effect = [
            "line1\nline2\n",
            "line3\nline4\n",
            "line5\nline6\n"
        ]

        running_job = pygenie.jobs.RunningJob('1234-update-stderr',
                                              info={'status': 'RUNNING'})

        for i in range(10):
            running_job.stderr()

        assert_equals(
            [
                call('1234-update-stderr', headers={'Range': 'bytes=0-'}),
                call('1234-update-stderr', headers={'Range': 'bytes=12-'}),
                call('1234-update-stderr', headers={'Range': 'bytes=24-'})
            ],
            get_stderr.call_args_list
        )

    @patch('pygenie.adapter.genie_3.Genie3Adapter.get_stderr')
    @patch('pygenie.adapter.genie_3.Genie3Adapter.get_status')
    def test_stderr_running(self, get_status, get_stderr):
        """Test RunningJob().stderr() for running job."""

        stderr = [
            "line1\nline2\n",
            "line3\nline4\n",
            "line5\nline6\n",
            "",
            "",
            "",
            ""
        ]

        get_status.return_value = 'RUNNING'
        get_stderr.side_effect = stderr

        running_job = pygenie.jobs.RunningJob('1234-stderr-running',
                                              info={'status': 'RUNNING'})

        for i in range(len(stderr)):
            start = len(running_job._cached_stderr or '')

            running_job.stderr()

            get_stderr.assert_called_with(
                '1234-stderr-running',
                headers={'Range': 'bytes={}-'.format(start)})

        assert_equals(36, len(running_job._cached_stderr))

    @patch('pygenie.jobs.running.RunningJob._write_to_stream')
    @patch('pygenie.adapter.genie_3.Genie3Adapter.get_stderr')
    @patch('pygenie.adapter.genie_3.Genie3Adapter.get_status')
    def test_stderr_watch(self, get_status, get_stderr, write_to_stream):
        """Test RunningJob().watch_stderr()."""

        get_status.side_effect = [
            'RUNNING',
            'RUNNING',
            'RUNNING',
            'RUNNING',
            'RUNNING',
            'SUCCEEDED'
        ]
        get_stderr.side_effect = [
            "line1\nline2\n",
            "line3\nline4\n",
            "line5\nline6\n"
        ]

        running_job = pygenie.jobs.RunningJob('1234-watch-stderr',
                                              info={'status': 'RUNNING'})
        running_job.watch_stderr(interval=0)

        assert_equals(36, len(running_job._cached_stderr))
        assert_equals(
            [
                call('line1\nline2\n'),
                call('line3\nline4\n'),
                call('line5\nline6\n'),
                call('')
            ],
            write_to_stream.call_args_list
        )
