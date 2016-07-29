from __future__ import absolute_import, division, print_function, unicode_literals

import unittest

from mock import call, patch
from nose.tools import assert_equals, assert_raises

import pygenie


assert_equals.__self__.maxDiff = None


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestingRunningJobIsDone(unittest.TestCase):
    """Test RunningJob().is_done."""

    @patch('pygenie.jobs.RunningJob.update')
    def test_init_status(self, rj_update):
        """Test RunningJob().is_done with 'INIT' status."""

        rj_update.side_effect = None

        running_job = pygenie.jobs.RunningJob('1234-init',
                                              info={'status': 'INIT'})

        assert_equals(
            running_job.is_done,
            False
        )

    @patch('pygenie.jobs.RunningJob.update')
    def test_running_status(self, rj_update):
        """Test RunningJob().is_done with 'RUNNING' status."""

        rj_update.side_effect = None

        running_job = pygenie.jobs.RunningJob('1234-running',
                                              info={'status': 'RUNNING'})

        assert_equals(
            running_job.is_done,
            False
        )

    @patch('pygenie.jobs.RunningJob.update')
    def test_killed_status(self, rj_update):
        """Test RunningJob().is_done with 'KILLED' status."""

        rj_update.side_effect = None

        running_job = pygenie.jobs.RunningJob('1234-killed',
                                              info={'status': 'KILLED'})

        assert_equals(
            running_job.is_done,
            True
        )

    @patch('pygenie.jobs.RunningJob.update')
    def test_succeeded_status(self, rj_update):
        """Test RunningJob().is_done with 'SUCCEEDED' status."""

        rj_update.side_effect = None

        running_job = pygenie.jobs.RunningJob('1234-succeeded',
                                              info={'status': 'SUCCEEDED'})

        assert_equals(
            running_job.is_done,
            True
        )

    @patch('pygenie.jobs.RunningJob.update')
    def test_failed_status(self, rj_update):
        """Test RunningJob().is_done with 'FAILED' status."""

        rj_update.side_effect = None

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

        get_info.assert_called_once_with(u'1234-no-info')

    @patch('pygenie.adapter.genie_3.Genie3Adapter.get_info_for_rj')
    def test_update(self, get_info):
        """Test calling update for RunningJob."""

        running_job = pygenie.jobs.RunningJob('1234-update')
        running_job.update()

        assert_equals(
            get_info.call_args_list,
            [
                call(u'1234-update'),
                call(u'1234-update', full=False)
            ]
        )

    @patch('pygenie.adapter.genie_3.Genie3Adapter.get_info_for_rj')
    def test_updating_info_for_attr(self, get_info):
        """Test getting updated information for RunningJob when accessing an attr."""

        get_info.side_effect = [
            {'status': 'RUNNING'},
            {'status': 'SUCCEEDED', 'finished': '2016-07-28T03:29:28.657Z'}
        ]

        running_job = pygenie.jobs.RunningJob('1234-attr')
        running_job.finish_time

        # should have 2 calls to get information
        # 1. get all information on RunningJob init
        # 2. get just job info for finished time
        assert_equals(
            get_info.call_args_list,
            [
                call(u'1234-attr'),
                call(u'1234-attr', full=False)
            ]
        )
