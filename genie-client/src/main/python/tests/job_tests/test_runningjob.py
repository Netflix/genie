from __future__ import absolute_import, division, print_function, unicode_literals

import unittest

from mock import patch
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
