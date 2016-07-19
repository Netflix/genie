from __future__ import absolute_import, division, print_function, unicode_literals

import unittest

from mock import patch
from nose.tools import assert_equals

from pygenie.utils import str_to_list
from pygenie.jobs.utils import (generate_job_id,
                                reattach_job)

from pygenie.exceptions import GenieJobNotFoundError

from .utils import FakeRunningJob


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestStringToList(unittest.TestCase):
    """Test converting string to list."""

    def test_string_to_list_string(self):
        """Test string to list with default delimiter (comma)."""

        assert_equals(
            str_to_list('a, b, c'),
            ['a', 'b', 'c']
        )

    def test_string_to_list_string_delimiter(self):
        """Test string to list with specified delimiter."""

        assert_equals(
            str_to_list(' a | b | c ', delimiter='|'),
            ['a', 'b', 'c']
        )

    def test_string_to_list_none(self):
        """Test string to list with input arg None."""

        assert_equals(
            str_to_list(None),
            None
        )


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestReattachJob(unittest.TestCase):
    """Test reattaching to a running job."""

    @patch('pygenie.jobs.running.RunningJob.update')
    def test_reattaching_job(self, rj_update):
        """Test reattaching to a running job."""

        running_job = reattach_job('test-reattach-job')

        rj_update.assert_called_once_with(info=None)


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestGeneratingJobId(unittest.TestCase):
    """Test generating job ids."""

    @patch('pygenie.jobs.utils.reattach_job')
    def test_gen_job_id_new(self, mock_reattach_job):
        """Test generating job id which is completely new."""

        job_id = 'brand-new'

        mock_reattach_job.side_effect = [
            GenieJobNotFoundError
        ]

        assert_equals(job_id, generate_job_id(job_id))

    @patch('pygenie.jobs.utils.reattach_job')
    def test_gen_job_id_return_success_true_running(self, mock_reattach_job):
        """Test generating job id with returning successful job set to True with a job that is running."""

        job_id = 'running'

        mock_reattach_job.side_effect = [
            FakeRunningJob(job_id=job_id, status='FAILED'),
            FakeRunningJob(job_id=job_id+'-1', status='KILLED'),
            FakeRunningJob(job_id=job_id+'-2', status='FAILED'),
            FakeRunningJob(job_id=job_id+'-3', status='RUNNING'),
            FakeRunningJob(job_id=job_id+'-4', status='SUCCEEDED'),
            GenieJobNotFoundError
        ]

        assert_equals(job_id+'-3', generate_job_id(job_id))

    @patch('pygenie.jobs.utils.reattach_job')
    def test_gen_job_id_return_success_true(self, mock_reattach_job):
        """Test generating job id with returning successful job set to True."""

        job_id = 'return-successful-true'

        mock_reattach_job.side_effect = [
            FakeRunningJob(job_id=job_id, status='FAILED'),
            FakeRunningJob(job_id=job_id+'-1', status='KILLED'),
            FakeRunningJob(job_id=job_id+'-2', status='SUCCEEDED'),
            FakeRunningJob(job_id=job_id+'-3', status='SUCCEEDED'),
            FakeRunningJob(job_id=job_id+'-4', status='KILLED'),
            GenieJobNotFoundError
        ]

        assert_equals(job_id+'-2', generate_job_id(job_id))

    @patch('pygenie.jobs.utils.reattach_job')
    def test_gen_job_id_with_return_success_false(self, mock_reattach_job):
        """Test generating job id with returning successful job set to False."""

        job_id = 'return-successful-false'

        mock_reattach_job.side_effect = [
            FakeRunningJob(job_id=job_id, status='FAILED'),
            FakeRunningJob(job_id=job_id+'-1', status='KILLED'),
            FakeRunningJob(job_id=job_id+'-2', status='SUCCEEDED'),
            FakeRunningJob(job_id=job_id+'-3', status='KILLED'),
            FakeRunningJob(job_id=job_id+'-4', status='FAILED'),
            FakeRunningJob(job_id=job_id+'-5', status='SUCCEEDED'),
            GenieJobNotFoundError
        ]

        assert_equals(job_id+'-6', generate_job_id(job_id, return_success=False))

    @patch('pygenie.jobs.utils.reattach_job')
    def test_gen_job_id_with_return_success_false_with_running(self, mock_reattach_job):
        """Test generating job id with returning successful job set to False with a job that is running."""

        job_id = 'return-successful-false-running'

        mock_reattach_job.side_effect = [
            FakeRunningJob(job_id=job_id, status='FAILED'),
            FakeRunningJob(job_id=job_id+'-1', status='KILLED'),
            FakeRunningJob(job_id=job_id+'-2', status='SUCCEEDED'),
            FakeRunningJob(job_id=job_id+'-3', status='KILLED'),
            FakeRunningJob(job_id=job_id+'-4', status='RUNNING'),
            FakeRunningJob(job_id=job_id+'-5', status='SUCCEEDED'),
            GenieJobNotFoundError
        ]

        assert_equals(job_id+'-4', generate_job_id(job_id, return_success=False))
