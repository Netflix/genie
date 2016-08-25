from __future__ import absolute_import, division, print_function, unicode_literals

import unittest

from mock import patch
from nose.tools import (assert_raises,
                        assert_equals)

from pygenie.utils import (call,
                           str_to_list)
from pygenie.jobs.utils import (generate_job_id,
                                reattach_job)

from pygenie.exceptions import (GenieHTTPError,
                                GenieJobNotFoundError)

from .utils import (FakeRunningJob,
                    fake_response)


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
@patch('pygenie.utils.requests.request')
class TestCall(unittest.TestCase):
    """Test HTTP request calls to Genie server."""

    def test_202(self, request):
        """Test HTTP request via call() with 200 response."""

        request.return_value = fake_response({}, 202)

        resp = call('http://genie-202')

        assert_equals(202, resp.status_code)
        assert_equals(1, request.call_count)

    @patch('pygenie.utils.time.sleep')
    def test_503s(self, sleep, request):
        """Test HTTP request via call() with 503 responses."""

        request.side_effect = [
            fake_response({}, 503),
            fake_response({}, 503),
            fake_response({}, 503),
            fake_response({}, 503),
            fake_response({}, 503)
        ]

        with assert_raises(GenieHTTPError):
            call('http://genie-503', attempts=4, backoff=0)

        assert_equals(4, request.call_count)
        assert_equals(3, sleep.call_count)

    @patch('pygenie.utils.time.sleep')
    def test_503_409(self, sleep, request):
        """Test HTTP request via call() with 503 then 409 responses."""

        request.side_effect = [
            fake_response({}, 503),
            fake_response({}, 409)
        ]

        with assert_raises(GenieHTTPError):
            call('http://genie-503-409', attempts=4, backoff=0)

        assert_equals(2, request.call_count)
        assert_equals(1, sleep.call_count)

    @patch('pygenie.utils.time.sleep')
    def test_503_202(self, sleep, request):
        """Test HTTP request via call() with 503 then 202 responses."""

        request.side_effect = [
            fake_response({}, 503),
            fake_response({}, 503),
            fake_response({}, 202)
        ]

        resp = call('http://genie-503-202', attempts=4, backoff=0)

        assert_equals(202, resp.status_code)
        assert_equals(3, request.call_count)
        assert_equals(2, sleep.call_count)

    @patch('pygenie.utils.time.sleep')
    def test_404_not_none(self, sleep, request):
        """Test HTTP request via call() with 404 response (raise error)."""

        request.return_value = fake_response({}, 404)

        with assert_raises(GenieHTTPError):
            call('http://genie-404-raise')

        assert_equals(1, request.call_count)
        assert_equals(0, sleep.call_count)

    @patch('pygenie.utils.time.sleep')
    def test_404_none(self, sleep, request):
        """Test HTTP request via call() with 404 response (return None)."""

        request.return_value = fake_response({}, 404)

        resp = call('http://genie-404-none', none_on_404=True)

        assert_equals(None, resp)
        assert_equals(1, request.call_count)
        assert_equals(0, sleep.call_count)

    @patch('pygenie.utils.time.sleep')
    def test_retry_codes(self, sleep, request):
        """Test HTTP request via call() with retry status codes."""

        request.side_effect = [
            fake_response({}, 500),
            fake_response({}, 503),
            fake_response({}, 500),
            fake_response({}, 503),
            fake_response({}, 500)
        ]

        with assert_raises(GenieHTTPError):
            call('http://genie-retry-codes', retry_status_codes=500, attempts=4, backoff=0)

        assert_equals(4, request.call_count)
        assert_equals(3, sleep.call_count)

    @patch('pygenie.utils.time.sleep')
    def test_retry_codes_2(self, sleep, request):
        """Test HTTP request via call() with retry status codes (finishing with 202)."""

        request.side_effect = [
            fake_response({}, 500),
            fake_response({}, 503),
            fake_response({}, 202)
        ]

        call('http://genie-retry-codes-202', retry_status_codes=500, attempts=4, backoff=0)

        assert_equals(3, request.call_count)
        assert_equals(2, sleep.call_count)


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

    @patch('pygenie.adapter.genie_3.Genie3Adapter.get_status')
    def test_reattaching_job(self, get_status):
        """Test reattaching to a running job."""

        running_job = reattach_job('test-reattach-job')

        get_status.assert_called_once_with('test-reattach-job')

    @patch('pygenie.adapter.genie_3.Genie3Adapter.get_status')
    def test_reattaching_job_does_not_exist(self, get_status):
        """Test reattaching to a running job that does not exist."""

        get_status.side_effect = GenieJobNotFoundError

        with assert_raises(GenieJobNotFoundError) as cm:
            running_job = reattach_job('test-reattach-job-dne')


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
