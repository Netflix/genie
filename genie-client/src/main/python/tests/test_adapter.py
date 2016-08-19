from __future__ import absolute_import, division, print_function, unicode_literals

import unittest

from mock import patch
from nose.tools import (assert_equals,
                        assert_raises)

from pygenie.adapter.adapter import execute_job
from pygenie.adapter.genie_x import substitute
from pygenie.adapter.genie_3 import (Genie3Adapter,
                                     get_payload)
from pygenie.jobs import PrestoJob
from pygenie.exceptions import (GenieHTTPError,
                                GenieLogNotFoundError)

from .utils import fake_response


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestStringSubstitution(unittest.TestCase):
    """Test script parameter substitution."""

    def test_substitute(self):
        """Test script parameter substitution."""

        assert_equals(
            substitute('hello $name, goodbye $last',
                       dict(name='test1', last='bye1')),
            'hello test1, goodbye bye1'
        )

    def test_substitute_expansion(self):
        """Test script parameter substitution (expansion)."""

        assert_equals(
            substitute('hello ${name}, goodbye ${last}',
                       dict(name='test2', last='bye2')),
            'hello test2, goodbye bye2'
        )

    def test_substitute_missing(self):
        """Test script parameter substitution with missing parameters."""

        assert_equals(
            substitute('hello $name, goodbye $last',
                       dict(name='tester3')),
            'hello tester3, goodbye $last'
        )

    def test_substitute_missing_expansion(self):
        """Test script parameter substitution with missing parameters (expansion)."""

        assert_equals(
            substitute('hello ${name}, goodbye ${last}',
                       dict(name='tester4')),
            'hello tester4, goodbye ${last}'
        )


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestGenie3Adapter(unittest.TestCase):
    """Test Genie 3 adapter."""

    @patch('pygenie.utils.requests.request')
    def test_stderr_log_not_found(self, request):
        """Test Genie 3 adapter getting stderr log which does not exist."""

        request.return_value = fake_response(None, status_code=404)

        adapter = Genie3Adapter()

        with assert_raises(GenieLogNotFoundError):
            adapter.get_stderr('job_id_dne')

    def test_set_job_name_with_script_has_params(self):
        """Test Genie 3 adapter setting job name (if not set) with script containing parameters."""

        job = PrestoJob() \
            .script('select * from ${table}') \
            .parameter('table', 'foo.fizz')

        payload = get_payload(job)

        assert_equals(
            'select * from {table}',
            payload['name']
        )


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
@patch('pygenie.adapter.adapter.get_adapter_for_version')
class TestExecuteJobRetries(unittest.TestCase):
    """Test adapter execute job retries."""

    @patch('pygenie.adapter.genie_3.Genie3Adapter.submit_job')
    def test_execute_job_no_issues(self, submit_job, get_adapter):
        """Test adapter execute job with no issues."""

        get_adapter.return_value = Genie3Adapter
        submit_job.side_effect = [None]

        execute_job(PrestoJob().script('select * from dual;'),
                    attempts=4,
                    backoff=0)

        assert_equals(1, submit_job.call_count)

    @patch('pygenie.adapter.genie_3.Genie3Adapter.submit_job')
    def test_execute_job_409(self, submit_job, get_adapter):
        """Test adapter execute job with server returning 409."""

        get_adapter.return_value = Genie3Adapter
        submit_job.side_effect = [
            GenieHTTPError(fake_response({}, 409))
        ]

        execute_job(PrestoJob().script('select * from dual;'),
                    attempts=4,
                    backoff=0)

        assert_equals(1, submit_job.call_count)

    @patch('pygenie.adapter.genie_3.Genie3Adapter.submit_job')
    def test_execute_job_503s(self, submit_job, get_adapter):
        """Test adapter execute job with server returning 503s."""

        get_adapter.return_value = Genie3Adapter
        submit_job.side_effect = [
            GenieHTTPError(fake_response({}, 503)),
            GenieHTTPError(fake_response({}, 503)),
            GenieHTTPError(fake_response({}, 503)),
            GenieHTTPError(fake_response({}, 503))
        ]

        with assert_raises(GenieHTTPError):
            execute_job(PrestoJob().script('select * from dual;'),
                        attempts=4,
                        backoff=0)

        assert_equals(4, submit_job.call_count)

    @patch('pygenie.adapter.genie_3.Genie3Adapter.submit_job')
    def test_execute_job_503_and_409(self, submit_job, get_adapter):
        """Test adapter execute job with server returning 503 then 409."""

        # This case will handle the original job submission getting saved by
        # Genie into the database but returning a HTTP 503.

        get_adapter.return_value = Genie3Adapter
        submit_job.side_effect = [
            GenieHTTPError(fake_response({}, 503)),
            GenieHTTPError(fake_response({}, 409))
        ]

        execute_job(PrestoJob().script('select * from dual;'),
                    attempts=4,
                    backoff=0)

        assert_equals(2, submit_job.call_count)

    @patch('pygenie.adapter.genie_3.Genie3Adapter.submit_job')
    def test_execute_job_unhandled_code(self, submit_job, get_adapter):
        """Test adapter execute job with server returning unhandled status code."""

        get_adapter.return_value = Genie3Adapter
        submit_job.side_effect = [
            GenieHTTPError(fake_response({}, 500))
        ]

        with assert_raises(GenieHTTPError):
            execute_job(PrestoJob().script('select * from dual;'),
                        attempts=4,
                        backoff=0)

        assert_equals(1, submit_job.call_count)
