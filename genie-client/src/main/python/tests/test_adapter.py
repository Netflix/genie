from __future__ import absolute_import, division, print_function, unicode_literals

import unittest

from mock import patch
from nose.tools import (assert_equals,
                        assert_raises)

from pygenie.adapter.genie_x import substitute
from pygenie.adapter.genie_3 import Genie3Adapter
from pygenie.exceptions import GenieLogNotFoundError

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
