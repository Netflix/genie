from __future__ import absolute_import, division, print_function, unicode_literals

import unittest

from mock import patch
from nose.tools import assert_equals

from pygenie.adapter.genie_x import substitute


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
