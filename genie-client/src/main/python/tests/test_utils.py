from __future__ import absolute_import, division, print_function, unicode_literals

import os
import unittest

from mock import patch
from nose.tools import assert_equals

from pygenie.utils import str_to_list


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
