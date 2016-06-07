from __future__ import absolute_import, division, print_function, unicode_literals

import os
import pkgutil
import requests
import unittest

from mock import patch
from nose.tools import assert_equals

from .utils import fake_response


assert_equals.__self__.maxDiff = None


DIRNAME = os.path.dirname(os.path.realpath(__file__))


import pygenie


def check_request_auth_kwargs(*args, **kwargs):
    auth = kwargs.get('auth')
    assert auth is not None
    assert_equals(unicode(auth.username), u'auth_user')
    assert_equals(unicode(auth.password), u'1234!!!')
    return fake_response('content')


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestingAuthHandler(unittest.TestCase):
    """Test Auth Handler."""

    def setUp(self):
        with patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'}):
            self.conf = pygenie.conf.GenieConf() \
                .load_config_file(os.path.join(DIRNAME, 'genie_auth.ini'))

    def test_init_auth_object(self):
        """Test initializing auth object specified in configuration."""

        auth_handler = pygenie.auth.AuthHandler(conf=self.conf)
        assert_equals(
            isinstance(auth_handler.auth, pygenie.auth.HTTPBasicGenieAuth),
            True
        )

    @patch('requests.request')
    def test_request_call(self, request):
        """Test request call kwargs for auth."""

        patcher = None
        if pkgutil.find_loader('eureq'):
            patcher = patch('eureq.request', check_request_auth_kwargs)
            patcher.start()

        request.side_effect = check_request_auth_kwargs

        pygenie.utils.call('http://localhost',
                           auth_handler=pygenie.auth.AuthHandler(conf=self.conf))

        if patcher:
            patcher.stop()
