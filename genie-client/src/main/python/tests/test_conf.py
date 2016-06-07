from __future__ import absolute_import, division, print_function, unicode_literals

import os
import sys
import unittest

from functools import wraps
from mock import patch
from nose.tools import assert_equals

from configurator import Configurator as C

from pygenie.conf import GenieConf


def reset_environment(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        C.initialize(sys.argv)
        for env_var in {'genie_username', 'USER'}:
            try:
                del os.environ[env_var]
            except Exception:
                pass
        return func(*args, **kwargs)
    return wrapper


@patch.dict('os.environ', {'GENIE_BYPASS_HOME_CONFIG': '1'})
class TestGenieConf(unittest.TestCase):
    """Test GenieConf."""

    def setUp(self):
        dirname = os.path.dirname(os.path.realpath(__file__))
        self.config_file = os.path.join(dirname, 'genie.ini')

    @reset_environment
    @patch('pygenie.conf.GenieConf.config_file_env')
    @patch('pygenie.conf.GenieConf.config_file_home_ini')
    @patch('configurator.configurator.Configurator.initialize')
    def test_load_config_file(self, c_initialize, config_home_ini, config_env):
        """Test loading configuration file."""

        config_env.return_value = None
        config_home_ini.return_value = None
        genie_config = GenieConf()
        genie_config.load_config_file(self.config_file)
        c_initialize.assert_called_once_with(
            [u'--configFile', self.config_file] +
            sys.argv
        )

    @reset_environment
    @patch('pygenie.conf.GenieConf.config_file_env')
    @patch('pygenie.conf.GenieConf.config_file_home_ini')
    @patch('pygenie.conf.GenieConf.load_config_file')
    def test_load_config_file_order_env(self,
                                        load_config_file,
                                        config_home_ini,
                                        config_env):
        """Test loading configuration file order with specified in environment variable."""

        C.initialize()
        config_env.return_value = self.config_file
        config_home_ini.return_value = None
        GenieConf()
        load_config_file.assert_called_once_with(self.config_file)

    @reset_environment
    @patch('pygenie.conf.GenieConf.config_file_env')
    @patch('pygenie.conf.GenieConf.config_file_home_ini')
    @patch('pygenie.conf.GenieConf.load_config_file')
    def test_load_config_file_order_homedir(self,
                                            load_config_file,
                                            config_home_ini,
                                            config_env):
        """Test loading configuration file order with home directory."""

        config_env.return_value = None
        config_home_ini.return_value = self.config_file
        GenieConf()
        load_config_file.assert_called_once_with(self.config_file)

    @reset_environment
    @patch('pygenie.conf.GenieConf.config_file_env')
    @patch('pygenie.conf.GenieConf.config_file_home_ini')
    @patch('pygenie.conf.GenieConf.load_config_file')
    def test_load_config_file_order_multiple_config(self,
                                                    load_config_file,
                                                    config_home_ini,
                                                    config_env):
        """Test loading configuration file order when specified in environment and from home directory. Only the environment config file should be loaded."""

        config_env.return_value = '/from/env/genie.ini'
        config_home_ini.return_value = '/from/home/genie.ini'
        GenieConf()
        load_config_file.assert_called_once_with('/from/env/genie.ini')

    @reset_environment
    @patch('pygenie.conf.GenieConf.config_file_env')
    @patch('pygenie.conf.GenieConf.config_file_home_ini')
    def test_to_dict(self, config_home_ini, config_env):
        """Test configuration to_dict()."""

        config_env.return_value = None
        config_home_ini.return_value = None
        genie_conf = GenieConf()
        genie_conf.load_config_file(self.config_file)
        assert_equals(
            genie_conf.to_dict(),
            {
                u'genie': {
                    u'url': u'http://foo:8080',
                    u'username': u'user_ini',
                    u'version': u'3'
                },
                'test_genie': {
                    u'from_env': u'from_ini_1',
                    u'from_cmd_line': u'from_ini_2',
                    u'from_ini': u'from_ini_3'
                }
            }
        )

    @reset_environment
    @patch('pygenie.conf.GenieConf.config_file_env')
    @patch('pygenie.conf.GenieConf.config_file_home_ini')
    @patch('pygenie.conf.GenieConf.sys_argv')
    def test_username_command_line(self, sys_argv, config_home_ini, config_env):
        """Test configuration getting user specified from command line."""

        config_env.return_value = None
        config_home_ini.return_value = None
        sys_argv.return_value = ['--config', 'genie.username=user_cmdline_1']
        os.environ['USER'] = 'os_user_env_1'
        os.environ['genie_username'] = 'genie_user_env_1'
        genie_conf = GenieConf()
        genie_conf.load_config_file(self.config_file)
        assert_equals(genie_conf.genie.username, 'user_cmdline_1')

    @reset_environment
    @patch('pygenie.conf.GenieConf.config_file_env')
    @patch('pygenie.conf.GenieConf.config_file_home_ini')
    def test_username_config_file(self, config_home_ini, config_env):
        """Test configuration getting user specified from configuration file."""

        config_env.return_value = None
        config_home_ini.return_value = None
        os.environ['USER'] = 'os_user_env_2'
        os.environ['genie_username'] = 'genie_user_env_2'
        genie_conf = GenieConf()
        genie_conf.load_config_file(self.config_file)
        assert_equals(genie_conf.genie.username, 'user_ini')

    @reset_environment
    @patch('pygenie.conf.GenieConf.config_file_env')
    @patch('pygenie.conf.GenieConf.config_file_home_ini')
    def test_username_environment_variable(self, config_home_ini, config_env):
        """Test configuration getting user specified from GENIE_USERNAME environment variable."""

        config_env.return_value = None
        config_home_ini.return_value = None
        os.environ['USER'] = 'os_user_env_3'
        os.environ['genie_username'] = 'genie_user_env_3'
        genie_conf = GenieConf()
        assert_equals(genie_conf.genie.username, 'genie_user_env_3')

    @reset_environment
    @patch('pygenie.conf.GenieConf.config_file_env')
    @patch('pygenie.conf.GenieConf.config_file_home_ini')
    def test_username_os_user(self, config_home_ini, config_env):
        """Test configuration getting user specified from os user (USER environment variable)."""

        config_env.return_value = None
        config_home_ini.return_value = None
        os.environ['USER'] = 'os_user_env_4'
        genie_conf = GenieConf()
        assert_equals(genie_conf.genie.username, 'os_user_env_4')
