"""
genie.conf

This module implements the loading of configurations for using Genie (submitting
jobs, etc).

Below is the order of loading config values (first = higher priority):
    1. Command line (--config SECTION.OPTION=, --configFile=/path/to/config_file).
    2. Additionally loaded config files.
    3. Environment config file (export GENIE_CONFIG=/path/to/config.ini).
    4. Home config file (~/.genie/genie.ini).
    5. Environment variable.

"""


from __future__ import absolute_import, division, print_function, unicode_literals

import json
import logging
import os
import sys

from configurator import Configurator as C

from .exceptions import GenieConfigOptionError, GenieConfigSectionError


logger = logging.getLogger('com.netflix.genie.conf')

CONFIG_HOME_DIR = os.path.join(os.path.expanduser('~'), '.genie')
CONFIG_FILE_HOME_INI = os.path.join(CONFIG_HOME_DIR, 'pygenie.ini')
CONFIG_FILE_ENV = os.environ.get('GENIE_CONFIG')

DEFAULT_GENIE_URL = 'http://localhost'
DEFAULT_GENIE_USERNAME = 'genie_python_client'
DEFAULT_GENIE_VERSION = '3'


class GenieConfSection(object):
    """Represents a section of a configuration."""

    def __init__(self, name):
        self.__name = name

    def __getattr__(self, attr):
        if attr not in self.to_dict():
            raise GenieConfigOptionError(
                "'{}' does not exist in loaded options for '{}' section ({})" \
                .format(attr, self.__name, sorted(self.to_dict().keys())))
        return self.to_dict().get(attr)

    def get(self, option, default=None):
        """
        Get the value of an option and if the option does not exist in the
        loaded configurations return the default value specified.

        Example:
            >>> genie_conf = GenieConf()
            >>> genie_conf_section = genie_conf.genie
            >>> genie_conf_section.get('option_does_not_exist', 'foo')
            'foo'
            >>> genie_conf.get('option_does_exist', 'foo')
            'bar'

        Args:
            option (str): The option to get the value for.
            default (optional): The value to return if the option is not loaded.

        Returns:
            str: The option value or the default value specified.
        """

        return self.to_dict().get(option, default)

    def set(self, name, value=None):
        """Sets an attribute (option) to the value."""

        logger.debug('setting config -> %s.%s=%s', self.__name, name, value)
        setattr(self, name, value)

    def to_dict(self):
        """Get a mapping of option names to values for the section."""

        _dict = self.__dict__.copy()
        del _dict['_GenieConfSection__name']
        return _dict


class GenieConf(object):
    """
    Stores configuration options for using Genie by reading command line,
    configuration files, and environment variables.
    """

    def __init__(self):
        self._config_files = list()

        # genie section is first-class section
        self.genie = GenieConfSection(name='genie')

        if self.config_file_env():
            self.load_config_file(self.config_file_env())
        elif self.config_file_home_ini():
            self.load_config_file(self.config_file_home_ini())
        else:
            self._load_options()

    def __getattr__(self, attr):
        if attr not in self.to_dict():
            raise GenieConfigSectionError(
                "'{}' does not exist in loaded sections ({})" \
                .format(attr, sorted(self.to_dict().keys())))
        return self.to_dict().get(attr)

    def __repr__(self):
        return '.'.join(['{}()'.format(self.__class__.__name__)] + \
            ['load_config_file("{}")'.format(i) for i in self._config_files])

    @staticmethod
    def config_file_env():
        """Get config file specified in environment."""

        if CONFIG_FILE_ENV and os.path.exists(CONFIG_FILE_ENV):
            return CONFIG_FILE_ENV

    @staticmethod
    def config_file_home_ini():
        """Get config file in home dir if exists and want to use."""

        if CONFIG_FILE_HOME_INI \
                and os.path.exists(CONFIG_FILE_HOME_INI) \
                and os.environ.get('GENIE_BYPASS_HOME_CONFIG') is None:
            return CONFIG_FILE_HOME_INI

    def get(self, option, default=None):
        """
        Get the value of an option and if the option does not exist in the
        loaded configurations return the default value specified.

        Example:
            >>> genie_conf = GenieConf()
            >>> genie_conf.get('section.option_does_not_exist', 'foo')
            'foo'
            >>> genie_conf.get('section.option_does_exist', 'foo')
            'bar'

        Args:
            option (str): The option to get the value for (format: section.option).
            default (optional): The value to return if the option is not loaded.

        Returns:
            str: The option value or the default value specified.
        """

        parts = option.split('.', 1)
        assert len(parts) == 2, \
            'invalid format for option (should be section.option)'
        try:
            return getattr(self, parts[0]).get(parts[1], default=default)
        except (AttributeError, GenieConfigSectionError, GenieConfigOptionError):
            return default

    def load_config_file(self, config_file):
        """Load a configuration file (ini)."""

        param_list = []
        if config_file is not None \
                and os.path.exists(config_file) \
                and config_file not in self._config_files:
            self._config_files.append(config_file)
        for cfile in self._config_files:
            logger.debug('adding config file: %s', cfile)
            param_list.extend(['--configFile', cfile])
        C.initialize(param_list + self.sys_argv())
        self._load_options()
        logger.debug('configuration:\n%s', self.to_json())
        return self

    def _load_options(self):
        """Load options. :py:class:`GenieConfSection` objects will be created from
        sections with options set to the :py:class:`GenieConfSection` object's
        attributes."""

        # explicitly set genie section object
        self.genie.set('url', C.get('genie.url', DEFAULT_GENIE_URL))
        self.genie.set('username', C.get('genie.username',
                                         os.environ.get('USER',
                                                        DEFAULT_GENIE_USERNAME)))
        self.genie.set('version', C.get('genie.version', DEFAULT_GENIE_VERSION))

        # create and set other section objects
        for section in C.config.sections():
            section_clean = section.replace(' ', '_').replace('.', '_')
            gcs = self.genie if section_clean == 'genie' \
                else GenieConfSection(name=section_clean)
            for option in C.config.options(section):
                option_clean = option.replace(' ', '_').replace('.', '_')
                if section_clean != 'genie' \
                        or option_clean not in {'url', 'username', 'version'}:
                    gcs.set(option_clean, C.get(section, option))
            setattr(self, section_clean, gcs)

    @staticmethod
    def sys_argv():
        """Get command line arguments passed to Python script."""

        return sys.argv

    def to_dict(self):
        """
        Get a mapping of attributes and loaded configs to values for the object.

        Example:
            >>> genie_conf = GenieConf()
            >>> genie_conf.to_dict()
            {...}

        Returns:
            dict: A mapping of attributes and configs to values.
        """

        _dict = {attr: value for attr, value in self.__dict__.iteritems() \
           if not attr.startswith('_')}
        for attr_name, attr_value in _dict.iteritems():
            if isinstance(attr_value, GenieConfSection):
                _dict[attr_name] = attr_value.to_dict()
        return _dict

    def to_json(self):
        """
        Get a JSON string of the object mapping.

        Example:
            >>> genie_conf = GenieConf()
            >>> print genie_conf.to_json()
            {...}

        Returns:
            str: A JSON string.
        """

        return json.dumps(self.to_dict(), sort_keys=True, indent=4)
