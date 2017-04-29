"""
genie.jobs.hive

This module implements creating Hive jobs.

"""


from __future__ import absolute_import, division, print_function, unicode_literals

import logging
import os
import sys

from collections import OrderedDict

from ..utils import unicodify
from .core import GenieJob
from .utils import (add_to_repr,
                    arg_list,
                    arg_string,
                    is_file)


logger = logging.getLogger('com.netflix.genie.jobs.hive')


class HiveJob(GenieJob):
    """
    Hive job.

    Example:
        >>> job = HiveJob() \\
        ...     .job_name('hive example') \\
        ...     .script('/Users/jsmith/my_script.hql') \\
        ...     .parameter('param_1', 'value_1') \\
        ...     .parameter('param_2', 'value_2') \\
        ...     .property('mapred.foo', 'fizz') \\
        ...     .property('mapred.bar', 'buzz') \\
        ...     .property_file('/Users/jsmith/my_properties.conf')
    """

    DEFAULT_SCRIPT_NAME = 'script.hive'

    def __init__(self, conf=None):
        super(HiveJob, self).__init__(conf=conf)

        self._parameters = OrderedDict()
        self._property_files = list()
        self._script = None

    @property
    def cmd_args(self):
        """
        The constructed command line arguments using the job's definition. If the
        command line arguments are set explicitly (by calling
        :py:meth:`command_arguments`) this will be the same.
        """

        if self._command_arguments is not None:
            return self._command_arguments

        filename = HiveJob.DEFAULT_SCRIPT_NAME
        if is_file(self._script):
            filename = os.path.basename(self._script)
            self._add_dependency(self._script)
        elif self._script is not None:
            self._add_dependency({'name': filename, 'data': self._script})

        # put parameters into a parameter file and specify parameter file on command line
        # this is to get around weird quoting issues in parameter values, etc
        param_str = self._parameter_file
        if param_str:
            self._add_dependency({
                'name': '_hive_parameters.txt',
                'data': param_str
            })

        props_str = ' '.join([
            '--hiveconf {name}={value}'.format(name=k, value=v) \
            for k, v in self._command_options.get('--hiveconf', {}).items()
        ])

        prop_file_str = ' '.join(['-i {}'.format(os.path.basename(f)) for f in self._property_files]) \
            if self._property_files \
            else ''

        return '{prop_file} {props} {params} -f {filename} {post_cmd_args}' \
            .format(prop_file=prop_file_str,
                    props=props_str,
                    filename=filename,
                    params='-i _hive_parameters.txt' if param_str else '',
                    post_cmd_args=' '.join(self._post_cmd_args)) \
            .strip()

    @property
    def _parameter_file(self):
        """Takes specified parameters and creates a string for the parameter file."""

        param_file = ""

        for name, value in self._parameters.items():
            if sys.version_info < (3,):
                value = unicode(value)
            param_file = '{p}SET hivevar:{name}={value};\n' \
                .format(p=param_file,
                        name=name,
                        value=value)

        return param_file.strip()

    def headers(self):
        """
        Sets hive.cli.print.header so that if the hive query is outputing
        to stdout it will append headers to the result.

        Example:
            >>> job = HiveJob() \\
            ...     .headers()

        Returns:
            :py:class:`HiveJob`: self
        """

        self.tags('headers')
        return self.hiveconf('hive.cli.print.header', 'true')

    @unicodify
    @add_to_repr('append')
    def hiveconf(self, name, value):
        """
        Sets a property for the job.

        Using the name and value passed in, the following will be constructed for
        the command-line when executing:
        '--hiveconf name=value'

        Example:
            >>> # hive --hiveconf mapred.foo=fizz --hiveconf mapred.bar=buzz
            >>> job = HiveJob() \\
            ...     .hiveconf('mapred.foo', 'fizz') \\
            ...     .hiveconf('mapred.bar', 'buzz')

        Args:
            name (str): The property name.
            value (str): The property value.

        Returns:
            :py:class:`HiveJob`: self
        """

        self._set_command_option('--hiveconf', name, value)

        return self

    def property(self, name, value):
        """Alias for :py:meth:`HiveJob.hiveconf`"""
        return self.hiveconf(name, value)

    @unicodify
    @arg_list
    @add_to_repr('append')
    def property_file(self, _property_files):
        """
        Sets a property file to use for specifying properties for the job.

        Using the value passed in, the following will be constructed for the
        command-line when executing:
        '-i value'

        Example:
            >>> #hive -i my_properties.prop
            >>> job = HiveJob() \\
            ...     .property_file('/Users/jsmith/my_properties.prop')

        Args:
            _property_file (str): The full path to the property file.

        Returns:
            :py:class:`HiveJob`: self
        """

        self._add_dependency(_property_files)

        return self

    def query(self, script):
        """Alias for :py:meth:`HiveJob.script`"""

        return self.script(script)

    @unicodify
    @arg_string
    @add_to_repr('overwrite')
    def script(self, _script):
        """
        Sets the script to run for the job. This can be a path to a script file or
        the code to execute.

        Example:
            >>> job = HiveJob() \\
            ...     .script('/Users/jsmith/my_script.hql')

        Args:
            script (str): A path to a script file or the code to run.

        Returns:
            :py:class:`HiveJob`: self
        """
