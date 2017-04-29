"""
genie.jobs.pig

This module implements creating Pig jobs.

"""


from __future__ import absolute_import, division, print_function, unicode_literals

import logging
import os
import sys

from ..utils import unicodify
from .core import GenieJob
from .utils import (add_to_repr,
                    arg_list,
                    arg_string,
                    is_file)


logger = logging.getLogger('com.netflix.genie.jobs.pig')


class PigJob(GenieJob):
    """
    Pig job.

    Example:
        >>> job = PigJob() \\
        ...     .job_name('pig example') \\
        ...     .script('/Users/jsmith/my_script.pig') \\
        ...     .parameter('param_1', 'value_1') \\
        ...     .parameter('param_2', 'value_2') \\
        ...     .parameter_file('/Users/jsmith/my_parameters.params') \\
        ...     .property('mapred.foo', 'fizz') \\
        ...     .property('mapred.bar', 'buzz') \\
        ...     .property_file('/Users/jsmith/my_properties.conf')
    """

    DEFAULT_SCRIPT_NAME = 'script.pig'

    def __init__(self, conf=None):
        super(PigJob, self).__init__(conf=conf)

        self._parameter_files = list()
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

        filename = PigJob.DEFAULT_SCRIPT_NAME
        if is_file(self._script):
            filename = os.path.basename(self._script)
            self._add_dependency(self._script)
        elif self._script is not None:
            self._add_dependency({'name': filename, 'data': self._script})

        param_files_str = ' '.join([
            '-param_file {}'.format(os.path.basename(p)) \
            for p in self._parameter_files
        ])

        # put parameters into a parameter file and specify parameter file on command line
        # this is to get around weird quoting issues in parameter values, etc
        param_str = self._parameter_file
        if param_str:
            self._add_dependency({
                'name': '_pig_parameters.txt',
                'data': param_str
            })

        props_str = ' '.join([
            '-D{name}={value}'.format(name=k, value=v) \
            for k, v in self._command_options.get('-D', {}).items()
        ])

        prop_file_str = ' '.join(['-P {}'.format(os.path.basename(f)) for f in self._property_files]) \
            if self._property_files \
            else ''

        return '{props} {prop_file} {param_files} {params} -f {filename} {post_cmd_args}' \
            .format(prop_file=prop_file_str,
                    props=props_str,
                    filename=filename,
                    param_files=param_files_str,
                    params='-param_file _pig_parameters.txt' if param_str else '',
                    post_cmd_args=' '.join(self._post_cmd_args)) \
            .strip()

    @property
    def _parameter_file(self):
        """Takes specified parameters and creates a string for the parameter file."""

        param_file = ""

        for name, value in self._parameters.items():
            if sys.version_info < (3,):
                value = unicode(value)
            else:
                value = str(value)
            param_file = '{p}{name} = "{value}"\n' \
                .format(p=param_file,
                        name=name,
                        value=value.replace('"', '\\"'))

        return param_file.strip()

    @unicodify
    @arg_list
    @add_to_repr('append')
    def parameter_file(self, _parameter_files):
        """
        Sets a parameter file to use for parameter substitution in the job's
        script.

        Using the value passed in, the following will be constructed for the
        command-line when executing:
        '-param_file value'

        Example:
            >>> #pig -param_file my_parameters.params
            >>> job = PigJob() \\
            ...     .parameter_file('/Users/jsmith/my_parameters.params')

        Args:
            _parameter_file (str): The full path to the parameter file.

        Returns:
            :py:class:`PigJob`: self
        """

        self._add_dependency(_parameter_files)

        return self

    @unicodify
    @add_to_repr('append')
    def property(self, name, value):
        """
        Sets a property for the job.

        Using the name and value passed in, the following will be constructed for
        the command-line when executing:
        '-Dname=value'

        Example:
            >>> # pig -Dmapred.foo=fizz -Dmapred.bar=buzz
            >>> job = PigJob() \\
            ...     .property('mapred.foo', 'fizz') \\
            ...     .property('mapred.bar', 'buzz')

        Args:
            name (str): The property name.
            value (str): The property value.

        Returns:
            :py:class:`PigJob`: self
        """

        self._set_command_option('-D', name, value)

        return self

    @unicodify
    @arg_list
    @add_to_repr('append')
    def property_file(self, _property_files):
        """
        Sets a property file to use for specifying properties for the job.

        Using the value passed in, the following will be constructed for the
        command-line when executing:
        '-P value'

        Example:
            >>> #pig -P my_properties.conf
            >>> job = PigJob() \\
            ...     .property_file('/Users/jsmith/my_properties.conf')

        Args:
            _property_file (str): The full path to the property file.

        Returns:
            :py:class:`PigJob`: self
        """

        self._add_dependency(_property_files)

        return self

    @unicodify
    @arg_string
    @add_to_repr('overwrite')
    def script(self, _script):
        """
        Sets the script to run for the job. This can be a path to a script file or
        the code to execute.

        Example:
            >>> job = PigJob() \\
            ...     .script('/Users/jsmith/my_script.pig')

        Args:
            script (str): A path to a script file or the code to run.

        Returns:
            :py:class:`PigJob`: self
        """
