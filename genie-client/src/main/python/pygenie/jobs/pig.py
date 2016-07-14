"""
genie.jobs.pig

This module implements creating Pig jobs.

"""


from __future__ import absolute_import, division, print_function, unicode_literals

import logging
import os

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

        self._filename = PigJob.DEFAULT_SCRIPT_NAME
        self._parameter_files = list()
        self._property_file = None
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

        param_files_str = ' '.join([
            '-param_file {}'.format(os.path.basename(p)) \
            for p in self._parameter_files
        ])

        params_str = ' '.join([
            '-p {qu}{name}={value}{qu}' \
                .format(name=k, value=v, qu='"' if ' ' in str(v) else '') \
            for k, v in self._parameters.items()
        ])

        props_str = ' '.join([
            '-D{name}={value}'.format(name=k, value=v) \
            for k, v in self._command_options.get('-D', {}).items()
        ])

        prop_file_str = '-P {}'.format(os.path.basename(self._property_file)) \
            if self._property_file \
            else ''

        return '{props} {prop_file} {param_files} {params} -f {filename}' \
            .format(prop_file=prop_file_str,
                    props=props_str,
                    filename=self._filename,
                    param_files=param_files_str,
                    params=params_str) \
            .strip()

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

    @arg_string
    @add_to_repr('overwrite')
    def property_file(self, _property_file):
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

        self._add_dependency(_property_file)

        return self

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

        if is_file(_script):
            self._filename = os.path.basename(_script)
            self._add_dependency(_script)
        else:
            self._filename = PigJob.DEFAULT_SCRIPT_NAME
            self._add_dependency({'name': self._filename, 'data': _script})

        return self
