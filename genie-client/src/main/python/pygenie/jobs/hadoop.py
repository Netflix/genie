"""
genie.jobs.hadoop

This module implements creating Hadoop jobs.

"""


from __future__ import absolute_import, division, print_function, unicode_literals

import logging
import os

from ..utils import unicodify
from .core import GenieJob
from .utils import (add_to_repr,
                    arg_string)


logger = logging.getLogger('com.netflix.genie.jobs.hadoop')


class HadoopJob(GenieJob):
    """Hadoop job."""

    def __init__(self, conf=None):
        super(HadoopJob, self).__init__(conf=conf)

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

        props_str = ' '.join([
            '-D{name}={value}'.format(name=k, value=v) \
            for k, v in self._command_options.get('-D', {}).items()
        ])

        prop_file_str = '-conf {}'.format(os.path.basename(self._property_file)) \
            if self._property_file \
            else ''

        return '{prop_file} {props} {cmd}' \
            .format(prop_file=prop_file_str,
                    props=props_str,
                    cmd=self._script or '') \
            .strip()

    def command(self, script):
        """Alias for :py:meth:`HadoopJob.script`"""

        return self.script(script)

    @unicodify
    @add_to_repr('append')
    def property(self, name, value):
        """
        Sets a property for the job.

        Using the name and value passed in, the following will be constructed in
        the command-line when executing:
        '-Dname=value'

        Example:
            >>> job = HadoopJob() \\
            ...     .property('mapred.foo', 'fizz') \\
            ...     .property('mapred.bar', 'buzz')

        Args:
            name (str): The property name.
            value (str): The property value.

        Returns:
            :py:class:`HadoopJob`: self
        """

        self._set_command_option('-D', name, value)

        return self

    @unicodify
    @arg_string
    @add_to_repr('overwrite')
    def property_file(self, _property_file):
        """
        Sets a configuration/property file for the job.

        Using the value passed in, the following will be constructed in the
        command-line when executing:
        '-conf file'

        Example:
            >>> job = HadoopJob() \\
            ...     .property_file('/Users/jsmith/my_properties.conf')

        Args:
            _property_file (str): The path to the property file.

        Returns:
            :py:class:`HadoopJob`: self
        """

        self._add_dependency(_property_file)

        return self

    @unicodify
    @arg_string
    @add_to_repr('overwrite')
    def script(self, _script):
        """
        Sets the script to run for the job.

        Example:
            >>> job = HadoopJob() \\
            ...     .script("/Users/jdoe/my_job.jar")
            >>> job = HadoopJob() \\
            ...     .script("version")
            >>> job = HadoopJob() \\
            ...     .script("fs -ls /dir/")

        Args:
            script (str): A path to a script file or the code to run.

        Returns:
            :py:class:`HadoopJob`: self
        """
