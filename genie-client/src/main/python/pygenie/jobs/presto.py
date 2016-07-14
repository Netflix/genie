"""
genie.jobs.presto

This module implements creating Presto jobs.

Example:
    >>> from genie.jobs import PrestoJob
    >>> job = PrestoJob() \\
    ...     .job_id('my-job-id-1234') \\
    ...     .job_name('my-job') \\
    ...     .script('select * from db.table') \\
    ...     .headers() \\
    ...     .option('debug') \\
    ...     .option('source', 'genie') \\
    ...     .session('hive.max_initial_split_size', '4MB')
    >>> running_job = job.execute()

"""


from __future__ import absolute_import, division, print_function, unicode_literals

import logging
import os

from .core import GenieJob
from .utils import (add_to_repr,
                    arg_string,
                    is_file)


logger = logging.getLogger('com.netflix.genie.jobs.presto')


class PrestoJob(GenieJob):
    """
    Presto job.

    Example:
        >>> job = PrestoJob() \\
        ...     .job_name('presto example') \\
        ...     .script("SELECT * FROM mydb.mytable") \\
        ...     .headers() \\
        ...     .option('debug') \\
        ...     .session('hive.max_initial_split_size', '4MB')
    """

    DEFAULT_SCRIPT_NAME = 'script.presto'

    def __init__(self, conf=None):
        super(PrestoJob, self).__init__(conf=conf)

        self._filename = PrestoJob.DEFAULT_SCRIPT_NAME
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

        options_str = ' '.join([
            '--{name}{space}{value}' \
                .format(name=k,
                        value=v if v is not None else '',
                        space=' ' if v is not None else '') \
            for k, v in self._command_options.get('--', {}).items()])

        sessions_str = ' '.join([
            '--session {}={}'.format(k, v) \
            for k, v in self._command_options.get('--session', {}).items()])

        return '{sessions} {options} -f {filename}' \
            .format(sessions=sessions_str,
                    options=options_str,
                    filename=self._filename) \
            .strip()

    def headers(self):
        """
        Sets the option to prepend headers in the output.
        --output-format CSV_HEADER

        Example:
            >>> job = PrestoJob() \\
            ...     .headers()

        Returns:
            :py:class:`PrestoJob`: self
        """

        self.tags('headers')

        return self.option('output-format', 'CSV_HEADER')

    @add_to_repr('append')
    def option(self, name, value=None):
        """
        Sets an option for the job.

        Using the name and value passed in, the following will be constructed for
        the command-line when executing:
        '--name value'

        Example:
            >>> # presto --output-format CSV_HEADER --debug
            >>> job = PrestoJob() \\
            ...     .option('output-format', 'CSV_HEADER') \\
            ...     .option('debug')

        Args:
            name (str): The option name.
            value (str, optional): The option value (this is optional since some
                options do not take values like '--debug').

        Returns:
            :py:class:`PrestoJob`: self
        """

        self._set_command_option('--', name.lstrip('-'), value)

        return self

    def query(self, script):
        """Alias for :py:meth:`PrestoJob.script`"""

        return self.script(script)

    @arg_string
    @add_to_repr('overwrite')
    def script(self, _script):
        """
        Sets the script to run for the job. This can be a path to a script file or
        the code to execute.

        Example:
            >>> job = PrestoJob() \\
            ...     .script("SELECT * FROM mydb.mytable")
            >>> job = PrestoJob() \\
            ...     .script("/Users/jdoe/my_query.presto")

        Args:
            script (str): A path to a script file or the code to run.

        Returns:
            :py:class:`PrestoJob`: self
        """

        if is_file(_script):
            self._filename = os.path.basename(_script)
            self._add_dependency(_script)
        else:
            self._filename = PrestoJob.DEFAULT_SCRIPT_NAME
            if not _script.strip().endswith(';'):
                _script = '{};'.format(_script)
            self._add_dependency({'name': self._filename, 'data': _script})

        return self

    @add_to_repr('append')
    def session(self, name, value):
        """
        Sets a session property for the job.

        Using the name and value passed in, the following will be constructed for
        the command-line when executing:
        '--session name=value'

        Example:
            >>> # presto --session hive.max_initial_split_size=4MB
            >>> job = PrestoJob() \\
            ...     .session('hive.max_initial_split_size', '4MB')

        Args:
            name (str): The session property name.
            value (str): The session property value.

        Returns:
            :py:class:`PrestoJob`: self
        """

        self._set_command_option('--session', name, value)

        return self
