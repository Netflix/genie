"""
genie.jobs.sqoop

This module implements creating Sqoop jobs.

"""


from __future__ import absolute_import, division, print_function, unicode_literals

import logging

from collections import defaultdict

from .core import GenieJob
from .utils import add_to_repr


logger = logging.getLogger('com.netflix.genie.jobs.sqoop')


class SqoopJob(GenieJob):
    """
    Sqoop job.

    Example:
        >>> job = SqoopJob() \\
        ...     .cmd('import') \\
        ...     .connect('jdbc:mysql://foo.bar.us-east-1.rds.amazonaws.com:3306/fizz') \\
        ...     .username('jsmith') \\
        ...     .password('password') \\
        ...     .table('my_table') \\
        ...     .split_by('some_id') \\
        ...     .target_dir('some_dir')
        >>> job \\
        ...     .option('username', 'jsmith') \\
        ...     .option('verbose')
    """

    def __init__(self, conf=None):
        super(SqoopJob, self).__init__(conf=conf)

        self._cmd = None
        self.__options = {}
        self.__properties = defaultdict(dict)

        self.cmd('import')

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
            '{} {}'.format(k, v if v is not None else "") \
            for k, v in self.__options.items()
        ])

        props_list = list()

        for flag, props in self.__properties.items():
            props_list.append(' '.join([
                '{flag}{space}{name}={value}' \
                    .format(flag=flag,
                            space='' if flag in ['-D', '--'] else ' ',
                            name=k,
                            value=v)
                for k, v in props.items()
            ]))

        return '{cmd} {properties} {options}' \
            .format(cmd=self._cmd,
                    properties=' '.join(props_list),
                    options=options_str) \
            .strip()

    @add_to_repr('overwrite')
    def cmd(self, cmd):
        """
        Set the command to use for the Sqoop tool ('import', 'export', etc).

        Example:
            >>> job = SqoopJob() \\
            ...     .cmd('import')

        Args:
            cmd (str): The Sqoop command to use.

        Returns:
            :py:class:`SqoopJob`: self
        """

        assert cmd, "cmd should be something ('import', 'export', etc)"

        self._cmd = cmd

        return self

    @add_to_repr('append')
    def option(self, name, value=None):
        """
        Set a command-line option for the Sqoop command.

        Example:
            >>> # sqoop --verbose --connect jdbc:mysql://
            >>> job = SqoopJob() \\
            ...     .option('verbose') \\
            ...     .option('connect', 'jdbc:mysql://foo.bar.us-east-1.rds.amazonaws.com:3306/db')

        Args:
            name (str): The option name as it appears on the command-line.
            value (str, optional): The value to set the option to (optional because
                some options do not take values like 'verbose').

        Returns:
            :py:class:`SqoopJob`: self
        """

        if not name.startswith('-'):
            name = '-{}'.format(name) if len(name) == 1 else '--{}'.format(name)

        self.__options[name] = value

        return self

    @add_to_repr('append')
    def property(self, prop, value, flag='-D'):
        """
        Sets configuration and Hadoop server settings.

        Example:
            >>> # sqoop -Dfoo=bar --someflag hello=world
            >>> job = SqoopJob() \\
            ...     .property('foo', 'bar') \\
            ...     .property('hello', 'world', flag='--someflag')

        Args:
            prop (str): The property to set.
            value (str): The value for the property.
            flag (str, optional): The command line flag for the property
                (default: '-D').

        Returns:
            :py:class:`SqoopJob`: self
        """

        self.__properties[flag][prop] = value

        return self

    def connect(self, value):
        """
        Set the '--connect' parameter for the Sqoop command.

        Example:
            >>> # sqoop --connect jdbc:mysql://
            >>> job = SqoopJob() \\
            ...     .connect('jdbc:mysql://billingtest.foo.us-east-1.rds.amazonaws.com:3306/billing')

        Args:
            value (str): The value to set the '--connect' option to.

        Returns:
            :py:class:`SqoopJob`: self
        """

        return self.option('connect', value)

    def password(self, value):
        """
        Set the '--password' parameter for the Sqoop command.

        Example:
            >>> # sqoop --password passw0rd
            >>> job = SqoopJob() \\
            ...     .username('passw0rd')

        Args:
            value (str): The value to set the '--password' option to.

        Returns:
            :py:class:`SqoopJob`: self
        """

        return self.option('password', value)

    def split_by(self, value):
        """
        Set the '--split-by' parameter for the Sqoop command.

        Example:
            >>> # sqoop --split-by customer_id
            >>> job = SqoopJob() \\
            ...     .split_by('customer_id')

        Args:
            value (str): The value to set the '--split-by' option to.

        Returns:
            :py:class:`SqoopJob`: self
        """

        return self.option('split-by', value)

    def table(self, value):
        """
        Set the '--table' parameter for the Sqoop command.

        Example:
            >>> # sqoop --table my_table
            >>> job = SqoopJob() \\
            ...     .table('my_table')

        Args:
            value (str): The value to set the '--table' option to.

        Returns:
            :py:class:`SqoopJob`: self
        """

        return self.option('table', value)

    def target_dir(self, value):
        """
        Set the '--target-dir' parameter for the Sqoop command.

        Example:
            >>> # sqoop --target-dir /path/to/target/
            >>> job = SqoopJob() \\
            ...     .target_dir('/path/to/target/')

        Args:
            value (str): The value to set the '--target-dir' option to.

        Returns:
            :py:class:`SqoopJob`: self
        """

        return self.option('target-dir', value)

    def username(self, value):
        """
        Set the '--username' parameter for the Sqoop command and the username
        for the Genie job.

        Example:
            >>> # sqoop --username jsmith
            >>> job = SqoopJob() \\
            ...     .username('jsmith')

        Args:
            value (str): The value to set the '--username' option to.

        Returns:
            :py:class:`SqoopJob`: self
        """

        self.option('username', value)

        return super(SqoopJob, self).username(value)
