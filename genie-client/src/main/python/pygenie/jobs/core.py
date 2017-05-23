"""
genie.jobs.core

This module implements creating a base Genie job.

"""


from __future__ import absolute_import, division, print_function, unicode_literals

import json
import logging
import re
import sys

from collections import defaultdict, OrderedDict

from ..conf import GenieConf
from ..utils import (convert_to_unicode,
                     is_str,
                     normalize_list,
                     str_to_list,
                     unicodify,
                     uuid_str)
from .utils import (add_to_repr,
                    arg_list,
                    arg_string,
                    generate_job_id,
                    is_file,
                    reattach_job)

from ..exceptions import (GenieJobError,
                          GenieJobNotFoundError)


logger = logging.getLogger('com.netflix.genie.jobs.core')


class Repr(object):
    """Handles creating repr for an object."""

    def __init__(self, class_name=None):
        self.__class_name = class_name[:-2] if class_name.endswith('()') \
            else class_name
        self.__repr_list = list()

    def __repr__(self):
        return self.__unicode__()

    def __str__(self):
        return self.__unicode__()

    def __unicode__(self):
        return '.'.join(self.repr_list)

    @staticmethod
    def __quote(val):
        """Get quote to use for string."""

        if '\n' in val or ('"' in val and "'" in val):
            return '"""'
        elif '"' in val:
            return "'"
        return '"'

    def append(self, func_name=None, args=None, kwargs=None):
        """Add a call string to the repr list."""

        args_str = self.args_to_str(args)
        kwargs_str = self.kwargs_to_str(kwargs)

        call_str = '{func}({args}{comma}{kwargs})' \
            .format(func=func_name,
                    args=args_str if args_str else '',
                    comma=', ' if kwargs_str and args_str else '',
                    kwargs=kwargs_str if kwargs_str else '')

        # remove any exact duplicate calls
        self.remove(call_str)

        self.__repr_list.append(call_str)

    def args_to_str(self, args):
        """Convert args tuple to string."""

        if args is not None:
            results = list()
            for arg in [convert_to_unicode(a) for a in args]:
                value = arg
                if isinstance(arg, list):
                    value = normalize_list(arg)
                results.append('{qu}{val}{qu}' \
                    .format(val=value,
                            qu=self.__quote(value) if is_str(arg) else ''))
            return ', '.join(results)

        return ''

    def kwargs_to_str(self, kwargs):
        """Convert kwargs dict to string."""

        return ', '.join([
            '{key}={qu}{val}{qu}' \
                .format(key=key,
                        val=val,
                        qu=self.__quote(val) if is_str(val) else '')
            for key, val in kwargs.items()
        ]) if kwargs is not None else ''

    def pop(self):
        """Pop off the last element in the repr list."""

        self.__repr_list.pop()

    def remove(self, regex_filter, flags=0):
        """Remove call string from the repr list based on a regex filter."""

        assert regex_filter is not None, 'must specify a regular expression filter'

        regex_filter = re.escape(regex_filter)

        self.__repr_list = [i for i in self.__repr_list \
            if not re.search(regex_filter, i, flags=flags) and regex_filter != i]
        return self

    @property
    def repr_list(self):
        """The repr represented as a list."""

        return ['{}()'.format(self.__class_name)] + sorted(self.__repr_list)


class GenieJob(object):
    """Base Genie job."""

    def __init__(self, conf=None):
        assert conf is None or isinstance(conf, GenieConf), \
            "invalid conf '{}', should be None or GenieConf".format(conf)

        cls = self.__class__.__name__
        job_type = cls.rsplit('Job', 1)[0].lower() if cls.endswith('Job') \
            else cls.lower()

        self._conf = conf or GenieConf()
        self.default_command_tags = str_to_list(
            self._conf.get('{}.default_command_tags'.format(cls),
                          ['type:{}'.format(job_type)])
        )
        self.default_cluster_tags = str_to_list(
            self._conf.get('{}.default_cluster_tags'.format(cls),
                          ['type:{}'.format(job_type)])
        )
        self.repr_obj = Repr(self.__class__.__name__)

        self._application_ids = list()
        self._archive = True
        self._cluster_tag_mapping = defaultdict(list)
        self._command_arguments = None
        self._command_options = defaultdict(OrderedDict)
        self._command_tags = list()
        self._dependencies = list()
        self._description = None
        self._email = None
        self._genie_cpu = None
        self._genie_memory = None
        self._group = None
        self._job_id = uuid_str()
        self._job_name = None
        self._job_version = 'NA'
        self._parameters = OrderedDict()
        self._post_cmd_args = list()
        self._setup_file = None
        self._tags = list()
        self._timeout = None
        self._username = self._conf.get('genie.username')

        #initialize repr
        self.repr_obj.append('job_id', (self._job_id,))
        self.repr_obj.append('genie_username', (self._username,))

        #initialize cluster tags with default set of tags
        self._cluster_tag_mapping[99999] = self.default_cluster_tags

    def __repr__(self):
        return self.__unicode__()

    def __str__(self):
        return self.__unicode__()

    def __unicode__(self):
        if sys.version_info < (3,):
            return unicode(self.repr_obj)
        return str(self.repr_obj)

    def _add_dependency(self, dep):
        """
        Add a dependency to the job. Will not add if the dependency is already
        added.
        """

        if dep not in self._dependencies:
            self._dependencies.append(dep)

    @unicodify
    def _add_cluster_tag(self, tags, priority=0):
        """
        Add a cluster tag to level. The priority is the level of precedence when
        Genie looks for a cluster to use (higher priority = higher precedence).
        """

        assert isinstance(tags, list), 'tags should be a list'

        # negate priority so can do sorted(self._cluster_tag_mapping.keys())
        self._cluster_tag_mapping[-int(priority)].extend(tags)

    @unicodify
    def _set_command_option(self, flag, name, value=None):
        """
        Convenience method for storing an option which can later be used
        when constructing the command line.

        Args:
            flag (str): The option flag.
            name (str): The option name.
            value (str, optional): The option value.
        """

        self._command_options[flag][name] = value

    @unicodify
    @arg_list
    @add_to_repr('append')
    def applications(self, _application_ids):
        """
        Overrides which applications to use when executing a command.

        When executing a command, Genie will use a preset list of applications
        for the command. For example, when Genie is executing the "pig" command
        it may use the preset list of "pig14" and "hadoop264" application ids (the
        preset list will be determined when the command is registered). Setting
        this will override the applications used for the command (only for this
        execution - not globally).

        Example:
            >>> job = GenieJob() \\
            ...     .applications('spark160') \\
            ...     .applications('myapp2') \\
            ...     .applications(['anotherapp', 'myapp3'])
            >>> print(job.to_dict().get('application_ids'))
            ['spark160', 'myapp2', 'anotherapp', 'myapp3']

        Args:
            _application_ids (str, list): The id of the application to use when
                executing the command.

        Returns:
            :py:class:`GenieJob`: self
        """

    @add_to_repr('overwrite')
    def archive(self, archive):
        """
        Sets whether Genie should archive the job (default: True).

        The archive is stored on Genie's specified file system.

        Example:
            >>> job = GenieJob() \\
            ...     .archive(False)

        Args:
             archive (boolean): If True, Genie will archive the job.

        Returns:
            :py:class:`GenieJob`: self
        """

        assert isinstance(archive, bool), "archive must be a boolean"

        self._archive = archive
        return self

    @unicodify
    @add_to_repr('append')
    def cluster_tags(self, cluster_tags):
        """
        Adds a tag for Genie to use when selecting which cluster to run the job on.

        Example:
            >>> job = GenieJob() \\
            ...     .cluster_tags('type:cluster1') \\
            ...     .cluster_tags('misc:c2, misc:c3') \\
            ...     .cluster_tags(['data:test', 'misc:c4'])
            >>> print(job.to_dict().get('cluster_tags'))
            ['type:cluster1', 'misc:c2', 'misc:c3', 'data:test', 'misc:c4']

        Args:
            cluster_tags (str, list): A tag for the cluster to use.

        Returns:
            :py:class:`GenieJob`: self
        """

        self._add_cluster_tag(str_to_list(cluster_tags))

        return self

    @property
    def cmd_args(self):
        """
        The constructed command line arguments using the job's definition. If the
        command line arguments are set explicitly (by calling
        :py:meth:`command_arguments`) this will be the same.

        Should never be called (child classes should overwrite).
        """

        if self._command_arguments is not None:
            return self._command_arguments

        raise GenieJobError('should not try to access core GenieJob ' \
                            'constructed command arguments')

    @unicodify
    @arg_string
    @add_to_repr('overwrite')
    def command_arguments(self, _command_arguments):
        """
        Overrides the constructed command line arguments for the command.

        By default, the client will construct the command line arguments using
        the job definition when submitting the job. This will override this
        behavior and instead use the value set.

        Args:
            cmd_args (str): The command line arguments for the command.

        Returns:
            :py:class:`GenieJob`: self
        """

    @unicodify
    @arg_list
    @add_to_repr('append')
    def command_tags(self, _command_tags):
        """
        Adds a tag for Genie to use when selecting which command to use when
        executing the job.

        Example:
            >>> job = GenieJob() \\
            ...     .command_tags('type:cmd') \\
            ...     .command_tags('ver:1.0')
            >>> print(job.to_dict().get('command_tags'))
            ['type:cmd', 'ver:1.0']

        Args:
            _command_tags (str, list): A tag for the command to use.

        Returns:
            :py:class:`GenieJob`: self
        """

    @arg_list
    @add_to_repr('append')
    def dependencies(self, _dependencies):
        """
        Add a file dependency for the Genie job.

        Example:
            >>> job = GenieJob() \\
            ...     .dependencies('/path/to/dep_file_1') \\
            ...     .dependencies(['/path/to/dep_file_2',
            ...                    '/path/to/dep_file_3'])
            >>> print(job.to_dict().get('dependencies'))
            ['/path/to/dep_file_1', '/path/to/dep_file_2', '/path/to/dep_file_3']

        Args:
            _dependencies (str, list): Path to a file dependency for the job.

        Returns:
            :py:class:`GenieJob`: self
        """

    @unicodify
    @arg_string
    @add_to_repr('overwrite')
    def description(self, _description):
        """
        Sets the description for the job.

        Args:
            description (str): The job description.

        Returns:
            :py:class:`GenieJob`: self
        """

    def disable_archive(self):
        """
        Disables Genie's job archiving for the job.

        Example:
            >>> job = GenieJob() \\
            ...     .disable_archive()

        Returns:
            :py:class:`GenieJob`: self
        """

        return self.archive(False)

    def execute(self, retry=False, force=False, catch_signal=False):
        """
        Send the job to Genie and execute.

        Example:
            >>> running_job = GenieJob().execute()

        Args:
            retry (bool, optional): If True, will check to see if a previous job
                with the same job id has previously executed and generate a new
                job id until finds a job id to a running job, a successful job, or
                a brand new job (Default: False).
            force (bool, optional): If True, will do the same thing as
                retry=True, but will generate a new job id even if there is
                a successful execution (Default: False).
            catch_signal (bool, optional): If True, will add signal handlers to
                kill the running job for SIGINT, SIGTERM, and SIGABRT
                (Default: False).

        Returns:
            :py:class:`RunningJob`: A running job object.
        """

        if catch_signal:
            import signal
            import sys
            def sig_handler(signum, frame):
                logger.warning("caught signal %s", signum)
                try:
                    if running_job.job_id:
                        logger.warning("killing job id %s", running_job.job_id)
                        response = running_job.kill()
                        response.raise_for_status()
                except Exception:
                    pass
                finally:
                    sys.exit(1)
            signal.signal(signal.SIGINT, sig_handler)
            signal.signal(signal.SIGTERM, sig_handler)
            signal.signal(signal.SIGABRT, sig_handler)

        if retry or force:
            uid = self._job_id
            try:
                # below uid should be uid of job with one of following status:
                #     - new
                #     - running
                #     - (if force=False) successful
                uid = generate_job_id(uid,
                                      return_success=not force,
                                      conf=self._conf)
                # new uid will raise and be handled in the except block
                # assigning to running_job variable for killing on signal
                running_job = reattach_job(uid, conf=self._conf)
                return running_job
            except GenieJobNotFoundError:
                self.job_id(uid)

        # execute_job is set in main __init__.py to get around circular imports
        # execute_job imports jobs, jobs need to import execute_job
        # assigning to running_job variable for killing on signal
        running_job = execute_job(self)
        return running_job

    @add_to_repr('overwrite')
    def genie_cpu(self, cpu):
        """
        Set the number of CPUs for Genie to allocate when executing the job.

        Example:
            >>> job = GenieJob() \\
            ...     .genie_cpu(2)

        Args:
            cpu (int): Number of CPUs to allocate.

        Returns:
            :py:class:`GenieJob`: self
        """

        assert int(cpu) > 0, 'number of CPUs cannot be less than 1'

        self._genie_cpu = int(cpu)

        return self

    @unicodify
    @arg_string
    @add_to_repr('overwrite')
    def genie_email(self, _email):
        """
        Sets an email address for Genie to send a notification to after the job
        is done.

        Example:
            >>> job = GenieJob() \\
            ...     .genie_email('jdoe@domain.com')

        Args:
            email_addr (str): The email address.

        Returns:
            :py:class:`GenieJob`: self
        """

    def email(self, email):
        # TODO: for legacy support (need to remove) - should use .genie_timeout()
        logger.warning("Use .genie_email('%s') to set Genie email.", email)
        return self.genie_email(email)

    @add_to_repr('overwrite')
    def genie_memory(self, memory):
        """
        Set the amount of memory (MB) for Genie to allocate when executing the job.

        Example:
            >>> # set Genie to allocate 6 GB of memory
            >>> job = GenieJob() \\
            ...     .genie_memory(6000)

        Args:
            memory (int): Amount of memory (MB) to allocate.

        Returns:
            :py:class:`GenieJob`: self
        """

        assert int(memory) > 0, 'memory amount (MB) cannot be less than 1'

        self._genie_memory = int(memory)

        return self

    @unicodify
    @add_to_repr('overwrite')
    def genie_setup_file(self, setup_file):
        """
        Sets a Bash file to source before the job is executed.

        Genie will source the Bash file before executing the job. This can be
        used to set environment variables before the job is run, etc. This
        should not be used to set job configuration properties via a file (job's
        should have separate interfaces for setting property files).

        The file must be stored externally, or available on the Genie nodes.

        Example:
            >>> job = GenieJob() \\
            ...     .genie_setup_file('/Users/jdoe/my_setup_file.sh')

        Args:
            setup_file (str): The local path to the Bash file to use for setup.

        Returns:
            :py:class:`GenieJob`: self
        """

        assert setup_file is not None and is_file(setup_file), \
            "setup file '{}' does not exist".format(setup_file)

        self._setup_file = setup_file

        return self

    def setup_file(self, setup_file):
        # TODO: for legacy support (need to remove) - should use .genie_timeout()
        logger.warning("Use .genie_setup_file('%s') to set Genie setup file.", setup_file)
        return self.genie_setup_file(setup_file)

    @add_to_repr('overwrite')
    def genie_timeout(self, timeout):
        """
        Sets a Genie job timeout (in seconds) for the job.

        If the job does not finish within the specified timeout, Genie will kill
        the job.

        Example:
            >>> job = GenieJob() \\
            ...     .genie_timeout(3600)

        Args:
            timeout (int): The timeout for the job in seconds.

        Returns:
            :py:class:`GenieJob`: self
        """

        assert timeout is not None, \
            'timeout cannot None and should be an int (number of seconds)'

        self._timeout = int(timeout)

        return self

    def timeout(self, timeout):
        # TODO: for legacy support (need to remove) - should use .genie_timeout()
        logger.warning("Use .genie_timeout(%s) to set Genie timeout.", timeout)
        return self.genie_timeout(timeout)

    @add_to_repr('overwrite')
    def genie_url(self, url):
        """
        Set the Genie url to use when submitting the job.

        Example:
            >>> job = GenieJob() \\
            ...     .genie_url('http://....')

        Args:
            url (str): The Genie url to use when submitting the job.

        Returns:
            :py:class:`GenieJob`: self
        """

        self._conf.genie.url = url[:-1] if url.endswith('/') else url

        return self

    @unicodify
    @arg_string
    @add_to_repr('overwrite')
    def genie_username(self, _username):
        """
        Sets the Genie username to use when executing the job.

        Example:
            >>> job = GenieJob() \\
            ...     .genie_username('jdoe')

        Args:
            username (str): The username.

        Returns:
            :py:class:`GenieJob`: self
        """

    def username(self, username):
        # TODO: for legacy support (need to remove) - should use .genie_username()
        logger.warning("Use .genie_username('%s') to set Genie user.", username)
        return self.genie_username(username)

    def get(self, attr, default=None):
        """
        Get the value for an attribute. If the attribute does not exist, return
        the default specified.

        This does a get on the job's to_dict() return value.

        Example:
            >>> job = GenieJob() \\
            ...     .name('my job name')
            >>> print(job.get('job_name'))
            'my job name'

        Args:
            attr: A key in the job's to_dict().

        Returns:
            None, str, int, list, etc: The attribute value or default.
        """

        return self.to_dict().get(attr, default)

    @unicodify
    @arg_string
    @add_to_repr('overwrite')
    def group(self, _group):
        """
        Sets which group to create a user in when Genie creates a user in the
        system.

        Example:
            >>> job = GenieJob() \\
            ...     .group('mygroup')

        Args:
             group (str): The group name to create the user in.

        Returns:
            :py:class:`GenieJob`: self
        """

    @unicodify
    @arg_string
    @add_to_repr('overwrite')
    def job_id(self, _job_id):
        """
        Sets an id for the job.

        If the id is not unique, when the job is executed it will reattach to the
        previously run job with the same id.

        Example:
            >>> job = GenieJob() \\
            ...     .job_id('1234-abcd')
            >>> print(job.id)
            '1234-abcd'

        Args:
            job_id (str): The unique id for the job.

        Returns:
            :py:class:`GenieJob`: self
        """

    @unicodify
    @arg_string
    @add_to_repr('overwrite')
    def job_name(self, _job_name):
        """
        Sets a name for the job. The name does not have to be unique.

        Example:
            >>> job = GenieJob() \\
            ...     .job_name('my job')

        Args:
            job_name (str): The name for the job.

        Returns:
            :py:class:`GenieJob`: self
        """

    @unicodify
    @arg_string
    @add_to_repr('overwrite')
    def job_version(self, _job_version):
        """
        Sets a version for the Genie job.

        Example:
            >>> job = GenieJob() \\
            ...     .job_version('1.5')

        Args:
            version (str): The version for the Genie job.

        Returns:
            :py:class:`GenieJob`: self
        """

    @unicodify
    @add_to_repr('append')
    def parameter(self, name, value):
        """
        Sets a parameter for parameter substitution in the job's script.

        If the script is not a file, the script will be replaced with the
        parameters before it is sent to Genie.

        If the job type supports passing parameters through the command-line,
        the parameters will be set in the command-line as well. If the job type
        does not, they will not be passed through.

        Example:
            >>> #For pig: -p foo=1 -p bar=2
            >>> job = PigJob() \\
            ...     .parameter('foo', '1') \\
            ...     .parameter('bar', '2')

        Args:
            name (str): The parameter name.
            value (str): The parameter value.

        Returns:
            :py:class:`GenieJob`: self
        """

        self._parameters[name] = value

        return self

    def parameters(self, **kwargs):
        """
        Convenience method for setting multiple parameters using kwargs.

        Example:
            >>> #For pig: -p foo=1 -p bar=2
            >>> job = PigJob() \\
            ...     .parameters(foo='1', bar='2')

        Args:
            **kwargs: Keyword arguments for each parameter.

        Returns:
            :py:class:`GenieJob`: self
        """

        for name, value in kwargs.items():
            self.parameter(name, value)

        return self

    @unicodify
    @arg_list
    @add_to_repr('append')
    def post_cmd_args(self, _post_cmd_args):
        """
        Add arguments, options, etc to the end of the constructed command line.

        Example:
            >>> job = HiveJob() \\
            ...     .script('select * from dual') \\
            ...     .headers() \\
            ...     .hiveconf('a', 'b') \\
            ...     .post_cmd_args('some_arg_1') \\
            ...     .post_cmd_args(['arg_2', 'arg_3'])
            >>> job.cmd_args
            "--hiveconf a=b --hiveconf hive.cli.print.header=true -f script.hive some_arg_1 arg_2 arg_3"

        Args:
            tag (str, list): A tag for the job.

        Returns:
            :py:class:`GenieJob`: self
        """

    @unicodify
    @arg_list
    @add_to_repr('append')
    def tags(self, _tags):
        """
        Adds a tag for the job. Tagging a job allows for easier searching, etc.

        Example:
            >>> job = GenieJob() \\
            ...     .tags('tag_1') \\
            ...     .tags('tag_2, tag_3') \\
            ...     .tags(['tag_4', 'tag_5'])
            >>> print(job.to_dict().get('tags'))
            ['tag_1', 'tag_2', 'tag_3', 'tag_4', 'tag_5']

        Args:
            tag (str, list): A tag for the job.

        Returns:
            :py:class:`GenieJob`: self
        """

    def to_dict(self):
        """
        Get a mapping of attributes to values for the object.

        Example:
            >>> GenieJob().to_dict()
            {...}

        Returns:
            dict: A mapping of attributes to values.
        """

        _dict = self.__dict__.copy()
        _dict['repr'] = self.repr_obj
        del _dict['_conf']
        del _dict['repr_obj']
        return {
            attr_name.lstrip('_'): attr_val \
            for attr_name, attr_val in _dict.items()
            if not attr_name.startswith('_{}__'.format(self.__class__.__name__))
        }

    def to_json(self):
        """
        Get a JSON string of the object mapping.

        Example:
            >>> print(GenieJob().to_json())
            {...}

        Returns:
            str: A JSON string.
        """

        return json.dumps(self.to_dict(), sort_keys=True, indent=4)
