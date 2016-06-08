"""
genie.jobs.running

This module implements the RunningJob model.

"""


from __future__ import absolute_import, division, print_function, unicode_literals

import logging
import re
import sys
import time

from ..conf import GenieConf
from ..utils import dttm_to_epoch


logger = logging.getLogger('com.netflix.genie.jobs.running')


class RunningJob(object):
    """RunningJob."""

    def __init__(self, job_id, adapter=None, conf=None):
        self.__cached_genie_log = None
        self.__cached_stderr = None
        self.__conf = conf or GenieConf()
        self.__sys_stream = None

        self._cached_genie_log = None
        self._cached_stderr = None
        self._info = dict()
        self._job_id = job_id

        # get_adapter_version is set in main __init__.py to get around circular imports
        self._adapter = adapter \
            or get_adapter_for_version(self.__conf.genie.version)(conf=self.__conf)

        stream = self.__conf.get('genie.progress_stream', 'stdout').lower()
        if stream in {'stderr', 'stdout'}:
            self.__sys_stream = getattr(sys, stream)

    def __getattr__(self, attr):
        if attr in self.info:
            return self.info.get(attr)
        raise AttributeError("'RunningJob' object has no attribute '{}'" \
            .format(attr))

    def __repr__(self):
        return '{cls}("{job_id}", adapter={adapter})'.format(
            cls=self.__class__.__name__,
            job_id=self._job_id,
            adapter=self._adapter
        )

    def __convert_dttm_to_epoch(self, info_key):
        epoch = 0
        dttm = self.info.get(info_key, '1970-01-01T00:00:00Z')
        if dttm is not None and re.search(r'\.\d\d\dZ', dttm):
            epoch = dttm_to_epoch(dttm, frmt='%Y-%m-%dT%H:%M:%S.%fZ')
        elif dttm is not None:
            epoch = dttm_to_epoch(dttm)
        return epoch * 1000

    @property
    def cluster_name(self):
        """
        Get the name of the cluster the job was executed on.

        Example:
            >>> running_job.cluster_name
            u'slacluster'

        Returns:
            str: The cluster name.
        """

        return self.info.get('cluster_name')

    @property
    def cmd_args(self):
        """
        Get the job's command line execution.

        Example:
            >>> running_job.cmd_args
            u'-f my_script.pig -p dateint=20140101'

        Returns:
            str: The command line execution.
        """

        return self.command_args

    @property
    def command_args(self):
        """
        Get the job's command line execution.

        Example:
            >>> running_job.command_args
            u'-f my_script.pig -p dateint=20140101'

        Returns:
            str: The command line execution.
        """

        return self.info.get('command_args')

    @property
    def duration(self):
        """
        Get the duration of the job's execution in milliseconds.

        A negative duration means the job is still running.

        Example:
            >>> running_job.duration
            1793
            >>> running_job.duration # job is still running
            -2182

        Returns:
            int: The duration of job execution in milliseconds.
        """

        return self.finish_time - self.start_time

    @property
    def file_dependencies(self):
        """
        Get the job's file dependencies.

        Example:
            >>> running_job.file_dependencies
            [u'x://file1.py']

        Returns:
            list: A list of the file dependencies.
        """

        return self.info.get('file_dependencies')

    @property
    def finish_time(self):
        """
        Get the job's finish epoch time (milliseconds). 0 means the job is still
        running.

        Example:
            >>> running_job.finish_time
            1234567890

        Returns:
            int: The finish time in epoch (milliseconds).
        """

        return self.__convert_dttm_to_epoch('finished')

    def genie_log(self, iterator=False):
        """
        Get the job's Genie log as either an iterator or full text.

        Example:
            >>> running_job.genie_log()
            '...'
            >>> for l in running_job.genie_log(iterator=True):
            >>>     print(l)

        Args:
            iterator (bool, optional): Set to True if want to return as iterator.

        Returns:
            str or iterator.
        """

        if self.is_done:
            if not self.__cached_genie_log:
                self.__cached_genie_log = self._adapter.get_genie_log(self._job_id)
                return self.__cached_genie_log.split('\n') if iterator \
                    else self.__cached_genie_log
        return self._adapter.get_genie_log(self._job_id, iterator=iterator)

    @property
    def info(self):
        if not self._info:
            self.update()
        return self._info

    @property
    def is_done(self):
        """
        Is the job done running?

        Example:
            >>> print running_job.is_done
            True

        Returns:
            boolean: True if the job is done, False if still running.
        """

        if self.status == 'RUNNING':
            self.update()
        return self.status != 'RUNNING'

    @property
    def is_successful(self):
        """
        Did the job complete successfully?

        Example:
            >>> print running_job.is_successful
            False

        Returns:
            boolean: True if job completed successfully, False otherwise.
        """

        if not self.is_done:
            self.update()
        return self.status == 'SUCCEEDED'

    @property
    def job_id(self):
        """
        Get the job's id.

        Example:
            >>> running_job.job_id
            u'1234-abcd-5678'

        Returns:
            str: The job id.
        """

        return self.info.get('id')

    @property
    def job_name(self):
        """
        Get the job's name.

        Example:
            >>> running_job.job_name
            u'my_job'

        Returns:
            str: Job name.
        """

        return self.info.get('name')

    @property
    def job_type(self):
        """
        Get the job's type.

        Example:
            >>> running_job.job_type
            u'PIG'

        Returns:
            str: Job type.
        """

        if 'hive' in self.info.get('command_name', '').lower():
            return 'HIVE'
        return self.info.get('command_name').upper()

    @property
    def json_link(self):
        """
        Get the json link for the job.

        Example:
            >>> print running_job.json_link
            'http://localhost/genie/1234-abcd'

        Returns:
            str: The link to job's json.
        """
        return self.info.get('json_link')

    def kill(self):
        """
        Kill the job.

        Example:
            >>> running_job.kill()

        Returns:
            request Response.
        """

        if self.info.get('kill_uri'):
            resp = self._adapter.kill_job(kill_uri=self.kill_uri)
        else:
            resp = self._adapter.kill_job(job_id=self._job_id)
        self.update()
        return resp

    @property
    def output_uri(self):
        """
        Get the output uri for the job.

        Example:
            >>> print running_job.output_uri
            'http://localhost/genie/1234-abcd/output'

        Returns:
            str: The output URI.
        """
        return self.info.get('output_uri')

    @property
    def start_time(self):
        """
        Get the job's start epoch time (milliseconds).

        Example:
            >>> running_job.start_time
            1234567890

        Returns:
            int: The start time in epoch (milliseconds).
        """

        return self.__convert_dttm_to_epoch('started')

    @property
    def status(self):
        """
        Get the job's status.

        Example:
            >>> running_job.status
            u'RUNNING'

        Returns:
            str: Job status.
        """

        status = self.info.get('status')

        return status.upper() if status else None

    def stderr(self, iterator=False):
        """
        Get the job's stderr as either an iterator or full text.

        Example:
            >>> running_job.stderr()
            '...'
            >>> for l in running_job.stderr(iterator=True):
            >>>     print(l)

        Args:
            iterator (bool, optional): Set to True if want to return as iterator.

        Returns:
            str or iterator.
        """

        if self.is_done:
            if not self.__cached_stderr:
                self.__cached_stderr = self._adapter.get_stderr(self._job_id)
                return self.__cached_stderr.split('\n') if iterator \
                    else self.__cached_stderr
        return self._adapter.get_stderr(self._job_id, iterator=iterator)

    def stdout(self, iterator=False):
        """
        Get the job's stdout as either an iterator or full text.

        Example:
            >>> running_job.stdout()
            '...'
            >>> for l in running_job.stdout(iterator=True):
            >>>     print(l)

        Args:
            iterator (bool, optional): Set to True if want to return as iterator.

        Returns:
            str or iterator.
        """

        return self._adapter.get_stdout(self._job_id, iterator=iterator)

    @property
    def status_msg(self):
        """
        Get the job's status message.

        Example:
            >>> running_job.status_msg
            u'Job is running'

        Returns:
            str: Job status message.
        """

        return self.info.get('status_msg')

    @property
    def tags(self):
        """
        Get the job's tags.

        Example:
            >>> running_job.tags
            [u'tag1', u'tag2']

        Returns:
            list: A list of tags.
        """

        return self.info.get('tags')

    def update(self, info=None):
        """Update the job information."""

        self._info = info if info is not None \
            else self._adapter.get_info_for_rj(self._job_id)

    @property
    def update_time(self):
        """
        Get the last update time for the job in epoch time (milliseconds).

        Example:
            >>> running_job.update_time
            1234567890

        Returns:
            int: The update time in epoch (milliseconds).
        """

        return self.__convert_dttm_to_epoch('updated')

    @property
    def username(self):
        """
        Get the username the job was executed as.

        Example:
            >>> running_job.username
            u'jsmith'

        Returns:
            str: The username.
        """

        return self.info.get('user')

    def wait(self, sleep_seconds=10, suppress_stream=False):
        """
        Blocking call that will wait for the job to complete.

        Example:
            >>> running_job.wait()

        Args:
            sleep_seconds (int, optional): The number of seconds to sleep while
                polling to get job status (default: 10).
            suppress_stream (bool, optional): If True, do not write anything to
                the sys stream (stderr/stdout) while waiting for the job to
                complete (default: False).

        Returns:
            :py:class:`RunningJob`: self
        """

        i = 0

        while self._adapter.get_status(self._job_id).upper() == 'RUNNING':
            if i % 3 == 0 and not suppress_stream:
                self._write_to_stream('.')
            time.sleep(sleep_seconds)
            i += 1

        if not suppress_stream:
            self._write_to_stream('\n')

        self.update()

        return self

    def _write_to_stream(self, msg):
        """Writes message to the configured sys stream."""

        if self.__sys_stream is not None:
            self.__sys_stream.write(msg)
            self.__sys_stream.flush()
