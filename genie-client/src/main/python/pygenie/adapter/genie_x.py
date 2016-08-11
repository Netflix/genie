"""
genie.jobs.adapter.genie_x

This module implements base Genie adapter.

"""


from __future__ import absolute_import, division, print_function, unicode_literals

import logging

from functools import wraps

from ..conf import GenieConf


logger = logging.getLogger('com.netflix.genie.jobs.adapter.genie_x')


def raise_not_implemented(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        raise NotImplementedError('cannot use GenieBaseAdapter.{}' \
            .format(func.func_name))
    return wrapper


def substitute(template, context):
    """
    Performs string substitution.

    Args:
        template (str): The string to perform the substitution on.
        context (dict): A mapping of keys to values for substitution.

    Returns:
        str: The substituted string.
    """

    from string import Template
    template = Template(template)

    return template.safe_substitute(context)


class GenieBaseAdapter(object):
    """Base Genie Adapter"""

    def __init__(self, conf=None):
        assert conf is None or isinstance(conf, GenieConf), \
            "invalid conf '{}', should be None or GenieConf".format(conf)

        self._conf = conf if conf else GenieConf()

    def __repr__(self):
        return '{}(conf={})'.format(self.__class__.__name__, self._conf)

    @staticmethod
    @raise_not_implemented
    def construct_base_payload(*args, **kwargs):
        """
        This needs to be implemented by adapter.

        Return a dict payload for the job.
        """

    @raise_not_implemented
    def get_info_for_rj(self, *args, **kwargs):
        """
        This needs to be implemented by adapter.

        Return a dict the RunningJob object will use for attributes, etc.
        """

    @raise_not_implemented
    def get_log(self, *args, **kwargs):
        """
        This needs to be implemented by adapter.

        Return the job's specified log.
        """

    @raise_not_implemented
    def get_genie_log(self, *args, **kwargs):
        """
        This needs to be implemented by adapter.

        Return job's genie.log.
        """

    @raise_not_implemented
    def get_status(self, *args, **kwargs):
        """
        This needs to be implemented by adapter.

        Return job's status.
        """

    @raise_not_implemented
    def get_stderr(self, *args, **kwargs):
        """
        This needs to be implemented by adapter.

        Return job's stderr.log.
        """

    @raise_not_implemented
    def get_stdout(self, *args, **kwargs):
        """
        This needs to be implemented by adapter.

        Return job's stdout.log.
        """

    @raise_not_implemented
    def kill_job(self, *args, **kwargs):
        """
        This needs to be implemented by adapter.

        Kill the job.
        """

    @raise_not_implemented
    def submit_job(self, *args, **kwargs):
        """
        This needs to be implemented by adapter.

        Submit the job execution to Genie.
        """
