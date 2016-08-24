"""
genie.jobs.utils

This module contains util functions to be used for jobs.

"""


from __future__ import absolute_import, division, print_function, unicode_literals

import inspect
import logging
import os

from decorator import decorator
from functools import wraps

from .running import RunningJob

from ..utils import (convert_to_unicode,
                     is_str,
                     str_to_list)

from ..exceptions import GenieJobNotFoundError


logger = logging.getLogger('com.netflix.genie.jobs.utils')

REPR_APPEND_MODES = {
    'append',
    'insert'
}

REPR_OVERWRITE_MODES = {
    'overwrite',
    'update'
}

REPR_MODES = REPR_APPEND_MODES.union(REPR_OVERWRITE_MODES)


def _remove_wrapper_from_args(func):
    """
    Using decorator.decorator, the first argument in *args is the function.
    Remove this because passing this into object methods breaks.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        """Wraps func."""

        return func(*args[1:], **kwargs)

    return wrapper


def add_to_repr(mode):
    """
    Decorator for adding a method call to an object's repr. The decorator
    assumes the object has a Repr object instantiated in the object's
    repr_obj attribute. If the object does not have this attribute, will do
    nothing.

    When adding a call to the repr list, the following modes are available:
        append: methods will just be added to the repr list.
        overwrite: if the method was previously called, overwrite the call in
            the repr list.

    Args:
        mode (str): The mode when adding to the object's repr list ('append',
            'overwrite', etc).
    """

    assert is_str(mode) and mode.lower() in REPR_MODES, \
        "invalid mode for adding to repr ('{}'), use: {}" \
            .format(mode, REPR_MODES)

    mode = mode.lower()

    def real_decorator(func):
        """Wrap the function call."""

        @_remove_wrapper_from_args
        def wrapper(*args, **kwargs):
            """Adds the method call to an object's repr list."""

            repr_obj = getattr(args[0], 'repr_obj', None)

            if repr_obj:
                if mode in REPR_OVERWRITE_MODES:
                    repr_obj.remove(r'{}('.format(func.__name__))
                repr_obj.append(func_name=func.__name__,
                                args=args[1:],
                                kwargs=kwargs)

            try:
                return func(*args, **kwargs)
            except Exception:
                if repr_obj:
                    repr_obj.pop()
                raise

        return decorator(wrapper, func)

    return real_decorator


def arg_list(func):
    """
    Decorator for accepting a string or list arg into a method and adding it to
    an object's attribute as a list (with duplicates removed). The object's
    attribute name is specified via the method's arg name when the method
    is defined.
    """

    @_remove_wrapper_from_args
    def wrapper(*args, **kwargs):
        """Add arg to object's attribute as a list."""

        assert len(args) == 2, 'incorrect arguments to {}()'.format(func.__name__)

        attr_name = inspect.getargspec(func).args[1]
        self = args[0]
        value = args[1]

        assert isinstance(value, list) or is_str(value), \
            '{}() argument value should be a string or list of strings' \
                .format(func.__name__)

        attr = getattr(self, attr_name)
        attr.extend(str_to_list(value))

        final = list()
        last = None
        for item in sorted(attr):
            if item != last:
                final.append(item)
                last = item

        setattr(self, attr_name, final)

        return func(*args, **kwargs) or self

    return decorator(wrapper, func)


def arg_string(func):
    """
    Decorator for accepting a string arg into a method and setting the attribute.
    The object's attribute name is specified via the method's arg name when the
    method is defined.
    """

    @_remove_wrapper_from_args
    def wrapper(*args, **kwargs):
        """Set arg to object's attribute as a string."""

        assert len(args) == 2, 'incorrect arguments to {}()'.format(func.__name__)

        attr_name = inspect.getargspec(func).args[1]
        self = args[0]
        value = args[1]

        assert is_str(value), \
            '{}() argument value should be a string'.format(func.__name__)

        setattr(self, attr_name, convert_to_unicode(value))

        return func(*args, **kwargs) or self

    return decorator(wrapper, func)


def generate_job_id(job_id, return_success=True, conf=None):
    """
    Generate a new job id.

    By default, will continue to generate an id until either
    1) the generated id is for a job that is running or successful
    2) the generated id is completely new

    If return_success is False, will continue to generate an id until either
    1) the generated id is for a job that is running
    2) the generated id is completely new
    """

    while True:
        try:
            running_job = reattach_job(job_id, conf=conf)
            logger.debug("job id '%s' exists with status '%s'",
                         job_id,
                         running_job.status)
            if not running_job.is_done \
                    or (running_job.is_done \
                        and running_job.is_successful \
                        and return_success):
                logger.debug("returning job id '%s' with status '%s'",
                            running_job.job_id,
                            running_job.status)
                return running_job.job_id
            id_parts = running_job.job_id.split('-')
            if id_parts[-1].isdigit():
                id_parts[-1] = unicode(int(id_parts[-1]) + 1)
            else:
                id_parts.append('1')
            job_id = '-'.join(id_parts)
            logger.debug("trying new job id '%s'", job_id)
        except GenieJobNotFoundError:
            logger.debug("returning new job id '%s'", job_id)
            return job_id


def is_attachment(dependency):
    """Return True if the dependency should be handled as an attachment."""

    return dependency is not None and \
        (isinstance(dependency, dict) \
            or os.path.isfile(dependency) \
            or os.path.isdir(dependency))


def is_file(path):
    """Checks if path is to a file."""

    path = convert_to_unicode(path)

    return path is not None and \
        (os.path.isfile(path.encode('utf-8')) \
         or path.startswith('s3://') \
         or path.startswith('s3n://'))


def reattach_job(job_id, conf=None):
    """
    Reattach to a running job.

    Search for a job (by job id). If found, return :py:class:`RunningJob` object.

    Args:
        job_id (str): The id of the job to reattach.
        conf (GenieConf, optional): The conf object to use for the RunningJob.

    Returns:
        :py:class:`RunningJob` object.
    """

    running_job = RunningJob(job_id, conf=conf)

    # try to access the RunningJob's status to see if the job exists
    running_job.status

    return running_job
