"""
genie.jobs.adapter.adapter

This module implements adapting a job into a JSON payload to
submit to the sevice.

"""


from __future__ import absolute_import, division, print_function, unicode_literals

import logging

from ..running import RunningJob

from ...exceptions import GenieAdapterError

# available adapters
from .genie_2 import Genie2Adapter
from .genie_3 import Genie3Adapter


logger = logging.getLogger('com.netflix.genie.jobs.adapter.adapter')


def get_adapter_for_version(version):
    """Given a version string, return a client object."""

    major_version = str(version).split('.', 1)[0]
    if major_version == '2':
        return Genie2Adapter()
    elif major_version == '3':
        return Genie3Adapter()
    raise GenieAdapterError("no adapter for version '{}'".format(version))


def execute_job(job):
    """
    Take a job and convert it to a JSON payload based on the job's
    configuration object's Genie version value and execute.

    Returns:
        response: HTTP response.
    """

    version = job.conf.get('genie.version')
    adapter = get_adapter_for_version(version)
    if adapter is not None:
        try:
            adapter.submit_job(job)
            return RunningJob(job.get('job_id'), adapter=adapter)
        except NotImplementedError:
            pass
    raise GenieAdapterError("no adapter for '{}' to version '{}'" \
        .format(job.__class__.__name__, version))
