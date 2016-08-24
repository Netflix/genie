"""
genie.jobs.adapter.adapter

This module implements adapting a job into a JSON payload to
submit to the sevice.

"""


from __future__ import absolute_import, division, print_function, unicode_literals

import logging

from ..jobs.running import RunningJob

from ..exceptions import (GenieAdapterError,
                          GenieHTTPError)

# available adapters
from .genie_2 import Genie2Adapter
from .genie_3 import Genie3Adapter


logger = logging.getLogger('com.netflix.genie.jobs.adapter.adapter')


def get_adapter_for_version(version):
    """Given a version string, return a client object."""

    major_version = str(version).split('.', 1)[0]
    if major_version == '2':
        return Genie2Adapter
    elif major_version == '3':
        return Genie3Adapter
    raise GenieAdapterError("no adapter for version '{}'".format(version))


def execute_job(job):
    """
    Take a job and convert it to a JSON payload based on the job's
    configuration object's Genie version value and execute.

    Returns:
        response: HTTP response.
    """

    version = job._conf.get('genie.version')
    adapter = get_adapter_for_version(version)(conf=job._conf)

    if adapter is not None:
        try:
            adapter.submit_job(job)
        except GenieHTTPError as err:
            if err.response.status_code == 409:
                logger.debug("reattaching to job id '%s'", job.get('job_id'))
            else:
                raise
        except NotImplementedError:
            pass

        return RunningJob(job.get('job_id'), adapter=adapter, conf=job._conf)

    raise GenieAdapterError("no adapter for '{}' to version '{}'" \
        .format(job.__class__.__name__, version))
