"""
   _______  _______  _   ___  ___   _______
  |       ||       || |/    ||   | |       |
  |    ___||    ___||       ||   | |    ___|
  |   | __ |   |___ |  /|   ||   | |   |___
  |   ||  ||    ___|| | |   ||   | |    ___|
  |   |_| ||   |___ | | |   ||   | |   |___
  |_______||_______||_| |___||___| |_______|


Genie Python Client.

Genie is a federated job execution engine developed by Netflix. Genie provides
REST-ful APIs to run a variety of big data jobs like Hadoop, Pig, Hive, Spark,
Presto, Sqoop and more. It also provides APIs for managing many distributed
processing cluster configurations and the commands and applications which run on
them.


Run Job Example:
    >>> import pygenie

"""


from __future__ import absolute_import, division, print_function, unicode_literals

import logging
import pkg_resources

__version__ = pkg_resources.get_distribution('nflx-genie-client').version

from .jobs.utils import (generate_job_id,
                         reattach_job)

# get around circular imports
# adapter imports jobs, jobs need to import execute_job
# adapter imports RunningJob, RunningJob needs to import get_adapter_for_version
from .adapter.adapter import (execute_job,
                              get_adapter_for_version)
from .jobs import core
from .jobs import running

core.execute_job = execute_job
running.get_adapter_for_version = get_adapter_for_version
