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
    >>> import genie
"""


from __future__ import absolute_import, print_function, unicode_literals

import logging
import pkg_resources

from .conf import GenieConf


logger = logging.getLogger('com.netflix.genie')

__version__ = pkg_resources.get_distribution('nflx-genie-client').version
