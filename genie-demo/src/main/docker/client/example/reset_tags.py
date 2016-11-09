#!/usr/bin/python2.7

# Copyright 2016 Netflix, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

from pygenie.client import Genie
from pygenie.conf import GenieConf

logging.basicConfig(level=logging.WARNING)

LOGGER = logging.getLogger(__name__)

genie_conf = GenieConf()
genie_conf.genie.url = "http://genie:8080"

genie = Genie(genie_conf)

LOGGER.warn("This script will simulate resetting production tags to the prod cluster after issue is resolved")
LOGGER.warn("Beginning process of moving production load back to prod cluster")

# Add the sched:sla tag to the prod cluster
clusters = genie.get_clusters(filters={"name": "GenieDemoProd"})
for cluster in clusters:
    cluster_id = cluster["id"]
    LOGGER.warn("Adding sched:sla tag to the GenieDemoProd cluster with id = [%s]" % cluster_id)
    LOGGER.warn("Tags before = [%s]" % cluster["tags"])
    genie.add_tags_for_cluster(cluster_id, ["sched:sla"])
    LOGGER.warn("Tags after = [%s]" % genie.get_cluster(cluster_id)["tags"])

# Remove the sched:sla tag from the test cluster
clusters = genie.get_clusters(filters={"name": "GenieDemoTest"})
for cluster in clusters:
    cluster_id = cluster["id"]
    LOGGER.warn("Removing sched:sla tag from GenieDemoTest cluster with id = [%s]" % cluster_id)
    LOGGER.warn("Tags before = [%s]" % cluster["tags"])
    genie.remove_tag_for_cluster(cluster_id, "sched:sla")
    LOGGER.warn("Tags after = [%s]" % genie.get_cluster(cluster_id)["tags"])

LOGGER.warn("Finished moving production tags to the prod cluster. All production jobs should now go to prod again")
