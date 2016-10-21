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
import yaml
from pygenie.conf import GenieConf
from pygenie.client import Genie

logging.basicConfig(level=logging.ERROR)

LOGGER = logging.getLogger(__name__)


def load_yaml(yaml_file):
    with open(yaml_file) as _file:
        return yaml.load(_file)

genie_conf = GenieConf()
genie_conf.genie.url = "http://genie:8080"

genie = Genie(genie_conf)

hadoop_application = load_yaml("applications/hadoop271.yml")
hadoop_application_id = genie.create_application(hadoop_application)
LOGGER.info("Created hadoop application with id = [%s]" % hadoop_application_id)

hadoop_command = load_yaml("commands/hadoop271.yml")
hadoop_command_id = genie.create_command(hadoop_command)
LOGGER.info("Created hadoop command with id = [%s]" % hadoop_command_id)

hdfs_command = load_yaml("commands/hdfs271.yml")
hdfs_command_id = genie.create_command(hdfs_command)
LOGGER.info("Created HDFS command with id = [%s]" % hdfs_command_id)

yarn_command = load_yaml("commands/yarn271.yml")
yarn_command_id = genie.create_command(yarn_command)
LOGGER.info("Created yarn command with id = [%s]" % yarn_command_id)

genie.set_application_for_command(hadoop_command_id, [hadoop_application_id])
genie.set_application_for_command(hdfs_command_id, [hadoop_application_id])
genie.set_application_for_command(yarn_command_id, [hadoop_application_id])

prod_cluster = load_yaml("clusters/prod.yml")
prod_cluster_id = genie.create_cluster(prod_cluster)
LOGGER.info("Created prod cluster with id = [%s]" % prod_cluster_id)

test_cluster = load_yaml("clusters/test.yml")
test_cluster_id = genie.create_cluster(test_cluster)
LOGGER.info("Created test cluster with id = [%s]" % test_cluster_id)

genie.set_commands_for_cluster(prod_cluster_id, [hadoop_command_id, hdfs_command_id, yarn_command_id])
genie.set_commands_for_cluster(test_cluster_id, [hadoop_command_id, hdfs_command_id, yarn_command_id])
