#!/usr/bin/env python

# Copyright 2016-2020 Netflix, Inc.
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
from pygenie.client import Genie
from pygenie.conf import GenieConf

logging.basicConfig(level=logging.INFO)

LOGGER = logging.getLogger(__name__)


def load_yaml(yaml_file: str):
    with open(yaml_file) as _file:
        return yaml.load(_file, Loader=yaml.FullLoader)


def create_spark_version(genie_client: Genie, version: str, hadoop_app_id: str) -> None:
    human_version: str = ".".join(version)
    app_id: str = genie_client.create_application(load_yaml(f"applications/spark{version}.yml"))
    LOGGER.info(f"Created Spark {human_version} application with id = {app_id}")

    shell_command_id: str = genie_client.create_command(load_yaml(f"commands/sparkShell{version}.yml"))
    LOGGER.info(f"Created Spark Shell {human_version} command with id = {shell_command_id}")

    submit_command_id = genie_client.create_command(load_yaml(f"commands/sparkSubmit{version}.yml"))
    LOGGER.info(f"Created Spark Submit {human_version} command with id = {submit_command_id}")

    genie_client.set_application_for_command(shell_command_id, [hadoop_app_id, app_id])
    LOGGER.info(f"Set applications for Spark Shell {human_version} command to = {','.join([hadoop_app_id, app_id])}")

    genie_client.set_application_for_command(submit_command_id, [hadoop_app_id, app_id])
    LOGGER.info(f"Set applications for Spark Submit {human_version} command to = {','.join([hadoop_app_id, app_id])}")


genie_conf: GenieConf = GenieConf()
genie_conf.genie.url = "http://genie:8080"

genie: Genie = Genie(genie_conf)

hadoop_application_id: str = genie.create_application(load_yaml("applications/hadoop271.yml"))
LOGGER.info(f"Created Hadoop 2.7.1 application with id = {hadoop_application_id}")

hadoop_command_id: str = genie.create_command(load_yaml("commands/hadoop271.yml"))
LOGGER.info(f"Created Hadoop command with id = {hadoop_command_id}")

hdfs_command_id: str = genie.create_command(load_yaml("commands/hdfs271.yml"))
LOGGER.info(f"Created HDFS command with id = {hdfs_command_id}")

yarn_command_id: str = genie.create_command(load_yaml("commands/yarn271.yml"))
LOGGER.info(f"Created Yarn command with id = {yarn_command_id}")

genie.set_application_for_command(hadoop_command_id, [hadoop_application_id])
LOGGER.info(f"Set applications for Hadoop command to = {hadoop_application_id}")

genie.set_application_for_command(hdfs_command_id, [hadoop_application_id])
LOGGER.info(f"Set applications for HDFS command to = {hadoop_application_id}")

genie.set_application_for_command(yarn_command_id, [hadoop_application_id])
LOGGER.info(f"Set applications for Yarn command to = {hadoop_application_id}")

presto_application_id: str = genie.create_application(load_yaml("applications/presto337.yml"))
LOGGER.info(f"Created Presto 337 application with id = {presto_application_id}")

presto_command_id: str = genie.create_command(load_yaml("commands/presto337.yml"))
LOGGER.info(f"Created Presto 337 command with id = {presto_command_id}")

genie.set_application_for_command(presto_command_id, [presto_application_id])
LOGGER.info(f"Set applications for presto command to = {presto_application_id}")

create_spark_version(genie, "201", hadoop_application_id)
create_spark_version(genie, "213", hadoop_application_id)
# create_spark_version(genie, "223", hadoop_application_id)
# create_spark_version(genie, "234", hadoop_application_id)
# create_spark_version(genie, "246", hadoop_application_id)
# create_spark_version(genie, "300", hadoop_application_id)

prod_cluster_id: str = genie.create_cluster(load_yaml("clusters/prod.yml"))
LOGGER.info(f"Created prod yarn cluster with id = {prod_cluster_id}")

test_cluster_id = genie.create_cluster(load_yaml("clusters/test.yml"))
LOGGER.info(f"Created test yarn cluster with id = {test_cluster_id}")

presto_cluster_id = genie.create_cluster(load_yaml("clusters/presto.yml"))
LOGGER.info(f"Created presto cluster with id = {presto_cluster_id}")
