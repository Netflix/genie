/*
 *
 *  Copyright 2017 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `application_configs`
--

DROP TABLE IF EXISTS `application_configs`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `application_configs` (
  `application_id` varchar(255) NOT NULL,
  `config` varchar(2048) NOT NULL,
  PRIMARY KEY (`application_id`,`config`),
  KEY `APPLICATION_CONFIGS_APPLICATION_ID_INDEX` (`application_id`),
  CONSTRAINT `APPLICATION_CONFIGS_APPLICATION_ID_FK` FOREIGN KEY (`application_id`) REFERENCES `applications` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1 ROW_FORMAT=DYNAMIC;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `application_dependencies`
--

DROP TABLE IF EXISTS `application_dependencies`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `application_dependencies` (
  `application_id` varchar(255) NOT NULL,
  `dependency` varchar(2048) NOT NULL,
  PRIMARY KEY (`application_id`,`dependency`),
  KEY `APPLICATION_DEPENDENCIES_APPLICATION_ID_INDEX` (`application_id`),
  CONSTRAINT `APPLICATION_DEPENDENCIES_APPLICATION_ID_FK` FOREIGN KEY (`application_id`) REFERENCES `applications` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1 ROW_FORMAT=DYNAMIC;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `applications`
--

DROP TABLE IF EXISTS `applications`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `applications` (
  `id` varchar(255) NOT NULL,
  `created` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  `name` varchar(255) NOT NULL,
  `genie_user` varchar(255) NOT NULL,
  `version` varchar(255) NOT NULL,
  `description` text,
  `tags` varchar(10000) DEFAULT NULL,
  `setup_file` varchar(1024) DEFAULT NULL,
  `status` varchar(20) NOT NULL DEFAULT 'INACTIVE',
  `type` varchar(255) DEFAULT NULL,
  `entity_version` int(11) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  KEY `APPLICATIONS_NAME_INDEX` (`name`),
  KEY `APPLICATIONS_STATUS_INDEX` (`status`),
  KEY `APPLICATIONS_TAGS_INDEX` (`tags`(3072)),
  KEY `APPLICATIONS_TYPE_INDEX` (`type`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 ROW_FORMAT=DYNAMIC;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `cluster_configs`
--

DROP TABLE IF EXISTS `cluster_configs`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cluster_configs` (
  `cluster_id` varchar(255) NOT NULL,
  `config` varchar(2048) NOT NULL,
  PRIMARY KEY (`cluster_id`,`config`),
  KEY `CLUSTER_CONFIGS_CLUSTER_ID_INDEX` (`cluster_id`),
  CONSTRAINT `CLUSTER_CONFIGS_CLUSTER_ID_FK` FOREIGN KEY (`cluster_id`) REFERENCES `clusters` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1 ROW_FORMAT=DYNAMIC;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `cluster_dependencies`
--

DROP TABLE IF EXISTS `cluster_dependencies`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cluster_dependencies` (
  `cluster_id` varchar(255) NOT NULL,
  `dependency` varchar(2048) NOT NULL,
  PRIMARY KEY (`cluster_id`,`dependency`),
  KEY `CLUSTER_DEPENDENCIES_CLUSTER_ID_INDEX` (`cluster_id`),
  CONSTRAINT `CLUSTER_DEPENDENCIES_CLUSTER_ID_FK` FOREIGN KEY (`cluster_id`) REFERENCES `clusters` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1 ROW_FORMAT=DYNAMIC;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `clusters`
--

DROP TABLE IF EXISTS `clusters`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `clusters` (
  `id` varchar(255) NOT NULL,
  `created` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  `name` varchar(255) NOT NULL,
  `genie_user` varchar(255) NOT NULL,
  `version` varchar(255) NOT NULL,
  `description` text,
  `tags` varchar(10000) DEFAULT NULL,
  `setup_file` varchar(1024) DEFAULT NULL,
  `status` varchar(20) NOT NULL DEFAULT 'OUT_OF_SERVICE',
  `entity_version` int(11) DEFAULT '0',
  PRIMARY KEY (`id`),
  KEY `CLUSTERS_NAME_INDEX` (`name`),
  KEY `CLUSTERS_STATUS_INDEX` (`status`),
  KEY `CLUSTERS_TAGS_INDEX` (`tags`(3072))
) ENGINE=InnoDB DEFAULT CHARSET=latin1 ROW_FORMAT=DYNAMIC;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `clusters_commands`
--

DROP TABLE IF EXISTS `clusters_commands`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `clusters_commands` (
  `cluster_id` varchar(255) NOT NULL,
  `command_id` varchar(255) NOT NULL,
  `command_order` int(11) NOT NULL,
  PRIMARY KEY (`cluster_id`,`command_id`,`command_order`),
  KEY `CLUSTERS_COMMANDS_CLUSTER_ID_INDEX` (`cluster_id`),
  KEY `CLUSTERS_COMMANDS_COMMAND_ID_INDEX` (`command_id`),
  CONSTRAINT `CLUSTERS_COMMANDS_CLUSTER_ID_FK` FOREIGN KEY (`cluster_id`) REFERENCES `clusters` (`id`) ON DELETE CASCADE,
  CONSTRAINT `CLUSTERS_COMMANDS_COMMAND_ID_FK` FOREIGN KEY (`command_id`) REFERENCES `commands` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 ROW_FORMAT=DYNAMIC;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `command_configs`
--

DROP TABLE IF EXISTS `command_configs`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `command_configs` (
  `command_id` varchar(255) NOT NULL,
  `config` varchar(2048) NOT NULL,
  PRIMARY KEY (`command_id`,`config`),
  KEY `COMMAND_CONFIGS_COMMAND_ID_INDEX` (`command_id`),
  CONSTRAINT `COMMAND_CONFIGS_COMMAND_ID_FK` FOREIGN KEY (`command_id`) REFERENCES `commands` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1 ROW_FORMAT=DYNAMIC;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `command_dependencies`
--

DROP TABLE IF EXISTS `command_dependencies`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `command_dependencies` (
  `command_id` varchar(255) NOT NULL,
  `dependency` varchar(2048) NOT NULL,
  PRIMARY KEY (`command_id`,`dependency`),
  KEY `COMMAND_DEPENDENCIES_COMMAND_ID_INDEX` (`command_id`),
  CONSTRAINT `COMMAND_DEPENDENCIES_COMMAND_ID_FK` FOREIGN KEY (`command_id`) REFERENCES `commands` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1 ROW_FORMAT=DYNAMIC;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `commands`
--

DROP TABLE IF EXISTS `commands`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `commands` (
  `id` varchar(255) NOT NULL,
  `created` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  `name` varchar(255) NOT NULL,
  `genie_user` varchar(255) NOT NULL,
  `version` varchar(255) NOT NULL,
  `description` text,
  `tags` varchar(10000) DEFAULT NULL,
  `setup_file` varchar(1024) DEFAULT NULL,
  `executable` varchar(255) NOT NULL,
  `check_delay` bigint(20) NOT NULL DEFAULT '10000',
  `memory` int(11) DEFAULT NULL,
  `status` varchar(20) NOT NULL DEFAULT 'INACTIVE',
  `entity_version` int(11) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  KEY `COMMANDS_NAME_INDEX` (`name`),
  KEY `COMMANDS_STATUS_INDEX` (`status`),
  KEY `COMMANDS_TAGS_INDEX` (`tags`(3072))
) ENGINE=InnoDB DEFAULT CHARSET=latin1 ROW_FORMAT=DYNAMIC;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `commands_applications`
--

DROP TABLE IF EXISTS `commands_applications`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `commands_applications` (
  `command_id` varchar(255) NOT NULL,
  `application_id` varchar(255) NOT NULL,
  `application_order` int(11) NOT NULL,
  PRIMARY KEY (`command_id`,`application_id`,`application_order`),
  KEY `COMMANDS_APPLICATIONS_APPLICATION_ID_INDEX` (`application_id`),
  KEY `COMMANDS_APPLICATIONS_COMMAND_ID_INDEX` (`command_id`),
  CONSTRAINT `COMMANDS_APPLICATIONS_APPLICATION_ID_FK` FOREIGN KEY (`application_id`) REFERENCES `applications` (`id`),
  CONSTRAINT `COMMANDS_APPLICATIONS_COMMAND_ID_FK` FOREIGN KEY (`command_id`) REFERENCES `commands` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1 ROW_FORMAT=DYNAMIC;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `job_executions`
--

DROP TABLE IF EXISTS `job_executions`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `job_executions` (
  `id` varchar(255) NOT NULL,
  `created` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  `entity_version` int(11) NOT NULL DEFAULT '0',
  `host_name` varchar(255) NOT NULL,
  `process_id` int(11) DEFAULT NULL,
  `exit_code` int(11) DEFAULT NULL,
  `check_delay` bigint(20) DEFAULT NULL,
  `timeout` datetime(3) DEFAULT NULL,
  `memory` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `JOB_EXECUTIONS_HOSTNAME_INDEX` (`host_name`),
  CONSTRAINT `JOB_EXECUTIONS_ID_FK` FOREIGN KEY (`id`) REFERENCES `jobs` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1 ROW_FORMAT=DYNAMIC;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `job_metadata`
--

DROP TABLE IF EXISTS `job_metadata`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `job_metadata` (
  `id` varchar(255) NOT NULL,
  `created` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  `entity_version` int(11) NOT NULL DEFAULT '0',
  `client_host` varchar(255) DEFAULT NULL,
  `user_agent` varchar(2048) DEFAULT NULL,
  `num_attachments` int(11) DEFAULT NULL,
  `total_size_of_attachments` bigint(20) DEFAULT NULL,
  `std_out_size` bigint(20) DEFAULT NULL,
  `std_err_size` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  CONSTRAINT `JOB_METADATA_ID_FK` FOREIGN KEY (`id`) REFERENCES `jobs` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1 ROW_FORMAT=DYNAMIC;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `job_requests`
--

DROP TABLE IF EXISTS `job_requests`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `job_requests` (
  `id` varchar(255) NOT NULL,
  `created` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  `name` varchar(255) NOT NULL,
  `genie_user` varchar(255) NOT NULL,
  `version` varchar(255) NOT NULL,
  `description` text,
  `entity_version` int(11) NOT NULL DEFAULT '0',
  `command_args` varchar(10000) NOT NULL,
  `group_name` varchar(255) DEFAULT NULL,
  `setup_file` varchar(1024) DEFAULT NULL,
  `cluster_criterias` text NOT NULL,
  `command_criteria` text NOT NULL,
  `dependencies` text NOT NULL,
  `disable_log_archival` bit(1) NOT NULL DEFAULT b'0',
  `email` varchar(255) DEFAULT NULL,
  `tags` varchar(10000) DEFAULT NULL,
  `cpu` int(11) DEFAULT NULL,
  `memory` int(11) DEFAULT NULL,
  `applications` varchar(2048) NOT NULL DEFAULT '[]',
  `timeout` int(11) DEFAULT NULL,
  `configs` text NOT NULL,
  PRIMARY KEY (`id`),
  KEY `JOB_REQUESTS_CREATED_INDEX` (`created`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 ROW_FORMAT=DYNAMIC;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jobs`
--

DROP TABLE IF EXISTS `jobs`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jobs` (
  `id` varchar(255) NOT NULL,
  `created` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  `name` varchar(255) NOT NULL,
  `genie_user` varchar(255) NOT NULL,
  `version` varchar(255) NOT NULL,
  `archive_location` varchar(1024) DEFAULT NULL,
  `command_args` varchar(10000) NOT NULL,
  `command_id` varchar(255) DEFAULT NULL,
  `command_name` varchar(255) DEFAULT NULL,
  `description` text,
  `cluster_id` varchar(255) DEFAULT NULL,
  `cluster_name` varchar(255) DEFAULT NULL,
  `finished` datetime(3) DEFAULT NULL,
  `started` datetime(3) DEFAULT NULL,
  `status` varchar(20) NOT NULL DEFAULT 'INIT',
  `status_msg` varchar(255) DEFAULT NULL,
  `entity_version` int(11) NOT NULL DEFAULT '0',
  `tags` varchar(10000) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `JOBS_CLUSTER_ID_INDEX` (`cluster_id`),
  KEY `JOBS_CLUSTER_NAME_INDEX` (`cluster_name`),
  KEY `JOBS_COMMAND_ID_INDEX` (`command_id`),
  KEY `JOBS_COMMAND_NAME_INDEX` (`command_name`),
  KEY `JOBS_CREATED_INDEX` (`created`),
  KEY `JOBS_FINISHED_INDEX` (`finished`),
  KEY `JOBS_NAME_INDEX` (`name`),
  KEY `JOBS_STARTED_INDEX` (`started`),
  KEY `JOBS_STATUS_INDEX` (`status`),
  KEY `JOBS_TAGS_INDEX` (`tags`(3072)),
  KEY `JOBS_USER_INDEX` (`genie_user`),
  CONSTRAINT `JOBS_CLUSTER_ID_FK` FOREIGN KEY (`cluster_id`) REFERENCES `clusters` (`id`),
  CONSTRAINT `JOBS_COMMAND_ID_FK` FOREIGN KEY (`command_id`) REFERENCES `commands` (`id`),
  CONSTRAINT `JOBS_ID_FK` FOREIGN KEY (`id`) REFERENCES `job_requests` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1 ROW_FORMAT=DYNAMIC;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `jobs_applications`
--

DROP TABLE IF EXISTS `jobs_applications`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `jobs_applications` (
  `job_id` varchar(255) NOT NULL,
  `application_id` varchar(255) NOT NULL,
  `application_order` int(11) NOT NULL,
  PRIMARY KEY (`job_id`,`application_id`,`application_order`),
  KEY `JOBS_APPLICATIONS_APPLICATION_ID_INDEX` (`application_id`),
  KEY `JOBS_APPLICATIONS_JOB_ID_INDEX` (`job_id`),
  CONSTRAINT `JOBS_APPLICATIONS_APPLICATION_ID_FK` FOREIGN KEY (`application_id`) REFERENCES `applications` (`id`),
  CONSTRAINT `JOBS_APPLICATIONS_JOB_ID_FK` FOREIGN KEY (`job_id`) REFERENCES `jobs` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1 ROW_FORMAT=DYNAMIC;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed
