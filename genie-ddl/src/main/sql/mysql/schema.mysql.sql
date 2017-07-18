-- MySQL dump 10.13  Distrib 5.7.17, for macos10.12 (x86_64)
--
-- Host: localhost    Database: genie
-- ------------------------------------------------------
-- Server version	5.6.35

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

CREATE DATABASE  IF NOT EXISTS `genie`;
USE `genie`;

--
-- Table structure for table `application_configs`
--

DROP TABLE IF EXISTS `application_configs`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `application_configs` (
  `application_id` varchar(255) NOT NULL,
  `config` varchar(1024) NOT NULL,
  KEY `application_id` (`application_id`),
  CONSTRAINT `application_configs_ibfk_1` FOREIGN KEY (`application_id`) REFERENCES `applications` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `application_dependencies`
--

DROP TABLE IF EXISTS `application_dependencies`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `application_dependencies` (
  `application_id` varchar(255) NOT NULL,
  `dependency` varchar(1024) NOT NULL,
  KEY `application_id` (`application_id`),
  CONSTRAINT `application_dependencies_ibfk_1` FOREIGN KEY (`application_id`) REFERENCES `applications` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
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
  `description` TEXT,
  `tags` varchar(10000) DEFAULT NULL,
  `setup_file` varchar(1024) DEFAULT NULL,
  `status` varchar(20) NOT NULL DEFAULT 'INACTIVE',
  `type` varchar(255) DEFAULT NULL,
  `entity_version` int(11) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  KEY `APPLICATIONS_NAME_INDEX` (`name`),
  KEY `APPLICATIONS_TAGS_INDEX` (`tags`),
  KEY `APPLICATIONS_STATUS_INDEX` (`status`),
  KEY `APPLICATIONS_TYPE_INDEX` (`type`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `cluster_configs`
--

DROP TABLE IF EXISTS `cluster_configs`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cluster_configs` (
  `cluster_id` varchar(255) NOT NULL,
  `config` varchar(1024) NOT NULL,
  KEY `cluster_id` (`cluster_id`),
  CONSTRAINT `cluster_configs_ibfk_1` FOREIGN KEY (`cluster_id`) REFERENCES `clusters` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `cluster_dependencies`
--

DROP TABLE IF EXISTS `cluster_dependencies`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cluster_dependencies` (
  `cluster_id` varchar(255) NOT NULL,
  `dependency` varchar(1024) NOT NULL,
  KEY `cluster_id` (`cluster_id`),
  CONSTRAINT `cluster_dependencies_ibfk_1` FOREIGN KEY (`cluster_id`) REFERENCES `clusters` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
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
  `description` TEXT,
  `tags` varchar(10000) DEFAULT NULL,
  `setup_file` varchar(1024) DEFAULT NULL,
  `status` varchar(20) NOT NULL DEFAULT 'OUT_OF_SERVICE',
  `entity_version` int(11) DEFAULT '0',
  PRIMARY KEY (`id`),
  KEY `CLUSTERS_NAME_INDEX` (`name`),
  KEY `CLUSTERS_TAG_INDEX` (`tags`),
  KEY `CLUSTERS_STATUS_INDEX` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
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
  KEY `cluster_id` (`cluster_id`),
  KEY `command_id` (`command_id`),
  CONSTRAINT `clusters_commands_ibfk_1` FOREIGN KEY (`cluster_id`) REFERENCES `clusters` (`id`) ON DELETE CASCADE,
  CONSTRAINT `clusters_commands_ibfk_2` FOREIGN KEY (`command_id`) REFERENCES `commands` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `command_configs`
--

DROP TABLE IF EXISTS `command_configs`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `command_configs` (
  `command_id` varchar(255) NOT NULL,
  `config` varchar(1024) NOT NULL,
  KEY `command_id` (`command_id`),
  CONSTRAINT `command_configs_ibfk_1` FOREIGN KEY (`command_id`) REFERENCES `commands` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `command_dependencies`
--

DROP TABLE IF EXISTS `command_dependencies`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `command_dependencies` (
  `command_id` varchar(255) NOT NULL,
  `dependency` varchar(1024) NOT NULL,
  KEY `command_id` (`command_id`),
  CONSTRAINT `command_dependencies_ibfk_1` FOREIGN KEY (`command_id`) REFERENCES `commands` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
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
  `description` TEXT,
  `tags` varchar(10000) DEFAULT NULL,
  `setup_file` varchar(1024) DEFAULT NULL,
  `executable` varchar(255) NOT NULL,
  `check_delay` bigint(20) NOT NULL DEFAULT '10000',
  `memory` int(11) DEFAULT NULL,
  `status` varchar(20) NOT NULL DEFAULT 'INACTIVE',
  `entity_version` int(11) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  KEY `COMMANDS_NAME_INDEX` (`name`),
  KEY `COMMANDS_TAGS_INDEX` (`tags`),
  KEY `COMMANDS_STATUS_INDEX` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
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
  KEY `command_id` (`command_id`),
  KEY `application_id` (`application_id`),
  CONSTRAINT `commands_applications_ibfk_1` FOREIGN KEY (`command_id`) REFERENCES `commands` (`id`) ON DELETE CASCADE,
  CONSTRAINT `commands_applications_ibfk_2` FOREIGN KEY (`application_id`) REFERENCES `applications` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
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
  KEY `id` (`id`),
  KEY `JOB_EXECUTIONS_HOSTNAME_INDEX` (`host_name`),
  KEY `JOB_EXECUTIONS_EXIT_CODE_INDEX` (`exit_code`),
  CONSTRAINT `job_executions_ibfk_1` FOREIGN KEY (`id`) REFERENCES `jobs` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
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
  KEY `id` (`id`),
  CONSTRAINT `job_metadata_ibfk_1` FOREIGN KEY (`id`) REFERENCES `job_requests` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
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
  `description` TEXT,
  `entity_version` int(11) NOT NULL DEFAULT '0',
  `command_args` varchar(10000) NOT NULL,
  `group_name` varchar(255) DEFAULT NULL,
  `setup_file` varchar(1024) DEFAULT NULL,
  `cluster_criterias` TEXT NOT NULL,
  `command_criteria` TEXT NOT NULL,
  `dependencies` TEXT NOT NULL,
  `disable_log_archival` bit(1) NOT NULL DEFAULT b'0',
  `email` varchar(255) DEFAULT NULL,
  `tags` varchar(10000) DEFAULT NULL,
  `cpu` int(11) DEFAULT NULL,
  `memory` int(11) DEFAULT NULL,
  `applications` varchar(2048) NOT NULL DEFAULT '[]',
  `timeout` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `JOB_REQUESTS_CREATED_INDEX` (`created`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
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
  `description` TEXT,
  `cluster_id` varchar(255) DEFAULT NULL,
  `cluster_name` varchar(255) DEFAULT NULL,
  `finished` datetime(3) DEFAULT NULL,
  `started` datetime(3) DEFAULT NULL,
  `status` varchar(20) NOT NULL DEFAULT 'INIT',
  `status_msg` varchar(255) DEFAULT NULL,
  `entity_version` int(11) NOT NULL DEFAULT '0',
  `tags` varchar(10000) DEFAULT NULL,
  KEY `id` (`id`),
  KEY `cluster_id` (`cluster_id`),
  KEY `command_id` (`command_id`),
  KEY `JOBS_NAME_INDEX` (`name`),
  KEY `JOBS_STARTED_INDEX` (`started`),
  KEY `JOBS_FINISHED_INDEX` (`finished`),
  KEY `JOBS_STATUS_INDEX` (`status`),
  KEY `JOBS_USER_INDEX` (`genie_user`),
  KEY `JOBS_CREATED_INDEX` (`created`),
  KEY `JOBS_CLUSTER_NAME_INDEX` (`cluster_name`),
  KEY `JOBS_COMMAND_NAME_INDEX` (`command_name`),
  KEY `JOBS_TAGS_INDEX` (`tags`),
  CONSTRAINT `jobs_ibfk_1` FOREIGN KEY (`id`) REFERENCES `job_requests` (`id`) ON DELETE CASCADE,
  CONSTRAINT `jobs_ibfk_2` FOREIGN KEY (`cluster_id`) REFERENCES `clusters` (`id`),
  CONSTRAINT `jobs_ibfk_3` FOREIGN KEY (`command_id`) REFERENCES `commands` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
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
  KEY `job_id` (`job_id`),
  KEY `application_id` (`application_id`),
  CONSTRAINT `jobs_applications_ibfk_1` FOREIGN KEY (`job_id`) REFERENCES `jobs` (`id`) ON DELETE CASCADE,
  CONSTRAINT `jobs_applications_ibfk_2` FOREIGN KEY (`application_id`) REFERENCES `applications` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2017-06-30 10:15:56
