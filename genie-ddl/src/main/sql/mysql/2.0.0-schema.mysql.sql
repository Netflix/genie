-- MySQL dump 10.13  Distrib 5.6.26, for osx10.10 (x86_64)
--
-- Host: localhost    Database: genie
-- ------------------------------------------------------
-- Server version	5.6.23-log

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
-- Table structure for table `Application`
--

DROP TABLE IF EXISTS `Application`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `Application` (
  `id` varchar(255) NOT NULL,
  `created` datetime DEFAULT NULL,
  `updated` datetime DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `user` varchar(255) DEFAULT NULL,
  `version` varchar(255) DEFAULT NULL,
  `envPropFile` varchar(255) DEFAULT NULL,
  `status` varchar(20) DEFAULT NULL,
  `entityVersion` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `Application_configs`
--

DROP TABLE IF EXISTS `Application_configs`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `Application_configs` (
  `APPLICATION_ID` varchar(255) DEFAULT NULL,
  `element` varchar(255) DEFAULT NULL,
  KEY `I_PPLCFGS_APPLICATION_ID` (`APPLICATION_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `Application_jars`
--

DROP TABLE IF EXISTS `Application_jars`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `Application_jars` (
  `APPLICATION_ID` varchar(255) DEFAULT NULL,
  `element` varchar(255) DEFAULT NULL,
  KEY `I_PPLCJRS_APPLICATION_ID` (`APPLICATION_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `Application_tags`
--

DROP TABLE IF EXISTS `Application_tags`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `Application_tags` (
  `APPLICATION_ID` varchar(255) DEFAULT NULL,
  `element` varchar(255) DEFAULT NULL,
  KEY `I_PPLCTGS_APPLICATION_ID` (`APPLICATION_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `Cluster`
--

DROP TABLE IF EXISTS `Cluster`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `Cluster` (
  `id` varchar(255) NOT NULL,
  `created` datetime DEFAULT NULL,
  `updated` datetime DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `user` varchar(255) DEFAULT NULL,
  `version` varchar(255) DEFAULT NULL,
  `clusterType` varchar(255) DEFAULT NULL,
  `status` varchar(20) DEFAULT NULL,
  `entityVersion` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `Cluster_Command`
--

DROP TABLE IF EXISTS `Cluster_Command`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `Cluster_Command` (
  `CLUSTERS_ID` varchar(255) DEFAULT NULL,
  `COMMANDS_ID` varchar(255) DEFAULT NULL,
  `commands_ORDER` int(11) DEFAULT NULL,
  KEY `I_CLSTMND_CLUSTERS_ID` (`CLUSTERS_ID`),
  KEY `I_CLSTMND_ELEMENT` (`COMMANDS_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `Cluster_configs`
--

DROP TABLE IF EXISTS `Cluster_configs`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `Cluster_configs` (
  `CLUSTER_ID` varchar(255) DEFAULT NULL,
  `element` varchar(255) DEFAULT NULL,
  KEY `I_CLSTFGS_CLUSTER_ID` (`CLUSTER_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `Cluster_tags`
--

DROP TABLE IF EXISTS `Cluster_tags`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `Cluster_tags` (
  `CLUSTER_ID` varchar(255) DEFAULT NULL,
  `element` varchar(255) DEFAULT NULL,
  KEY `I_CLSTTGS_CLUSTER_ID` (`CLUSTER_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `Command`
--

DROP TABLE IF EXISTS `Command`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `Command` (
  `id` varchar(255) NOT NULL,
  `created` datetime DEFAULT NULL,
  `updated` datetime DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `user` varchar(255) DEFAULT NULL,
  `version` varchar(255) DEFAULT NULL,
  `envPropFile` varchar(255) DEFAULT NULL,
  `executable` varchar(255) DEFAULT NULL,
  `jobType` varchar(255) DEFAULT NULL,
  `status` varchar(20) DEFAULT NULL,
  `entityVersion` int(11) DEFAULT NULL,
  `APPLICATION_ID` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `I_COMMAND_APPLICATION` (`APPLICATION_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `Command_configs`
--

DROP TABLE IF EXISTS `Command_configs`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `Command_configs` (
  `COMMAND_ID` varchar(255) DEFAULT NULL,
  `element` varchar(255) DEFAULT NULL,
  KEY `I_CMMNFGS_COMMAND_ID` (`COMMAND_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `Command_tags`
--

DROP TABLE IF EXISTS `Command_tags`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `Command_tags` (
  `COMMAND_ID` varchar(255) DEFAULT NULL,
  `element` varchar(255) DEFAULT NULL,
  KEY `I_CMMNTGS_COMMAND_ID` (`COMMAND_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `Job`
--

DROP TABLE IF EXISTS `Job`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `Job` (
  `id` varchar(255) NOT NULL,
  `created` datetime DEFAULT NULL,
  `updated` datetime DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `user` varchar(255) DEFAULT NULL,
  `version` varchar(255) DEFAULT NULL,
  `applicationId` varchar(255) DEFAULT NULL,
  `applicationName` varchar(255) DEFAULT NULL,
  `archiveLocation` text,
  `chosenClusterCriteriaString` text,
  `clientHost` varchar(255) DEFAULT NULL,
  `clusterCriteriasString` text,
  `commandArgs` text,
  `commandCriteriaString` text,
  `commandId` varchar(255) DEFAULT NULL,
  `commandName` varchar(255) DEFAULT NULL,
  `description` varchar(255) DEFAULT NULL,
  `disableLogArchival` bit(1) DEFAULT NULL,
  `email` varchar(255) DEFAULT NULL,
  `envPropFile` varchar(255) DEFAULT NULL,
  `executionClusterId` varchar(255) DEFAULT NULL,
  `executionClusterName` varchar(255) DEFAULT NULL,
  `exitCode` int(11) DEFAULT NULL,
  `fileDependencies` text,
  `finished` datetime DEFAULT NULL,
  `forwarded` bit(1) DEFAULT NULL,
  `groupName` varchar(255) DEFAULT NULL,
  `hostName` varchar(255) DEFAULT NULL,
  `killURI` varchar(255) DEFAULT NULL,
  `outputURI` varchar(255) DEFAULT NULL,
  `processHandle` int(11) DEFAULT NULL,
  `started` datetime DEFAULT NULL,
  `status` varchar(20) DEFAULT NULL,
  `statusMsg` varchar(255) DEFAULT NULL,
  `entityVersion` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `updated_index` (`updated`),
  KEY `user_index` (`user`),
  KEY `status_index` (`status`),
  KEY `finished_index` (`finished`),
  KEY `started_index` (`started`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `Job_tags`
--

DROP TABLE IF EXISTS `Job_tags`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `Job_tags` (
  `JOB_ID` varchar(255) DEFAULT NULL,
  `element` varchar(255) DEFAULT NULL,
  KEY `I_JOB_TGS_JOB_ID` (`JOB_ID`),
  KEY `element_index` (`element`)
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

-- Dump completed on 2015-08-10 12:55:31
