BEGIN;
SELECT CURRENT_TIMESTAMP AS '', 'Fixing https://github.com/Netflix/genie/issues/148' AS '';
-- Fix referential integrity problem described in https://github.com/Netflix/genie/issues/148
DELETE FROM `Application_configs` WHERE `APPLICATION_ID` NOT IN (SELECT `id` from `Application`);
DELETE FROM `Application_jars` WHERE `APPLICATION_ID` NOT IN (SELECT `id` from `Application`);
DELETE FROM `Application_tags` WHERE `APPLICATION_ID` NOT IN (SELECT `id` from `Application`);
DELETE FROM `Cluster_Command` WHERE `CLUSTERS_ID` NOT IN (SELECT `id` FROM `Cluster`);
DELETE FROM `Cluster_tags` WHERE `CLUSTER_ID` NOT IN (SELECT `id` from `Cluster`);
DELETE FROM `Cluster_configs` WHERE `CLUSTER_ID` NOT IN (SELECT `id` FROM `Cluster`);
DELETE FROM `Cluster_Command` WHERE `COMMANDS_ID` NOT IN (SELECT `id` FROM `Command`);
DELETE FROM `Command_tags` WHERE `COMMAND_ID` NOT IN (SELECT `id` from `Command`);
DELETE FROM `Command_configs` WHERE `COMMAND_ID` NOT IN (SELECT `id` FROM `Command`);
DELETE FROM `Job_tags` WHERE `JOB_ID` NOT IN (SELECT `id` FROM `Job`);
SELECT CURRENT_TIMESTAMP AS '', 'Finished applying fix for https://github.com/Netflix/genie/issues/148' AS '';
COMMIT;

BEGIN;
SELECT CURRENT_TIMESTAMP AS '', 'Beginning upgrade of Genie schema from version 2.0.0 to 3.0.0' AS '';

-- Rename the tables to be a little bit nicer
SELECT CURRENT_TIMESTAMP AS '', 'Renaming all the tables to be more friendly...' AS '';
RENAME TABLE `Application` TO `applications`;
RENAME TABLE `Application_configs` TO `application_configs_tmp`;
RENAME TABLE `application_configs_tmp` TO `application_configs`;
RENAME TABLE `Application_jars` TO `application_dependencies`;
RENAME TABLE `Application_tags` TO `application_tags_tmp`;
RENAME TABLE `application_tags_tmp` TO `application_tags`;
RENAME TABLE `Cluster` TO `clusters`;
RENAME TABLE `Cluster_Command` TO `clusters_commands`;
RENAME TABLE `Cluster_configs` TO `cluster_configs_tmp`;
RENAME TABLE `cluster_configs_tmp` TO `cluster_configs`;
RENAME TABLE `Cluster_tags` TO `cluster_tags_tmp`;
RENAME TABLE `cluster_tags_tmp` TO `cluster_tags`;
RENAME TABLE `Command` TO `commands`;
RENAME TABLE `Command_configs` TO `command_configs_tmp`;
RENAME TABLE `command_configs_tmp` TO `command_configs`;
RENAME TABLE `Command_tags` TO `command_tags_tmp`;
RENAME TABLE `command_tags_tmp` TO `command_tags`;
RENAME TABLE `Job` TO `jobs`;
RENAME TABLE `Job_tags` TO `Job_tags_tmp`;
RENAME TABLE `Job_tags_tmp` TO `job_tags`;
SELECT CURRENT_TIMESTAMP AS '', 'Successfully renamed all tables' AS '';

-- Create a new Many to Many table for commands to applications
SELECT CURRENT_TIMESTAMP AS '', 'Creating commands_applications table...' AS '';
CREATE TABLE `commands_applications` (
  `command_id` VARCHAR(255) NOT NULL,
  `application_id` VARCHAR(255) NOT NULL,
  `application_order` INT(11) NOT NULL,
  FOREIGN KEY (`command_id`) REFERENCES `commands` (`id`) ON DELETE CASCADE,
  FOREIGN KEY (`application_id`) REFERENCES `applications` (`id`) ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
SELECT CURRENT_TIMESTAMP AS '', 'Successfully created commands_applications table.' AS '';

-- Save the values of the current command to application relationship for the new table
SELECT CURRENT_TIMESTAMP AS '', 'Adding existing applications to commands...' AS '';
INSERT INTO `commands_applications` (`command_id`, `application_id`, `application_order`)
  SELECT `id`, `APPLICATION_ID`, 0 FROM `commands` WHERE `APPLICATION_ID` IS NOT NULL;
SELECT CURRENT_TIMESTAMP AS '', 'Successfully added existing applications to commands.' AS '';

-- Create a new Many to Many table for commands to applications
SELECT CURRENT_TIMESTAMP AS '', 'Creating jobs_applications table...' AS '';
CREATE TABLE `jobs_applications` (
  `job_id` VARCHAR(255) NOT NULL,
  `application_id` VARCHAR(255) NOT NULL,
  `application_order` INT(11) NOT NULL,
  FOREIGN KEY (`job_id`) REFERENCES `jobs` (`id`) ON DELETE CASCADE,
  FOREIGN KEY (`application_id`) REFERENCES `applications` (`id`) ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
SELECT CURRENT_TIMESTAMP AS '', 'Successfully created jobs_applications table.' AS '';

-- Save the values of the current job to application relationship for the new table
SELECT CURRENT_TIMESTAMP AS '', 'Adding existing applications to jobs...' AS '';
INSERT INTO `jobs_applications` (`job_id`, `application_id`, `application_order`)
  SELECT `id`, `applicationId`, 0 FROM `jobs` WHERE `applicationId` IS NOT NULL AND `applicationId` IN (
    SELECT `id` FROM `applications`
  );
SELECT CURRENT_TIMESTAMP AS '', 'Successfully added existing applications to jobs.' AS '';

-- Modify the applications and the associated children tables
SELECT CURRENT_TIMESTAMP AS '', 'Altering the applications table for 3.0...' AS '';
ALTER TABLE `applications`
  MODIFY `created` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  MODIFY `updated` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  MODIFY `name` VARCHAR(255) NOT NULL,
  MODIFY `user` VARCHAR(255) NOT NULL,
  MODIFY `version` VARCHAR(255) NOT NULL,
  ADD COLUMN `description` TEXT DEFAULT NULL AFTER `version`,
  ADD COLUMN `tags` VARCHAR(2048) DEFAULT NULL AFTER `description`,
  MODIFY `status` VARCHAR(20) NOT NULL DEFAULT 'INACTIVE',
  ADD COLUMN `type` VARCHAR(255) DEFAULT NULL AFTER `status`,
  CHANGE `envPropFile` `setup_file` VARCHAR(1024) DEFAULT NULL,
  CHANGE `entityVersion` `entity_version` INT(11) NOT NULL DEFAULT 0,
  ADD INDEX `APPLICATIONS_NAME_INDEX` (`name`),
  ADD INDEX `APPLICATIONS_TAGS_INDEX` (`tags`),
  ADD INDEX `APPLICATIONS_STATUS_INDEX` (`status`),
  ADD INDEX `APPLICATIONS_TYPE_INDEX` (`type`);
SELECT CURRENT_TIMESTAMP AS '', 'Successfully updated the applications table.' AS '';

SELECT CURRENT_TIMESTAMP AS '', 'De-normalizing application tags for 3.0...' AS '';
UPDATE `applications` AS `a` SET `a`.`tags` =
(
  SELECT GROUP_CONCAT(DISTINCT `t`.`element` ORDER BY `t`.`element` SEPARATOR '|')
  FROM `application_tags` AS `t`
  WHERE `a`.`id` = `t`.`APPLICATION_ID`
  GROUP BY `t`.`APPLICATION_ID`
);
SELECT CURRENT_TIMESTAMP AS '', 'Finished de-normalizing application tags for 3.0.' AS '';

SELECT CURRENT_TIMESTAMP AS '', 'Altering the application_configs table for 3.0...' AS '';
ALTER TABLE `application_configs` DROP KEY `I_PPLCFGS_APPLICATION_ID`;
ALTER TABLE `application_configs`
  CHANGE `APPLICATION_ID` `application_id` VARCHAR(255) NOT NULL,
  CHANGE `element` `config` VARCHAR(1024) NOT NULL,
  ADD FOREIGN KEY (`application_id`) REFERENCES `applications` (`id`) ON DELETE CASCADE;
SELECT CURRENT_TIMESTAMP AS '', 'Successfully updated the application_configs table.' AS '';

SELECT CURRENT_TIMESTAMP AS '', 'Altering the application_dependencies table for 3.0...' AS '';
ALTER TABLE `application_dependencies` DROP KEY `I_PPLCJRS_APPLICATION_ID`;
ALTER TABLE `application_dependencies`
  CHANGE `APPLICATION_ID` `application_id` VARCHAR(255) NOT NULL,
  CHANGE `element` `dependency` VARCHAR(1024) NOT NULL,
  ADD FOREIGN KEY (`application_id`) REFERENCES `applications` (`id`) ON DELETE CASCADE;
SELECT CURRENT_TIMESTAMP AS '', 'Successfully updated the application_dependencies table.' AS '';

SELECT CURRENT_TIMESTAMP AS '', 'Dropping the application_tags table from 3.0...' AS '';
DROP TABLE `application_tags`;
SELECT CURRENT_TIMESTAMP AS '', 'Successfully dropped the application_tags table.' AS '';

-- Modify the clusters and associated children tables
SELECT CURRENT_TIMESTAMP AS '', 'Updating the clusters table for 3.0...' AS '';
ALTER TABLE `clusters`
  MODIFY `created` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  MODIFY `updated` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  MODIFY `name` VARCHAR(255) NOT NULL,
  MODIFY `user` VARCHAR(255) NOT NULL,
  MODIFY `version` VARCHAR(255) NOT NULL,
  ADD COLUMN `description` TEXT DEFAULT NULL AFTER `version`,
  ADD COLUMN `tags` VARCHAR(2048) DEFAULT NULL AFTER `description`,
  ADD COLUMN `setup_file` VARCHAR(1024) DEFAULT NULL AFTER `tags`,
  MODIFY `status` VARCHAR(20) NOT NULL DEFAULT 'OUT_OF_SERVICE',
  CHANGE `entityVersion` `entity_version` INT(11) DEFAULT 0,
  DROP `clusterType`,
  ADD INDEX `CLUSTERS_NAME_INDEX` (`name`),
  ADD INDEX `CLUSTERS_TAG_INDEX` (`tags`),
  ADD INDEX `CLUSTERS_STATUS_INDEX` (`status`);
SELECT CURRENT_TIMESTAMP AS '', 'Successfully updated the clusters table.' AS '';

SELECT CURRENT_TIMESTAMP AS '', 'De-normalizing cluster tags for 3.0...' AS '';
UPDATE `clusters` AS `c` SET `c`.`tags` =
(
  SELECT GROUP_CONCAT(DISTINCT `t`.`element` ORDER BY `t`.`element` SEPARATOR '|')
  FROM `cluster_tags` AS `t`
  WHERE `c`.`id` = `t`.`CLUSTER_ID`
  GROUP BY `t`.`CLUSTER_ID`
);
SELECT CURRENT_TIMESTAMP AS '', 'Finished de-normalizing cluster tags for 3.0.' AS '';

SELECT CURRENT_TIMESTAMP AS '', 'Updating the clusters_commands table for 3.0...' AS '';
ALTER TABLE `clusters_commands`
  DROP KEY `I_CLSTMND_CLUSTERS_ID`,
  DROP KEY `I_CLSTMND_ELEMENT`;
ALTER TABLE `clusters_commands`
  CHANGE `CLUSTERS_ID` `cluster_id` VARCHAR(255) NOT NULL,
  CHANGE `COMMANDS_ID` `command_id` VARCHAR(255) NOT NULL,
  CHANGE `commands_ORDER` `command_order` INT(11) NOT NULL,
  ADD FOREIGN KEY (`cluster_id`) REFERENCES `clusters` (`id`) ON DELETE CASCADE,
  ADD FOREIGN KEY (`command_id`) REFERENCES `commands` (`id`) ON DELETE RESTRICT;
SELECT CURRENT_TIMESTAMP AS '', 'Successfully updated the clusters_commands table.' AS '';

SELECT CURRENT_TIMESTAMP AS '', 'Updating the cluster_configs table for 3.0...' AS '';
ALTER TABLE `cluster_configs` DROP KEY `I_CLSTFGS_CLUSTER_ID`;
ALTER TABLE `cluster_configs`
  CHANGE `CLUSTER_ID` `cluster_id` VARCHAR(255) NOT NULL,
  CHANGE `element` `config` VARCHAR(1024) NOT NULL,
  ADD FOREIGN KEY (`cluster_id`) REFERENCES `clusters` (`id`) ON DELETE CASCADE;
SELECT CURRENT_TIMESTAMP AS '', 'Successfully updated the cluster_configs table.' AS '';

SELECT CURRENT_TIMESTAMP AS '', 'Dropping the cluster_tags table for 3.0...' AS '';
DROP TABLE `cluster_tags`;
SELECT CURRENT_TIMESTAMP AS '', 'Dropped the cluster_tags table.' AS '';

-- Modify the commands and associated children tables
SELECT CURRENT_TIMESTAMP AS '', 'Updating the commands table for 3.0...' AS '';
ALTER TABLE `commands`
  MODIFY `created` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  MODIFY `updated` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  MODIFY `name` VARCHAR(255) NOT NULL,
  MODIFY `user` VARCHAR(255) NOT NULL,
  MODIFY `version` VARCHAR(255) NOT NULL,
  ADD COLUMN `description` TEXT DEFAULT NULL AFTER `version`,
  ADD COLUMN `tags` VARCHAR(2048) DEFAULT NULL AFTER `description`,
  ADD COLUMN `check_delay` BIGINT NOT NULL DEFAULT 10000 AFTER `executable`,
  MODIFY `status` VARCHAR(20) NOT NULL DEFAULT 'INACTIVE',
  MODIFY `executable` VARCHAR(255) NOT NULL,
  CHANGE `envPropFile` `setup_file` VARCHAR(1024) DEFAULT NULL,
  CHANGE `entityVersion` `entity_version` INT(11) NOT NULL DEFAULT 0,
  DROP `APPLICATION_ID`,
  DROP `jobType`,
  ADD INDEX `COMMANDS_NAME_INDEX` (`name`),
  ADD INDEX `COMMANDS_TAGS_INDEX` (`tags`),
  ADD INDEX `COMMANDS_STATUS_INDEX` (`status`);
SELECT CURRENT_TIMESTAMP AS '', 'Successfully updated the commands table.' AS '';

SELECT CURRENT_TIMESTAMP AS '', 'De-normalizing command tags for 3.0...' AS '';
UPDATE `commands` AS `c` SET `c`.`tags` =
(
  SELECT GROUP_CONCAT(DISTINCT `t`.`element` ORDER BY `t`.`element` SEPARATOR '|')
  FROM `command_tags` AS `t`
  WHERE `c`.`id` = `t`.`COMMAND_ID`
  GROUP BY `t`.`COMMAND_ID`
);
SELECT CURRENT_TIMESTAMP AS '', 'Finished de-normalizing command tags for 3.0.' AS '';

SELECT CURRENT_TIMESTAMP AS '', 'Updating the command_configs table for 3.0...' AS '';
ALTER TABLE `command_configs` DROP KEY `I_CMMNFGS_COMMAND_ID`;
ALTER TABLE `command_configs`
  CHANGE `COMMAND_ID` `command_id` VARCHAR(255) NOT NULL,
  CHANGE `element` `config` VARCHAR(1024) NOT NULL,
  ADD FOREIGN KEY (`command_id`) REFERENCES `commands` (`id`) ON DELETE CASCADE;
SELECT CURRENT_TIMESTAMP AS '', 'Successfully updated the command_configs table.' AS '';

SELECT CURRENT_TIMESTAMP AS '', 'Dropping the command_tags table for 3.0...' AS '';
DROP TABLE `command_tags`;
SELECT CURRENT_TIMESTAMP AS '', 'Successfully dropped the command_tags table.' AS '';

SELECT CURRENT_TIMESTAMP AS '', 'Creating the job_requests table...' AS '';
CREATE TABLE `job_requests` (
  `id` VARCHAR(255) NOT NULL,
  `created` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `name` VARCHAR(255) NOT NULL,
  `user` VARCHAR(255) NOT NULL,
  `version` VARCHAR(255) NOT NULL,
  `description` TEXT DEFAULT NULL,
  `entity_version` INT(11) NOT NULL DEFAULT 0,
  `command_args` TEXT NOT NULL,
  `group_name` VARCHAR(255) DEFAULT NULL,
  `setup_file` VARCHAR(1024) DEFAULT NULL,
  `cluster_criterias` VARCHAR(2048) NOT NULL DEFAULT '[]',
  `command_criteria` VARCHAR(1024) NOT NULL DEFAULT '[]',
  `dependencies` TEXT DEFAULT NULL,
  `disable_log_archival` BIT(1) NOT NULL DEFAULT 0,
  `email` VARCHAR(255) DEFAULT NULL,
  `tags` VARCHAR(2048) DEFAULT NULL,
  `cpu` INT(11) NOT NULL DEFAULT 1,
  `memory` INT(11) NOT NULL DEFAULT 1560,
  `client_host` VARCHAR(255) DEFAULT NULL,
  `applications` VARCHAR(2048) NOT NULL DEFAULT '[]',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
SELECT CURRENT_TIMESTAMP AS '', 'Successfully created the job_requests table.' AS '';

SELECT CURRENT_TIMESTAMP AS '', 'Inserting values into job_requests table from the jobs table...' AS '';
SET GROUP_CONCAT_MAX_LEN = 1024;
INSERT INTO `job_requests` (
  `id`,
  `created`,
  `updated`,
  `name`,
  `user`,
  `version`,
  `description`,
  `entity_version`,
  `command_args`,
  `group_name`,
  `setup_file`,
  `cluster_criterias`,
  `command_criteria`,
  `dependencies`,
  `disable_log_archival`,
  `email`,
  `tags`,
  `cpu`,
  `memory`,
  `client_host`
) SELECT
    `j`.`id`,
    `j`.`created`,
    `j`.`updated`,
    `j`.`name`,
    `j`.`user`,
    `j`.`version`,
    `j`.`description`,
    1,
    `j`.`commandArgs`,
    `j`.`groupName`,
    `j`.`envPropFile`,
    `j`.`clusterCriteriasString`,
    `j`.`commandCriteriaString`,
    `j`.`fileDependencies`,
    `j`.`disableLogArchival`,
    `j`.`email`,
    (
      SELECT GROUP_CONCAT(DISTINCT `t`.`element` ORDER BY `t`.`element` SEPARATOR '|')
      FROM `job_tags` `t`
      WHERE `j`.`id` = `t`.`JOB_ID`
      GROUP BY `t`.`JOB_ID`
    ),
    1,
    1560,
    `j`.`clientHost`
  FROM `jobs` `j`;
SELECT CURRENT_TIMESTAMP AS '', 'Successfully inserted values into job_requests table.' AS '';

SELECT CURRENT_TIMESTAMP AS '', 'Attempting to convert command_criteria to JSON in job_requests table...' AS '';
UPDATE `job_requests`
  SET `command_criteria` = RPAD(`command_criteria`, LENGTH(`command_criteria`) + 2, '"]')
  WHERE `command_criteria` IS NOT NULL;
UPDATE `job_requests`
  SET `command_criteria` = LPAD(`command_criteria`, LENGTH(`command_criteria`) + 2, '["')
  WHERE `command_criteria` IS NOT NULL;
UPDATE `job_requests`
  SET `command_criteria` = REPLACE(`command_criteria`, ',', '","')
  WHERE `command_criteria` IS NOT NULL;
UPDATE `job_requests`
  SET `command_criteria` = '[]'
  WHERE `command_criteria` = '[""]' OR `command_criteria` IS NULL;
SELECT CURRENT_TIMESTAMP AS '', 'Successfully converted command_criteria to JSON in job_requests table.' AS '';

SELECT CURRENT_TIMESTAMP AS '', 'Attempting to convert cluster_criterias to JSON in job_requests table...' AS '';
UPDATE `job_requests`
  SET `cluster_criterias` = RPAD(`cluster_criterias`, LENGTH(`cluster_criterias`) + 4, '"]}]')
  WHERE `cluster_criterias` IS NOT NULL;
UPDATE `job_requests`
  SET `cluster_criterias` = LPAD(`cluster_criterias`, LENGTH(`cluster_criterias`) + 11, '[{"tags":["')
  WHERE `cluster_criterias` IS NOT NULL;
UPDATE `job_requests`
  SET `cluster_criterias` = REPLACE(`cluster_criterias`, ',', '","')
  WHERE `cluster_criterias` IS NOT NULL;
UPDATE `job_requests`
  SET `cluster_criterias` = REPLACE(`cluster_criterias`, '|', '"]},{"tags":["')
  WHERE `cluster_criterias` IS NOT NULL;
UPDATE `job_requests`
  SET `cluster_criterias` = '[]'
  WHERE `cluster_criterias` = '[""]' OR `cluster_criterias` IS NULL;
SELECT CURRENT_TIMESTAMP AS '', 'Successfully converted to cluster_criterias to JSON in job_requests table.' AS '';

SELECT CURRENT_TIMESTAMP AS '', 'Attempting to convert dependencies to JSON in job_requests table...' AS '';
UPDATE `job_requests`
  SET `dependencies` = RPAD(`dependencies`, LENGTH(`dependencies`) + 2, '"]')
  WHERE `dependencies` IS NOT NULL;
UPDATE `job_requests`
  SET `dependencies` = LPAD(`dependencies`, LENGTH(`dependencies`) + 2, '["')
  WHERE `dependencies` IS NOT NULL;
UPDATE `job_requests`
  SET `dependencies` = REPLACE(`dependencies`, ',', '","')
  WHERE `dependencies` IS NOT NULL;
UPDATE `job_requests`
  SET `dependencies` = '[]'
  WHERE `dependencies` = '[""]' OR `dependencies` IS NULL;
SELECT CURRENT_TIMESTAMP AS '', 'Successfully converted dependencies to JSON in job_requests table...' AS '';

SELECT CURRENT_TIMESTAMP AS '', 'Attempting to make dependencies field not null in job_requests table...' AS '';
ALTER TABLE `job_requests` MODIFY `dependencies` TEXT NOT NULL;
SELECT CURRENT_TIMESTAMP AS '', 'Successfully made dependencies field not null in job_requests table.' AS '';

SELECT CURRENT_TIMESTAMP AS '', 'Creating the job_executions table...' AS '';
CREATE TABLE `job_executions` (
  `id` VARCHAR(255) NOT NULL,
  `created` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `entity_version` INT(11) NOT NULL DEFAULT 0,
  `hostname` VARCHAR(255) NOT NULL,
  `process_id` INT(11) NOT NULL,
  `exit_code` INT(11) NOT NULL DEFAULT -1,
  `check_delay` BIGINT NOT NULL DEFAULT 10000,
  FOREIGN KEY (`id`) REFERENCES `jobs` (`id`) ON DELETE CASCADE,
  INDEX `JOB_EXECUTIONS_HOSTNAME_INDEX` (`hostname`),
  INDEX `JOB_EXECUTIONS_EXIT_CODE_INDEX` (`exit_code`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
SELECT CURRENT_TIMESTAMP AS '', 'Successfully created the job_executions table.' AS '';

SELECT CURRENT_TIMESTAMP AS '', 'Inserting values into job_executions from the jobs table...' AS '';
INSERT INTO `job_executions` (
  `id`,
  `created`,
  `updated`,
  `entity_version`,
  `hostname`,
  `process_id`,
  `exit_code`
) SELECT
    `id`,
    `created`,
    `updated`,
    `entityVersion`,
    `hostName`,
    `processHandle`,
    `exitCode`
  FROM `jobs`;
SELECT CURRENT_TIMESTAMP AS '', 'Successfully inserted values into the job_executions table.' AS '';

-- Modify the job table to remove the cluster id if cluster doesn't exist to prepare for foreign key constraints
SELECT CURRENT_TIMESTAMP AS '', 'Setting executionClusterId in jobs table to NULL if cluster no longer exists...' AS '';
UPDATE `jobs` AS `j` SET `j`.`executionClusterId` = NULL WHERE `j`.`executionClusterId` NOT IN (SELECT `id` FROM `clusters`);
SELECT CURRENT_TIMESTAMP AS '', 'Successfully updated executionClusterId.' AS '';

-- Modify the job table to remove the command id if the command doesn't exist to prepare for foreign key constraints
SELECT CURRENT_TIMESTAMP AS '', 'Setting commandId in Job table to NULL if command no longer exists...' AS '';
UPDATE `jobs` AS `j` SET `j`.`commandId` = NULL WHERE `j`.`commandId` NOT IN (SELECT `id` FROM `commands`);
SELECT CURRENT_TIMESTAMP AS '', 'Successfully updated commandId.' AS '';

-- Modify the jobs and associated children tables
SELECT CURRENT_TIMESTAMP AS '', 'Updating the jobs table for 3.0...' AS '';
ALTER TABLE `jobs`
  DROP KEY `started_index`,
  DROP KEY `finished_index`,
  DROP KEY `status_index`,
  DROP KEY `user_index`,
  DROP KEY `updated_index`;
ALTER TABLE `jobs`
  MODIFY `created` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  MODIFY `updated` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  MODIFY `name` VARCHAR(255) NOT NULL,
  MODIFY `user` VARCHAR(255) NOT NULL,
  MODIFY `version` VARCHAR(255) NOT NULL,
  MODIFY `description` TEXT DEFAULT NULL,
  CHANGE `entityVersion` `entity_version` INT(11) NOT NULL DEFAULT 0,
  MODIFY `status` VARCHAR(20) NOT NULL DEFAULT 'INIT',
  CHANGE `statusMsg` `status_msg` VARCHAR(255) DEFAULT NULL,
  CHANGE `archiveLocation` `archive_location` VARCHAR(1024) DEFAULT NULL,
  CHANGE `executionClusterId` `cluster_id` VARCHAR(255) DEFAULT NULL,
  CHANGE `executionClusterName` `cluster_name` VARCHAR(255) DEFAULT NULL,
  CHANGE `commandId` `command_id` VARCHAR(255) DEFAULT NULL,
  CHANGE `commandName` `command_name` VARCHAR(255) DEFAULT NULL,
  CHANGE `commandArgs` `command_args` TEXT NOT NULL,
  ADD COLUMN `tags` VARCHAR(2048) DEFAULT NULL,
  DROP `forwarded`,
  DROP `applicationId`,
  DROP `applicationName`,
  DROP `hostName`,
  DROP `clientHost`,
  DROP `fileDependencies`,
  DROP `envPropFile`,
  DROP `exitCode`,
  DROP `disableLogArchival`,
  DROP `clusterCriteriasString`,
  DROP `commandCriteriaString`,
  DROP `chosenClusterCriteriaString`,
  DROP `processHandle`,
  DROP `email`,
  DROP `groupName`,
  DROP `killURI`,
  DROP `outputURI`,
  DROP PRIMARY KEY,
  ADD FOREIGN KEY (`id`) REFERENCES `job_requests` (`id`) ON DELETE CASCADE,
  ADD FOREIGN KEY (`cluster_id`) REFERENCES `clusters` (`id`) ON DELETE RESTRICT,
  ADD FOREIGN KEY (`command_id`) REFERENCES `commands` (`id`) ON DELETE RESTRICT,
  ADD INDEX `JOBS_STARTED_INDEX` (`started`),
  ADD INDEX `JOBS_FINISHED_INDEX` (`finished`),
  ADD INDEX `JOBS_STATUS_INDEX` (`status`),
  ADD INDEX `JOBS_USER_INDEX` (`user`),
  ADD INDEX `JOBS_UPDATED_INDEX` (`updated`),
  ADD INDEX `JOBS_CLUSTER_NAME_INDEX` (`cluster_name`),
  ADD INDEX `JOBS_COMMAND_NAME_INDEX` (`command_name`),
  ADD INDEX `JOBS_TAGS_INDEX` (`tags`);
SELECT CURRENT_TIMESTAMP AS '', 'Successfully updated the jobs table.' AS '';

SELECT CURRENT_TIMESTAMP AS '', 'De-normalizing jobs tags for 3.0...' AS '';
UPDATE `jobs` AS `j` SET `j`.`tags` =
(
  SELECT GROUP_CONCAT(DISTINCT `t`.`element` ORDER BY `t`.`element` SEPARATOR '|')
  FROM `job_tags` AS `t`
  WHERE `j`.`id` = `t`.`JOB_ID`
  GROUP BY `t`.`JOB_ID`
);
SELECT CURRENT_TIMESTAMP AS '', 'Finished de-normalizing job tags for 3.0.' AS '';

SELECT CURRENT_TIMESTAMP AS '', 'Dropping the job_tags table for 3.0...' AS '';
DROP TABLE `job_tags`;
SELECT CURRENT_TIMESTAMP AS '', 'Successfully dropped the job_tags table.' AS '';

SELECT CURRENT_TIMESTAMP AS '', 'Finished upgrading Genie schema from version 2.0.0 to 3.0.0' AS '';
COMMIT;
