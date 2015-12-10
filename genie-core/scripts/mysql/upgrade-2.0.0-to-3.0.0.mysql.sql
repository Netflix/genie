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

-- Create a new Many to Many table for commands to applications
SELECT CURRENT_TIMESTAMP AS '', 'Creating commands_applications table...' AS '';
CREATE TABLE `commands_applications` (
  `command_id` VARCHAR(255) NOT NULL,
  `application_id` VARCHAR(255) NOT NULL,
  FOREIGN KEY (`command_id`) REFERENCES `Command` (`id`) ON DELETE CASCADE,
  FOREIGN KEY (`application_id`) REFERENCES `Application` (`id`) ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
SELECT CURRENT_TIMESTAMP AS '', 'Successfully created commands_applications table' AS '';

-- Save the values of the current command to application relationship for the new table
SELECT CURRENT_TIMESTAMP AS '', 'Adding existing applications to commands...' AS '';
INSERT INTO `commands_applications` (`command_id`, `application_id`)
  SELECT `id`, `APPLICATION_ID` FROM Command WHERE `APPLICATION_ID` IS NOT NULL;
SELECT CURRENT_TIMESTAMP AS '', 'Successfully added existing applications to commands.' AS '';

-- Modify the applications and the associated children tables
SELECT CURRENT_TIMESTAMP AS '', 'Altering the Application table for 3.0...' AS '';
ALTER TABLE `Application`
  MODIFY `created` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  MODIFY `updated` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  MODIFY `name` VARCHAR(255) NOT NULL,
  MODIFY `user` VARCHAR(255) NOT NULL,
  MODIFY `version` VARCHAR(255) NOT NULL,
  ADD COLUMN `description` TEXT DEFAULT NULL AFTER `version`,
  ADD COLUMN `sorted_tags` VARCHAR(2048) DEFAULT NULL,
  MODIFY `status` VARCHAR(20) NOT NULL DEFAULT 'INACTIVE',
  CHANGE `envPropFile` `setup_file` VARCHAR(1024) DEFAULT NULL,
  CHANGE `entityVersion` `entity_version` INT(11) NOT NULL DEFAULT 0,
  ADD INDEX `APPLICATIONS_NAME_INDEX` (`name`),
  ADD INDEX `APPLICATIONS_STATUS_INDEX` (`status`);
SELECT CURRENT_TIMESTAMP AS '', 'Successfully updated the Application table...' AS '';

SELECT CURRENT_TIMESTAMP AS '', 'De-normalizing application tags for 3.0...' AS '';
UPDATE `Application` as `a` set `a`.`sorted_tags` =
(
  SELECT GROUP_CONCAT(DISTINCT `t`.`element` ORDER BY `t`.`element` SEPARATOR ',')
  FROM `Application_tags` `t`
  WHERE `a`.`id` = `t`.`APPLICATION_ID`
  GROUP BY `t`.`APPLICATION_ID`
);
SELECT CURRENT_TIMESTAMP AS '', 'Finished de-normalizing application tags for 3.0' AS '';

SELECT CURRENT_TIMESTAMP AS '', 'Altering the Application_configs table for 3.0...' AS '';
ALTER TABLE `Application_configs` DROP KEY `I_PPLCFGS_APPLICATION_ID`;
ALTER TABLE `Application_configs`
  CHANGE `APPLICATION_ID` `application_id` VARCHAR(255) NOT NULL,
  CHANGE `element` `config` VARCHAR(1024) NOT NULL,
  ADD FOREIGN KEY (`application_id`) REFERENCES `Application` (`id`) ON DELETE CASCADE;
SELECT CURRENT_TIMESTAMP AS '', 'Successfully updated the Application_configs table' AS '';

SELECT CURRENT_TIMESTAMP AS '', 'Altering the Application_jars table for 3.0...' AS '';
ALTER TABLE `Application_jars` DROP KEY `I_PPLCJRS_APPLICATION_ID`;
ALTER TABLE `Application_jars`
  CHANGE `APPLICATION_ID` `application_id` VARCHAR(255) NOT NULL,
  CHANGE `element` `dependency` VARCHAR(1024) NOT NULL,
  ADD FOREIGN KEY (`application_id`) REFERENCES `Application` (`id`) ON DELETE CASCADE;
SELECT CURRENT_TIMESTAMP AS '', 'Successfully updated the Application_jars table' AS '';

SELECT CURRENT_TIMESTAMP AS '', 'Updating the Application_tags table for 3.0...' AS '';
ALTER TABLE `Application_tags` DROP KEY `I_PPLCTGS_APPLICATION_ID`;
ALTER TABLE `Application_tags`
  CHANGE `APPLICATION_ID` `application_id` VARCHAR(255) NOT NULL,
  CHANGE `element` `tag` VARCHAR(255) NOT NULL,
  ADD FOREIGN KEY (`application_id`) REFERENCES `Application` (`id`) ON DELETE CASCADE,
  ADD INDEX `APPLICATION_TAGS_TAG_INDEX` (`tag`);
SELECT CURRENT_TIMESTAMP AS '', 'Successfully updated the Application_tags table' AS '';

-- Modify the clusters and associated children tables
SELECT CURRENT_TIMESTAMP AS '', 'Updating the Cluster table for 3.0...' AS '';
ALTER TABLE `Cluster`
  MODIFY `created` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  MODIFY `updated` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  MODIFY `name` VARCHAR(255) NOT NULL,
  MODIFY `user` VARCHAR(255) NOT NULL,
  MODIFY `version` VARCHAR(255) NOT NULL,
  ADD COLUMN `description` TEXT DEFAULT NULL AFTER `version`,
  ADD COLUMN `sorted_tags` VARCHAR(2048) DEFAULT NULL,
  MODIFY `status` VARCHAR(20) NOT NULL DEFAULT 'OUT_OF_SERVICE',
  CHANGE `clusterType` `cluster_type` VARCHAR(255) NOT NULL,
  CHANGE `entityVersion` `entity_version` INT(11) DEFAULT 0,
  ADD INDEX `CLUSTERS_NAME_INDEX` (`name`),
  ADD INDEX `CLUSTERS_STATUS_INDEX` (`status`);
SELECT CURRENT_TIMESTAMP AS '', 'Successfully updated the Cluster table' AS '';

SELECT CURRENT_TIMESTAMP AS '', 'De-normalizing cluster tags for 3.0...' AS '';
UPDATE `Cluster` as `c` set `c`.`sorted_tags` =
(
  SELECT GROUP_CONCAT(DISTINCT `t`.`element` ORDER BY `t`.`element` SEPARATOR ',')
  FROM `Cluster_tags` `t`
  WHERE `c`.`id` = `t`.`CLUSTER_ID`
  GROUP BY `t`.`CLUSTER_ID`
);
SELECT CURRENT_TIMESTAMP AS '', 'Finished de-normalizing cluster tags for 3.0' AS '';

SELECT CURRENT_TIMESTAMP AS '', 'Updating the Cluster_Command table for 3.0...' AS '';
ALTER TABLE `Cluster_Command`
  DROP KEY `I_CLSTMND_CLUSTERS_ID`,
  DROP KEY `I_CLSTMND_ELEMENT`;
  ALTER TABLE `Cluster_Command`
  CHANGE `CLUSTERS_ID` `cluster_id` VARCHAR(255) NOT NULL,
  CHANGE `COMMANDS_ID` `command_id` VARCHAR(255) NOT NULL,
  CHANGE `commands_ORDER` `command_order` INT(11) NOT NULL,
  ADD FOREIGN KEY (`cluster_id`) REFERENCES `Cluster` (`id`) ON DELETE CASCADE,
  ADD FOREIGN KEY (`command_id`) REFERENCES `Command` (`id`) ON DELETE RESTRICT;
SELECT CURRENT_TIMESTAMP AS '', 'Successfully updated the Cluster_Command table' AS '';

SELECT CURRENT_TIMESTAMP AS '', 'Updating the Cluster_configs table for 3.0...' AS '';
ALTER TABLE `Cluster_configs` DROP KEY `I_CLSTFGS_CLUSTER_ID`;
ALTER TABLE `Cluster_configs`
  CHANGE `CLUSTER_ID` `cluster_id` VARCHAR(255) NOT NULL,
  CHANGE `element` `config` VARCHAR(1024) NOT NULL,
  ADD FOREIGN KEY (`cluster_id`) REFERENCES `Cluster` (`id`) ON DELETE CASCADE;
SELECT CURRENT_TIMESTAMP AS '', 'Successfully updated the Cluster_configs table' AS '';

SELECT CURRENT_TIMESTAMP AS '', 'Updating the Cluster_tags table for 3.0...' AS '';
ALTER TABLE `Cluster_tags` DROP KEY `I_CLSTTGS_CLUSTER_ID`;
ALTER TABLE `Cluster_tags`
  CHANGE `CLUSTER_ID` `cluster_id` VARCHAR(255) NOT NULL,
  CHANGE `element` `tag` VARCHAR(255) NOT NULL,
  ADD FOREIGN KEY (`cluster_id`) REFERENCES `Cluster` (`id`) ON DELETE CASCADE;
SELECT CURRENT_TIMESTAMP AS '', 'Updated the Cluster_tags table...' AS '';

-- Modify the commands and associated children tables
SELECT CURRENT_TIMESTAMP AS '', 'Updating the Command table for 3.0...' AS '';
ALTER TABLE `Command`
  MODIFY `created` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  MODIFY `updated` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  MODIFY `name` VARCHAR(255) NOT NULL,
  MODIFY `user` VARCHAR(255) NOT NULL,
  MODIFY `version` VARCHAR(255) NOT NULL,
  ADD COLUMN `description` TEXT DEFAULT NULL AFTER `version`,
  ADD COLUMN `sorted_tags` VARCHAR(2048) DEFAULT NULL,
  MODIFY `status` VARCHAR(20) NOT NULL DEFAULT 'INACTIVE',
  MODIFY `executable` VARCHAR(255) NOT NULL,
  CHANGE `envPropFile` `setup_file` VARCHAR(1024) DEFAULT NULL,
  CHANGE `entityVersion` `entity_version` INT(11) NOT NULL DEFAULT 0,
  DROP `APPLICATION_ID`,
  DROP `jobType`,
  ADD INDEX `COMMAND_NAME_INDEX` (`name`),
  ADD INDEX `COMMAND_STATUS_INDEX` (`status`);
SELECT CURRENT_TIMESTAMP AS '', 'Successfully updated the Command table' AS '';

SELECT CURRENT_TIMESTAMP AS '', 'De-normalizing command tags for 3.0...' AS '';
UPDATE `Command` as `c` set `c`.`sorted_tags` =
(
  SELECT GROUP_CONCAT(DISTINCT `t`.`element` ORDER BY `t`.`element` SEPARATOR ',')
  FROM `Command_tags` `t`
  WHERE `c`.`id` = `t`.`COMMAND_ID`
  GROUP BY `t`.`COMMAND_ID`
);
SELECT CURRENT_TIMESTAMP AS '', 'Finished de-normalizing command tags for 3.0' AS '';

SELECT CURRENT_TIMESTAMP AS '', 'Updating the Command_configs table for 3.0...' AS '';
ALTER TABLE `Command_configs` DROP KEY `I_CMMNFGS_COMMAND_ID`;
ALTER TABLE `Command_configs`
  CHANGE `COMMAND_ID` `command_id` VARCHAR(255) NOT NULL,
  CHANGE `element` `config` VARCHAR(1024) NOT NULL,
  ADD FOREIGN KEY (`command_id`) REFERENCES `Command` (`id`) ON DELETE CASCADE;
SELECT CURRENT_TIMESTAMP AS '', 'Successfully updated the Command_configs table' AS '';

SELECT CURRENT_TIMESTAMP AS '', 'Updating the Command_tags table for 3.0...' AS '';
ALTER TABLE `Command_tags` DROP KEY `I_CMMNTGS_COMMAND_ID`;
ALTER TABLE `Command_tags`
  CHANGE `COMMAND_ID` `command_id` VARCHAR(255) NOT NULL,
  CHANGE `element` `tag` VARCHAR(255) NOT NULL,
  ADD FOREIGN KEY (`command_id`) REFERENCES `Command` (`id`) ON DELETE CASCADE,
  ADD INDEX `COMMAND_TAGS_TAG_INDEX` (`tag`);
SELECT CURRENT_TIMESTAMP AS '', 'Successfully updated the Command_tags table' AS '';

-- TODO: May want these TEXT fields to be large varchars instead?
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
  `cluster_criterias` VARCHAR(2048) NOT NULL,
  `command_criteria` VARCHAR(1024) NOT NULL,
  `file_dependencies` TEXT DEFAULT NULL,
  `disable_log_archival` BIT(1) NOT NULL DEFAULT 0,
  `email` VARCHAR(255) DEFAULT NULL,
  `sorted_tags` VARCHAR(2048) DEFAULT NULL,
  `cpu` INT(11) NOT NULL DEFAULT 1,
  `memory` INT(11) NOT NULL DEFAULT 1560,
  `client_host` VARCHAR(255) DEFAULT NULL,
  FOREIGN KEY (`id`) REFERENCES `Job` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
SELECT CURRENT_TIMESTAMP AS '', 'Successfully created the job_requests table' AS '';

SELECT CURRENT_TIMESTAMP AS '', 'Inserting values into job_requests table from the Job table...' AS '';
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
  `file_dependencies`,
  `disable_log_archival`,
  `email`,
  `sorted_tags`,
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
                  SELECT GROUP_CONCAT(DISTINCT `t`.`element` ORDER BY `t`.`element` SEPARATOR ',')
                  FROM `Job_tags` `t`
                  WHERE `j`.`id` = `t`.`JOB_ID`
                ),
                1,
                1560,
                `j`.`clientHost`
  FROM `Job` `j`;
-- TODO: Do this in one pass instead of 3?
UPDATE `job_requests` SET `command_criteria` = RPAD(`command_criteria`, LENGTH(`command_criteria`) + 2, '"]');
UPDATE `job_requests` SET `command_criteria` = LPAD(`command_criteria`, LENGTH(`command_criteria`) + 2, '["');
UPDATE `job_requests` SET `command_criteria` = REPLACE(`command_criteria`, ',', '","');
UPDATE `job_requests` SET `command_criteria` = '[]' WHERE `command_criteria` = '[""]' OR `command_criteria` IS NULL;
-- TODO: Less passes?
UPDATE `job_requests` SET `cluster_criterias` = RPAD(`cluster_criterias`, LENGTH(`cluster_criterias`) + 4, '"]}]');
UPDATE `job_requests` SET `cluster_criterias` = LPAD(`cluster_criterias`, LENGTH(`cluster_criterias`) + 11, '[{"tags":["');
UPDATE `job_requests` SET `cluster_criterias` = REPLACE(`cluster_criterias`, ',', '","');
UPDATE `job_requests` SET `cluster_criterias` = REPLACE(`cluster_criterias`, '|', '"]},{"tags":["');
UPDATE `job_requests` SET `cluster_criterias` = '[]' WHERE `cluster_criterias` = '[""]' OR `cluster_criterias` IS NULL;
-- TODO: Do this in one pass instead of 3?
UPDATE `job_requests` SET `file_dependencies` = RPAD(`file_dependencies`, LENGTH(`file_dependencies`) + 2, '"]');
UPDATE `job_requests` SET `file_dependencies` = LPAD(`file_dependencies`, LENGTH(`file_dependencies`) + 2, '["');
UPDATE `job_requests` SET `file_dependencies` = REPLACE(`file_dependencies`, ',', '","');
UPDATE `job_requests` SET `file_dependencies` = '[]' WHERE `file_dependencies` = '[""]' OR `file_dependencies` IS NULL;
SELECT CURRENT_TIMESTAMP AS '', 'Successfully inserted values...' AS '';

SELECT CURRENT_TIMESTAMP AS '', 'Creating the job_executions table...' AS '';
CREATE TABLE `job_executions` (
  `id` VARCHAR(255) NOT NULL,
  `created` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `entity_version` INT(11) NOT NULL DEFAULT 0,
  `cluster_criteria` VARCHAR(1024),
  `host_name` VARCHAR(255) NOT NULL,
  `process_id` INT(11) NOT NULL,
  `exit_code` INT(11) NOT NULL DEFAULT -1,
  FOREIGN KEY (`id`) REFERENCES `Job` (`id`) ON DELETE CASCADE,
  INDEX `HOST_NAME_INDEX` (`host_name`),
  INDEX `EXIT_CODE_INDEX` (`exit_code`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
SELECT CURRENT_TIMESTAMP AS '', 'Successfully created the job_executions table' AS '';

SELECT CURRENT_TIMESTAMP AS '', 'Inserting values into job_executions from the Job table...' AS '';
INSERT INTO `job_executions` (
  `id`,
  `created`,
  `updated`,
  `entity_version`,
  `cluster_criteria`,
  `host_name`,
  `process_id`,
  `exit_code`
) SELECT
                `id`,
                `created`,
                `updated`,
                `entityVersion`,
                `chosenClusterCriteriaString`,
                `hostName`,
                `processHandle`,
                `exitCode`
  FROM `Job`;
SELECT CURRENT_TIMESTAMP AS '', 'Successfully inserted values into the job_executions table' AS '';

UPDATE `job_executions` SET `cluster_criteria` = RPAD(`cluster_criteria`, LENGTH(`cluster_criteria`) + 2, '"]');
UPDATE `job_executions` SET `cluster_criteria` = LPAD(`cluster_criteria`, LENGTH(`cluster_criteria`) + 2, '["');
UPDATE `job_executions` SET `cluster_criteria` = REPLACE(`cluster_criteria`, ',', '","');
UPDATE `job_executions` SET `cluster_criteria` = '[]' WHERE `cluster_criteria` = '[""]' OR `cluster_criteria` IS NULL;

ALTER TABLE `job_executions` CHANGE `cluster_criteria` `cluster_criteria` VARCHAR(1024) NOT NULL DEFAULT '[]';

-- Modify the job table to remove the cluster id if cluster doesn't exist to prepare for foreign key constraints
SELECT CURRENT_TIMESTAMP AS '', 'Setting executionClusterId in Job table to NULL if cluster no longer exists...' AS '';
UPDATE `Job` SET `executionClusterId` = NULL WHERE `executionClusterId` NOT IN (SELECT `id` FROM `Cluster`);
SELECT CURRENT_TIMESTAMP AS '', 'Successfully updated executionClusterId' AS '';

-- Modify the job table to remove the command id if the command doesn't exist to prepare for foreign key constraints
SELECT CURRENT_TIMESTAMP AS '', 'Setting commandId in Job table to NULL if command no longer exists...' AS '';
UPDATE `Job` SET `commandId` = NULL WHERE `commandId` NOT IN (SELECT `id` FROM `Command`);
SELECT CURRENT_TIMESTAMP AS '', 'Successfully updated commandId' AS '';

-- Modify the jobs and associated children tables
SELECT CURRENT_TIMESTAMP AS '', 'Updating the Job table for 3.0...' AS '';
ALTER TABLE `Job`
  DROP KEY `started_index`,
  DROP KEY `finished_index`,
  DROP KEY `status_index`,
  DROP KEY `user_index`,
  DROP KEY `updated_index`;
ALTER TABLE `Job`
  MODIFY `created` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  MODIFY `updated` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  MODIFY `name` VARCHAR(255) NOT NULL,
  MODIFY `user` VARCHAR(255) NOT NULL,
  MODIFY `version` VARCHAR(255) NOT NULL,
  MODIFY `description` TEXT DEFAULT NULL,
  CHANGE `entityVersion` `entity_version` INT(11) NOT NULL DEFAULT 0,
  MODIFY `status` VARCHAR(20) NOT NULL DEFAULT 'INIT',
  CHANGE `statusMsg` `status_msg` VARCHAR(255) DEFAULT NULL,
  CHANGE `exitCode` `exit_code` INT(11) NOT NULL DEFAULT -1,
  CHANGE `archiveLocation` `archive_location` VARCHAR(1024) DEFAULT NULL,
  CHANGE `executionClusterId` `cluster_id` VARCHAR(255) DEFAULT NULL,
  CHANGE `executionClusterName` `cluster_name` VARCHAR(255) DEFAULT NULL,
  CHANGE `commandId` `command_id` VARCHAR(255) DEFAULT NULL,
  CHANGE `commandName` `command_name` VARCHAR(255) DEFAULT NULL,
  ADD COLUMN `sorted_tags` VARCHAR(2048) DEFAULT NULL,
  DROP `forwarded`,
  DROP `applicationId`,
  DROP `applicationName`,
  DROP `hostName`,
  DROP `clientHost`,
  DROP `fileDependencies`,
  DROP `envPropFile`,
  DROP `disableLogArchival`,
  DROP `clusterCriteriasString`,
  DROP `commandCriteriaString`,
  DROP `chosenClusterCriteriaString`,
  DROP `processHandle`,
  DROP `email`,
  DROP `groupName`,
  DROP `commandArgs`,
  DROP `killURI`,
  DROP `outputURI`,
  ADD FOREIGN KEY (`cluster_id`) REFERENCES `Cluster` (`id`) ON DELETE RESTRICT,
  ADD FOREIGN KEY (`command_id`) REFERENCES `Command` (`id`) ON DELETE RESTRICT,
  ADD INDEX `JOBS_STARTED_INDEX` (`started`),
  ADD INDEX `JOBS_FINISHED_INDEX` (`finished`),
  ADD INDEX `JOBS_STATUS_INDEX` (`status`),
  ADD INDEX `JOBS_USER_INDEX` (`user`),
  ADD INDEX `JOBS_UPDATED_INDEX` (`updated`),
  ADD INDEX `JOBS_CLUSTER_NAME_INDEX` (`cluster_name`),
  ADD INDEX `JOBS_COMMAND_NAME_INDEX` (`command_name`),
  ADD INDEX `JOBS_SORTED_TAGS_INDEX` (`sorted_tags`);
SELECT CURRENT_TIMESTAMP AS '', 'Successfully updated the Job table' AS '';

SELECT CURRENT_TIMESTAMP AS '', 'De-normalizing job tags for 3.0...' AS '';
UPDATE `Job` as `j` set `j`.`sorted_tags` =
  (
    SELECT GROUP_CONCAT(DISTINCT `t`.`element` ORDER BY `t`.`element` SEPARATOR ',')
    FROM `Job_tags` `t`
    WHERE `j`.`id` = `t`.`JOB_ID`
    GROUP BY `t`.`JOB_ID`
  );
SELECT CURRENT_TIMESTAMP AS '', 'Finished de-normalizing job tags for 3.0' AS '';

SELECT CURRENT_TIMESTAMP AS '', 'Updating the Job_tags table for 3.0...' AS '';
ALTER TABLE `Job_tags`
  DROP KEY `I_JOB_TGS_JOB_ID`,
  DROP KEY `element_index`;
ALTER TABLE `Job_tags`
  CHANGE `JOB_ID` `job_id` VARCHAR(255) NOT NULL,
  CHANGE `element` `tag` VARCHAR(255) NOT NULL,
  ADD FOREIGN KEY (`job_id`) REFERENCES `Job` (`id`) ON DELETE CASCADE,
  ADD INDEX `JOB_TAGS_TAG_INDEX` (`tag`);
SELECT CURRENT_TIMESTAMP AS '', 'Successfully updated the Job_tags table' AS '';

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

SELECT CURRENT_TIMESTAMP AS '', 'Finished upgrading Genie schema from version 2.0.0 to 3.0.0' AS '';
COMMIT;
