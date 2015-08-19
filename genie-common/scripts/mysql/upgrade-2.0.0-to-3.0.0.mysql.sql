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

-- Modify the applications and the associated children tables
ALTER TABLE `Application`
    MODIFY `created` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    MODIFY `updated` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    MODIFY `name` VARCHAR(255) NOT NULL,
    MODIFY `user` VARCHAR(255) NOT NULL,
    MODIFY `version` VARCHAR(255) NOT NULL,
    ADD COLUMN `description` TEXT DEFAULT NULL AFTER `version`,
    MODIFY `status` VARCHAR(20) NOT NULL DEFAULT 'INACTIVE',
    CHANGE `envPropFile` `setup_file` TEXT DEFAULT NULL,
    CHANGE `entityVersion` `entity_version` INT(11) NOT NULL DEFAULT 0,
    ADD INDEX `APPLICATIONS_NAME_INDEX` (`name`),
    ADD INDEX `APPLICATIONS_STATUS_INDEX` (`status`);

ALTER TABLE `application_configs` DROP KEY `I_PPLCFGS_APPLICATION_ID`;
ALTER TABLE `Application_configs`
    CHANGE `APPLICATION_ID` `application_id` VARCHAR(255) NOT NULL,
    CHANGE `element` `config` VARCHAR(255) NOT NULL,
    ADD FOREIGN KEY (`application_id`) REFERENCES Application(`id`) ON DELETE CASCADE;

ALTER TABLE `Application_jars` DROP KEY `I_PPLCJRS_APPLICATION_ID`;
ALTER TABLE `Application_jars`
    CHANGE `APPLICATION_ID` `application_id` VARCHAR(255) NOT NULL,
    CHANGE `element` `dependency` VARCHAR(255) NOT NULL,
    ADD FOREIGN KEY (`application_id`) REFERENCES Application(`id`) ON DELETE CASCADE;

ALTER TABLE `application_tags` DROP KEY `I_PPLCTGS_APPLICATION_ID`;
ALTER TABLE `Application_tags`
    CHANGE `APPLICATION_ID` `application_id` VARCHAR(255) NOT NULL,
    CHANGE `element` `tag` VARCHAR(255) NOT NULL,
    ADD FOREIGN KEY (`application_id`) REFERENCES Application(`id`) ON DELETE CASCADE,
    ADD INDEX `APPLICATION_TAGS_TAG_INDEX` (`tag`);

-- Modify the clusters and associated children tables
ALTER TABLE `Cluster`
    MODIFY `created` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    MODIFY `updated` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    MODIFY `name` VARCHAR(255) NOT NULL,
    MODIFY `user` VARCHAR(255) NOT NULL,
    MODIFY `version` VARCHAR(255) NOT NULL,
    ADD COLUMN `description` TEXT DEFAULT NULL AFTER `version`,
    MODIFY `status` VARCHAR(20) NOT NULL DEFAULT 'OUT_OF_SERVICE',
    CHANGE `clusterType` `cluster_type` VARCHAR(255) NOT NULL,
    CHANGE `entityVersion` `entity_version` INT(11) DEFAULT 0,
    ADD INDEX `CLUSTERS_NAME_INDEX` (`name`),
    ADD INDEX `CLUSTERS_STATUS_INDEX` (`status`);

ALTER TABLE `Cluster_Command`
    DROP KEY `I_CLSTMND_CLUSTERS_ID`,
    DROP KEY `I_CLSTMND_ELEMENT`;
ALTER TABLE `Cluster_Command`
    CHANGE `CLUSTERS_ID` `cluster_id` VARCHAR(255) NOT NULL,
    CHANGE `COMMANDS_ID` `command_id` VARCHAR(255) NOT NULL,
    CHANGE `commands_ORDER` `command_order` INT(11) NOT NULL,
    ADD FOREIGN KEY (`cluster_id`) REFERENCES Cluster(`id`) ON DELETE CASCADE,
    ADD FOREIGN KEY (`command_id`) REFERENCES Command(`id`) ON DELETE CASCADE;

ALTER TABLE `cluster_configs` DROP KEY `I_CLSTFGS_CLUSTER_ID`;
ALTER TABLE `Cluster_configs`
    CHANGE `CLUSTER_ID` `cluster_id` VARCHAR(255) NOT NULL,
    CHANGE `element` `config` VARCHAR(255) NOT NULL,
    ADD FOREIGN KEY (`cluster_id`) REFERENCES Cluster(`id`) ON DELETE CASCADE;

ALTER TABLE `Cluster_tags` DROP KEY `I_CLSTTGS_CLUSTER_ID`;
ALTER TABLE `Cluster_tags`
    CHANGE `CLUSTER_ID` `cluster_id` VARCHAR(255) NOT NULL,
    CHANGE `element` `tag` VARCHAR(255) NOT NULL,
    ADD FOREIGN KEY (`cluster_id`) REFERENCES Cluster(`id`) ON DELETE CASCADE;

-- Modify the commands and associated children tables
ALTER TABLE `Command`
    MODIFY `created` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    MODIFY `updated` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    MODIFY `name` VARCHAR(255) NOT NULL,
    MODIFY `user` VARCHAR(255) NOT NULL,
    MODIFY `version` VARCHAR(255) NOT NULL,
    ADD COLUMN `description` TEXT DEFAULT NULL AFTER `version`,
    MODIFY `status` VARCHAR(20) NOT NULL DEFAULT 'INACTIVE',
    MODIFY `executable` VARCHAR(255) NOT NULL,
    CHANGE `envPropFile` `setup_file` VARCHAR(255) DEFAULT NULL,
    CHANGE `jobType` `job_type` VARCHAR(255),
    CHANGE `entityVersion` `entity_version` INT(11) NOT NULL DEFAULT 0,
    CHANGE `APPLICATION_ID` `application_id` VARCHAR(255) DEFAULT NULL,
    ADD FOREIGN KEY (`application_id`) REFERENCES Application(`id`) ON DELETE RESTRICT,
    ADD INDEX `COMMAND_NAME_INDEX` (`name`),
    ADD INDEX `COMMAND_STATUS_INDEX` (`status`);

ALTER TABLE `Command_configs` DROP KEY `I_CMMNFGS_COMMAND_ID`;
ALTER TABLE `Command_configs`
    CHANGE `COMMAND_ID` `command_id` VARCHAR(255) NOT NULL,
    CHANGE `element` `config` VARCHAR(255) NOT NULL,
    ADD FOREIGN KEY (`command_id`) REFERENCES Command(`id`) ON DELETE CASCADE;

ALTER TABLE `Command_tags` DROP KEY `I_CMMNTGS_COMMAND_ID`;
ALTER TABLE `Command_tags`
    CHANGE `COMMAND_ID` `command_id` VARCHAR(255) NOT NULL,
    CHANGE `element` `tag` VARCHAR(255) NOT NULL,
    ADD FOREIGN KEY (`command_id`) REFERENCES Command(`id`) ON DELETE CASCADE,
    ADD INDEX `COMMAND_TAGS_TAG_INDEX` (`tag`);

-- Modify the jobs and associated children tables
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
    CHANGE `commandArgs` `command_args` TEXT NOT NULL,
    CHANGE `groupName` `group_name` VARCHAR(255) DEFAULT NULL,
    CHANGE `envPropFile` `setup_file` TEXT DEFAULT NULL,
    CHANGE `fileDependencies` `file_dependencies` TEXT DEFAULT NULL,
    CHANGE `disableLogArchival` `disable_log_archival` BIT(1) NOT NULL DEFAULT 0,
    CHANGE `clusterCriteriasString` `cluster_criterias_string` TEXT NOT NULL,
    CHANGE `commandCriteriaString` `command_criteria_string` TEXT NOT NULL,
    CHANGE `chosenClusterCriteriaString` `chosen_cluster_criteria_string` TEXT DEFAULT NULL,
    CHANGE `executionClusterName` `execution_cluster_name` VARCHAR(255) DEFAULT NULL,
    CHANGE `executionClusterId` `execution_cluster_id` VARCHAR(255) DEFAULT NULL,
    CHANGE `applicationName` `application_name` VARCHAR(255) DEFAULT NULL,
    CHANGE `applicationId` `application_id` VARCHAR(255) DEFAULT NULL,
    CHANGE `commandName` `command_name` VARCHAR(255) DEFAULT NULL,
    CHANGE `commandId` `command_id` VARCHAR(255) DEFAULT NULL,
    CHANGE `processHandle` `process_handle` INT(11) NOT NULL DEFAULT -1,
    CHANGE `statusMsg` `status_msg` VARCHAR(255) DEFAULT NULL,
    CHANGE `clientHost` `client_host` VARCHAR(255) DEFAULT NULL,
    CHANGE `hostName` `host_name` VARCHAR(255) DEFAULT NULL,
    CHANGE `killURI` `kill_uri` TEXT DEFAULT NULL,
    CHANGE `outputURI` `output_uri` TEXT DEFAULT NULL,
    CHANGE `exitCode` `exit_code` INT(11) NOT NULL DEFAULT -1,
    CHANGE `archiveLocation` `archive_location` TEXT DEFAULT NULL,
    MODIFY `forwarded` BIT(1) NOT NULL DEFAULT 0,
    ADD INDEX `JOBS_STARTED_INDEX` (`started`),
    ADD INDEX `JOBS_FINISHED_INDEX` (`finished`),
    ADD INDEX `JOBS_STATUS_INDEX` (`status`),
    ADD INDEX `JOBS_USER_INDEX` (`user`),
    ADD INDEX `JOBS_UPDATED_INDEX` (`updated`);

ALTER TABLE `Job_tags`
    DROP KEY `I_JOB_TGS_JOB_ID`,
    DROP KEY `element_index`;
ALTER TABLE `Job_tags`
    CHANGE `JOB_ID` `job_id` VARCHAR(255) NOT NULL,
    CHANGE `element` `tag` VARCHAR(255) NOT NULL,
    ADD FOREIGN KEY (`job_id`) REFERENCES Job(`id`) ON DELETE CASCADE,
    ADD INDEX `JOB_TAGS_TAG_INDEX` (`tag`);

-- Rename the tables to be a little bit nicer
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

SELECT CURRENT_TIMESTAMP AS '', 'Finished upgrading Genie schema from version 2.0.0 to 3.0.0' AS '';
COMMIT;
