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

SELECT
  CURRENT_TIMESTAMP                    AS '',
  'Upgrading database schema to 3.3.0' AS '';

SELECT
  CURRENT_TIMESTAMP                           AS '',
  'Dropping existing foreign key constraints' AS '';

ALTER TABLE `application_configs`
  DROP FOREIGN KEY `APPLICATION_CONFIGS_APPLICATION_ID_FK`;
ALTER TABLE `application_dependencies`
  DROP FOREIGN KEY `APPLICATION_DEPENDENCIES_APPLICATION_ID_FK`;
ALTER TABLE `cluster_configs`
  DROP FOREIGN KEY `CLUSTER_CONFIGS_CLUSTER_ID_FK`;
ALTER TABLE `cluster_dependencies`
  DROP FOREIGN KEY `CLUSTER_DEPENDENCIES_CLUSTER_ID_FK`;
ALTER TABLE `command_configs`
  DROP FOREIGN KEY `COMMAND_CONFIGS_COMMAND_ID_FK`;
ALTER TABLE `command_dependencies`
  DROP FOREIGN KEY `COMMAND_DEPENDENCIES_COMMAND_ID_FK`;
ALTER TABLE `clusters_commands`
  DROP FOREIGN KEY `CLUSTERS_COMMANDS_CLUSTER_ID_FK`,
  DROP FOREIGN KEY `CLUSTERS_COMMANDS_COMMAND_ID_FK`;
ALTER TABLE `commands_applications`
  DROP FOREIGN KEY `COMMANDS_APPLICATIONS_APPLICATION_ID_FK`,
  DROP FOREIGN KEY `COMMANDS_APPLICATIONS_COMMAND_ID_FK`;
ALTER TABLE `jobs`
  DROP FOREIGN KEY `JOBS_CLUSTER_ID_FK`,
  DROP FOREIGN KEY `JOBS_COMMAND_ID_FK`,
  DROP FOREIGN KEY `JOBS_ID_FK`;
ALTER TABLE `jobs_applications`
  DROP FOREIGN KEY `JOBS_APPLICATIONS_APPLICATION_ID_FK`,
  DROP FOREIGN KEY `JOBS_APPLICATIONS_JOB_ID_FK`;

SELECT
  CURRENT_TIMESTAMP                                    AS '',
  'Finished dropping existing foreign key constraints' AS '';

SELECT
  CURRENT_TIMESTAMP         AS '',
  'Renaming current tables' AS '';

RENAME TABLE
    `applications` TO `applications_320`,
    `application_configs` TO `application_configs_320`,
    `application_dependencies` TO `application_dependencies_320`,
    `clusters` TO `clusters_320`,
    `cluster_configs` TO `cluster_configs_320`,
    `cluster_dependencies` TO `cluster_dependencies_320`,
    `commands` TO `commands_320`,
    `command_configs` TO `command_configs_320`,
    `command_dependencies` TO `command_dependencies_320`,
    `commands_applications` TO `commands_applications_320`,
    `clusters_commands` TO `clusters_commands_320`,
    `job_requests` TO `job_requests_320`,
    `job_metadata` TO `job_metadata_320`,
    `job_executions` TO `job_executions_320`,
    `jobs` TO `jobs_320`,
    `jobs_applications` TO `jobs_applications_320`;

SELECT
  CURRENT_TIMESTAMP                  AS '',
  'Finished renaming current tables' AS '';

SELECT
  CURRENT_TIMESTAMP     AS '',
  'Creating tags table' AS '';

CREATE TABLE `tags` (
  `id`             BIGINT(20) AUTO_INCREMENT                               NOT NULL,
  `created`        DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3)                NOT NULL,
  `updated`        DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3)                NOT NULL ON UPDATE CURRENT_TIMESTAMP(3),
  `entity_version` INT(11) DEFAULT '0'                                     NOT NULL,
  `tag`            VARCHAR(255)                                            NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `TAGS_TAG_UNIQUE_INDEX` (`tag`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  DEFAULT COLLATE = utf8_bin
  ROW_FORMAT DYNAMIC;

SELECT
  CURRENT_TIMESTAMP              AS '',
  'Finished creating tags table' AS '';

SELECT
  CURRENT_TIMESTAMP      AS '',
  'Creating files table' AS '';

CREATE TABLE `files` (
  `id`             BIGINT(20) AUTO_INCREMENT                               NOT NULL,
  `created`        DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3)                NOT NULL,
  `updated`        DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3)                NOT NULL ON UPDATE CURRENT_TIMESTAMP(3),
  `entity_version` INT(11) DEFAULT '0'                                     NOT NULL,
  `file`           VARCHAR(1024)                                           NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `FILES_FILE_UNIQUE_INDEX` (`file`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  DEFAULT COLLATE = utf8_bin
  ROW_FORMAT DYNAMIC;

SELECT
  CURRENT_TIMESTAMP               AS '',
  'Finished creating files table' AS '';

SELECT
  CURRENT_TIMESTAMP             AS '',
  'Creating applications table' AS '';

CREATE TABLE `applications` (
  `id`             BIGINT(20) AUTO_INCREMENT                               NOT NULL,
  `created`        DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3)                NOT NULL,
  `updated`        DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3)                NOT NULL ON UPDATE CURRENT_TIMESTAMP(3),
  `entity_version` INT(11) DEFAULT '0'                                     NOT NULL,
  `unique_id`      VARCHAR(255)                                            NOT NULL,
  `name`           VARCHAR(255)                                            NOT NULL,
  `genie_user`     VARCHAR(255)                                            NOT NULL,
  `version`        VARCHAR(255)                                            NOT NULL,
  `description`    TEXT         DEFAULT NULL,
  `setup_file`     BIGINT(20)   DEFAULT NULL,
  `status`         VARCHAR(20) DEFAULT 'INACTIVE'                          NOT NULL,
  `type`           VARCHAR(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `APPLICATIONS_UNIQUE_ID_UNIQUE_INDEX` (`unique_id`),
  KEY `APPLICATIONS_NAME_INDEX` (`name`),
  KEY `APPLICATIONS_SETUP_FILE_INDEX` (`setup_file`),
  KEY `APPLICATIONS_STATUS_INDEX` (`status`),
  KEY `APPLICATIONS_TYPE_INDEX` (`type`),
  CONSTRAINT `APPLICATIONS_SETUP_FILE_FK` FOREIGN KEY (`setup_file`) REFERENCES `files` (`id`)
    ON DELETE RESTRICT
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  DEFAULT COLLATE = utf8_bin
  ROW_FORMAT = DYNAMIC;

SELECT
  CURRENT_TIMESTAMP            AS '',
  'Created applications table' AS '';

SELECT
  CURRENT_TIMESTAMP                     AS '',
  'Creating applications_configs table' AS '';

CREATE TABLE `applications_configs` (
  `application_id` BIGINT(20) NOT NULL,
  `file_id`        BIGINT(20) NOT NULL,
  PRIMARY KEY (`application_id`, `file_id`),
  KEY `APPLICATIONS_CONFIGS_APPLICATION_ID_INDEX` (`application_id`),
  KEY `APPLICATIONS_CONFIGS_FILE_ID_INDEX` (`file_id`),
  CONSTRAINT `APPLICATIONS_CONFIGS_APPLICATION_ID_FK` FOREIGN KEY (`application_id`) REFERENCES `applications` (`id`)
    ON DELETE CASCADE,
  CONSTRAINT `APPLICATIONS_CONFIGS_FILE_ID_FK` FOREIGN KEY (`file_id`) REFERENCES `files` (`id`)
    ON DELETE RESTRICT
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  DEFAULT COLLATE = utf8_bin
  ROW_FORMAT = DYNAMIC;

SELECT
  CURRENT_TIMESTAMP                    AS '',
  'Created applications_configs table' AS '';

SELECT
  CURRENT_TIMESTAMP                          AS '',
  'Creating applications_dependencies table' AS '';

CREATE TABLE `applications_dependencies` (
  `application_id` BIGINT(20) NOT NULL,
  `file_id`        BIGINT(20) NOT NULL,
  PRIMARY KEY (`application_id`, `file_id`),
  KEY `APPLICATIONS_DEPENDENCIES_APPLICATION_ID_INDEX` (`application_id`),
  KEY `APPLICATIONS_DEPENDENCIES_FILE_ID_INDEX` (`file_id`),
  CONSTRAINT `APPLICATIONS_DEPENDENCIES_APPLICATION_ID_FK` FOREIGN KEY (`application_id`)
  REFERENCES `applications` (`id`)
    ON DELETE CASCADE,
  CONSTRAINT `APPLICATIONS_DEPENDENCIES_FILE_ID_FK` FOREIGN KEY (`file_id`) REFERENCES `files` (`id`)
    ON DELETE RESTRICT
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  DEFAULT COLLATE = utf8_bin
  ROW_FORMAT = DYNAMIC;

SELECT
  CURRENT_TIMESTAMP                                      AS '',
  'Finished creating new application_dependencies table' AS '';

SELECT
  CURRENT_TIMESTAMP             AS '',
  'Creating new clusters table' AS '';

CREATE TABLE `clusters` (
  `id`             BIGINT(20) AUTO_INCREMENT                               NOT NULL,
  `created`        DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3)                NOT NULL,
  `updated`        DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3)                NOT NULL ON UPDATE CURRENT_TIMESTAMP(3),
  `entity_version` INT(11) DEFAULT '0'                                     NOT NULL,
  `unique_id`      VARCHAR(255)                                            NOT NULL,
  `name`           VARCHAR(255)                                            NOT NULL,
  `genie_user`     VARCHAR(255)                                            NOT NULL,
  `version`        VARCHAR(255)                                            NOT NULL,
  `description`    TEXT       DEFAULT NULL,
  `setup_file`     BIGINT(20) DEFAULT NULL,
  `status`         VARCHAR(20) DEFAULT 'OUT_OF_SERVICE'                    NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `CLUSTERS_UNIQUE_ID_UNIQUE_INDEX` (`unique_id`),
  KEY `CLUSTERS_NAME_INDEX` (`name`),
  KEY `CLUSTERS_SETUP_FILE_INDEX` (`setup_file`),
  KEY `CLUSTERS_STATUS_INDEX` (`status`),
  CONSTRAINT `CLUSTERS_SETUP_FILE_FK` FOREIGN KEY (`setup_file`) REFERENCES `files` (`id`)
    ON DELETE RESTRICT
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  DEFAULT COLLATE utf8_bin
  ROW_FORMAT = DYNAMIC;

SELECT
  CURRENT_TIMESTAMP                      AS '',
  'Finished creating new clusters table' AS '';

SELECT
  CURRENT_TIMESTAMP                     AS '',
  'Creating new clusters_configs table' AS '';

CREATE TABLE `clusters_configs` (
  `cluster_id` BIGINT(20) NOT NULL,
  `file_id`    BIGINT(20) NOT NULL,
  PRIMARY KEY (`cluster_id`, `file_id`),
  KEY `CLUSTERS_CONFIGS_CLUSTER_ID_INDEX` (`cluster_id`),
  KEY `CLUSTERS_CONFIGS_FILE_ID_INDEX` (`file_id`),
  CONSTRAINT `CLUSTERS_CONFIGS_CLUSTER_ID_FK` FOREIGN KEY (`cluster_id`) REFERENCES `clusters` (`id`)
    ON DELETE CASCADE,
  CONSTRAINT `CLUSTERS_CONFIGS_FILE_ID_FK` FOREIGN KEY (`file_id`) REFERENCES `files` (`id`)
    ON DELETE RESTRICT
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  DEFAULT COLLATE = utf8_bin
  ROW_FORMAT = DYNAMIC;

SELECT
  CURRENT_TIMESTAMP                              AS '',
  'Finished creating new clusters_configs table' AS '';

SELECT
  CURRENT_TIMESTAMP                          AS '',
  'Creating new clusters_dependencies table' AS '';

CREATE TABLE `clusters_dependencies` (
  `cluster_id` BIGINT(20) NOT NULL,
  `file_id`    BIGINT(20) NOT NULL,
  PRIMARY KEY (`cluster_id`, `file_id`),
  KEY `CLUSTERS_DEPENDENCIES_CLUSTER_ID_INDEX` (`cluster_id`),
  KEY `CLUSTERS_DEPENDENCIES_FILE_ID_INDEX` (`file_id`),
  CONSTRAINT `CLUSTERS_DEPENDENCIES_CLUSTER_ID_FK` FOREIGN KEY (`cluster_id`) REFERENCES `clusters` (`id`)
    ON DELETE CASCADE,
  CONSTRAINT `CLUSTERS_DEPENDENCIES_FILE_ID_FK` FOREIGN KEY (`file_id`) REFERENCES `files` (`id`)
    ON DELETE RESTRICT
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  DEFAULT COLLATE = utf8_bin
  ROW_FORMAT = DYNAMIC;

SELECT
  CURRENT_TIMESTAMP                                   AS '',
  'Finished creating new clusters_dependencies table' AS '';

SELECT
  CURRENT_TIMESTAMP             AS '',
  'Creating new commands table' AS '';

CREATE TABLE `commands` (
  `id`             BIGINT(20) AUTO_INCREMENT                               NOT NULL,
  `created`        DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3)                NOT NULL,
  `updated`        DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3)                NOT NULL ON UPDATE CURRENT_TIMESTAMP(3),
  `entity_version` INT(11) DEFAULT '0'                                     NOT NULL,
  `unique_id`      VARCHAR(255)                                            NOT NULL,
  `name`           VARCHAR(255)                                            NOT NULL,
  `genie_user`     VARCHAR(255)                                            NOT NULL,
  `version`        VARCHAR(255)                                            NOT NULL,
  `description`    TEXT       DEFAULT NULL,
  `setup_file`     BIGINT(20) DEFAULT NULL,
  `executable`     VARCHAR(255)                                            NOT NULL,
  `check_delay`    BIGINT(20) DEFAULT '10000'                              NOT NULL,
  `memory`         INT(11)    DEFAULT NULL,
  `status`         VARCHAR(20) DEFAULT 'INACTIVE'                          NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `COMMANDS_UNIQUE_ID_UNIQUE_INDEX` (`unique_id`),
  KEY `COMMANDS_NAME_INDEX` (`name`),
  KEY `COMMANDS_SETUP_FILE_INDEX` (`setup_file`),
  KEY `COMMANDS_STATUS_INDEX` (`status`),
  CONSTRAINT `COMMANDS_SETUP_FILE_FK` FOREIGN KEY (`setup_file`) REFERENCES `files` (`id`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  DEFAULT COLLATE = utf8_bin
  ROW_FORMAT = DYNAMIC;

SELECT
  CURRENT_TIMESTAMP                      AS '',
  'Finished creating new commands table' AS '';

SELECT
  CURRENT_TIMESTAMP                     AS '',
  'Creating new commands_configs table' AS '';

CREATE TABLE `commands_configs` (
  `command_id` BIGINT(20) NOT NULL,
  `file_id`    BIGINT(20) NOT NULL,
  PRIMARY KEY (`command_id`, `file_id`),
  KEY `COMMANDS_CONFIGS_COMMAND_ID_INDEX` (`command_id`),
  KEY `COMMANDS_CONFIGS_FILE_ID_INDEX` (`file_id`),
  CONSTRAINT `COMMANDS_CONFIGS_COMMAND_ID_FK` FOREIGN KEY (`command_id`) REFERENCES `commands` (`id`)
    ON DELETE CASCADE,
  CONSTRAINT `COMMANDS_CONFIGS_FILE_ID_FK` FOREIGN KEY (`file_id`) REFERENCES `files` (`id`)
    ON DELETE RESTRICT
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  DEFAULT COLLATE = utf8_bin
  ROW_FORMAT = DYNAMIC;

SELECT
  CURRENT_TIMESTAMP                              AS '',
  'Finished creating new commands_configs table' AS '';

SELECT
  CURRENT_TIMESTAMP                          AS '',
  'Creating new commands_dependencies table' AS '';

CREATE TABLE `commands_dependencies` (
  `command_id` BIGINT(20) NOT NULL,
  `file_id`    BIGINT(20) NOT NULL,
  PRIMARY KEY (`command_id`, `file_id`),
  KEY `COMMANDS_DEPENDENCIES_COMMAND_ID_INDEX` (`command_id`),
  KEY `COMMANDS_DEPENDENCIES_FILE_ID_INDEX` (`file_id`),
  CONSTRAINT `COMMANDS_DEPENDENCIES_COMMAND_ID_FK` FOREIGN KEY (`command_id`) REFERENCES `commands` (`id`)
    ON DELETE CASCADE,
  CONSTRAINT `COMMANDS_DEPENDENCIES_FILE_ID_FK` FOREIGN KEY (`file_id`) REFERENCES `files` (`id`)
    ON DELETE RESTRICT
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  DEFAULT COLLATE = utf8_bin
  ROW_FORMAT = DYNAMIC;

SELECT
  CURRENT_TIMESTAMP                                   AS '',
  'Finished creating new commands_dependencies table' AS '';

SELECT
  CURRENT_TIMESTAMP                      AS '',
  'Creating new clusters_commands table' AS '';

CREATE TABLE `clusters_commands` (
  `cluster_id`    BIGINT(20) NOT NULL,
  `command_id`    BIGINT(20) NOT NULL,
  `command_order` INT(11)    NOT NULL,
  PRIMARY KEY (`cluster_id`, `command_id`, `command_order`),
  KEY `CLUSTERS_COMMANDS_CLUSTER_ID_INDEX` (`cluster_id`),
  KEY `CLUSTERS_COMMANDS_COMMAND_ID_INDEX` (`command_id`),
  CONSTRAINT `CLUSTERS_COMMANDS_CLUSTER_ID_FK` FOREIGN KEY (`cluster_id`) REFERENCES `clusters` (`id`)
    ON DELETE CASCADE,
  CONSTRAINT `CLUSTERS_COMMANDS_FILE_ID_FK` FOREIGN KEY (`command_id`) REFERENCES `commands` (`id`)
    ON DELETE RESTRICT
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  DEFAULT COLLATE = utf8_bin
  ROW_FORMAT = DYNAMIC;

SELECT
  CURRENT_TIMESTAMP                               AS '',
  'Finished creating new clusters_commands table' AS '';

SELECT
  CURRENT_TIMESTAMP                               AS '',
  'Creating into new commands_applications table' AS '';

CREATE TABLE `commands_applications` (
  `command_id`        BIGINT(20) NOT NULL,
  `application_id`    BIGINT(20) NOT NULL,
  `application_order` INT(11)    NOT NULL,
  PRIMARY KEY (`command_id`, `application_id`, `application_order`),
  KEY `COMMANDS_APPLICATIONS_APPLICATION_ID_INDEX` (`application_id`),
  KEY `COMMANDS_APPLICATIONS_COMMAND_ID_INDEX` (`command_id`),
  CONSTRAINT `COMMANDS_APPLICATIONS_APPLICATION_ID_FK` FOREIGN KEY (`application_id`) REFERENCES `applications` (`id`)
    ON DELETE RESTRICT,
  CONSTRAINT `COMMANDS_APPLICATIONS_COMMAND_ID_FK` FOREIGN KEY (`command_id`) REFERENCES `commands` (`id`)
    ON DELETE CASCADE
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  DEFAULT COLLATE = utf8_bin
  ROW_FORMAT = DYNAMIC;

SELECT
  CURRENT_TIMESTAMP                                        AS '',
  'Finished creating into new commands_applications table' AS '';

SELECT
  CURRENT_TIMESTAMP         AS '',
  'Creating new jobs table' AS '';

CREATE TABLE `jobs` (
  # common
  `id`                        BIGINT(20) AUTO_INCREMENT                               NOT NULL,
  `created`                   DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3)                NOT NULL,
  `updated`                   DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3)                NOT NULL
  ON UPDATE CURRENT_TIMESTAMP(3),
  `entity_version`            INT(11) DEFAULT '0'                                     NOT NULL,

  # Job Request
  `unique_id`                 VARCHAR(255)                                            NOT NULL,
  `name`                      VARCHAR(255)                                            NOT NULL,
  `genie_user`                VARCHAR(255)                                            NOT NULL,
  `version`                   VARCHAR(255)                                            NOT NULL,
  `command_args`              TEXT          DEFAULT NULL,
  `description`               TEXT          DEFAULT NULL,
  `setup_file`                BIGINT(20)    DEFAULT NULL,
  `tags`                      VARCHAR(1024) DEFAULT NULL,
  `genie_user_group`          VARCHAR(255)  DEFAULT NULL,
  `disable_log_archival`      BIT(1) DEFAULT b'0'                                     NOT NULL,
  `email`                     VARCHAR(255)  DEFAULT NULL,
  `cpu_requested`             INT(11)       DEFAULT NULL,
  `memory_requested`          INT(11)       DEFAULT NULL,
  `timeout_requested`         INT(11)       DEFAULT NULL,
  `grouping`                  VARCHAR(255)  DEFAULT NULL,
  `grouping_instance`         VARCHAR(255)  DEFAULT NULL,

  # Job Metadata
  `client_host`               VARCHAR(255)  DEFAULT NULL,
  `user_agent`                VARCHAR(1024) DEFAULT NULL,
  `num_attachments`           INT(11)       DEFAULT NULL,
  `total_size_of_attachments` BIGINT(20)    DEFAULT NULL,
  `std_out_size`              BIGINT(20)    DEFAULT NULL,
  `std_err_size`              BIGINT(20)    DEFAULT NULL,

  # Job
  `command_id`                BIGINT(20)    DEFAULT NULL,
  `command_name`              VARCHAR(255)  DEFAULT NULL,
  `cluster_id`                BIGINT(20)    DEFAULT NULL,
  `cluster_name`              VARCHAR(255)  DEFAULT NULL,
  `started`                   DATETIME(3)   DEFAULT NULL,
  `finished`                  DATETIME(3)   DEFAULT NULL,
  `status`                    VARCHAR(20) DEFAULT 'INIT'                              NOT NULL,
  `status_msg`                VARCHAR(255)  DEFAULT NULL,

  # Job Execution
  `host_name`                 VARCHAR(255)                                            NOT NULL,
  `process_id`                INT(11)       DEFAULT NULL,
  `exit_code`                 INT(11)       DEFAULT NULL,
  `check_delay`               BIGINT(20)    DEFAULT NULL,
  `timeout`                   DATETIME(3)   DEFAULT NULL,
  `memory_used`               INT(11)       DEFAULT NULL,

  # Post Job Info
  `archive_location`          VARCHAR(1024) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `JOBS_UNIQUE_ID_UNIQUE_INDEX` (`unique_id`),
  KEY `JOBS_CLUSTER_ID_INDEX` (`cluster_id`),
  KEY `JOBS_CLUSTER_NAME_INDEX` (`cluster_name`),
  KEY `JOBS_COMMAND_ID_INDEX` (`command_id`),
  KEY `JOBS_COMMAND_NAME_INDEX` (`command_name`),
  KEY `JOBS_CREATED_INDEX` (`created`),
  KEY `JOBS_FINISHED_INDEX` (`finished`),
  KEY `JOBS_GROUPING_INDEX` (`grouping`),
  KEY `JOBS_GROUPING_INSTANCE_INDEX` (`grouping_instance`),
  KEY `JOBS_NAME_INDEX` (`name`),
  KEY `JOBS_SETUP_FILE_INDEX` (`setup_file`),
  KEY `JOBS_STARTED_INDEX` (`started`),
  KEY `JOBS_STATUS_INDEX` (`status`),
  KEY `JOBS_TAGS_INDEX` (`tags`),
  KEY `JOBS_USER_INDEX` (`genie_user`),
  CONSTRAINT `JOBS_CLUSTER_ID_FK` FOREIGN KEY (`cluster_id`) REFERENCES `clusters` (`id`)
    ON DELETE RESTRICT,
  CONSTRAINT `JOBS_COMMAND_ID_FK` FOREIGN KEY (`command_id`) REFERENCES `commands` (`id`)
    ON DELETE RESTRICT,
  CONSTRAINT `JOBS_SETUP_FILE_ID_FK` FOREIGN KEY (`setup_file`) REFERENCES `files` (`id`)
    ON DELETE RESTRICT
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  DEFAULT COLLATE = utf8_bin
  ROW_FORMAT = DYNAMIC;

SELECT
  CURRENT_TIMESTAMP                  AS '',
  'Finished creating new jobs table' AS '';

SELECT
  CURRENT_TIMESTAMP                      AS '',
  'Creating new jobs_applications table' AS '';

CREATE TABLE `jobs_applications` (
  `job_id`            BIGINT(20) NOT NULL,
  `application_id`    BIGINT(20) NOT NULL,
  `application_order` INT(11)    NOT NULL,
  PRIMARY KEY (`job_id`, `application_id`, `application_order`),
  KEY `JOBS_APPLICATIONS_APPLICATION_ID_INDEX` (`application_id`),
  KEY `JOBS_APPLICATIONS_JOB_ID_INDEX` (`job_id`),
  CONSTRAINT `JOBS_APPLICATIONS_APPLICATION_ID_FK` FOREIGN KEY (`application_id`) REFERENCES `applications` (`id`)
    ON DELETE RESTRICT,
  CONSTRAINT `JOBS_APPLICATIONS_JOB_ID_FK` FOREIGN KEY (`job_id`) REFERENCES `jobs` (`id`)
    ON DELETE CASCADE
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  DEFAULT COLLATE = utf8_bin
  ROW_FORMAT = DYNAMIC;

SELECT
  CURRENT_TIMESTAMP                               AS '',
  'Finished creating new jobs_applications table' AS '';

SELECT
  CURRENT_TIMESTAMP                  AS '',
  'Creating applications_tags table' AS '';

CREATE TABLE `applications_tags` (
  `application_id` BIGINT(20) NOT NULL,
  `tag_id`         BIGINT(20) NOT NULL,
  PRIMARY KEY (`application_id`, `tag_id`),
  KEY `APPLICATIONS_TAGS_APPLICATION_ID_INDEX` (`application_id`),
  KEY `APPLICATIONS_TAGS_TAG_ID_INDEX` (tag_id),
  CONSTRAINT `APPLICATIONS_TAGS_APPLICATION_ID_FK` FOREIGN KEY (`application_id`) REFERENCES `applications` (`id`)
    ON DELETE CASCADE,
  CONSTRAINT `APPLICATIONS_TAGS_TAG_ID_FK` FOREIGN KEY (`tag_id`) REFERENCES `tags` (`id`)
    ON DELETE RESTRICT
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  DEFAULT COLLATE = utf8_bin
  ROW_FORMAT DYNAMIC;

SELECT
  CURRENT_TIMESTAMP                           AS '',
  'Finished creating applications_tags table' AS '';

SELECT
  CURRENT_TIMESTAMP              AS '',
  'Creating clusters_tags table' AS '';

CREATE TABLE `clusters_tags` (
  `cluster_id` BIGINT(20) NOT NULL,
  `tag_id`     BIGINT(20) NOT NULL,
  PRIMARY KEY (`cluster_id`, `tag_id`),
  KEY `CLUSTERS_TAGS_CLUSTER_ID_INDEX` (`cluster_id`),
  KEY `CLUSTERS_TAGS_TAG_ID_INDEX` (tag_id),
  CONSTRAINT `CLUSTERS_TAGS_CLUSTER_ID_FK` FOREIGN KEY (`cluster_id`) REFERENCES `clusters` (`id`)
    ON DELETE CASCADE,
  CONSTRAINT `CLUSTERS_TAGS_TAG_ID_FK` FOREIGN KEY (`tag_id`) REFERENCES `tags` (`id`)
    ON DELETE RESTRICT
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  DEFAULT COLLATE = utf8_bin
  ROW_FORMAT DYNAMIC;

SELECT
  CURRENT_TIMESTAMP                       AS '',
  'Finished creating clusters_tags table' AS '';

SELECT
  CURRENT_TIMESTAMP              AS '',
  'Creating commands_tags table' AS '';

CREATE TABLE `commands_tags` (
  `command_id` BIGINT(20) NOT NULL,
  `tag_id`     BIGINT(20) NOT NULL,
  PRIMARY KEY (`command_id`, `tag_id`),
  KEY `COMMANDS_TAGS_COMMAND_ID_INDEX` (`command_id`),
  KEY `COMMANDS_TAGS_TAG_ID_INDEX` (tag_id),
  CONSTRAINT `COMMANDS_TAGS_COMMAND_ID_FK` FOREIGN KEY (`command_id`) REFERENCES `commands` (`id`)
    ON DELETE CASCADE,
  CONSTRAINT `COMMANDS_TAGS_TAG_ID_FK` FOREIGN KEY (`tag_id`) REFERENCES `tags` (`id`)
    ON DELETE RESTRICT
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  DEFAULT COLLATE = utf8_bin
  ROW_FORMAT DYNAMIC;

SELECT
  CURRENT_TIMESTAMP                       AS '',
  'Finished creating commands_tags table' AS '';

SELECT
  CURRENT_TIMESTAMP          AS '',
  'Creating jobs_tags table' AS '';

CREATE TABLE `jobs_tags` (
  `job_id` BIGINT(20) NOT NULL,
  `tag_id` BIGINT(20) NOT NULL,
  PRIMARY KEY (`job_id`, `tag_id`),
  KEY `JOBS_TAGS_JOB_ID_INDEX` (`job_id`),
  KEY `JOBS_TAGS_TAG_ID_INDEX` (`tag_id`),
  CONSTRAINT `JOBS_TAGS_JOB_ID_FK` FOREIGN KEY (`job_id`) REFERENCES `jobs` (`id`)
    ON DELETE CASCADE,
  CONSTRAINT `JOBS_TAGS_TAG_ID_FK` FOREIGN KEY (`tag_id`) REFERENCES `tags` (`id`)
    ON DELETE RESTRICT
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  DEFAULT COLLATE = utf8_bin
  ROW_FORMAT DYNAMIC;

SELECT
  CURRENT_TIMESTAMP                  AS '',
  'Finished creating job_tags table' AS '';

SELECT
  CURRENT_TIMESTAMP                  AS '',
  'Creating cluster_criterias table' AS '';

CREATE TABLE `cluster_criterias` (
  `id`             BIGINT(20) AUTO_INCREMENT                               NOT NULL,
  `created`        DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3)                NOT NULL,
  `updated`        DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3)                NOT NULL ON UPDATE CURRENT_TIMESTAMP(3),
  `entity_version` INT(11) DEFAULT '0'                                     NOT NULL,
  `job_id`         BIGINT(20)                                              NOT NULL,
  `priority_order` INT(11)                                                 NOT NULL,
  PRIMARY KEY (`id`),
  KEY `CLUSTER_CRITERIAS_JOB_ID_INDEX` (`job_id`),
  CONSTRAINT `CLUSTER_CRITERIAS_JOB_ID_FK` FOREIGN KEY (`job_id`) REFERENCES `jobs` (`id`)
    ON DELETE CASCADE
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  DEFAULT COLLATE = utf8_bin
  ROW_FORMAT = DYNAMIC;

SELECT
  CURRENT_TIMESTAMP                 AS '',
  'Created cluster_criterias table' AS '';

SELECT
  CURRENT_TIMESTAMP                       AS '',
  'Creating cluster_criterias_tags table' AS '';

CREATE TABLE `cluster_criterias_tags` (
  `cluster_criteria_id` BIGINT(20) NOT NULL,
  `tag_id`              BIGINT(20) NOT NULL,
  PRIMARY KEY (`cluster_criteria_id`, `tag_id`),
  KEY `CLUSTER_CRITERIAS_TAGS_CLUSTER_CRITERIA_ID_INDEX` (`cluster_criteria_id`),
  KEY `CLUSTER_CRITERIAS_TAGS_TAG_ID_INDEX` (`tag_id`),
  CONSTRAINT `CLUSTER_CRITERIAS_CRITERIA_TAGS_CLUSTER_CRITERIA_ID_FK` FOREIGN KEY (`cluster_criteria_id`)
  REFERENCES `cluster_criterias` (`id`)
    ON DELETE CASCADE,
  CONSTRAINT `CLUSTER_CRITERIAS_TAGS_TAG_ID_FK` FOREIGN KEY (`tag_id`) REFERENCES `tags` (`id`)
    ON DELETE RESTRICT
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  DEFAULT COLLATE = utf8_bin
  ROW_FORMAT = DYNAMIC;

SELECT
  CURRENT_TIMESTAMP                      AS '',
  'Created cluster_criterias_tags table' AS '';

SELECT
  CURRENT_TIMESTAMP                           AS '',
  'Creating job_applications_requested table' AS '';

-- NOTE: Don't think we need to applications foreign key here cause user could request some bad apps
CREATE TABLE `job_applications_requested` (
  `job_id`            BIGINT(20)   NOT NULL,
  `application_id`    VARCHAR(255) NOT NULL,
  `application_order` INT(11)      NOT NULL,
  PRIMARY KEY (`job_id`, `application_id`, `application_order`),
  KEY `JOB_APPLICATIONS_REQUESTED_APPLICATION_ID_INDEX` (`application_id`),
  KEY `JOB_APPLICATIONS_REQUESTED_JOB_ID_INDEX` (`job_id`),
  CONSTRAINT `JOB_APPLICATIONS_REQUESTED_JOB_ID_FK` FOREIGN KEY (`job_id`) REFERENCES `jobs` (`id`)
    ON DELETE CASCADE
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  DEFAULT COLLATE = utf8_bin
  ROW_FORMAT = DYNAMIC;

SELECT
  CURRENT_TIMESTAMP                          AS '',
  'Created job_applications_requested table' AS '';

SELECT
  CURRENT_TIMESTAMP                          AS '',
  'Creating job_command_criteria_tags table' AS '';

CREATE TABLE `job_command_criteria_tags` (
  `job_id` BIGINT(20) NOT NULL,
  `tag_id` BIGINT(20) NOT NULL,
  PRIMARY KEY (`job_id`, `tag_id`),
  KEY `JOB_COMMAND_CRITERIA_TAGS_JOB_ID_INDEX` (`job_id`),
  KEY `JOB_COMMAND_CRITERIA_TAGS_TAG_ID_INDEX` (`tag_id`),
  CONSTRAINT `JOB_COMMAND_CRITERIA_TAGS_JOB_ID_FK` FOREIGN KEY (`job_id`) REFERENCES `jobs` (`id`)
    ON DELETE CASCADE,
  CONSTRAINT `JOB_COMMAND_CRITERIA_TAGS_TAG_ID_FK` FOREIGN KEY (`tag_id`) REFERENCES `tags` (`id`)
    ON DELETE RESTRICT
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  DEFAULT COLLATE = utf8_bin
  ROW_FORMAT = DYNAMIC;

SELECT
  CURRENT_TIMESTAMP                         AS '',
  'Created job_command_criteria_tags table' AS '';

SELECT
  CURRENT_TIMESTAMP             AS '',
  'Creating jobs_configs table' AS '';

CREATE TABLE `jobs_configs` (
  `job_id`  BIGINT(20) NOT NULL,
  `file_id` BIGINT(20) NOT NULL,
  PRIMARY KEY (`job_id`, `file_id`),
  KEY `JOBS_CONFIGS_JOB_ID_INDEX` (`job_id`),
  KEY `JOBS_CONFIGS_FILE_ID_INDEX` (`file_id`),
  CONSTRAINT `JOBS_CONFIGS_JOB_ID_FK` FOREIGN KEY (`job_id`) REFERENCES `jobs` (`id`)
    ON DELETE CASCADE,
  CONSTRAINT `JOBS_CONFIGS_FILE_ID_FK` FOREIGN KEY (`file_id`) REFERENCES `files` (`id`)
    ON DELETE RESTRICT
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  DEFAULT COLLATE = utf8_bin
  ROW_FORMAT = DYNAMIC;

SELECT
  CURRENT_TIMESTAMP            AS '',
  'Created jobs_configs table' AS '';

SELECT
  CURRENT_TIMESTAMP                  AS '',
  'Creating jobs_dependencies table' AS '';

CREATE TABLE `jobs_dependencies` (
  `job_id`  BIGINT(20) NOT NULL,
  `file_id` BIGINT(20) NOT NULL,
  PRIMARY KEY (`job_id`, `file_id`),
  KEY `JOBS_DEPENDENCIES_JOB_ID_INDEX` (`job_id`),
  KEY `JOBS_DEPENDENCIES_FILE_ID_INDEX` (`file_id`),
  CONSTRAINT `JOBS_DEPENDENCIES_JOB_ID_FK` FOREIGN KEY (`job_id`) REFERENCES `jobs` (`id`)
    ON DELETE CASCADE,
  CONSTRAINT `JOBS_DEPENDENCIES_FILE_ID_FK` FOREIGN KEY (`file_id`) REFERENCES `files` (`id`)
    ON DELETE RESTRICT
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  DEFAULT COLLATE = utf8_bin
  ROW_FORMAT = DYNAMIC;

SELECT
  CURRENT_TIMESTAMP                 AS '',
  'Created jobs_dependencies table' AS '';

SELECT
  CURRENT_TIMESTAMP                             AS '',
  'Finished upgrading database schema to 3.3.0' AS '';

SELECT
  CURRENT_TIMESTAMP                                              AS '',
  'Beginning to load data from old 3.2.0 tables to 3.3.0 tables' AS '';

SELECT
  CURRENT_TIMESTAMP                      AS '',
  'Loading data into applications table' AS '';

INSERT INTO `applications` (
  `created`,
  `updated`,
  `entity_version`,
  `unique_id`,
  `name`,
  `genie_user`,
  `version`,
  `description`,
  `status`,
  `type`
) SELECT
    `created`,
    `updated`,
    `entity_version`,
    `id`,
    `name`,
    `genie_user`,
    `version`,
    `description`,
    `status`,
    `type`
  FROM `applications_320`;

DELIMITER $$
CREATE PROCEDURE GENIE_SPLIT_APPLICATIONS_320()
  BEGIN
    DECLARE `done` INT DEFAULT FALSE;
    DECLARE `old_application_id` VARCHAR(255)
    CHARSET utf8;
    DECLARE `new_application_id` BIGINT(20);
    DECLARE `application_tags` VARCHAR(10000)
    CHARSET utf8;
    DECLARE `old_setup_file` VARCHAR(1024)
    CHARSET utf8;
    DECLARE `found_tag_id` BIGINT(20);

    DECLARE `applications_cursor` CURSOR FOR
      SELECT
        `id`,
        `tags`,
        `setup_file`
      FROM `applications_320`;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    START TRANSACTION;
    OPEN `applications_cursor`;
    READ_LOOP: LOOP
      SET `done` = FALSE;

      FETCH `applications_cursor`
      INTO `old_application_id`, `application_tags`, `old_setup_file`;

      IF `done`
      THEN
        LEAVE READ_LOOP;
      END IF;

      SELECT `a`.`id`
      INTO `new_application_id`
      FROM `applications` `a`
      WHERE `a`.`unique_id` = `old_application_id` COLLATE utf8_bin;

      IF `old_setup_file` IS NOT NULL
      THEN
        INSERT IGNORE INTO `files` (`file`) VALUES (`old_setup_file`);

        SELECT `f`.`id`
        INTO @file_id
        FROM `files` `f`
        WHERE `f`.`file` = `old_setup_file`;

        UPDATE `applications`
        SET `setup_file` = @file_id
        WHERE `id` = `new_application_id`;
      END IF;

      SET @tags_local = `application_tags`;
      TAGS_LOOP: WHILE LENGTH(@tags_local) > 0 DO
        # Tear off the leading |
        SET @tags_local = TRIM(LEADING '|' FROM @tags_local);
        SET @application_tag = SUBSTRING_INDEX(@tags_local, '|', 1);
        SET @tags_local = TRIM(LEADING @application_tag FROM @tags_local);
        SET @tags_local = TRIM(LEADING '|' FROM @tags_local);

        INSERT IGNORE INTO `tags` (`tag`) VALUES (@application_tag);

        SELECT `t`.`id`
        INTO `found_tag_id`
        FROM `tags` `t`
        WHERE `t`.`tag` = @application_tag;

        INSERT INTO `applications_tags` VALUES (`new_application_id`, `found_tag_id`);
      END WHILE TAGS_LOOP;

    END LOOP READ_LOOP;
    CLOSE `applications_cursor`;
    COMMIT;
  END;
$$
DELIMITER ;

CALL GENIE_SPLIT_APPLICATIONS_320();
DROP PROCEDURE GENIE_SPLIT_APPLICATIONS_320;

SELECT
  CURRENT_TIMESTAMP                     AS '',
  'Loaded data into applications table' AS '';

SELECT
  CURRENT_TIMESTAMP                                AS '',
  'Inserting data into applications_configs table' AS '';

DELIMITER $$
CREATE PROCEDURE GENIE_LOAD_APPLICATION_CONFIGS_320()
  BEGIN
    DECLARE `done` INT DEFAULT FALSE;
    DECLARE `old_application_id` VARCHAR(255)
    CHARSET utf8;
    DECLARE `config_file` VARCHAR(1024)
    CHARSET utf8;
    DECLARE `new_application_id` BIGINT(20);

    DECLARE `configs_cursor` CURSOR FOR
      SELECT
        `application_id`,
        `config`
      FROM `application_configs_320`;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    START TRANSACTION;
    OPEN `configs_cursor`;
    READ_LOOP: LOOP
      SET `done` = FALSE;

      FETCH `configs_cursor`
      INTO `old_application_id`, `config_file`;

      IF `done`
      THEN
        LEAVE READ_LOOP;
      END IF;

      SELECT `a`.`id`
      INTO `new_application_id`
      FROM `applications` `a`
      WHERE `a`.`unique_id` = `old_application_id` COLLATE utf8_bin;

      INSERT IGNORE INTO `files` (`file`) VALUES (`config_file`);

      SELECT `f`.`id`
      INTO @file_id
      FROM `files` `f`
      WHERE `f`.`file` = `config_file`;

      INSERT INTO `applications_configs` VALUES (`new_application_id`, @file_id);
    END LOOP READ_LOOP;
    CLOSE `configs_cursor`;
    COMMIT;
  END;
$$
DELIMITER ;

CALL GENIE_LOAD_APPLICATION_CONFIGS_320();
DROP PROCEDURE GENIE_LOAD_APPLICATION_CONFIGS_320;

SELECT
  CURRENT_TIMESTAMP                                         AS '',
  'Finished inserting data into applications_configs table' AS '';

SELECT
  CURRENT_TIMESTAMP                                     AS '',
  'Inserting data into applications_dependencies table' AS '';

DELIMITER $$
CREATE PROCEDURE GENIE_LOAD_APPLICATION_DEPENDENCIES_320()
  BEGIN
    DECLARE `done` INT DEFAULT FALSE;
    DECLARE `old_application_id` VARCHAR(255)
    CHARSET utf8;
    DECLARE `dependency_file` VARCHAR(1024)
    CHARSET utf8;
    DECLARE `new_application_id` BIGINT(20);

    DECLARE `dependencies_cursor` CURSOR FOR
      SELECT
        `application_id`,
        `dependency`
      FROM `application_dependencies_320`;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    START TRANSACTION;
    OPEN `dependencies_cursor`;
    READ_LOOP: LOOP
      SET `done` = FALSE;

      FETCH `dependencies_cursor`
      INTO `old_application_id`, `dependency_file`;

      IF `done`
      THEN
        LEAVE READ_LOOP;
      END IF;

      SELECT `a`.`id`
      INTO `new_application_id`
      FROM `applications` `a`
      WHERE `a`.`unique_id` = `old_application_id` COLLATE utf8_bin;

      INSERT IGNORE INTO `files` (`file`) VALUES (`dependency_file`);

      SELECT `f`.`id`
      INTO @file_id
      FROM `files` `f`
      WHERE `f`.`file` = `dependency_file`;

      INSERT INTO `applications_dependencies` VALUES (`new_application_id`, @file_id);
    END LOOP READ_LOOP;
    CLOSE `dependencies_cursor`;
    COMMIT;
  END;
$$
DELIMITER ;

CALL GENIE_LOAD_APPLICATION_DEPENDENCIES_320();
DROP PROCEDURE GENIE_LOAD_APPLICATION_DEPENDENCIES_320;

SELECT
  CURRENT_TIMESTAMP                                              AS '',
  'Finished inserting data into applications_dependencies table' AS '';

SELECT
  CURRENT_TIMESTAMP                  AS '',
  'Loading data into clusters table' AS '';

INSERT INTO `clusters` (
  `created`,
  `updated`,
  `entity_version`,
  `unique_id`,
  `name`,
  `genie_user`,
  `version`,
  `description`,
  `status`
) SELECT
    `created`,
    `updated`,
    `entity_version`,
    `id`,
    `name`,
    `genie_user`,
    `version`,
    `description`,
    `status`
  FROM `clusters_320`;

DELIMITER $$
CREATE PROCEDURE GENIE_SPLIT_CLUSTERS_320()
  BEGIN
    DECLARE `done` INT DEFAULT FALSE;
    DECLARE `old_cluster_id` VARCHAR(255)
    CHARSET utf8;
    DECLARE `cluster_tags` VARCHAR(10000)
    CHARSET utf8;
    DECLARE `old_setup_file` VARCHAR(1024)
    CHARSET utf8;
    DECLARE `new_cluster_id` BIGINT(20);
    DECLARE `found_tag_id` BIGINT(20);

    DECLARE `clusters_cursor` CURSOR FOR
      SELECT
        `id`,
        `tags`,
        `setup_file`
      FROM `clusters_320`;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    START TRANSACTION;
    OPEN `clusters_cursor`;
    READ_LOOP: LOOP
      SET `done` = FALSE;

      FETCH `clusters_cursor`
      INTO `old_cluster_id`, `cluster_tags`, `old_setup_file`;

      IF `done`
      THEN
        LEAVE READ_LOOP;
      END IF;

      SELECT `c`.`id`
      INTO `new_cluster_id`
      FROM `clusters` `c`
      WHERE `c`.`unique_id` = `old_cluster_id` COLLATE utf8_bin;

      IF `old_setup_file` IS NOT NULL
      THEN
        INSERT IGNORE INTO `files` (`file`) VALUES (`old_setup_file`);

        SELECT `f`.`id`
        INTO @file_id
        FROM `files` `f`
        WHERE `f`.`file` = `old_setup_file`;

        UPDATE `clusters`
        SET `setup_file` = @file_id
        WHERE `id` = `new_cluster_id`;
      END IF;

      SET @tags_local = `cluster_tags`;
      TAGS_LOOP: WHILE LENGTH(@tags_local) > 0 DO
        # Tear off the leading |
        SET @tags_local = TRIM(LEADING '|' FROM @tags_local);
        SET @cluster_tag = SUBSTRING_INDEX(@tags_local, '|', 1);
        SET @tags_local = TRIM(LEADING @cluster_tag FROM @tags_local);
        SET @tags_local = TRIM(LEADING '|' FROM @tags_local);

        INSERT IGNORE INTO `tags` (`tag`) VALUES (@cluster_tag);

        SELECT `t`.`id`
        INTO `found_tag_id`
        FROM `tags` `t`
        WHERE `t`.`tag` = @cluster_tag;

        INSERT INTO `clusters_tags` VALUES (`new_cluster_id`, `found_tag_id`);
      END WHILE TAGS_LOOP;

    END LOOP READ_LOOP;
    CLOSE `clusters_cursor`;
    COMMIT;
  END $$

DELIMITER ;

CALL GENIE_SPLIT_CLUSTERS_320();
DROP PROCEDURE GENIE_SPLIT_CLUSTERS_320;

SELECT
  CURRENT_TIMESTAMP                           AS '',
  'Finished loading data into clusters table' AS '';

SELECT
  CURRENT_TIMESTAMP                            AS '',
  'Inserting data into clusters_configs table' AS '';

DELIMITER $$
CREATE PROCEDURE GENIE_LOAD_CLUSTER_CONFIGS_320()
  BEGIN
    DECLARE `done` INT DEFAULT FALSE;
    DECLARE `old_cluster_id` VARCHAR(255)
    CHARSET utf8;
    DECLARE `config_file` VARCHAR(1024)
    CHARSET utf8;
    DECLARE `new_cluster_id` BIGINT(20);

    DECLARE `configs_cursor` CURSOR FOR
      SELECT
        `cluster_id`,
        `config`
      FROM `cluster_configs_320`;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    START TRANSACTION;
    OPEN `configs_cursor`;
    READ_LOOP: LOOP
      SET `done` = FALSE;

      FETCH `configs_cursor`
      INTO `old_cluster_id`, `config_file`;

      IF `done`
      THEN
        LEAVE READ_LOOP;
      END IF;

      SELECT `c`.`id`
      INTO `new_cluster_id`
      FROM `clusters` `c`
      WHERE `c`.`unique_id` = `old_cluster_id` COLLATE utf8_bin;

      INSERT IGNORE INTO `files` (`file`) VALUES (`config_file`);

      SELECT `f`.`id`
      INTO @file_id
      FROM `files` `f`
      WHERE `f`.`file` = `config_file`;

      INSERT INTO `clusters_configs` VALUES (`new_cluster_id`, @file_id);
    END LOOP READ_LOOP;
    CLOSE `configs_cursor`;
    COMMIT;
  END;
$$
DELIMITER ;

CALL GENIE_LOAD_CLUSTER_CONFIGS_320();
DROP PROCEDURE GENIE_LOAD_CLUSTER_CONFIGS_320;

SELECT
  CURRENT_TIMESTAMP                                     AS '',
  'Finished inserting data into clusters_configs table' AS '';

SELECT
  CURRENT_TIMESTAMP                                 AS '',
  'Inserting data into clusters_dependencies table' AS '';

DELIMITER $$
CREATE PROCEDURE GENIE_LOAD_CLUSTER_DEPENDENCIES_320()
  BEGIN
    DECLARE `done` INT DEFAULT FALSE;
    DECLARE `old_cluster_id` VARCHAR(255)
    CHARSET utf8;
    DECLARE `dependency_file` VARCHAR(1024)
    CHARSET utf8;
    DECLARE `new_cluster_id` BIGINT(20);

    DECLARE `dependencies_cursor` CURSOR FOR
      SELECT
        `cluster_id`,
        `dependency`
      FROM `cluster_dependencies_320`;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    START TRANSACTION;
    OPEN `dependencies_cursor`;
    READ_LOOP: LOOP
      SET `done` = FALSE;

      FETCH `dependencies_cursor`
      INTO `old_cluster_id`, `dependency_file`;

      IF `done`
      THEN
        LEAVE READ_LOOP;
      END IF;

      SELECT `c`.`id`
      INTO `new_cluster_id`
      FROM `clusters` `c`
      WHERE `c`.`unique_id` = `old_cluster_id` COLLATE utf8_bin;

      INSERT IGNORE INTO `files` (`file`) VALUES (`dependency_file`);

      SELECT `f`.`id`
      INTO @file_id
      FROM `files` `f`
      WHERE `f`.`file` = `dependency_file`;

      INSERT INTO `clusters_dependencies` VALUES (`new_cluster_id`, @file_id);
    END LOOP READ_LOOP;
    CLOSE `dependencies_cursor`;
    COMMIT;
  END;
$$
DELIMITER ;

CALL GENIE_LOAD_CLUSTER_DEPENDENCIES_320();
DROP PROCEDURE GENIE_LOAD_CLUSTER_DEPENDENCIES_320;

SELECT
  CURRENT_TIMESTAMP                                          AS '',
  'Finished inserting data into clusters_dependencies table' AS '';

SELECT
  CURRENT_TIMESTAMP                  AS '',
  'Loading data into commands table' AS '';

INSERT INTO `commands` (
  `created`,
  `updated`,
  `entity_version`,
  `unique_id`,
  `name`,
  `genie_user`,
  `version`,
  `description`,
  `executable`,
  `check_delay`,
  `memory`,
  `status`
) SELECT
    `created`,
    `updated`,
    `entity_version`,
    `id`,
    `name`,
    `genie_user`,
    `version`,
    `description`,
    `executable`,
    `check_delay`,
    `memory`,
    `status`
  FROM `commands_320`;

DELIMITER $$
CREATE PROCEDURE GENIE_SPLIT_COMMANDS_320()
  BEGIN
    DECLARE `done` INT DEFAULT FALSE;
    DECLARE `old_command_id` VARCHAR(255)
    CHARSET utf8;
    DECLARE `command_tags` VARCHAR(10000)
    CHARSET utf8;
    DECLARE `old_setup_file` VARCHAR(1024)
    CHARSET utf8;
    DECLARE `new_command_id` BIGINT(20);
    DECLARE `found_tag_id` BIGINT(20);

    DECLARE `commands_cursor` CURSOR FOR
      SELECT
        `id`,
        `tags`,
        `setup_file`
      FROM `commands_320`;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    START TRANSACTION;
    OPEN `commands_cursor`;
    READ_LOOP: LOOP
      SET `done` = FALSE;

      FETCH `commands_cursor`
      INTO `old_command_id`, `command_tags`, `old_setup_file`;

      IF `done`
      THEN
        LEAVE READ_LOOP;
      END IF;

      SELECT `c`.`id`
      INTO `new_command_id`
      FROM `commands` `c`
      WHERE `c`.`unique_id` = `old_command_id` COLLATE utf8_bin;

      IF `old_setup_file` IS NOT NULL
      THEN
        INSERT IGNORE INTO `files` (`file`) VALUES (`old_setup_file`);

        SELECT `f`.`id`
        INTO @file_id
        FROM `files` `f`
        WHERE `f`.`file` = `old_setup_file`;

        UPDATE `clusters`
        SET `setup_file` = @file_id
        WHERE `id` = `new_command_id`;
      END IF;

      SET @tags_local = `command_tags`;
      TAGS_LOOP: WHILE LENGTH(@tags_local) > 0 DO
        # Tear off the leading |
        SET @tags_local = TRIM(LEADING '|' FROM @tags_local);
        SET @command_tag = SUBSTRING_INDEX(@tags_local, '|', 1);
        SET @tags_local = TRIM(LEADING @command_tag FROM @tags_local);
        SET @tags_local = TRIM(LEADING '|' FROM @tags_local);

        INSERT IGNORE INTO `tags` (`tag`) VALUES (@command_tag);

        SELECT `t`.`id`
        INTO `found_tag_id`
        FROM `tags` `t`
        WHERE `t`.`tag` = @command_tag;

        INSERT INTO `commands_tags` VALUES (`new_command_id`, `found_tag_id`);
      END WHILE TAGS_LOOP;

    END LOOP READ_LOOP;
    CLOSE `commands_cursor`;
    COMMIT;
  END;
$$

DELIMITER ;

CALL GENIE_SPLIT_COMMANDS_320();
DROP PROCEDURE GENIE_SPLIT_COMMANDS_320;

SELECT
  CURRENT_TIMESTAMP                           AS '',
  'Finished loading data into commands table' AS '';

SELECT
  CURRENT_TIMESTAMP                            AS '',
  'Inserting data into commands_configs table' AS '';

DELIMITER $$
CREATE PROCEDURE GENIE_LOAD_COMMAND_CONFIGS_320()
  BEGIN
    DECLARE `done` INT DEFAULT FALSE;
    DECLARE `old_command_id` VARCHAR(255)
    CHARSET utf8;
    DECLARE `config_file` VARCHAR(1024)
    CHARSET utf8;
    DECLARE `new_command_id` BIGINT(20);

    DECLARE `configs_cursor` CURSOR FOR
      SELECT
        `command_id`,
        `config`
      FROM `command_configs_320`;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    START TRANSACTION;
    OPEN `configs_cursor`;
    READ_LOOP: LOOP
      SET `done` = FALSE;

      FETCH `configs_cursor`
      INTO `old_command_id`, `config_file`;

      IF `done`
      THEN
        LEAVE READ_LOOP;
      END IF;

      SELECT `c`.`id`
      INTO `new_command_id`
      FROM `commands` `c`
      WHERE `c`.`unique_id` = `old_command_id` COLLATE utf8_bin;

      INSERT IGNORE INTO `files` (`file`) VALUES (`config_file`);

      SELECT `f`.`id`
      INTO @file_id
      FROM `files` `f`
      WHERE `f`.`file` = `config_file`;

      INSERT INTO `commands_configs` VALUES (`new_command_id`, @file_id);
    END LOOP READ_LOOP;
    CLOSE `configs_cursor`;
    COMMIT;
  END;
$$
DELIMITER ;

CALL GENIE_LOAD_COMMAND_CONFIGS_320();
DROP PROCEDURE GENIE_LOAD_COMMAND_CONFIGS_320;

SELECT
  CURRENT_TIMESTAMP                                     AS '',
  'Finished inserting data into commands_configs table' AS '';

SELECT
  CURRENT_TIMESTAMP                                 AS '',
  'Inserting data into commands_dependencies table' AS '';

DELIMITER $$
CREATE PROCEDURE GENIE_LOAD_COMMAND_DEPENDENCIES_320()
  BEGIN
    DECLARE `done` INT DEFAULT FALSE;
    DECLARE `old_command_id` VARCHAR(255)
    CHARSET utf8;
    DECLARE `dependency_file` VARCHAR(1024)
    CHARSET utf8;
    DECLARE `new_command_id` BIGINT(20);

    DECLARE `dependencies_cursor` CURSOR FOR
      SELECT
        `command_id`,
        `dependency`
      FROM `command_dependencies_320`;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    START TRANSACTION;
    OPEN `dependencies_cursor`;
    READ_LOOP: LOOP
      SET `done` = FALSE;

      FETCH `dependencies_cursor`
      INTO `old_command_id`, `dependency_file`;

      IF `done`
      THEN
        LEAVE READ_LOOP;
      END IF;

      SELECT `c`.`id`
      INTO `new_command_id`
      FROM `commands` `c`
      WHERE `c`.`unique_id` = `old_command_id` COLLATE utf8_bin;

      INSERT IGNORE INTO `files` (`file`) VALUES (`dependency_file`);

      SELECT `f`.`id`
      INTO @file_id
      FROM `files` `f`
      WHERE `f`.`file` = `dependency_file`;

      INSERT INTO `commands_dependencies` VALUES (`new_command_id`, @file_id);
    END LOOP READ_LOOP;
    CLOSE `dependencies_cursor`;
    COMMIT;
  END;
$$
DELIMITER ;

CALL GENIE_LOAD_COMMAND_DEPENDENCIES_320();
DROP PROCEDURE GENIE_LOAD_COMMAND_DEPENDENCIES_320;

SELECT
  CURRENT_TIMESTAMP                                          AS '',
  'Finished inserting data into commands_dependencies table' AS '';

SELECT
  CURRENT_TIMESTAMP                               AS '',
  'Loading data into new clusters_commands table' AS '';

INSERT INTO `clusters_commands` (`cluster_id`, `command_id`, `command_order`)
  SELECT
    `cl`.`id`,
    `co`.`id`,
    `cc`.`command_order`
  FROM `clusters_commands_320` `cc`
    JOIN `clusters` `cl` ON `cc`.`cluster_id` = `cl`.`unique_id` COLLATE utf8_bin
    JOIN `commands` `co` ON `cc`.`command_id` = `co`.`unique_id` COLLATE utf8_bin;

SELECT
  CURRENT_TIMESTAMP                                        AS '',
  'Finished loading data into new clusters_commands table' AS '';

SELECT
  CURRENT_TIMESTAMP                                   AS '',
  'Loading data into new commands_applications table' AS '';

INSERT INTO `commands_applications` (`command_id`, `application_id`, `application_order`)
  SELECT
    `c`.`id`,
    `a`.`id`,
    `ca`.`application_order`
  FROM `commands_applications_320` `ca`
    JOIN `commands` `c` ON `ca`.`command_id` = `c`.`unique_id` COLLATE utf8_bin
    JOIN `applications` `a` ON `ca`.`application_id` = `a`.`unique_id` COLLATE utf8_bin;

SELECT
  CURRENT_TIMESTAMP                                            AS '',
  'Finished loading data into new commands_applications table' AS '';

SELECT
  CURRENT_TIMESTAMP                                             AS '',
  'Finished loading data from old 3.2.0 tables to 3.3.0 tables' AS '';
