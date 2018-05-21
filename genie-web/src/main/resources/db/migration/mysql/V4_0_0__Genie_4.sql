/*
 *
 *  Copyright 2018 Netflix, Inc.
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

DROP TABLE IF EXISTS
`job_metadata_320`,
`job_executions_320`,
`jobs_applications_320`,
`jobs_320`,
`job_requests_320`,
`application_configs_320`,
`application_dependencies_320`,
`cluster_configs_320`,
`cluster_dependencies_320`,
`command_configs_320`,
`command_dependencies_320`,
`commands_applications_320`,
`clusters_commands_320`,
`applications_320`,
`clusters_320`,
`commands_320`;

CREATE TABLE `command_executable_arguments` (
  `command_id`     BIGINT(20)    NOT NULL,
  `argument`       VARCHAR(1024) NOT NULL,
  `argument_order` INT(11)       NOT NULL,
  PRIMARY KEY (`command_id`, `argument_order`),
  KEY `COMMAND_EXECUTABLE_ARGUMENTS_COMMAND_ID_INDEX` (`command_id`),
  CONSTRAINT `COMMAND_EXECUTABLE_ARGUMENTS_COMMAND_ID_FK` FOREIGN KEY (`command_id`) REFERENCES `commands` (`id`)
    ON DELETE CASCADE
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  DEFAULT COLLATE = utf8_bin
  ROW_FORMAT = DYNAMIC;

DELIMITER $$
CREATE PROCEDURE GENIE_SPLIT_COMMANDS_330()
  BEGIN
    DECLARE `done` INT DEFAULT FALSE;
    DECLARE `command_id` BIGINT(20);
    DECLARE `command_executable` VARCHAR(255);

    DECLARE `commands_cursor` CURSOR FOR
      SELECT
        `id`,
        `executable`
      FROM `commands`;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    START TRANSACTION;
    OPEN `commands_cursor`;
    READ_LOOP: LOOP
      SET `done` = FALSE;

      FETCH `commands_cursor`
      INTO `command_id`, `command_executable`;

      IF `done`
      THEN
        LEAVE READ_LOOP;
      END IF;

      SET @argument_order = 0;
      SET @command_executable_local = `command_executable`;
      COMMAND_EXECUTABLE_LOOP: WHILE LENGTH(@command_executable_local) > 0 DO
        SET @argument = SUBSTRING_INDEX(@command_executable_local, ' ', 1);
        SET @command_executable_local = TRIM(LEADING @argument FROM @command_executable_local);
        SET @command_executable_local = TRIM(LEADING ' ' FROM @command_executable_local);
        IF LENGTH(@argument) > 0
        THEN
          INSERT INTO `command_executable_arguments`
          VALUES (`command_id`, @argument, @argument_order);
          SET @argument_order = @argument_order + 1;
        END IF;
      END WHILE COMMAND_EXECUTABLE_LOOP;

    END LOOP READ_LOOP;
    CLOSE `commands_cursor`;
    COMMIT;
  END;
$$
DELIMITER ;

CALL GENIE_SPLIT_COMMANDS_330();
DROP PROCEDURE GENIE_SPLIT_COMMANDS_330;

ALTER TABLE `applications`
  ADD COLUMN `requested_id` BOOLEAN DEFAULT FALSE NOT NULL;

ALTER TABLE `clusters`
  ADD COLUMN `requested_id` BOOLEAN DEFAULT FALSE NOT NULL;

ALTER TABLE `commands`
  DROP COLUMN `executable`,
  ADD COLUMN `requested_id` BOOLEAN DEFAULT FALSE NOT NULL;

ALTER TABLE `criteria`
  ADD COLUMN `unique_id` VARCHAR(255) DEFAULT NULL,
  ADD COLUMN `name`      VARCHAR(255) DEFAULT NULL,
  ADD COLUMN `version`   VARCHAR(255) DEFAULT NULL,
  ADD COLUMN `status`    VARCHAR(255) DEFAULT NULL;

CREATE TABLE `job_requested_environment_variables` (
  `job_id` BIGINT(20)    NOT NULL,
  `name`   VARCHAR(255)  NOT NULL,
  `value`  VARCHAR(1024) NOT NULL,
  PRIMARY KEY (`job_id`, `name`),
  KEY `JOB_REQUESTED_ENVIRONMENT_VARIABLES_JOB_ID_INDEX` (`job_id`),
  CONSTRAINT `JOB_REQUESTED_ENVIRONMENT_VARIABLES_JOB_ID_FK` FOREIGN KEY (`job_id`) REFERENCES `jobs` (`id`)
    ON DELETE CASCADE
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  DEFAULT COLLATE = utf8_bin
  ROW_FORMAT = DYNAMIC;

CREATE TABLE `job_environment_variables` (
  `job_id` BIGINT(20)    NOT NULL,
  `name`   VARCHAR(255)  NOT NULL,
  `value`  VARCHAR(1024) NOT NULL,
  PRIMARY KEY (`job_id`, `name`),
  KEY `JOB_ENVIRONMENT_VARIABLES_JOB_ID_INDEX` (`job_id`),
  CONSTRAINT `JOB_ENVIRONMENT_VARIABLES_JOB_ID_FK` FOREIGN KEY (`job_id`) REFERENCES `jobs` (`id`)
    ON DELETE CASCADE
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  DEFAULT COLLATE = utf8_bin
  ROW_FORMAT = DYNAMIC;

ALTER TABLE `jobs`
  ADD COLUMN    `interactive`                                BOOLEAN       DEFAULT FALSE NOT NULL,
  ADD COLUMN    `requested_job_directory_location`           VARCHAR(1024) DEFAULT NULL,
  ADD COLUMN    `requested_agent_config_ext`                 TEXT          DEFAULT NULL,
  ADD COLUMN    `requested_agent_environment_ext`            TEXT          DEFAULT NULL,
  CHANGE COLUMN `disable_log_archival` `archiving_disabled`  BOOLEAN       DEFAULT FALSE NOT NULL,
  CHANGE COLUMN `cpu_requested` `requested_cpu`              INT(11)       DEFAULT NULL,
  CHANGE COLUMN `memory_requested` `requested_memory`        INT(11)       DEFAULT NULL,
  CHANGE COLUMN `timeout_requested` `requested_timeout`      INT(11)       DEFAULT NULL,
  CHANGE COLUMN `host_name` `agent_hostname`                 VARCHAR(255)  DEFAULT NULL,
  CHANGE COLUMN `client_host` `request_api_client_hostname`  VARCHAR(255)  DEFAULT NULL,
  CHANGE COLUMN `user_agent` `request_api_client_user_agent` VARCHAR(255)  DEFAULT NULL,
  ADD COLUMN    `request_agent_client_hostname`              VARCHAR(255)  DEFAULT NULL,
  ADD COLUMN    `request_agent_client_version`               VARCHAR(255)  DEFAULT NULL,
  ADD COLUMN    `request_agent_client_pid`                   INT(11)       DEFAULT NULL,
  ALTER COLUMN `status` SET DEFAULT 'RESERVED',
  ADD COLUMN    `requested_id`                               BOOLEAN       DEFAULT FALSE NOT NULL,
  ADD COLUMN    `job_directory_location`                     VARCHAR(1024) DEFAULT NULL,
  ADD COLUMN    `resolved`                                   BOOLEAN       DEFAULT FALSE NOT NULL,
  ADD COLUMN    `agent_version`                              VARCHAR(255)  DEFAULT NULL,
  ADD COLUMN    `agent_pid`                                  INT(11)       DEFAULT NULL,
  ADD COLUMN    `claimed`                                    BOOLEAN       DEFAULT FALSE NOT NULL,
  ADD COLUMN    `v4`                                         BOOLEAN       DEFAULT FALSE NOT NULL;

ALTER TABLE `job_applications_requested` RENAME TO `job_requested_applications`;
ALTER TABLE `job_requested_applications`
  DROP FOREIGN KEY `JOB_APPLICATIONS_REQUESTED_JOB_ID_FK`,
  DROP KEY `JOB_APPLICATIONS_REQUESTED_APPLICATION_ID_INDEX`,
  DROP KEY `JOB_APPLICATIONS_REQUESTED_JOB_ID_INDEX`,
  ADD CONSTRAINT `JOB_REQUESTED_APPLICATIONS_JOB_ID_FK` FOREIGN KEY (`job_id`) REFERENCES `jobs` (`id`)
    ON DELETE CASCADE,
  ADD KEY `JOB_REQUESTED_APPLICATIONS_APPLICATION_ID_INDEX` (`application_id`),
  ADD KEY `JOB_REQUESTED_APPLICATIONS_JOB_ID_INDEX` (`job_id`);
