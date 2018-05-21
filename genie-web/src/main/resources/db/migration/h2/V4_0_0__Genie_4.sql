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

CREATE TABLE `command_executable_arguments` (
  `command_id`     BIGINT(20)    NOT NULL,
  `argument`       VARCHAR(1024) NOT NULL,
  `argument_order` INT(11)       NOT NULL,
  PRIMARY KEY (`command_id`, `argument_order`),
  CONSTRAINT `COMMAND_EXECUTABLE_ARGUMENTS_COMMAND_ID_FK` FOREIGN KEY (`command_id`) REFERENCES `commands` (`id`)
  ON DELETE CASCADE
);

CREATE INDEX `COMMAND_EXECUTABLE_ARGUMENTS_COMMAND_ID_INDEX`
  ON `command_executable_arguments` (`command_id`);

DROP ALIAS IF EXISTS SPLIT_COMMAND_EXECUTABLE;

CREATE ALIAS SPLIT_COMMAND_EXECUTABLE AS $$
import java.sql.Connection;
import java.sql.ResultSet;
@CODE
void splitCommandExecutable(final Connection con) throws Exception {
	final ResultSet rs = con.createStatement().executeQuery("SELECT `id`, `executable` FROM `commands`;");

	while (rs.next()) {
	    final long commandId = rs.getLong(1);
	    final String executable = rs.getString(2);
      final String[] arguments = executable.split("\\s");
      for (int i = 0; i < arguments.length; i++) {
          con
              .createStatement()
              .executeUpdate("INSERT INTO `command_executable_arguments` VALUES (" + commandId + ", '" + arguments[i] + "', " + i + ");");
      }
	}
}
$$;

CALL SPLIT_COMMAND_EXECUTABLE();

DROP ALIAS IF EXISTS SPLIT_COMMAND_EXECUTABLE;

ALTER TABLE `applications`
  ADD COLUMN `requested_id` BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE `clusters`
  ADD COLUMN `requested_id` BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE `commands`
  DROP COLUMN `executable`;
ALTER TABLE `commands`
  ADD COLUMN `requested_id` BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE `criteria`
  ADD COLUMN `unique_id` VARCHAR(255) DEFAULT NULL;
ALTER TABLE `criteria`
  ADD COLUMN `name` VARCHAR(255) DEFAULT NULL;
ALTER TABLE `criteria`
  ADD COLUMN `version` VARCHAR(255) DEFAULT NULL;
ALTER TABLE `criteria`
  ADD COLUMN `status` VARCHAR(255) DEFAULT NULL;

CREATE TABLE `job_requested_environment_variables` (
  `job_id` BIGINT(20)    NOT NULL,
  `name`   VARCHAR(255)  NOT NULL,
  `value`  VARCHAR(1024) NOT NULL,
  PRIMARY KEY (`job_id`, `name`),
  CONSTRAINT `JOB_REQUESTED_ENVIRONMENT_VARIABLES_JOB_ID_FK` FOREIGN KEY (`job_id`) REFERENCES `jobs` (`id`)
  ON DELETE CASCADE
);

CREATE INDEX `JOB_REQUESTED_ENVIRONMENT_VARIABLES_JOB_ID_INDEX`
  ON `job_requested_environment_variables` (`job_id`);

CREATE TABLE `job_environment_variables` (
  `job_id` BIGINT(20)    NOT NULL,
  `name`   VARCHAR(255)  NOT NULL,
  `value`  VARCHAR(1024) NOT NULL,
  PRIMARY KEY (`job_id`, `name`),
  CONSTRAINT `JOB_ENVIRONMENT_VARIABLES_JOB_ID_FK` FOREIGN KEY (`job_id`) REFERENCES `jobs` (`id`)
  ON DELETE CASCADE
);

CREATE INDEX `JOB_ENVIRONMENT_VARIABLES_JOB_ID_INDEX`
  ON `job_environment_variables` (`job_id`);

ALTER TABLE `jobs`
  ADD COLUMN `interactive` BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE `jobs`
  ADD COLUMN `requested_job_directory_location` VARCHAR(1024) DEFAULT NULL;
ALTER TABLE `jobs`
  ADD COLUMN `requested_agent_config_ext` TEXT DEFAULT NULL;
ALTER TABLE `jobs`
  ADD COLUMN `requested_agent_environment_ext` TEXT DEFAULT NULL;
ALTER TABLE `jobs`
  ALTER COLUMN `disable_log_archival` RENAME TO `archiving_disabled`;
ALTER TABLE `jobs`
  ALTER COLUMN `cpu_requested` RENAME TO `requested_cpu`;
ALTER TABLE `jobs`
  ALTER COLUMN `memory_requested` RENAME TO `requested_memory`;
ALTER TABLE `jobs`
  ALTER COLUMN `timeout_requested` RENAME TO `requested_timeout`;
ALTER TABLE `jobs`
  ALTER COLUMN `host_name` RENAME TO `agent_hostname`;
ALTER TABLE `jobs`
  ALTER COLUMN `agent_hostname` SET NULL;
ALTER TABLE `jobs`
  ALTER COLUMN `agent_hostname` SET DEFAULT NULL;
ALTER TABLE `jobs`
  ALTER COLUMN `client_host` RENAME TO `request_api_client_hostname`;
ALTER TABLE `jobs`
  ALTER COLUMN `user_agent` RENAME TO `request_api_client_user_agent`;
ALTER TABLE `jobs`
  ADD COLUMN `request_agent_client_hostname` VARCHAR(255) DEFAULT NULL;
ALTER TABLE `jobs`
  ADD COLUMN `request_agent_client_version` VARCHAR(255) DEFAULT NULL;
ALTER TABLE `jobs`
  ADD COLUMN `request_agent_client_pid` INT(11) DEFAULT NULL;
ALTER TABLE `jobs`
  ALTER COLUMN `status` SET DEFAULT 'RESERVED';
ALTER TABLE `jobs`
  ADD COLUMN `requested_id` BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE `jobs`
  ADD COLUMN `job_directory_location` VARCHAR(1024) DEFAULT NULL;
ALTER TABLE `jobs`
  ADD COLUMN `resolved` BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE `jobs`
  ADD COLUMN `agent_version` VARCHAR(255) DEFAULT NULL;
ALTER TABLE `jobs`
  ADD COLUMN `agent_pid` INT(11) DEFAULT NULL;
ALTER TABLE `jobs`
  ADD COLUMN `claimed` BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE `jobs`
  ADD COLUMN `v4` BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE `job_applications_requested`
  RENAME TO `job_requested_applications`;
ALTER TABLE `job_requested_applications`
  RENAME CONSTRAINT `JOB_APPLICATIONS_REQUESTED_JOB_ID_FK` TO `JOB_REQUESTED_APPLICATIONS_JOB_ID_FK`;
ALTER INDEX `JOB_APPLICATIONS_REQUESTED_APPLICATION_ID_INDEX`
  RENAME TO `JOB_REQUESTED_APPLICATIONS_APPLICATION_ID_INDEX`;
ALTER INDEX `JOB_APPLICATIONS_REQUESTED_JOB_ID_INDEX`
  RENAME TO `JOB_REQUESTED_APPLICATIONS_JOB_ID_INDEX`;
