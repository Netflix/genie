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
job_metadata_320,
job_executions_320,
jobs_applications_320,
jobs_320,
job_requests_320,
application_configs_320,
application_dependencies_320,
cluster_configs_320,
cluster_dependencies_320,
command_configs_320,
command_dependencies_320,
commands_applications_320,
clusters_commands_320,
applications_320,
clusters_320,
commands_320;

CREATE TABLE command_executable_arguments (
  command_id     BIGINT        NOT NULL,
  argument       VARCHAR(1024) NOT NULL,
  argument_order INT           NOT NULL,
  PRIMARY KEY (command_id, argument_order),
  CONSTRAINT command_executable_arguments_command_id_fkey FOREIGN KEY (command_id) REFERENCES commands (id)
  ON DELETE CASCADE
);

CREATE INDEX command_executable_arguments_command_id_index
  ON command_executable_arguments (command_id);

CREATE OR REPLACE FUNCTION genie_split_commands_330()
  RETURNS VOID AS $$
DECLARE
  command_record   RECORD;
  executable_local VARCHAR(255);
  argument         VARCHAR(1024);
  argument_order   INT;
BEGIN

  << COMMANDS_LOOP >>
  FOR command_record IN
  SELECT
    id,
    executable
  FROM commands
  LOOP

    argument_order = 0;
    executable_local = command_record.executable;
    << COMMAND_ARGS_LOOP >> WHILE LENGTH(executable_local) > 0 LOOP
      argument = SPLIT_PART(executable_local, ' ', 1);
      executable_local = TRIM(LEADING argument FROM executable_local);
      executable_local = TRIM(LEADING ' ' FROM executable_local);
      IF LENGTH(argument) > 0
      THEN
        INSERT INTO command_executable_arguments
        VALUES (command_record.id, argument, argument_order);
        argument_order = argument_order + 1;
      END IF;
    END LOOP COMMAND_ARGS_LOOP;

  END LOOP COMMANDS_LOOP;
END;
$$
LANGUAGE plpgsql;

SELECT genie_split_commands_330();
DROP FUNCTION genie_split_commands_330();

ALTER TABLE applications
  ADD COLUMN requested_id BOOLEAN DEFAULT FALSE NOT NULL;

ALTER TABLE clusters
  ADD COLUMN requested_id BOOLEAN DEFAULT FALSE NOT NULL;

ALTER TABLE commands
  DROP COLUMN executable,
  ADD COLUMN requested_id BOOLEAN DEFAULT FALSE NOT NULL;

ALTER TABLE criteria
  ADD COLUMN unique_id VARCHAR(255) DEFAULT NULL,
  ADD COLUMN name      VARCHAR(255) DEFAULT NULL,
  ADD COLUMN version   VARCHAR(255) DEFAULT NULL,
  ADD COLUMN status    VARCHAR(255) DEFAULT NULL;

CREATE TABLE job_requested_environment_variables (
  job_id     BIGINT        NOT NULL,
  name       VARCHAR(255)  NOT NULL,
  value      VARCHAR(1024) NOT NULL,
  PRIMARY KEY (job_id, name),
  CONSTRAINT job_requested_environment_variables_job_id_fkey FOREIGN KEY (job_id) REFERENCES jobs (id)
  ON DELETE CASCADE
);

CREATE INDEX job_requested_environment_variables_job_id_index
  ON job_requested_environment_variables (job_id);

CREATE TABLE job_environment_variables (
  job_id     BIGINT        NOT NULL,
  name       VARCHAR(255)  NOT NULL,
  value      VARCHAR(1024) NOT NULL,
  PRIMARY KEY (job_id, name),
  CONSTRAINT job_environment_variables_job_id_fkey FOREIGN KEY (job_id) REFERENCES jobs (id)
  ON DELETE CASCADE
);

CREATE INDEX job_environment_variables_job_id_index
  ON job_environment_variables (job_id);

ALTER TABLE jobs RENAME COLUMN disable_log_archival TO archiving_disabled;
ALTER TABLE jobs RENAME COLUMN cpu_requested        TO requested_cpu;
ALTER TABLE jobs RENAME COLUMN memory_requested     TO requested_memory;
ALTER TABLE jobs RENAME COLUMN timeout_requested    TO requested_timeout;
ALTER TABLE jobs RENAME COLUMN host_name            TO agent_hostname;
ALTER TABLE jobs RENAME COLUMN client_host          TO request_api_client_hostname;
ALTER TABLE jobs RENAME COLUMN user_agent           TO request_api_client_user_agent;

ALTER TABLE jobs
  ADD COLUMN interactive                      BOOLEAN       DEFAULT FALSE NOT NULL,
  ADD COLUMN requested_job_directory_location VARCHAR(1024) DEFAULT NULL,
  ADD COLUMN requested_agent_config_ext       TEXT          DEFAULT NULL,
  ADD COLUMN requested_agent_environment_ext  TEXT          DEFAULT NULL,
  ALTER COLUMN agent_hostname DROP NOT NULL,
  ALTER COLUMN agent_hostname SET DEFAULT NULL,
  ADD COLUMN request_agent_client_hostname    VARCHAR(255)  DEFAULT NULL,
  ADD COLUMN request_agent_client_version     VARCHAR(255)  DEFAULT NULL,
  ADD COLUMN request_agent_client_pid         INT           DEFAULT NULL,
  ALTER COLUMN status SET DEFAULT 'RESERVED',
  ADD COLUMN requested_id                     BOOLEAN       DEFAULT FALSE NOT NULL,
  ADD COLUMN job_directory_location           VARCHAR(1024) DEFAULT NULL,
  ADD COLUMN resolved                         BOOLEAN       DEFAULT FALSE NOT NULL,
  ADD COLUMN agent_version                    VARCHAR(255)  DEFAULT NULL,
  ADD COLUMN agent_pid                        INT           DEFAULT NULL,
  ADD COLUMN claimed                          BOOLEAN       DEFAULT FALSE NOT NULL,
  ADD COLUMN v4                               BOOLEAN       DEFAULT FALSE NOT NULL;

ALTER TABLE job_applications_requested RENAME TO job_requested_applications;
ALTER TABLE job_requested_applications
  DROP CONSTRAINT job_applications_requested_job_id_fkey,
  ADD CONSTRAINT job_requested_applications_job_id_fkey FOREIGN KEY (job_id) REFERENCES jobs (id) ON DELETE CASCADE;

DROP INDEX job_applications_requested_application_id_index;
CREATE INDEX job_requested_applications_application_id_index
  ON job_requested_applications (application_id);
DROP INDEX job_applications_requested_job_id_index;
CREATE INDEX job_requested_applications_job_id_index
  ON job_requested_applications (job_id);

CREATE TABLE agent_connections (
  id              BIGSERIAL                                    NOT NULL,
  created         TIMESTAMP(3) WITHOUT TIME ZONE DEFAULT now() NOT NULL,
  updated         TIMESTAMP(3) WITHOUT TIME ZONE DEFAULT now() NOT NULL,
  entity_version  INTEGER      DEFAULT '0'                     NOT NULL,
  job_id          VARCHAR(255)                                 NOT NULL,
  server_hostname VARCHAR(255)                                 NOT NULL,
  PRIMARY KEY (id)
);

CREATE UNIQUE INDEX agent_connections_job_id_index
  ON agent_connections (job_id);
