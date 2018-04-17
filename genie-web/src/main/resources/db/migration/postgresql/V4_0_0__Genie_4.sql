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

ALTER TABLE commands
  DROP COLUMN executable;

ALTER TABLE criteria
  ADD COLUMN unique_id VARCHAR(255) DEFAULT NULL,
  ADD COLUMN name VARCHAR(255) DEFAULT NULL,
  ADD COLUMN version VARCHAR(255) DEFAULT NULL,
  ADD COLUMN status VARCHAR(255) DEFAULT NULL;
