SELECT
  CURRENT_TIMESTAMP,
  'Upgrading database schema for Genie 3.2.0';

SELECT
  CURRENT_TIMESTAMP,
  'Altering application_configs';

ALTER TABLE application_configs
  DROP CONSTRAINT IF EXISTS application_configs_pkey,
  ADD CONSTRAINT application_configs_pkey PRIMARY KEY (application_id, config);

CREATE INDEX application_configs_application_id_index
  ON application_configs USING BTREE (application_id);

SELECT
  CURRENT_TIMESTAMP,
  'Finished altering application_configs';

SELECT
  CURRENT_TIMESTAMP,
  'Altering application_dependencies';

ALTER TABLE application_dependencies
  DROP CONSTRAINT IF EXISTS application_dependencies_pkey,
  ADD CONSTRAINT application_dependencies_pkey PRIMARY KEY (application_id, dependency);

CREATE INDEX application_dependencies_application_id_index
  ON application_dependencies USING BTREE (application_id);

SELECT
  CURRENT_TIMESTAMP,
  'Finished altering application_dependencies';

SELECT
  CURRENT_TIMESTAMP,
  'Altering cluster_configs';

ALTER TABLE cluster_configs
  DROP CONSTRAINT IF EXISTS cluster_configs_pkey,
  ADD CONSTRAINT cluster_configs_pkey PRIMARY KEY (cluster_id, config);

CREATE INDEX cluster_configs_cluster_id_index
  ON cluster_configs USING BTREE (cluster_id);

SELECT
  CURRENT_TIMESTAMP,
  'Finished altering cluster_configs';

SELECT
  CURRENT_TIMESTAMP,
  'Altering cluster_dependencies';

ALTER TABLE cluster_dependencies
  ADD CONSTRAINT cluster_dependency_pkey PRIMARY KEY (cluster_id, dependency);

CREATE INDEX cluster_dependencies_cluster_id_index
  ON cluster_dependencies USING BTREE (cluster_id);

SELECT
  CURRENT_TIMESTAMP,
  'Finished altering cluster_dependencies';

SELECT
  CURRENT_TIMESTAMP,
  'Altering clusters_commands';

ALTER TABLE clusters_commands
  DROP CONSTRAINT IF EXISTS clusters_commands_pkey,
  ADD CONSTRAINT clusters_commands_pkey PRIMARY KEY (cluster_id, command_id, command_order);

CREATE INDEX clusters_commands_cluster_id_index
  ON clusters_commands USING BTREE (cluster_id);

CREATE INDEX clusters_commands_command_id_index
  ON clusters_commands USING BTREE (command_id);

SELECT
  CURRENT_TIMESTAMP,
  'Finished altering clusters_commands';

SELECT
  CURRENT_TIMESTAMP,
  'Altering command_configs';

ALTER TABLE command_configs
  DROP CONSTRAINT IF EXISTS command_configs_pkey,
  ADD CONSTRAINT command_configs_pkey PRIMARY KEY (command_id, config);

CREATE INDEX command_configs_command_id_index
  ON command_configs USING BTREE (command_id);

SELECT
  CURRENT_TIMESTAMP,
  'Finished altering command_configs';

SELECT
  CURRENT_TIMESTAMP,
  'Altering command_dependencies';

ALTER TABLE command_dependencies
  ADD CONSTRAINT command_dependency_pkey PRIMARY KEY (command_id, dependency);

CREATE INDEX command_dependencies_command_id_index
  ON command_dependencies USING BTREE (command_id);

SELECT
  CURRENT_TIMESTAMP,
  'Finished altering command_dependencies';

SELECT
  CURRENT_TIMESTAMP,
  'Altering commands_applications';

ALTER TABLE commands_applications
  DROP CONSTRAINT IF EXISTS commands_applications_pkey,
  ADD CONSTRAINT commands_applications_pkey PRIMARY KEY (command_id, application_id, application_order);

CREATE INDEX commands_applications_command_id_index
  ON commands_applications USING BTREE (command_id);

CREATE INDEX commands_applications_application_id_index
  ON commands_applications USING BTREE (application_id);

SELECT
  CURRENT_TIMESTAMP,
  'Finished altering commands_applications';

SELECT
  CURRENT_TIMESTAMP,
  'Altering job_executions';

ALTER TABLE job_executions
  DROP CONSTRAINT IF EXISTS job_executions_pkey,
  ADD CONSTRAINT job_executions_pkey PRIMARY KEY (id);

DROP INDEX IF EXISTS job_executions_exit_code_index;

SELECT
  CURRENT_TIMESTAMP,
  'Finished altering jobs_executions';

SELECT
  CURRENT_TIMESTAMP,
  'Altering job_metadata';

ALTER TABLE job_metadata
  DROP CONSTRAINT IF EXISTS job_metadata_pkey,
  ADD CONSTRAINT job_metadata_pkey PRIMARY KEY (id);

SELECT
  CURRENT_TIMESTAMP,
  'Finished altering job_metadata';

SELECT
  CURRENT_TIMESTAMP,
  'Altering jobs';

CREATE INDEX jobs_cluster_id_index
  ON jobs USING BTREE (cluster_id);

CREATE INDEX jobs_command_id_index
  ON jobs USING BTREE (command_id);

SELECT
  CURRENT_TIMESTAMP,
  'Finished altering jobs';

SELECT
  CURRENT_TIMESTAMP,
  'Altering jobs_applications';

ALTER TABLE jobs_applications
  DROP CONSTRAINT IF EXISTS jobs_applications_pkey,
  ADD CONSTRAINT jobs_applications_pkey PRIMARY KEY (job_id, application_id, application_order);

CREATE INDEX jobs_applications_job_id_index
  ON jobs_applications USING BTREE (job_id);

CREATE INDEX jobs_applications_application_id_index
  ON jobs_applications USING BTREE (application_id);

SELECT
  CURRENT_TIMESTAMP,
  'Finished altering jobs_applications';

SELECT
  CURRENT_TIMESTAMP,
  'Finished upgrading database schema for Genie 3.2.0';
