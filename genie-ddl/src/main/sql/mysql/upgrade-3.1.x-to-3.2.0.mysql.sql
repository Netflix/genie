SELECT
  CURRENT_TIMESTAMP                           AS '',
  'Upgrading database schema for Genie 3.2.0' AS '';

SELECT
  CURRENT_TIMESTAMP       AS '',
  'Dropping tag indicies' AS '';

ALTER TABLE `applications`
  DROP KEY `APPLICATIONS_TAGS_INDEX`;
ALTER TABLE `clusters`
  DROP KEY `CLUSTERS_TAG_INDEX`;
ALTER TABLE `commands`
  DROP KEY `COMMANDS_TAGS_INDEX`;
ALTER TABLE `jobs`
  DROP KEY `JOBS_TAGS_INDEX`;

SELECT
  CURRENT_TIMESTAMP                AS '',
  'Finished dropping tag indicies' AS '';

SELECT
  CURRENT_TIMESTAMP                        AS '',
  'Ensuring table row formats are DYNAMIC' AS '';

ALTER TABLE `application_configs`
  ROW_FORMAT = DYNAMIC;
ALTER TABLE `application_dependencies`
  ROW_FORMAT = DYNAMIC;
ALTER TABLE `applications`
  ROW_FORMAT = DYNAMIC;
ALTER TABLE `cluster_configs`
  ROW_FORMAT = DYNAMIC;
ALTER TABLE `cluster_dependencies`
  ROW_FORMAT = DYNAMIC;
ALTER TABLE `clusters`
  ROW_FORMAT = DYNAMIC;
ALTER TABLE `clusters_commands`
  ROW_FORMAT = DYNAMIC;
ALTER TABLE `command_configs`
  ROW_FORMAT = DYNAMIC;
ALTER TABLE `command_dependencies`
  ROW_FORMAT = DYNAMIC;
ALTER TABLE `commands`
  ROW_FORMAT = DYNAMIC;
ALTER TABLE `commands_applications`
  ROW_FORMAT = DYNAMIC;
ALTER TABLE `job_executions`
  ROW_FORMAT = DYNAMIC;
ALTER TABLE `job_metadata`
  ROW_FORMAT = DYNAMIC;
ALTER TABLE `jobs`
  ROW_FORMAT = DYNAMIC;
ALTER TABLE `job_requests`
  ROW_FORMAT = DYNAMIC;
ALTER TABLE `jobs_applications`
  ROW_FORMAT = DYNAMIC;

SELECT
  CURRENT_TIMESTAMP                           AS '',
  'Finished switching row formats to DYNAMIC' AS '';

SELECT
  CURRENT_TIMESTAMP              AS '',
  'Altering application_configs' AS '';

ALTER TABLE `application_configs`
  DROP KEY `application_id`,
  DROP FOREIGN KEY `application_configs_ibfk_1`,
  ADD PRIMARY KEY (`application_id`, `config`),
  ADD KEY `APPLICATION_CONFIGS_APPLICATION_ID_INDEX` (`application_id`),
  ADD CONSTRAINT `APPLICATION_CONFIGS_APPLICATION_ID_FK` FOREIGN KEY (`application_id`) REFERENCES `applications` (`id`)
  ON DELETE CASCADE;

SELECT
  CURRENT_TIMESTAMP                       AS '',
  'Finished altering application_configs' AS '';

SELECT
  CURRENT_TIMESTAMP                   AS '',
  'Altering application_dependencies' AS '';

ALTER TABLE `application_dependencies`
  DROP KEY `application_id`,
  DROP FOREIGN KEY `application_dependencies_ibfk_1`,
  ADD PRIMARY KEY (`application_id`, `dependency`),
  ADD KEY `APPLICATION_DEPENDENCIES_APPLICATION_ID_INDEX` (`application_id`),
  ADD CONSTRAINT `APPLICATION_DEPENDENCIES_APPLICATION_ID_FK` FOREIGN KEY (`application_id`) REFERENCES `applications` (`id`)
  ON DELETE CASCADE;

SELECT
  CURRENT_TIMESTAMP                            AS '',
  'Finished altering application_dependencies' AS '';

SELECT
  CURRENT_TIMESTAMP       AS '',
  'Altering applications' AS '';

ALTER TABLE `applications`
  DROP KEY `APPLICATIONS_NAME_INDEX`,
  DROP KEY `APPLICATIONS_STATUS_INDEX`,
  DROP KEY `APPLICATIONS_TYPE_INDEX`,
  ADD KEY `APPLICATIONS_NAME_INDEX` (`name`),
  ADD KEY `APPLICATIONS_STATUS_INDEX` (`status`),
  ADD KEY `APPLICATIONS_TAGS_INDEX` (`tags`(3072)),
  ADD KEY `APPLICATIONS_TYPE_INDEX` (`type`);

SELECT
  CURRENT_TIMESTAMP                AS '',
  'Finished altering applications' AS '';

SELECT
  CURRENT_TIMESTAMP          AS '',
  'Altering cluster_configs' AS '';

ALTER TABLE `cluster_configs`
  DROP KEY `cluster_id`,
  DROP FOREIGN KEY `cluster_configs_ibfk_1`,
  ADD PRIMARY KEY (`cluster_id`, `config`),
  ADD KEY `CLUSTER_CONFIGS_CLUSTER_ID_INDEX` (`cluster_id`),
  ADD CONSTRAINT `CLUSTER_CONFIGS_CLUSTER_ID_FK` FOREIGN KEY (`cluster_id`) REFERENCES `clusters` (`id`)
  ON DELETE CASCADE;

SELECT
  CURRENT_TIMESTAMP                   AS '',
  'Finished altering cluster_configs' AS '';

SELECT
  CURRENT_TIMESTAMP               AS '',
  'Altering cluster_dependencies' AS '';

ALTER TABLE `cluster_dependencies`
  DROP KEY `cluster_id`,
  DROP FOREIGN KEY `cluster_dependencies_ibfk_1`,
  ADD PRIMARY KEY (`cluster_id`, `dependency`),
  ADD KEY `CLUSTER_DEPENDENCIES_CLUSTER_ID_INDEX` (`cluster_id`),
  ADD CONSTRAINT `CLUSTER_DEPENDENCIES_CLUSTER_ID_FK` FOREIGN KEY (`cluster_id`) REFERENCES `clusters` (`id`)
  ON DELETE CASCADE;

SELECT
  CURRENT_TIMESTAMP                        AS '',
  'Finished altering cluster_dependencies' AS '';

SELECT
  CURRENT_TIMESTAMP   AS '',
  'Altering clusters' AS '';

ALTER TABLE `clusters`
  DROP KEY `CLUSTERS_NAME_INDEX`,
  DROP KEY `CLUSTERS_STATUS_INDEX`,
  ADD KEY `CLUSTERS_NAME_INDEX` (`name`),
  ADD KEY `CLUSTERS_STATUS_INDEX` (`status`),
  ADD KEY `CLUSTERS_TAGS_INDEX` (`tags`(3072));

SELECT
  CURRENT_TIMESTAMP            AS '',
  'Finished altering clusters' AS '';

SELECT
  CURRENT_TIMESTAMP            AS '',
  'Altering clusters_commands' AS '';

ALTER TABLE `clusters_commands`
  DROP KEY `cluster_id`,
  DROP KEY `command_id`,
  DROP FOREIGN KEY `clusters_commands_ibfk_1`,
  DROP FOREIGN KEY `clusters_commands_ibfk_2`,
  ADD PRIMARY KEY (`cluster_id`, `command_id`, `command_order`),
  ADD KEY `CLUSTERS_COMMANDS_CLUSTER_ID_INDEX` (`cluster_id`),
  ADD KEY `CLUSTERS_COMMANDS_COMMAND_ID_INDEX` (`command_id`),
  ADD CONSTRAINT `CLUSTERS_COMMANDS_CLUSTER_ID_FK` FOREIGN KEY (`cluster_id`) REFERENCES `clusters` (`id`)
  ON DELETE CASCADE,
  ADD CONSTRAINT `CLUSTERS_COMMANDS_COMMAND_ID_FK` FOREIGN KEY (`command_id`) REFERENCES `commands` (`id`);

SELECT
  CURRENT_TIMESTAMP                     AS '',
  'Finished altering commands_commands' AS '';

SELECT
  CURRENT_TIMESTAMP          AS '',
  'Altering command_configs' AS '';

ALTER TABLE `command_configs`
  DROP KEY `command_id`,
  DROP FOREIGN KEY `command_configs_ibfk_1`,
  ADD PRIMARY KEY (`command_id`, `config`),
  ADD KEY `COMMAND_CONFIGS_COMMAND_ID_INDEX` (`command_id`),
  ADD CONSTRAINT `COMMAND_CONFIGS_COMMAND_ID_FK` FOREIGN KEY (`command_id`) REFERENCES `commands` (`id`)
  ON DELETE CASCADE;

SELECT
  CURRENT_TIMESTAMP                       AS '',
  'Finished altering application_configs' AS '';

SELECT
  CURRENT_TIMESTAMP               AS '',
  'Altering command_dependencies' AS '';

ALTER TABLE `command_dependencies`
  DROP KEY `command_id`,
  DROP FOREIGN KEY `command_dependencies_ibfk_1`,
  ADD PRIMARY KEY (`command_id`, `dependency`),
  ADD KEY `COMMAND_DEPENDENCIES_COMMAND_ID_INDEX` (`command_id`),
  ADD CONSTRAINT `COMMAND_DEPENDENCIES_COMMAND_ID_FK` FOREIGN KEY (`command_id`) REFERENCES `commands` (`id`)
  ON DELETE CASCADE;

SELECT
  CURRENT_TIMESTAMP                        AS '',
  'Finished altering command_dependencies' AS '';

SELECT
  CURRENT_TIMESTAMP   AS '',
  'Altering commands' AS '';

ALTER TABLE `commands`
  DROP KEY `COMMANDS_NAME_INDEX`,
  DROP KEY `COMMANDS_STATUS_INDEX`,
  ADD KEY `COMMANDS_NAME_INDEX` (`name`),
  ADD KEY `COMMANDS_STATUS_INDEX` (`status`),
  ADD KEY `COMMANDS_TAGS_INDEX` (`tags`(3072));

SELECT
  CURRENT_TIMESTAMP            AS '',
  'Finished altering commands' AS '';

SELECT
  CURRENT_TIMESTAMP                AS '',
  'Altering commands_applications' AS '';

ALTER TABLE `commands_applications`
  DROP KEY `command_id`,
  DROP KEY `application_id`,
  DROP FOREIGN KEY `commands_applications_ibfk_1`,
  DROP FOREIGN KEY `commands_applications_ibfk_2`,
  ADD PRIMARY KEY (`command_id`, `application_id`, `application_order`),
  ADD KEY `COMMANDS_APPLICATIONS_APPLICATION_ID_INDEX` (`application_id`),
  ADD KEY `COMMANDS_APPLICATIONS_COMMAND_ID_INDEX` (`command_id`),
  ADD CONSTRAINT `COMMANDS_APPLICATIONS_APPLICATION_ID_FK` FOREIGN KEY (`application_id`) REFERENCES `applications` (`id`),
  ADD CONSTRAINT `COMMANDS_APPLICATIONS_COMMAND_ID_FK` FOREIGN KEY (`command_id`) REFERENCES `commands` (`id`)
  ON DELETE CASCADE;

SELECT
  CURRENT_TIMESTAMP                         AS '',
  'Finished altering commands_applications' AS '';

SELECT
  CURRENT_TIMESTAMP          AS '',
  'Altering jobs_executions' AS '';

ALTER TABLE `job_executions`
  DROP KEY `id`,
  DROP KEY `JOB_EXECUTIONS_HOSTNAME_INDEX`,
  DROP KEY `JOB_EXECUTIONS_EXIT_CODE_INDEX`, -- never used
  DROP FOREIGN KEY `job_executions_ibfk_1`,
  ADD PRIMARY KEY (`id`),
  ADD KEY `JOB_EXECUTIONS_HOSTNAME_INDEX` (`host_name`),
  ADD CONSTRAINT `JOB_EXECUTIONS_ID_FK` FOREIGN KEY (`id`) REFERENCES `jobs` (`id`)
  ON DELETE CASCADE;

SELECT
  CURRENT_TIMESTAMP                   AS '',
  'Finished altering jobs_executions' AS '';

SELECT
  CURRENT_TIMESTAMP       AS '',
  'Altering job_metadata' AS '';

ALTER TABLE `job_metadata`
  DROP KEY `id`,
  DROP FOREIGN KEY `job_metadata_ibfk_1`,
  ADD PRIMARY KEY (`id`),
  ADD CONSTRAINT `JOB_METADATA_ID_FK` FOREIGN KEY (`id`) REFERENCES `jobs` (`id`)
  ON DELETE CASCADE;

SELECT
  CURRENT_TIMESTAMP                AS '',
  'Finished altering job_metadata' AS '';

SELECT
  CURRENT_TIMESTAMP       AS '',
  'Altering job_requests' AS '';

ALTER TABLE `job_requests`
  MODIFY COLUMN `configs` TEXT NOT NULL
  AFTER `timeout`;

SELECT
  CURRENT_TIMESTAMP                AS '',
  'Finished altering job_requests' AS '';

SELECT
  CURRENT_TIMESTAMP AS '',
  'Altering jobs'   AS '';

ALTER TABLE `jobs`
  DROP KEY `id`,
  DROP KEY `cluster_id`,
  DROP KEY `command_id`,
  DROP KEY `JOBS_NAME_INDEX`,
  DROP KEY `JOBS_STARTED_INDEX`,
  DROP KEY `JOBS_FINISHED_INDEX`,
  DROP KEY `JOBS_STATUS_INDEX`,
  DROP KEY `JOBS_USER_INDEX`,
  DROP KEY `JOBS_CREATED_INDEX`,
  DROP KEY `JOBS_CLUSTER_NAME_INDEX`,
  DROP KEY `JOBS_COMMAND_NAME_INDEX`,
  DROP FOREIGN KEY `jobs_ibfk_1`,
  DROP FOREIGN KEY `jobs_ibfk_2`,
  DROP FOREIGN KEY `jobs_ibfk_3`,
  ADD PRIMARY KEY (`id`),
  ADD KEY `JOBS_CLUSTER_ID_INDEX` (`cluster_id`),
  ADD KEY `JOBS_CLUSTER_NAME_INDEX` (`cluster_name`),
  ADD KEY `JOBS_COMMAND_ID_INDEX` (`command_id`),
  ADD KEY `JOBS_COMMAND_NAME_INDEX` (`command_name`),
  ADD KEY `JOBS_CREATED_INDEX` (`created`),
  ADD KEY `JOBS_FINISHED_INDEX` (`finished`),
  ADD KEY `JOBS_NAME_INDEX` (`name`),
  ADD KEY `JOBS_STARTED_INDEX` (`started`),
  ADD KEY `JOBS_STATUS_INDEX` (`status`),
  ADD KEY `JOBS_TAGS_INDEX` (`tags`(3072)),
  ADD KEY `JOBS_USER_INDEX` (`genie_user`),
  ADD CONSTRAINT `JOBS_CLUSTER_ID_FK` FOREIGN KEY (`cluster_id`) REFERENCES `clusters` (`id`),
  ADD CONSTRAINT `JOBS_COMMAND_ID_FK` FOREIGN KEY (`command_id`) REFERENCES `commands` (`id`),
  ADD CONSTRAINT `JOBS_ID_FK` FOREIGN KEY (`id`) REFERENCES `job_requests` (`id`)
  ON DELETE CASCADE;

SELECT
  CURRENT_TIMESTAMP        AS '',
  'Finished altering jobs' AS '';

SELECT
  CURRENT_TIMESTAMP            AS '',
  'Altering jobs_applications' AS '';

ALTER TABLE `jobs_applications`
  DROP KEY `job_id`,
  DROP KEY `application_id`,
  DROP FOREIGN KEY `jobs_applications_ibfk_1`,
  DROP FOREIGN KEY `jobs_applications_ibfk_2`,
  ADD PRIMARY KEY (`job_id`, `application_id`, `application_order`),
  ADD KEY `JOBS_APPLICATIONS_APPLICATION_ID_INDEX` (`application_id`),
  ADD KEY `JOBS_APPLICATIONS_JOB_ID_INDEX` (`job_id`),
  ADD CONSTRAINT `JOBS_APPLICATIONS_APPLICATION_ID_FK` FOREIGN KEY (`application_id`) REFERENCES `applications` (`id`),
  ADD CONSTRAINT `JOBS_APPLICATIONS_JOB_ID_FK` FOREIGN KEY (`job_id`) REFERENCES `jobs` (`id`)
  ON DELETE CASCADE;

SELECT
  CURRENT_TIMESTAMP                     AS '',
  'Finished altering jobs_applications' AS '';

SELECT
  CURRENT_TIMESTAMP                                    AS '',
  'Finished upgrading database schema for Genie 3.2.0' AS '';
