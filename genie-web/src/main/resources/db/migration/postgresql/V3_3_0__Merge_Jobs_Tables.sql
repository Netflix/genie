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
  CURRENT_TIMESTAMP,
  'Upgrading database schema to 3.3.0';

SELECT
  CURRENT_TIMESTAMP,
  'Installing UUID extension';

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

SELECT
  CURRENT_TIMESTAMP,
  'Finished installing UUID extension';

SELECT
  CURRENT_TIMESTAMP,
  'Dropping existing foreign key constraints';

ALTER TABLE ONLY application_configs
  DROP CONSTRAINT application_configs_application_id_fkey;
ALTER TABLE ONLY application_dependencies
  DROP CONSTRAINT application_dependencies_application_id_fkey;
ALTER TABLE ONLY cluster_configs
  DROP CONSTRAINT cluster_configs_cluster_id_fkey;
ALTER TABLE ONLY cluster_dependencies
  DROP CONSTRAINT cluster_dependencies_cluster_id_fkey;
ALTER TABLE command_configs
  DROP CONSTRAINT command_configs_command_id_fkey;
ALTER TABLE ONLY command_dependencies
  DROP CONSTRAINT command_dependencies_command_id_fkey;
ALTER TABLE ONLY clusters_commands
  DROP CONSTRAINT clusters_commands_cluster_id_fkey,
  DROP CONSTRAINT clusters_commands_command_id_fkey;
ALTER TABLE ONLY commands_applications
  DROP CONSTRAINT commands_applications_application_id_fkey,
  DROP CONSTRAINT commands_applications_command_id_fkey;
ALTER TABLE ONLY jobs
  DROP CONSTRAINT jobs_cluster_id_fkey,
  DROP CONSTRAINT jobs_command_id_fkey,
  DROP CONSTRAINT jobs_id_fkey;
ALTER TABLE ONLY jobs_applications
  DROP CONSTRAINT jobs_applications_application_id_fkey,
  DROP CONSTRAINT jobs_applications_job_id_fkey;

SELECT
  CURRENT_TIMESTAMP,
  'Finished dropping existing foreign key constraints';

SELECT
  CURRENT_TIMESTAMP,
  'Dropping existing indices';

DROP INDEX
applications_name_index,
applications_status_index,
applications_type_index,
clusters_name_index,
clusters_status_index,
commands_name_index,
commands_status_index,
clusters_commands_cluster_id_index,
clusters_commands_command_id_index,
commands_applications_application_id_index,
commands_applications_command_id_index,
jobs_cluster_id_index,
jobs_cluster_name_index,
jobs_command_id_index,
jobs_command_name_index,
jobs_created_index,
jobs_finished_index,
jobs_name_index,
jobs_started_index,
jobs_status_index,
jobs_tags_index,
jobs_user_index,
jobs_applications_application_id_index,
jobs_applications_job_id_index;

SELECT
  CURRENT_TIMESTAMP,
  'Dropping existing indices';

SELECT
  CURRENT_TIMESTAMP,
  'Renaming current tables';

ALTER TABLE ONLY applications
  RENAME TO applications_320;
ALTER TABLE ONLY application_configs
  RENAME TO application_configs_320;
ALTER TABLE ONLY application_dependencies
  RENAME TO application_dependencies_320;
ALTER TABLE ONLY clusters
  RENAME TO clusters_320;
ALTER TABLE ONLY cluster_configs
  RENAME TO cluster_configs_320;
ALTER TABLE ONLY cluster_dependencies
  RENAME TO cluster_dependencies_320;
ALTER TABLE ONLY commands
  RENAME TO commands_320;
ALTER TABLE ONLY command_configs
  RENAME TO command_configs_320;
ALTER TABLE ONLY command_dependencies
  RENAME TO command_dependencies_320;
ALTER TABLE ONLY commands_applications
  RENAME TO commands_applications_320;
ALTER TABLE ONLY clusters_commands
  RENAME TO clusters_commands_320;
ALTER TABLE ONLY job_requests
  RENAME TO job_requests_320;
ALTER TABLE ONLY job_metadata
  RENAME TO job_metadata_320;
ALTER TABLE ONLY job_executions
  RENAME TO job_executions_320;
ALTER TABLE ONLY jobs
  RENAME TO jobs_320;
ALTER TABLE ONLY jobs_applications
  RENAME TO jobs_applications_320;

SELECT
  CURRENT_TIMESTAMP,
  'Finished renaming current tables';

SELECT
  CURRENT_TIMESTAMP,
  'Creating tags table';

CREATE TABLE tags (
  id             BIGSERIAL                                    NOT NULL,
  created        TIMESTAMP(3) WITHOUT TIME ZONE DEFAULT now() NOT NULL,
  updated        TIMESTAMP(3) WITHOUT TIME ZONE DEFAULT now() NOT NULL,
  entity_version INTEGER DEFAULT '0'                          NOT NULL,
  tag            VARCHAR(255)                                 NOT NULL,
  PRIMARY KEY (id)
);

CREATE UNIQUE INDEX tags_tag_unique_index
  ON tags (tag);

SELECT
  CURRENT_TIMESTAMP,
  'Finished creating tags table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating files table';

CREATE TABLE files (
  id             BIGSERIAL                                    NOT NULL,
  created        TIMESTAMP(3) WITHOUT TIME ZONE DEFAULT now() NOT NULL,
  updated        TIMESTAMP(3) WITHOUT TIME ZONE DEFAULT now() NOT NULL,
  entity_version INT DEFAULT '0'                              NOT NULL,
  file           VARCHAR(1024)                                NOT NULL,
  PRIMARY KEY (id)
);

CREATE UNIQUE INDEX files_file_unique_index
  ON files (file);

SELECT
  CURRENT_TIMESTAMP,
  'Finished creating files table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating criteria table';

CREATE TABLE criteria (
  id BIGSERIAL NOT NULL,
  PRIMARY KEY (id)
);

SELECT
  CURRENT_TIMESTAMP,
  'Finished creating criteria table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating criteria_tags table';

CREATE TABLE criteria_tags (
  criterion_id BIGINT NOT NULL,
  tag_id       BIGINT NOT NULL,
  PRIMARY KEY (criterion_id, tag_id),
  CONSTRAINT criteria_tags_criterion_id_fkey FOREIGN KEY (criterion_id) REFERENCES criteria (id)
  ON DELETE CASCADE,
  CONSTRAINT criteria_tags_tag_id_fkey FOREIGN KEY (tag_id) REFERENCES tags (id)
  ON DELETE RESTRICT
);

CREATE INDEX criteria_tags_criterion_id_index
  ON criteria_tags (criterion_id);
CREATE INDEX criteria_tags_tag_id_index
  ON criteria_tags (tag_id);

SELECT
  CURRENT_TIMESTAMP,
  'Created criteria_tags table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating applications table';

CREATE TABLE applications (
  id             BIGSERIAL                                                                        NOT NULL,
  created        TIMESTAMP(3) WITHOUT TIME ZONE DEFAULT now()                                     NOT NULL,
  updated        TIMESTAMP(3) WITHOUT TIME ZONE DEFAULT now()                                     NOT NULL,
  entity_version INT DEFAULT 0                                                                    NOT NULL,
  unique_id      VARCHAR(255) DEFAULT uuid_generate_v1()                                          NOT NULL,
  name           VARCHAR(255)                                                                     NOT NULL,
  genie_user     VARCHAR(255)                                                                     NOT NULL,
  version        VARCHAR(255)                                                                     NOT NULL,
  description    TEXT         DEFAULT NULL,
  setup_file     BIGINT       DEFAULT NULL,
  status         VARCHAR(20) DEFAULT 'INACTIVE'                                                   NOT NULL,
  type           VARCHAR(255) DEFAULT NULL,
  PRIMARY KEY (id),
  CONSTRAINT applications_setup_file_fkey FOREIGN KEY (setup_file) REFERENCES files (id) ON DELETE RESTRICT
);

CREATE UNIQUE INDEX applications_unique_id_unique_index
  ON applications (unique_id);
CREATE INDEX applications_name_index
  ON applications (name);
CREATE INDEX applications_setup_file_index
  ON applications (setup_file);
CREATE INDEX applications_status_index
  ON applications (status);
CREATE INDEX applications_type_index
  ON applications (type);

SELECT
  CURRENT_TIMESTAMP,
  'Created applications table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating applications_configs table';

CREATE TABLE applications_configs (
  application_id BIGINT NOT NULL,
  file_id        BIGINT NOT NULL,
  PRIMARY KEY (application_id, file_id),
  CONSTRAINT applications_configs_application_id_fkey FOREIGN KEY (application_id) REFERENCES applications (id)
  ON DELETE CASCADE,
  CONSTRAINT applications_configs_file_id_fkey FOREIGN KEY (file_id) REFERENCES files (id) ON DELETE RESTRICT
);

CREATE INDEX applications_configs_application_id_index
  ON applications_configs (application_id);
CREATE INDEX applications_configs_file_id_index
  ON applications_configs (file_id);

SELECT
  CURRENT_TIMESTAMP,
  'Created applications_configs table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating applications_dependencies table';

CREATE TABLE applications_dependencies (
  application_id BIGINT NOT NULL,
  file_id        BIGINT NOT NULL,
  PRIMARY KEY (application_id, file_id),
  CONSTRAINT applications_dependencies_application_id_fkey FOREIGN KEY (application_id) REFERENCES applications (id)
  ON DELETE CASCADE,
  CONSTRAINT applications_dependencies_file_id_fkey FOREIGN KEY (file_id) REFERENCES files (id) ON DELETE RESTRICT
);

CREATE INDEX applications_dependencies_application_id_index
  ON applications_dependencies (application_id);
CREATE INDEX applications_dependencies_file_id_index
  ON applications_dependencies (file_id);

SELECT
  CURRENT_TIMESTAMP,
  'Finished creating new application_dependencies table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating new clusters table';

CREATE TABLE clusters (
  id             BIGSERIAL                                                                       NOT NULL,
  created        TIMESTAMP(3) WITHOUT TIME ZONE DEFAULT now()                                    NOT NULL,
  updated        TIMESTAMP(3) WITHOUT TIME ZONE DEFAULT now()                                    NOT NULL,
  entity_version INT DEFAULT '0'                                                                 NOT NULL,
  unique_id      VARCHAR(255) DEFAULT uuid_generate_v1()                                         NOT NULL,
  name           VARCHAR(255)                                                                    NOT NULL,
  genie_user     VARCHAR(255)                                                                    NOT NULL,
  version        VARCHAR(255)                                                                    NOT NULL,
  description    TEXT   DEFAULT NULL,
  setup_file     BIGINT DEFAULT NULL,
  status         VARCHAR(20) DEFAULT 'OUT_OF_SERVICE'                                            NOT NULL,
  PRIMARY KEY (id),
  CONSTRAINT clusters_setup_file_fkey FOREIGN KEY (setup_file) REFERENCES files (id) ON DELETE RESTRICT
);

CREATE UNIQUE INDEX clusters_unique_id_unique_index
  ON clusters (unique_id);
CREATE INDEX clusters_name_index
  ON clusters (name);
CREATE INDEX clusters_setup_file_index
  ON clusters (name);
CREATE INDEX clusters_status_index
  ON clusters (status);

SELECT
  CURRENT_TIMESTAMP,
  'Finished creating new clusters table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating clusters_configs table';

CREATE TABLE clusters_configs (
  cluster_id BIGINT NOT NULL,
  file_id    BIGINT NOT NULL,
  PRIMARY KEY (cluster_id, file_id),
  CONSTRAINT clusters_configs_cluster_id_fkey FOREIGN KEY (cluster_id) REFERENCES clusters (id)
  ON DELETE CASCADE,
  CONSTRAINT clusters_configs_file_id_fkey FOREIGN KEY (file_id) REFERENCES files (id) ON DELETE RESTRICT
);

CREATE INDEX clusters_configs_cluster_id_index
  ON clusters_configs (cluster_id);
CREATE INDEX clusters_configs_file_id_index
  ON clusters_configs (file_id);

SELECT
  CURRENT_TIMESTAMP,
  'Created clusters_configs table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating clusters_dependencies table';

CREATE TABLE clusters_dependencies (
  cluster_id BIGINT NOT NULL,
  file_id    BIGINT NOT NULL,
  PRIMARY KEY (cluster_id, file_id),
  CONSTRAINT clusters_dependencies_cluster_id_fkey FOREIGN KEY (cluster_id) REFERENCES clusters (id)
  ON DELETE CASCADE,
  CONSTRAINT clusters_dependencies_file_id_fkey FOREIGN KEY (file_id) REFERENCES files (id) ON DELETE RESTRICT
);

CREATE INDEX clusters_dependencies_cluster_id_index
  ON clusters_dependencies (cluster_id);
CREATE INDEX clusters_dependencies_file_id_index
  ON clusters_dependencies (file_id);

SELECT
  CURRENT_TIMESTAMP,
  'Finished creating new cluster_dependencies table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating new commands table';

CREATE TABLE commands (
  id             BIGSERIAL                                                                        NOT NULL,
  created        TIMESTAMP(3) WITHOUT TIME ZONE DEFAULT now()                                     NOT NULL,
  updated        TIMESTAMP(3) WITHOUT TIME ZONE DEFAULT now()                                     NOT NULL,
  entity_version INT DEFAULT '0'                                                                  NOT NULL,
  unique_id      VARCHAR(255) DEFAULT uuid_generate_v1()                                          NOT NULL,
  name           VARCHAR(255)                                                                     NOT NULL,
  genie_user     VARCHAR(255)                                                                     NOT NULL,
  version        VARCHAR(255)                                                                     NOT NULL,
  description    TEXT   DEFAULT NULL,
  setup_file     BIGINT DEFAULT NULL,
  executable     VARCHAR(255)                                                                     NOT NULL,
  check_delay    BIGINT DEFAULT '10000'                                                           NOT NULL,
  memory         INT    DEFAULT NULL,
  status         VARCHAR(20) DEFAULT 'INACTIVE'                                                   NOT NULL,
  PRIMARY KEY (id),
  CONSTRAINT commands_setup_file_fkey FOREIGN KEY (setup_file) REFERENCES files (id) ON DELETE RESTRICT
);

CREATE UNIQUE INDEX commands_unique_id_unique_index
  ON commands (unique_id);
CREATE INDEX commands_name_index
  ON commands (name);
CREATE INDEX commands_setup_file_index
  ON commands (setup_file);
CREATE INDEX commands_status_index
  ON commands (status);

SELECT
  CURRENT_TIMESTAMP,
  'Finished creating new commands table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating commands_configs table';

CREATE TABLE commands_configs (
  command_id BIGINT NOT NULL,
  file_id    BIGINT NOT NULL,
  PRIMARY KEY (command_id, file_id),
  CONSTRAINT commands_configs_command_id_fkey FOREIGN KEY (command_id) REFERENCES commands (id)
  ON DELETE CASCADE,
  CONSTRAINT commands_configs_file_id_fkey FOREIGN KEY (file_id) REFERENCES files (id) ON DELETE RESTRICT
);

CREATE INDEX commands_configs_command_id_index
  ON commands_configs (command_id);
CREATE INDEX commands_configs_file_id_index
  ON commands_configs (file_id);

SELECT
  CURRENT_TIMESTAMP,
  'Created commands_configs table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating commands_dependencies table';

CREATE TABLE commands_dependencies (
  command_id BIGINT NOT NULL,
  file_id    BIGINT NOT NULL,
  PRIMARY KEY (command_id, file_id),
  CONSTRAINT commands_dependencies_command_id_fkey FOREIGN KEY (command_id) REFERENCES commands (id)
  ON DELETE CASCADE,
  CONSTRAINT commands_dependencies_file_id_fkey FOREIGN KEY (file_id) REFERENCES files (id) ON DELETE RESTRICT
);

CREATE INDEX commands_dependencies_command_id_index
  ON commands_dependencies (command_id);
CREATE INDEX commands_dependencies_file_id_index
  ON commands_dependencies (file_id);

SELECT
  CURRENT_TIMESTAMP,
  'Finished creating new command_dependencies table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating new clusters_commands table';

CREATE TABLE clusters_commands (
  cluster_id    BIGINT NOT NULL,
  command_id    BIGINT NOT NULL,
  command_order INT    NOT NULL,
  PRIMARY KEY (cluster_id, command_id, command_order),
  CONSTRAINT clusters_commands_cluster_id_fkey FOREIGN KEY (cluster_id) REFERENCES clusters (id)
  ON DELETE CASCADE,
  CONSTRAINT clusters_commands_command_id_fkey FOREIGN KEY (command_id) REFERENCES commands (id) ON DELETE RESTRICT
);

CREATE INDEX clusters_commands_cluster_id_index
  ON clusters_commands (cluster_id);
CREATE INDEX clusters_commands_command_id_index
  ON clusters_commands (command_id);

SELECT
  CURRENT_TIMESTAMP,
  'Finished creating new clusters_commands table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating into new commands_applications table';

CREATE TABLE commands_applications (
  command_id        BIGINT NOT NULL,
  application_id    BIGINT NOT NULL,
  application_order INT    NOT NULL,
  PRIMARY KEY (command_id, application_id, application_order),
  CONSTRAINT commands_applications_application_id_fkey FOREIGN KEY (application_id) REFERENCES applications (id)
  ON DELETE RESTRICT,
  CONSTRAINT commands_applications_command_id_fkey FOREIGN KEY (command_id) REFERENCES commands (id)
  ON DELETE CASCADE
);

CREATE INDEX commands_applications_application_id_index
  ON commands_applications (application_id);
CREATE INDEX commands_applications_command_id_index
  ON commands_applications (command_id);

SELECT
  CURRENT_TIMESTAMP,
  'Finished creating into new commands_applications table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating new jobs table';

CREATE TABLE jobs (
  -- common
  id                        BIGSERIAL                                                                               NOT NULL,
  created                   TIMESTAMP(3) WITHOUT TIME ZONE DEFAULT now()                                            NOT NULL,
  updated                   TIMESTAMP(3) WITHOUT TIME ZONE DEFAULT now()                                            NOT NULL,
  entity_version            INT DEFAULT '0'                                                                         NOT NULL,

  -- Job Request
  unique_id                 VARCHAR(255) DEFAULT uuid_generate_v1()                                                 NOT NULL,
  name                      VARCHAR(255)                                                                            NOT NULL,
  genie_user                VARCHAR(255)                                                                            NOT NULL,
  version                   VARCHAR(255)                                                                            NOT NULL,
  command_criterion         BIGINT                         DEFAULT NULL,
  command_args              TEXT                           DEFAULT NULL,
  description               TEXT                           DEFAULT NULL,
  setup_file                BIGINT                         DEFAULT NULL,
  tags                      VARCHAR(1024)                  DEFAULT NULL,
  genie_user_group          VARCHAR(255)                   DEFAULT NULL,
  disable_log_archival      BOOLEAN DEFAULT FALSE                                                                   NOT NULL,
  email                     VARCHAR(255)                   DEFAULT NULL,
  cpu_requested             INT                            DEFAULT NULL,
  memory_requested          INT                            DEFAULT NULL,
  timeout_requested         INT                            DEFAULT NULL,
  grouping                  VARCHAR(255)                   DEFAULT NULL,
  grouping_instance         VARCHAR(255)                   DEFAULT NULL,

  -- Job Metadata
  client_host               VARCHAR(255)                   DEFAULT NULL,
  user_agent                VARCHAR(1024)                  DEFAULT NULL,
  num_attachments           INT                            DEFAULT NULL,
  total_size_of_attachments BIGINT                         DEFAULT NULL,
  std_out_size              BIGINT                         DEFAULT NULL,
  std_err_size              BIGINT                         DEFAULT NULL,

  -- Job
  command_id                BIGINT                         DEFAULT NULL,
  command_name              VARCHAR(255)                   DEFAULT NULL,
  cluster_id                BIGINT                         DEFAULT NULL,
  cluster_name              VARCHAR(255)                   DEFAULT NULL,
  started                   TIMESTAMP(3) WITHOUT TIME ZONE DEFAULT NULL,
  finished                  TIMESTAMP(3) WITHOUT TIME ZONE DEFAULT NULL,
  status                    VARCHAR(20) DEFAULT 'INIT'                                                              NOT NULL,
  status_msg                VARCHAR(255)                   DEFAULT NULL,

  -- Job Execution
  host_name                 VARCHAR(255)                                                                            NOT NULL,
  process_id                INT                            DEFAULT NULL,
  exit_code                 INT                            DEFAULT NULL,
  check_delay               BIGINT                         DEFAULT NULL,
  timeout                   TIMESTAMP(3) WITHOUT TIME ZONE DEFAULT NULL,
  memory_used               INT                            DEFAULT NULL,

  -- Post Job Info
  archive_location          VARCHAR(1024)                  DEFAULT NULL,
  PRIMARY KEY (id),
  CONSTRAINT jobs_command_criterion_fkey FOREIGN KEY (command_criterion) REFERENCES criteria (id) ON DELETE RESTRICT,
  CONSTRAINT jobs_cluster_id_fkey FOREIGN KEY (cluster_id) REFERENCES clusters (id) ON DELETE RESTRICT,
  CONSTRAINT jobs_command_id_fkey FOREIGN KEY (command_id) REFERENCES commands (id) ON DELETE RESTRICT,
  CONSTRAINT jobs_setup_file_fkey FOREIGN KEY (setup_file) REFERENCES files (id) ON DELETE RESTRICT
);

CREATE UNIQUE INDEX jobs_unique_id_unique_index
  ON jobs (unique_id);
CREATE INDEX jobs_cluster_id_index
  ON jobs (cluster_id);
CREATE INDEX jobs_cluster_name_index
  ON jobs (cluster_name);
CREATE INDEX jobs_command_criterion_index
  ON jobs (command_criterion);
CREATE INDEX jobs_command_id_index
  ON jobs (command_id);
CREATE INDEX jobs_command_name_index
  ON jobs (command_name);
CREATE INDEX jobs_created_index
  ON jobs (created);
CREATE INDEX jobs_finished_index
  ON jobs (finished);
CREATE INDEX jobs_grouping_index
  ON jobs (grouping);
CREATE INDEX jobs_grouping_instance_index
  ON jobs (grouping_instance);
CREATE INDEX jobs_name_index
  ON jobs (name);
CREATE INDEX jobs_setup_file_index
  ON jobs (setup_file);
CREATE INDEX jobs_started_index
  ON jobs (started);
CREATE INDEX jobs_status_index
  ON jobs (status);
CREATE INDEX jobs_tags_index
  ON jobs (tags);
CREATE INDEX jobs_user_index
  ON jobs (genie_user);

SELECT
  CURRENT_TIMESTAMP,
  'Finished creating new jobs table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating new jobs_applications table';

CREATE TABLE jobs_applications (
  job_id            BIGINT NOT NULL,
  application_id    BIGINT NOT NULL,
  application_order INT    NOT NULL,
  PRIMARY KEY (job_id, application_id, application_order),
  CONSTRAINT jobs_applications_application_id_fkey FOREIGN KEY (application_id) REFERENCES applications (id)
  ON DELETE RESTRICT,
  CONSTRAINT jobs_applications_job_id_fkey FOREIGN KEY (job_id) REFERENCES jobs (id) ON DELETE CASCADE
);

CREATE INDEX jobs_applications_application_id_index
  ON jobs_applications (application_id);
CREATE INDEX jobs_applications_job_id_index
  ON jobs_applications (job_id);

SELECT
  CURRENT_TIMESTAMP,
  'Finished creating new jobs_applications table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating applications_tags table';

CREATE TABLE applications_tags (
  application_id BIGINT NOT NULL,
  tag_id         BIGINT NOT NULL,
  PRIMARY KEY (application_id, tag_id),
  CONSTRAINT applications_tags_application_id_fkey FOREIGN KEY (application_id) REFERENCES applications (id)
  ON DELETE CASCADE,
  CONSTRAINT applications_tags_tag_id_fkey FOREIGN KEY (tag_id) REFERENCES tags (id) ON DELETE RESTRICT
);

CREATE INDEX applications_tags_application_id_index
  ON applications_tags (application_id);
CREATE INDEX applications_tags_tags_id_index
  ON applications_tags (tag_id);

SELECT
  CURRENT_TIMESTAMP,
  'Finished creating applications_tags table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating clusters_tags table';

CREATE TABLE clusters_tags (
  cluster_id BIGINT NOT NULL,
  tag_id     BIGINT NOT NULL,
  PRIMARY KEY (cluster_id, tag_id),
  CONSTRAINT clusters_tags_cluster_id_fkey FOREIGN KEY (cluster_id) REFERENCES clusters (id) ON DELETE CASCADE,
  CONSTRAINT clusters_tags_tag_id_fkey FOREIGN KEY (tag_id) REFERENCES tags (id) ON DELETE RESTRICT
);

CREATE INDEX clusters_tags_cluster_id_index
  ON clusters_tags (cluster_id);
CREATE INDEX clusters_tags_tags_id_index
  ON clusters_tags (tag_id);

SELECT
  CURRENT_TIMESTAMP,
  'Finished creating clusters_tags table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating commands_tags table';

CREATE TABLE commands_tags (
  command_id BIGINT NOT NULL,
  tag_id     BIGINT NOT NULL,
  PRIMARY KEY (command_id, tag_id),
  CONSTRAINT commands_tags_command_id_fkey FOREIGN KEY (command_id) REFERENCES commands (id) ON DELETE CASCADE,
  CONSTRAINT commands_tags_tag_id_fkey FOREIGN KEY (tag_id) REFERENCES tags (id) ON DELETE RESTRICT
);

CREATE INDEX commands_tags_command_id_index
  ON commands_tags (command_id);
CREATE INDEX commands_tags_tags_id_index
  ON commands_tags (tag_id);

SELECT
  CURRENT_TIMESTAMP,
  'Finished creating commands_tags table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating jobs_tags table';

CREATE TABLE jobs_tags (
  job_id BIGINT NOT NULL,
  tag_id BIGINT NOT NULL,
  PRIMARY KEY (job_id, tag_id),
  CONSTRAINT jobs_tags_job_id_fkey FOREIGN KEY (job_id) REFERENCES jobs (id) ON DELETE CASCADE,
  CONSTRAINT jobs_tags_tag_id_fkey FOREIGN KEY (tag_id) REFERENCES tags (id) ON DELETE RESTRICT
);

CREATE INDEX jobs_tags_job_id_index
  ON jobs_tags (job_id);
CREATE INDEX jobs_tags_tags_id_index
  ON jobs_tags (tag_id);

SELECT
  CURRENT_TIMESTAMP,
  'Finished creating job_tags table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating jobs_cluster_criteria table';

CREATE TABLE jobs_cluster_criteria (
  job_id         BIGINT NOT NULL,
  criterion_id   BIGINT NOT NULL,
  priority_order INT    NOT NULL,
  PRIMARY KEY (job_id, criterion_id, priority_order),
  CONSTRAINT jobs_cluster_criteria_job_id_fkey FOREIGN KEY (job_id) REFERENCES jobs (id)
  ON DELETE CASCADE,
  CONSTRAINT jobs_cluster_criteria_criterion_id_fkey FOREIGN KEY (criterion_id) REFERENCES criteria (id)
  ON DELETE RESTRICT
);

CREATE INDEX jobs_cluster_criteria_job_id_index
  ON jobs_cluster_criteria (job_id);
CREATE INDEX jobs_cluster_criteria_criterion_id_index
  ON jobs_cluster_criteria (criterion_id);

SELECT
  CURRENT_TIMESTAMP,
  'Created jobs_cluster_criteria table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating job_applications_requested table';

-- NOTE: Don't think we need to applications foreign key here cause user could request some bad apps
CREATE TABLE job_applications_requested (
  job_id            BIGINT       NOT NULL,
  application_id    VARCHAR(255) NOT NULL,
  application_order INT          NOT NULL,
  PRIMARY KEY (job_id, application_id, application_order),
  CONSTRAINT job_applications_requested_job_id_fkey FOREIGN KEY (job_id) REFERENCES jobs (id) ON DELETE CASCADE
);

CREATE INDEX job_applications_requested_application_id_index
  ON job_applications_requested (application_id);
CREATE INDEX job_applications_requested_job_id_index
  ON job_applications_requested (job_id);

SELECT
  CURRENT_TIMESTAMP,
  'Created job_applications_requested table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating jobs_configs table';

CREATE TABLE jobs_configs (
  job_id  BIGINT NOT NULL,
  file_id BIGINT NOT NULL,
  PRIMARY KEY (job_id, file_id),
  CONSTRAINT jobs_configs_job_id_fkey FOREIGN KEY (job_id) REFERENCES jobs (id) ON DELETE CASCADE,
  CONSTRAINT jobs_configs_file_id_fkey FOREIGN KEY (file_id) REFERENCES files (id) ON DELETE RESTRICT
);

CREATE INDEX jobs_configs_job_id_index
  ON jobs_configs (job_id);
CREATE INDEX jobs_configs_file_id_index
  ON jobs_configs (file_id);

SELECT
  CURRENT_TIMESTAMP,
  'Created jobs_configs table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating jobs_dependencies table';

CREATE TABLE jobs_dependencies (
  job_id  BIGINT NOT NULL,
  file_id BIGINT NOT NULL,
  PRIMARY KEY (job_id, file_id),
  CONSTRAINT jobs_dependencies_job_id_fkey FOREIGN KEY (job_id) REFERENCES jobs (id) ON DELETE CASCADE,
  CONSTRAINT jobs_dependencies_file_id_fkey FOREIGN KEY (file_id) REFERENCES files (id) ON DELETE RESTRICT
);

CREATE INDEX jobs_dependencies_job_id_index
  ON jobs_dependencies (job_id);
CREATE INDEX jobs_dependencies_file_id_index
  ON jobs_dependencies (file_id);

SELECT
  CURRENT_TIMESTAMP,
  'Finished creating new job_dependencies table';

SELECT
  CURRENT_TIMESTAMP,
  'Finished upgrading database schema to 3.3.0';

SELECT
  CURRENT_TIMESTAMP,
  'Beginning to load data from old 3.2.0 tables to 3.3.0 tables';

SELECT
  CURRENT_TIMESTAMP,
  'Loading data into applications table';

INSERT INTO applications (
  created,
  updated,
  entity_version,
  unique_id,
  name,
  genie_user,
  version,
  description,
  status,
  type
) SELECT
    created,
    updated,
    entity_version,
    id,
    name,
    genie_user,
    version,
    description,
    status,
    type
  FROM applications_320;

CREATE OR REPLACE FUNCTION genie_split_applications_320()
  RETURNS VOID AS $$
DECLARE
  app_record         RECORD;
  new_application_id BIGINT;
  found_tag_id       BIGINT;
  file_id            BIGINT;
  tags_local         VARCHAR(10000);
  application_tag    VARCHAR(255);
BEGIN
  << APPS_LOOP >>
  FOR app_record IN
  SELECT
    id,
    tags,
    setup_File
  FROM applications_320 LOOP

    SELECT a.id
    INTO new_application_id
    FROM applications a
    WHERE a.unique_id = app_record.id;

    IF app_record.setup_file IS NOT NULL
    THEN
      INSERT INTO files (file) VALUES (app_record.setup_file)
      ON CONFLICT DO NOTHING;

      SELECT f.id
      INTO file_id
      FROM files f
      WHERE f.file = app_record.setup_file;

      UPDATE applications
      SET setup_file = file_id
      WHERE id = new_application_id;
    END IF;

    tags_local = app_record.tags;
    << TAGS_LOOP >> WHILE LENGTH(tags_local) > 0 LOOP
      -- Tear OFF the LEADING |
      tags_local = TRIM(LEADING '|' FROM tags_local);
      application_tag = SPLIT_PART(tags_local, '|', 1);
      tags_local = TRIM(LEADING application_tag FROM tags_local);
      tags_local = TRIM(LEADING '|' FROM tags_local);

      INSERT INTO tags (tag) VALUES (application_tag)
      ON CONFLICT DO NOTHING;

      SELECT t.id
      INTO found_tag_id
      FROM tags t
      WHERE t.tag = application_tag;

      INSERT INTO applications_tags VALUES (new_application_id, found_tag_id);
    END LOOP TAGS_LOOP;

  END LOOP APPS_LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT genie_split_applications_320();
DROP FUNCTION genie_split_applications_320();

SELECT
  CURRENT_TIMESTAMP,
  'Loaded data into applications table';

SELECT
  CURRENT_TIMESTAMP,
  'Inserting data into applications_configs table';

CREATE OR REPLACE FUNCTION genie_load_applications_configs_320()
  RETURNS VOID AS $$
DECLARE
  configs            RECORD;
  config_file        VARCHAR(1024);
  new_application_id BIGINT;
  file_id            BIGINT;
BEGIN
  << CONFIGS_LOOP >>
  FOR configs IN
  SELECT
    application_id,
    config
  FROM application_configs_320
  LOOP

    SELECT a.id
    INTO new_application_id
    FROM applications a
    WHERE a.unique_id = configs.application_id;

    INSERT INTO files (file) VALUES (configs.config)
    ON CONFLICT DO NOTHING;

    SELECT f.id
    INTO file_id
    FROM files f
    WHERE f.file = config_file;

    INSERT INTO applications_configs VALUES (new_application_id, file_id);
  END LOOP CONFIGS_LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT genie_load_applications_configs_320();
DROP FUNCTION genie_load_applications_configs_320();

SELECT
  CURRENT_TIMESTAMP,
  'Finished inserting data into applications_configs table';

SELECT
  CURRENT_TIMESTAMP,
  'Inserting data into applications_dependencies table';

CREATE OR REPLACE FUNCTION genie_load_applications_dependencies_320()
  RETURNS VOID AS $$
DECLARE
  dependencies       RECORD;
  dependency_file    VARCHAR(1024);
  new_application_id BIGINT;
  file_id            BIGINT;
BEGIN
  << DEPENDENCIES_LOOP >>
  FOR dependencies IN
  SELECT
    application_id,
    dependency
  FROM application_dependencies_320
  LOOP

    SELECT a.id
    INTO new_application_id
    FROM applications a
    WHERE a.unique_id = dependencies.application_id;

    INSERT INTO files (file) VALUES (dependencies.dependency)
    ON CONFLICT DO NOTHING;

    SELECT f.id
    INTO file_id
    FROM files f
    WHERE f.file = dependency_file;

    INSERT INTO application_dependencies VALUES (new_application_id, file_id);
  END LOOP DEPENDENCIES_LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT genie_load_applications_dependencies_320();
DROP FUNCTION genie_load_applications_dependencies_320();

SELECT
  CURRENT_TIMESTAMP,
  'Finished inserting data into applications_dependencies table';

SELECT
  CURRENT_TIMESTAMP,
  'Loading data into clusters table';

INSERT INTO clusters (
  created,
  updated,
  entity_version,
  unique_id,
  name,
  genie_user,
  version,
  description,
  status
) SELECT
    created,
    updated,
    entity_version,
    id,
    name,
    genie_user,
    version,
    description,
    status
  FROM clusters_320;

CREATE OR REPLACE FUNCTION genie_split_clusters_320()
  RETURNS VOID AS $$
DECLARE
  cluster_record RECORD;
  new_cluster_id BIGINT;
  found_tag_id   BIGINT;
  file_id        BIGINT;
  tags_local     VARCHAR(10000);
  cluster_tag    VARCHAR(255);
BEGIN

  << CLUSTERS_LOOP >>
  FOR cluster_record IN
  SELECT
    id,
    tags,
    setup_file
  FROM clusters_320
  LOOP

    SELECT c.id
    INTO new_cluster_id
    FROM clusters c
    WHERE c.unique_id = cluster_record.id;

    IF cluster_record.setup_file IS NOT NULL
    THEN
      INSERT INTO files (file) VALUES (cluster_record.setup_file)
      ON CONFLICT DO NOTHING;

      SELECT f.id
      INTO file_id
      FROM files f
      WHERE f.file = cluster_record.setup_file;

      UPDATE clusters c
      SET setup_file = file_id
      WHERE c.id = new_cluster_id;
    END IF;

    tags_local = cluster_record.tags;
    << TAGS_LOOP >> WHILE LENGTH(@tags_local) > 0 LOOP
      -- Tear OFF the LEADING |
      tags_local = TRIM(LEADING '|' FROM tags_local);
      cluster_tag = SPLIT_PART(tags_local, '|', 1);
      tags_local = TRIM(LEADING cluster_tag FROM tags_local);
      tags_local = TRIM(LEADING '|' FROM tags_local);

      INSERT INTO tags (tag) VALUES (cluster_tag)
      ON CONFLICT DO NOTHING;

      SELECT t.id
      INTO found_tag_id
      FROM tags t
      WHERE t.tag = cluster_tag;

      INSERT INTO clusters_tags VALUES (new_cluster_id, found_tag_id);
    END LOOP TAGS_LOOP;

  END LOOP CLUSTERS_LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT genie_split_clusters_320();
DROP FUNCTION genie_split_clusters_320();

SELECT
  CURRENT_TIMESTAMP,
  'Finished loading data into clusters table';

SELECT
  CURRENT_TIMESTAMP,
  'Inserting data into clusters_configs table';

CREATE OR REPLACE FUNCTION genie_load_clusters_configs_320()
  RETURNS VOID AS $$
DECLARE
  configs        RECORD;
  config_file    VARCHAR(1024);
  new_cluster_id BIGINT;
  file_id        BIGINT;
BEGIN
  << CONFIGS_LOOP >>
  FOR configs IN
  SELECT
    cluster_id,
    config
  FROM cluster_configs_320
  LOOP

    SELECT c.id
    INTO new_cluster_id
    FROM clusters c
    WHERE c.unique_id = configs.cluster_id;

    INSERT INTO files (file) VALUES (configs.config)
    ON CONFLICT DO NOTHING;

    SELECT f.id
    INTO file_id
    FROM files f
    WHERE f.file = config_file;

    INSERT INTO clusters_configs VALUES (new_cluster_id, file_id);
  END LOOP CONFIGS_LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT genie_load_clusters_configs_320();
DROP FUNCTION genie_load_clusters_configs_320();

SELECT
  CURRENT_TIMESTAMP,
  'Finished inserting data into clusters_configs table';

SELECT
  CURRENT_TIMESTAMP,
  'Inserting data into clusters_dependencies table';

CREATE OR REPLACE FUNCTION genie_load_clusters_dependencies_320()
  RETURNS VOID AS $$
DECLARE
  dependencies    RECORD;
  dependency_file VARCHAR(1024);
  new_cluster_id  BIGINT;
  file_id         BIGINT;
BEGIN
  << DEPENDENCIES_LOOP >>
  FOR dependencies IN
  SELECT
    cluster_id,
    dependency
  FROM cluster_dependencies_320
  LOOP

    SELECT c.id
    INTO new_cluster_id
    FROM clusters c
    WHERE c.unique_id = dependencies.cluster_id;

    INSERT INTO files (file) VALUES (dependencies.dependency)
    ON CONFLICT DO NOTHING;

    SELECT f.id
    INTO file_id
    FROM files f
    WHERE f.file = dependency_file;

    INSERT INTO cluster_dependencies VALUES (new_cluster_id, file_id);
  END LOOP DEPENDENCIES_LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT genie_load_clusters_dependencies_320();
DROP FUNCTION genie_load_clusters_dependencies_320();

SELECT
  CURRENT_TIMESTAMP,
  'Finished inserting data into clusters_dependencies table';

SELECT
  CURRENT_TIMESTAMP,
  'Loading data into commands table';

INSERT INTO commands (
  created,
  updated,
  entity_version,
  unique_id,
  name,
  genie_user,
  version,
  description,
  executable,
  check_delay,
  memory,
  status
) SELECT
    created,
    updated,
    entity_version,
    id,
    name,
    genie_user,
    version,
    description,
    executable,
    check_delay,
    memory,
    status
  FROM commands_320;

CREATE OR REPLACE FUNCTION genie_split_commands_320()
  RETURNS VOID AS $$
DECLARE
  command_record RECORD;
  tags_local     VARCHAR(10000);
  new_command_id BIGINT;
  found_tag_id   BIGINT;
  file_id        BIGINT;
  command_tag    VARCHAR(255);
BEGIN

  << COMMANDS_LOOP >>
  FOR command_record IN
  SELECT
    id,
    tags,
    setup_file
  FROM commands_320
  LOOP

    SELECT c.id
    INTO new_command_id
    FROM commands c
    WHERE c.unique_id = command_record.id;

    IF command_record.setup_file IS NOT NULL
    THEN
      INSERT INTO files (file) VALUES (command_record.setup_file)
      ON CONFLICT DO NOTHING;

      SELECT f.id
      INTO file_id
      FROM files f
      WHERE f.file = command_record.setup_file;

      UPDATE clusters c
      SET c.setup_file = file_id
      WHERE c.id = new_command_id;
    END IF;

    tags_local = command_record.tags;
    << TAGS_LOOP >> WHILE LENGTH(tags_local) > 0 LOOP
      -- Tear OFF the LEADING |
      tags_local = TRIM(LEADING '|' FROM @tags_local);
      command_tag = SPLIT_PART(tags_local, '|', 1);
      tags_local = TRIM(LEADING command_tag FROM tags_local);
      tags_local = TRIM(LEADING '|' FROM tags_local);

      INSERT INTO tags (tag) VALUES (command_tag)
      ON CONFLICT DO NOTHING;

      SELECT t.id
      INTO found_tag_id
      FROM tags t
      WHERE t.tag = command_tag;

      INSERT INTO commands_tags VALUES (new_command_id, found_tag_id);
    END LOOP TAGS_LOOP;

  END LOOP COMMANDS_LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT genie_split_commands_320();
DROP FUNCTION genie_split_commands_320();

SELECT
  CURRENT_TIMESTAMP,
  'Finished loading data into commands table';

SELECT
  CURRENT_TIMESTAMP,
  'Inserting data into commands_configs table';

CREATE OR REPLACE FUNCTION genie_load_commands_configs_320()
  RETURNS VOID AS $$
DECLARE
  configs        RECORD;
  config_file    VARCHAR(1024);
  new_command_id BIGINT;
  file_id        BIGINT;
BEGIN
  << CONFIGS_LOOP >>
  FOR configs IN
  SELECT
    command_id,
    config
  FROM command_configs_320
  LOOP

    SELECT c.id
    INTO new_command_id
    FROM commands c
    WHERE c.unique_id = configs.command_id;

    INSERT INTO files (file) VALUES (configs.config)
    ON CONFLICT DO NOTHING;

    SELECT f.id
    INTO file_id
    FROM files f
    WHERE f.file = config_file;

    INSERT INTO commands_configs VALUES (new_command_id, file_id);
  END LOOP CONFIGS_LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT genie_load_commands_configs_320();
DROP FUNCTION genie_load_commands_configs_320();

SELECT
  CURRENT_TIMESTAMP,
  'Finished inserting data into commands_configs table';

SELECT
  CURRENT_TIMESTAMP,
  'Inserting data into commands_dependencies table';

CREATE OR REPLACE FUNCTION genie_load_commands_dependencies_320()
  RETURNS VOID AS $$
DECLARE
  dependencies    RECORD;
  dependency_file VARCHAR(1024);
  new_command_id  BIGINT;
  file_id         BIGINT;
BEGIN
  << DEPENDENCIES_LOOP >>
  FOR dependencies IN
  SELECT
    command_id,
    dependency
  FROM command_dependencies_320
  LOOP

    SELECT c.id
    INTO new_command_id
    FROM commands c
    WHERE c.unique_id = dependencies.command_id;

    INSERT INTO files (file) VALUES (dependencies.dependency)
    ON CONFLICT DO NOTHING;

    SELECT f.id
    INTO file_id
    FROM files f
    WHERE f.file = dependency_file;

    INSERT INTO command_dependencies VALUES (new_command_id, file_id);
  END LOOP DEPENDENCIES_LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT genie_load_commands_dependencies_320();
DROP FUNCTION genie_load_commands_dependencies_320();

SELECT
  CURRENT_TIMESTAMP,
  'Finished inserting data into commands_dependencies table';

SELECT
  CURRENT_TIMESTAMP,
  'Loading data into new clusters_commands table';

INSERT INTO clusters_commands (cluster_id, command_id, command_order)
  SELECT
    cl.id,
    co.id,
    cc.command_order
  FROM clusters_commands_320 cc
    JOIN clusters cl ON cc.cluster_id = cl.unique_id
    JOIN commands co ON cc.command_id = co.unique_id;

SELECT
  CURRENT_TIMESTAMP,
  'Finished loading data into new clusters_commands table';

SELECT
  CURRENT_TIMESTAMP,
  'Loading data into new commands_applications table';

INSERT INTO commands_applications (command_id, application_id, application_order)
  SELECT
    c.id,
    a.id,
    ca.application_order
  FROM commands_applications_320 ca
    JOIN commands c ON ca.command_id = c.unique_id
    JOIN applications a ON ca.application_id = a.unique_id;

SELECT
  CURRENT_TIMESTAMP,
  'Finished loading data into new commands_applications table';

SELECT
  CURRENT_TIMESTAMP,
  'Finished loading data from old 3.2.0 tables to 3.3.0 tables';
