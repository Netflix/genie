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
  'Dropping old tables';

DROP TABLE job_metadata;
DROP TABLE job_executions;
DROP TABLE jobs_applications;
DROP TABLE jobs;
DROP TABLE job_requests;
DROP TABLE application_configs;
DROP TABLE application_dependencies;
DROP TABLE cluster_configs;
DROP TABLE cluster_dependencies;
DROP TABLE command_configs;
DROP TABLE command_dependencies;
DROP TABLE commands_applications;
DROP TABLE clusters_commands;
DROP TABLE applications;
DROP TABLE clusters;
DROP TABLE commands;

SELECT
  CURRENT_TIMESTAMP,
  'Finished dropping old tables';

SELECT
  CURRENT_TIMESTAMP,
  'Creating tags table';

CREATE TABLE tags (
  id             BIGINT IDENTITY                           NOT NULL,
  created        DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3)  NOT NULL,
  updated        DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3)  NOT NULL,
  entity_version INT DEFAULT '0'                           NOT NULL,
  tag            VARCHAR(255)                              NOT NULL,
  PRIMARY KEY (id)
);

CREATE UNIQUE INDEX TAGS_TAG_UNIQUE_INDEX
  ON tags (tag);

SELECT
  CURRENT_TIMESTAMP,
  'Finished creating tags table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating files table';

CREATE TABLE files (
  id             BIGINT IDENTITY                           NOT NULL,
  created        DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3)  NOT NULL,
  updated        DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3)  NOT NULL,
  entity_version INT DEFAULT '0'                           NOT NULL,
  file           VARCHAR(1024)                             NOT NULL,
  PRIMARY KEY (id)
);

CREATE UNIQUE INDEX FILES_FILE_UNIQUE_INDEX
  ON files (file);

SELECT
  CURRENT_TIMESTAMP,
  'Finished creating files table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating new applications table';

CREATE TABLE applications (
  id             BIGINT IDENTITY                                        NOT NULL,
  created        DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3)               NOT NULL,
  updated        DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3)               NOT NULL,
  entity_version INT DEFAULT '0'                                        NOT NULL,
  unique_id      VARCHAR(255) DEFAULT UUID()                            NOT NULL,
  name           VARCHAR(255)                                           NOT NULL,
  genie_user     VARCHAR(255)                                           NOT NULL,
  version        VARCHAR(255)                                           NOT NULL,
  description    LONGVARCHAR  DEFAULT NULL,
  setup_file     BIGINT       DEFAULT NULL,
  status         VARCHAR(20) DEFAULT 'INACTIVE'                         NOT NULL,
  type           VARCHAR(255) DEFAULT NULL,
  PRIMARY KEY (id),
  CONSTRAINT APPLICATIONS_SETUP_FILE_FK FOREIGN KEY (setup_file) REFERENCES files (id)
    ON DELETE RESTRICT
);

CREATE UNIQUE INDEX APPLICATIONS_UNIQUE_ID_UNIQUE_INDEX
  ON applications (unique_id);
CREATE INDEX APPLICATIONS_NAME_INDEX
  ON applications (name);
CREATE INDEX APPLICATIONS_SETUP_FILE_INDEX
  ON applications (setup_file);
CREATE INDEX APPLICATIONS_STATUS_INDEX
  ON applications (status);
CREATE INDEX APPLICATIONS_TYPE_INDEX
  ON applications (type);

SELECT
  CURRENT_TIMESTAMP,
  'Created new applications table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating new application_configs table';

CREATE TABLE application_configs (
  application_id BIGINT NOT NULL,
  file_id        BIGINT NOT NULL,
  PRIMARY KEY (application_id, file_id),
  CONSTRAINT APPLICATION_CONFIGS_APPLICATION_ID_FK FOREIGN KEY (application_id) REFERENCES applications (id)
    ON DELETE CASCADE,
  CONSTRAINT APPLICATION_CONFIGS_FILE_ID_FK FOREIGN KEY (file_id) REFERENCES files (id)
    ON DELETE RESTRICT
);

CREATE INDEX APPLICATION_CONFIGS_APPLICATION_ID_INDEX
  ON application_configs (application_id);
CREATE INDEX APPLICATION_CONFIGS_FILE_ID_INDEX
  ON application_configs (file_id);

SELECT
  CURRENT_TIMESTAMP,
  'Created new application_configs table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating new application_dependencies table';

CREATE TABLE application_dependencies (
  application_id BIGINT NOT NULL,
  file_id        BIGINT NOT NULL,
  PRIMARY KEY (application_id, file_id),
  CONSTRAINT APPLICATION_DEPENDENCIES_APPLICATION_ID_FK FOREIGN KEY (application_id) REFERENCES applications (id)
    ON DELETE CASCADE,
  CONSTRAINT APPLICATION_DEPENDENCIES_FILE_ID_FK FOREIGN KEY (file_id) REFERENCES files (id)
    ON DELETE RESTRICT
);

CREATE INDEX APPLICATION_DEPENDENCIES_APPLICATION_ID_INDEX
  ON application_dependencies (application_id);
CREATE INDEX APPLICATION_DEPENDENCIES_FILE_ID_INDEX
  ON application_dependencies (file_id);

SELECT
  CURRENT_TIMESTAMP,
  'Finished creating new application_dependencies table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating new clusters table';

CREATE TABLE clusters (
  id             BIGINT IDENTITY                                     NOT NULL,
  created        DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3)            NOT NULL,
  updated        DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3)            NOT NULL,
  entity_version INT DEFAULT '0'                                     NOT NULL,
  unique_id      VARCHAR(255) DEFAULT UUID()                         NOT NULL,
  name           VARCHAR(255)                                        NOT NULL,
  genie_user     VARCHAR(255)                                        NOT NULL,
  version        VARCHAR(255)                                        NOT NULL,
  description    LONGVARCHAR DEFAULT NULL,
  setup_file     BIGINT      DEFAULT NULL,
  status         VARCHAR(20) DEFAULT 'OUT_OF_SERVICE'                NOT NULL,
  PRIMARY KEY (id),
  CONSTRAINT CLUSTERS_SETUP_FILE_FK FOREIGN KEY (setup_file) REFERENCES files (id)
    ON DELETE RESTRICT
);

CREATE UNIQUE INDEX CLUSTERS_UNIQUE_ID_UNIQUE_INDEX
  ON clusters (unique_id);
CREATE INDEX CLUSTERS_NAME_INDEX
  ON clusters (name);
CREATE INDEX CLUSTERS_SETUP_FILE_INDEX
  ON clusters (setup_file);
CREATE INDEX CLUSTERS_STATUS_INDEX
  ON clusters (status);

SELECT
  CURRENT_TIMESTAMP,
  'Finished creating new clusters table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating new cluster_configs table';

CREATE TABLE cluster_configs (
  cluster_id BIGINT NOT NULL,
  file_id    BIGINT NOT NULL,
  PRIMARY KEY (cluster_id, file_id),
  CONSTRAINT CLUSTER_CONFIGS_CLUSTER_ID_FK FOREIGN KEY (cluster_id) REFERENCES clusters (id)
    ON DELETE CASCADE,
  CONSTRAINT CLUSTER_CONFIGS_FILE_ID_FK FOREIGN KEY (file_id) REFERENCES files (id)
    ON DELETE RESTRICT
);

CREATE INDEX CLUSTER_CONFIGS_CLUSTER_ID_INDEX
  ON cluster_configs (cluster_id);
CREATE INDEX CLUSTER_CONFIGS_FILE_ID_INDEX
  ON cluster_configs (file_id);

SELECT
  CURRENT_TIMESTAMP,
  'Created new cluster_configs table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating new cluster_dependencies table';

CREATE TABLE cluster_dependencies (
  cluster_id BIGINT NOT NULL,
  file_id    BIGINT NOT NULL,
  PRIMARY KEY (cluster_id, file_id),
  CONSTRAINT CLUSTER_DEPENDENCIES_CLUSTER_ID_FK FOREIGN KEY (cluster_id) REFERENCES clusters (id)
    ON DELETE CASCADE,
  CONSTRAINT CLUSTER_DEPENDENCIES_FILE_ID_FK FOREIGN KEY (file_id) REFERENCES files (id)
    ON DELETE RESTRICT
);

CREATE INDEX CLUSTER_DEPENDENCIES_CLUSTER_ID_INDEX
  ON cluster_dependencies (cluster_id);
CREATE INDEX CLUSTER_DEPENDENCIES_FILE_ID_INDEX
  ON cluster_dependencies (file_id);

SELECT
  CURRENT_TIMESTAMP,
  'Finished creating new cluster_dependencies table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating new commands table';

CREATE TABLE commands (
  id             BIGINT IDENTITY                                      NOT NULL,
  created        DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3)             NOT NULL,
  updated        DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3)             NOT NULL,
  entity_version INT DEFAULT '0'                                      NOT NULL,
  unique_id      VARCHAR(255) DEFAULT UUID()                          NOT NULL,
  name           VARCHAR(255)                                         NOT NULL,
  genie_user     VARCHAR(255)                                         NOT NULL,
  version        VARCHAR(255)                                         NOT NULL,
  description    LONGVARCHAR DEFAULT NULL,
  setup_file     BIGINT      DEFAULT NULL,
  executable     VARCHAR(255)                                         NOT NULL,
  check_delay    BIGINT DEFAULT '10000'                               NOT NULL,
  memory         INT         DEFAULT NULL,
  status         VARCHAR(20) DEFAULT 'INACTIVE'                       NOT NULL,
  PRIMARY KEY (id),
  CONSTRAINT COMMANDS_SETUP_FILE_FK FOREIGN KEY (setup_file) REFERENCES files (id)
    ON DELETE RESTRICT
);

CREATE UNIQUE INDEX COMMANDS_UNIQUE_ID_UNIQUE_INDEX
  ON commands (unique_id);
CREATE INDEX COMMANDS_NAME_INDEX
  ON commands (name);
CREATE INDEX COMMANDS_SETUP_FILE_INDEX
  ON commands (setup_file);
CREATE INDEX COMMANDS_STATUS_INDEX
  ON commands (status);

SELECT
  CURRENT_TIMESTAMP,
  'Finished creating new commands table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating new command_configs table';

CREATE TABLE command_configs (
  command_id BIGINT NOT NULL,
  file_id    BIGINT NOT NULL,
  PRIMARY KEY (command_id, file_id),
  CONSTRAINT COMMAND_CONFIGS_COMMAND_ID_FK FOREIGN KEY (command_id) REFERENCES commands (id)
    ON DELETE CASCADE,
  CONSTRAINT COMMAND_CONFIGS_FILE_ID_FK FOREIGN KEY (file_id) REFERENCES files (id)
    ON DELETE RESTRICT
);

CREATE INDEX COMMAND_CONFIGS_COMMAND_ID_INDEX
  ON command_configs (command_id);
CREATE INDEX COMMAND_CONFIGS_FILE_ID_INDEX
  ON command_configs (file_id);

SELECT
  CURRENT_TIMESTAMP,
  'Created new command_configs table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating new command_dependencies table';

CREATE TABLE command_dependencies (
  command_id BIGINT NOT NULL,
  file_id    BIGINT NOT NULL,
  PRIMARY KEY (command_id, file_id),
  CONSTRAINT COMMAND_DEPENDENCIES_COMMAND_ID_FK FOREIGN KEY (command_id) REFERENCES commands (id)
    ON DELETE CASCADE,
  CONSTRAINT COMMAND_DEPENDENCIES_FILE_ID_FK FOREIGN KEY (file_id) REFERENCES files (id)
    ON DELETE RESTRICT
);

CREATE INDEX COMMAND_DEPENDENCIES_COMMAND_ID_INDEX
  ON command_dependencies (command_id);
CREATE INDEX COMMAND_DEPENDENCIES_FILE_ID_INDEX
  ON command_dependencies (file_id);

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
  CONSTRAINT CLUSTERS_COMMANDS_CLUSTER_ID_FK FOREIGN KEY (cluster_id) REFERENCES clusters (id)
    ON DELETE CASCADE,
  CONSTRAINT CLUSTERS_COMMANDS_COMMAND_ID_FK FOREIGN KEY (command_id) REFERENCES commands (id)
    ON DELETE RESTRICT
);

CREATE INDEX CLUSTERS_COMMANDS_CLUSTER_ID_INDEX
  ON clusters_commands (cluster_id);
CREATE INDEX CLUSTERS_COMMANDS_COMMAND_ID_INDEX
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
  CONSTRAINT COMMANDS_APPLICATIONS_APPLICATION_ID_FK FOREIGN KEY (application_id) REFERENCES applications (id)
    ON DELETE RESTRICT,
  CONSTRAINT COMMANDS_APPLICATIONS_COMMAND_ID_FK FOREIGN KEY (command_id) REFERENCES commands (id)
    ON DELETE CASCADE
);

CREATE INDEX COMMANDS_APPLICATIONS_APPLICATION_ID_INDEX
  ON commands_applications (application_id);
CREATE INDEX COMMANDS_APPLICATIONS_COMMAND_ID_INDEX
  ON commands_applications (command_id);

SELECT
  CURRENT_TIMESTAMP,
  'Finished creating into new commands_applications table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating new jobs table';

CREATE TABLE jobs (
  -- common
  id                        BIGINT IDENTITY                                       NOT NULL,
  created                   DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3)              NOT NULL,
  updated                   DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3)              NOT NULL,
  entity_version            INT DEFAULT '0'                                       NOT NULL,

  -- Job Request
  unique_id                 VARCHAR(255) DEFAULT UUID()                           NOT NULL,
  name                      VARCHAR(255)                                          NOT NULL,
  genie_user                VARCHAR(255)                                          NOT NULL,
  version                   VARCHAR(255)                                          NOT NULL,
  command_args              LONGVARCHAR   DEFAULT NULL,
  description               LONGVARCHAR   DEFAULT NULL,
  setup_file                BIGINT        DEFAULT NULL,
  tags                      VARCHAR(1024) DEFAULT NULL,
  genie_user_group          VARCHAR(255)  DEFAULT NULL,
  disable_log_archival      BIT(1) DEFAULT b'0'                                   NOT NULL,
  email                     VARCHAR(255)  DEFAULT NULL,
  cpu_requested             INT           DEFAULT NULL,
  memory_requested          INT           DEFAULT NULL,
  timeout_requested         INT           DEFAULT NULL,
  grouping                  VARCHAR(255)  DEFAULT NULL,
  grouping_instance         VARCHAR(255)  DEFAULT NULL,

  -- Job Metadata
  client_host               VARCHAR(255)  DEFAULT NULL,
  user_agent                VARCHAR(2048) DEFAULT NULL,
  num_attachments           INT           DEFAULT NULL,
  total_size_of_attachments BIGINT        DEFAULT NULL,
  std_out_size              BIGINT        DEFAULT NULL,
  std_err_size              BIGINT        DEFAULT NULL,

  -- Job
  command_id                INT           DEFAULT NULL,
  command_name              VARCHAR(255)  DEFAULT NULL,
  cluster_id                INT           DEFAULT NULL,
  cluster_name              VARCHAR(255)  DEFAULT NULL,
  started                   DATETIME(3)   DEFAULT NULL,
  finished                  DATETIME(3)   DEFAULT NULL,
  status                    VARCHAR(20) DEFAULT 'INIT'                            NOT NULL,
  status_msg                VARCHAR(255)  DEFAULT NULL,

  -- Job Execution
  host_name                 VARCHAR(255)                                          NOT NULL,
  process_id                INT           DEFAULT NULL,
  exit_code                 INT           DEFAULT NULL,
  check_delay               BIGINT        DEFAULT NULL,
  timeout                   DATETIME(3)   DEFAULT NULL,
  memory_used               INT           DEFAULT NULL,

  -- Post Job Info
  archive_location          VARCHAR(1024) DEFAULT NULL,
  PRIMARY KEY (id),
  CONSTRAINT JOBS_CLUSTER_ID_FK FOREIGN KEY (cluster_id) REFERENCES clusters (id)
    ON DELETE RESTRICT,
  CONSTRAINT JOBS_COMMAND_ID_FK FOREIGN KEY (command_id) REFERENCES commands (id)
    ON DELETE RESTRICT,
  CONSTRAINT JOBS_SETUP_FILE_FK FOREIGN KEY (setup_file) REFERENCES files (id)
    ON DELETE RESTRICT
);

CREATE UNIQUE INDEX JOBS_UNIQUE_ID_UNIQUE_INDEX
  ON jobs (job_id);
CREATE INDEX JOBS_CLUSTER_ID_INDEX
  ON jobs (cluster_id);
CREATE INDEX JOBS_CLUSTER_NAME_INDEX
  ON jobs (cluster_name);
CREATE INDEX JOBS_COMMAND_ID_INDEX
  ON jobs (command_id);
CREATE INDEX JOBS_COMMAND_NAME_INDEX
  ON jobs (command_name);
CREATE INDEX JOBS_CREATED_INDEX
  ON jobs (created);
CREATE INDEX JOBS_FINISHED_INDEX
  ON jobs (finished);
CREATE INDEX JOBS_GROUPING_INDEX
  ON jobs (grouping);
CREATE INDEX JOBS_GROUPING_INSTANCE_INDEX
  ON jobs (grouping_instance);
CREATE INDEX JOBS_NAME_INDEX
  ON jobs (name);
CREATE INDEX JOBS_SETUP_FILE_INDEX
  ON jobs (setup_file);
CREATE INDEX JOBS_STARTED_INDEX
  ON jobs (started);
CREATE INDEX JOBS_STATUS_INDEX
  ON jobs (status);
CREATE INDEX JOBS_TAGS_INDEX
  ON jobs (tags);
CREATE INDEX JOBS_USER_INDEX
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
  CONSTRAINT JOBS_APPLICATIONS_APPLICATION_ID_FK FOREIGN KEY (application_id) REFERENCES applications (id)
    ON DELETE RESTRICT,
  CONSTRAINT JOBS_APPLICATIONS_JOB_ID_FK FOREIGN KEY (job_id) REFERENCES jobs (id)
    ON DELETE CASCADE
);

CREATE INDEX JOBS_APPLICATIONS_APPLICATION_ID_INDEX
  ON jobs_applications (application_id);
CREATE INDEX JOBS_APPLICATIONS_JOB_ID_INDEX
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
  CONSTRAINT APPLICATIONS_TAGS_APPLICATION_ID_FK FOREIGN KEY (application_id) REFERENCES applications (id)
    ON DELETE CASCADE,
  CONSTRAINT APPLICATIONS_TAGS_TAG_ID_FK FOREIGN KEY (tag_id) REFERENCES tags (id)
    ON DELETE RESTRICT
);

CREATE INDEX APPLICATIONS_TAGS_APPLICATION_ID_INDEX
  ON applications_tags (application_id);
CREATE INDEX APPLICATIONS_TAGS_TAG_ID_INDEX
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
  CONSTRAINT CLUSTERS_TAGS_CLUSTER_ID_FK FOREIGN KEY (cluster_id) REFERENCES clusters (id)
    ON DELETE CASCADE,
  CONSTRAINT CLUSTERS_TAGS_TAG_ID_FK FOREIGN KEY (tag_id) REFERENCES tags (id)
    ON DELETE RESTRICT
);

CREATE INDEX CLUSTERS_TAGS_CLUSTER_ID_INDEX
  ON clusters_tags (cluster_id);
CREATE INDEX CLUSTERS_TAGS_TAG_ID_INDEX
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
  CONSTRAINT COMMANDS_TAGS_COMMAND_ID_FK FOREIGN KEY (command_id) REFERENCES commands (id)
    ON DELETE CASCADE,
  CONSTRAINT COMMANDS_TAGS_TAG_ID_FK FOREIGN KEY (tag_id) REFERENCES tags (id)
    ON DELETE RESTRICT
);

CREATE INDEX COMMANDS_TAGS_COMMAND_ID_INDEX
  ON commands_tags (command_id);
CREATE INDEX COMMANDS_TAGS_TAG_ID_INDEX
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
  CONSTRAINT JOBS_TAGS_COMMAND_ID_FK FOREIGN KEY (job_id) REFERENCES commands (id)
    ON DELETE CASCADE,
  CONSTRAINT JOBS_TAGS_TAG_ID_FK FOREIGN KEY (tag_id) REFERENCES tags (id)
    ON DELETE RESTRICT
);

CREATE INDEX JOBS_TAGS_COMMAND_ID_INDEX
  ON jobs_tags (job_id);
CREATE INDEX JOBS_TAGS_TAG_ID_INDEX
  ON jobs_tags (tag_id);

SELECT
  CURRENT_TIMESTAMP,
  'Finished creating jobs_tags table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating cluster_criterias table';

CREATE TABLE cluster_criterias (
  id             BIGINT IDENTITY                           NOT NULL,
  created        DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3)  NOT NULL,
  updated        DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3)  NOT NULL,
  entity_version INT DEFAULT '0'                           NOT NULL,
  job_id         INT                                       NOT NULL,
  priority_order INT                                       NOT NULL,
  PRIMARY KEY (id),
  CONSTRAINT CLUSTER_CRITERIAS_JOB_ID_FK FOREIGN KEY (job_id) REFERENCES jobs (id)
    ON DELETE CASCADE
);

CREATE INDEX CLUSTER_CRITERIAS_JOB_ID_INDEX
  ON cluster_criterias (job_id);

SELECT
  CURRENT_TIMESTAMP,
  'Created cluster_criterias table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating cluster_criterias_tags table';

CREATE TABLE cluster_criterias_tags (
  cluster_criteria_id BIGINT NOT NULL,
  tag_id              BIGINT NOT NULL,
  PRIMARY KEY (cluster_criteria_id, tag_id),
  CONSTRAINT CLUSTER_CRITERIAS_CLUSTER_CRITERIA_ID_FK FOREIGN KEY (cluster_criteria_id) REFERENCES cluster_criterias (id)
    ON DELETE CASCADE,
  CONSTRAINT CLUSTER_CRITERIAS_TAG_ID_FK FOREIGN KEY (tag_id) REFERENCES tags (id)
    ON DELETE RESTRICT
);

CREATE INDEX CLUSTER_CRITERIAS_TAGS_CLUSTER_CRITERIA_ID_INDEX
  ON cluster_criterias_tags (cluster_criteria_id);
CREATE INDEX CLUSTER_CRITERIAS_TAGS_ID_INDEX
  ON cluster_criterias_tags (tag_id);

SELECT
  CURRENT_TIMESTAMP,
  'Created cluster_criterias_tags table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating job_applications_requested table';

-- NOTE: Don't think we need to applications foreign key here cause user could request some bad apps
CREATE TABLE job_applications_requested (
  job_id            BIGINT       NOT NULL,
  application_id    VARCHAR(255) NOT NULL,
  application_order INT          NOT NULL,
  PRIMARY KEY (job_id, application_id, application_order),
  CONSTRAINT JOB_APPLICATIONS_REQUESTED_JOB_ID_FK FOREIGN KEY (job_id) REFERENCES jobs (id)
    ON DELETE CASCADE
);

CREATE INDEX JOB_APPLICATIONS_REQUESTED_APPLICATION_ID_INDEX
  ON job_applications_requested (application_id);
CREATE INDEX JOB_APPLICATIONS_REQUESTED_JOB_ID_INDEX
  ON job_applications_requested (job_id);

SELECT
  CURRENT_TIMESTAMP,
  'Created job_applications_requested table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating job_command_criteria_tags table';

CREATE TABLE job_command_criteria_tags (
  job_id BIGINT NOT NULL,
  tag_id BIGINT NOT NULL,
  PRIMARY KEY (job_id, tag_id),
  CONSTRAINT JOB_COMMAND_CRITERIA_TAGS_JOB_ID_FK FOREIGN KEY (job_id) REFERENCES jobs (id)
    ON DELETE CASCADE,
  CONSTRAINT JOB_COMMAND_CRITERIA_TAGS_TAG_ID_FK FOREIGN KEY (tag_id) REFERENCES tags (id)
    ON DELETE RESTRICT
);

CREATE INDEX JOB_COMMAND_CRITERIA_TAGS_JOB_ID_INDEX
  ON job_command_criteria_tags (job_id);
CREATE INDEX JOB_COMMAND_CRITERIA_TAGS_TAG_ID_INDEX
  ON job_command_criteria_tags (tag_id);

SELECT
  CURRENT_TIMESTAMP,
  'Created job_command_criteria_tags table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating job_configs table';

CREATE TABLE job_configs (
  job_id  BIGINT NOT NULL,
  file_id BIGINT NOT NULL,
  PRIMARY KEY (job_id, file_id),
  CONSTRAINT JOB_CONFIGS_JOB_ID_FK FOREIGN KEY (job_id) REFERENCES jobs (id)
    ON DELETE CASCADE,
  CONSTRAINT JOB_CONFIGS_FILE_ID_FK FOREIGN KEY (file_id) REFERENCES files (id)
    ON DELETE RESTRICT
);

CREATE INDEX JOB_CONFIGS_JOB_ID_INDEX
  ON job_configs (job_id);
CREATE INDEX JOB_CONFIGS_FILE_ID_INDEX
  ON job_configs (file_id);

SELECT
  CURRENT_TIMESTAMP,
  'Created job_configs table';

SELECT
  CURRENT_TIMESTAMP,
  'Creating job_dependencies table';

CREATE TABLE job_dependencies (
  job_id  BIGINT NOT NULL,
  fild_id BIGINT NOT NULL,
  PRIMARY KEY (job_id, fild_id),
  CONSTRAINT JOB_DEPENDENCIES_JOB_ID_FK FOREIGN KEY (job_id) REFERENCES jobs (id)
    ON DELETE CASCADE,
  CONSTRAINT JOB_DEPENDENCIES_FILE_ID_FK FOREIGN KEY (fild_id) REFERENCES files (id)
    ON DELETE RESTRICT
);

CREATE INDEX JOB_DEPENDENCIES_JOB_ID_INDEX
  ON job_dependencies (job_id);
CREATE INDEX JOB_DEPENDENCIES_FILE_ID_INDEX
  ON job_dependencies (fild_id);

SELECT
  CURRENT_TIMESTAMP,
  'Created job_dependencies table';

SELECT
  CURRENT_TIMESTAMP,
  'Finished upgrading database schema to 3.3.0';
