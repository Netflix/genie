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

ALTER TABLE jobs
    DROP CONSTRAINT JOBS_CLUSTER_ID_FK;
ALTER TABLE jobs
    DROP CONSTRAINT JOBS_COMMAND_ID_FK;
ALTER TABLE jobs
    DROP CONSTRAINT JOBS_ID_FK;
ALTER TABLE job_executions
    DROP CONSTRAINT JOB_EXECUTIONS_ID_FK;
ALTER TABLE job_metadata
    DROP CONSTRAINT JOB_METADATA_ID_FK;
ALTER TABLE application_configs
    DROP CONSTRAINT APPLICATION_CONFIGS_APPLICATION_ID_FK;
ALTER TABLE application_dependencies
    DROP CONSTRAINT APPLICATION_DEPENDENCIES_APPLICATION_ID_FK;
ALTER TABLE cluster_configs
    DROP CONSTRAINT CLUSTER_CONFIGS_CLUSTER_ID_FK;
ALTER TABLE cluster_dependencies
    DROP CONSTRAINT CLUSTER_DEPENDENCIES_CLUSTER_ID_FK;
ALTER TABLE clusters_commands
    DROP CONSTRAINT CLUSTERS_COMMANDS_CLUSTER_ID_FK;
ALTER TABLE clusters_commands
    DROP CONSTRAINT CLUSTERS_COMMANDS_COMMAND_ID_FK;
ALTER TABLE command_configs
    DROP CONSTRAINT COMMAND_CONFIGS_COMMAND_ID_FK;
ALTER TABLE command_dependencies
    DROP CONSTRAINT COMMAND_DEPENDENCIES_COMMAND_ID_FK;
ALTER TABLE commands_applications
    DROP CONSTRAINT COMMANDS_APPLICATIONS_APPLICATION_ID_FK;
ALTER TABLE commands_applications
    DROP CONSTRAINT COMMANDS_APPLICATIONS_COMMAND_ID_FK;
ALTER TABLE jobs_applications
    DROP CONSTRAINT JOBS_APPLICATIONS_APPLICATION_ID_FK;
ALTER TABLE jobs_applications
    DROP CONSTRAINT JOBS_APPLICATIONS_JOB_ID_FK;

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

CREATE TABLE tags (
  id             BIGINT IDENTITY                            NOT NULL,
  created        TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)  NOT NULL,
  updated        TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)  NOT NULL,
  entity_version INT DEFAULT '0'                            NOT NULL,
  tag            VARCHAR(255)                               NOT NULL,
  CONSTRAINT TAGS_TAG_UNIQUE_INDEX UNIQUE (tag)
);

CREATE TABLE files (
  id             BIGINT IDENTITY                            NOT NULL,
  created        TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)  NOT NULL,
  updated        TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)  NOT NULL,
  entity_version INT DEFAULT '0'                            NOT NULL,
  file           VARCHAR(1024)                              NOT NULL,
  CONSTRAINT FILES_FILE_UNIQUE_INDEX UNIQUE (file)
);

CREATE TABLE criteria (
  id BIGINT IDENTITY NOT NULL
);

CREATE TABLE criteria_tags (
  criterion_id BIGINT NOT NULL,
  tag_id       BIGINT NOT NULL,
  PRIMARY KEY (criterion_id, tag_id),
  CONSTRAINT CRITERIA_TAGS_CRITERION_ID_FK FOREIGN KEY (criterion_id) REFERENCES criteria (id)
    ON DELETE CASCADE,
  CONSTRAINT CRITERIA_TAGS_TAG_ID_FK FOREIGN KEY (tag_id) REFERENCES tags (id)
    ON DELETE RESTRICT
);

CREATE INDEX CRITERIA_TAGS_CRITERION_ID_INDEX
  ON criteria_tags (criterion_id);
CREATE INDEX CRITERIA_TAGS_TAG_ID_INDEX
  ON criteria_tags (tag_id);

CREATE TABLE applications (
  id             BIGINT IDENTITY                                         NOT NULL,
  created        TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)               NOT NULL,
  updated        TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)               NOT NULL,
  entity_version INT DEFAULT '0'                                         NOT NULL,
  unique_id      VARCHAR(255)                                            NOT NULL,
  name           VARCHAR(255)                                            NOT NULL,
  genie_user     VARCHAR(255)                                            NOT NULL,
  version        VARCHAR(255)                                            NOT NULL,
  description    LONGVARCHAR  DEFAULT NULL,
  setup_file     BIGINT       DEFAULT NULL,
  status         VARCHAR(20) DEFAULT 'INACTIVE'                          NOT NULL,
  type           VARCHAR(255) DEFAULT NULL,
  CONSTRAINT APPLICATIONS_UNIQUE_ID_UNIQUE_INDEX UNIQUE (unique_id),
  CONSTRAINT APPLICATIONS_SETUP_FILE_FK FOREIGN KEY (setup_file) REFERENCES files (id)
    ON DELETE RESTRICT
);

CREATE INDEX APPLICATIONS_NAME_INDEX
  ON applications (name);
CREATE INDEX APPLICATIONS_SETUP_FILE_INDEX
  ON applications (setup_file);
CREATE INDEX APPLICATIONS_STATUS_INDEX
  ON applications (status);
CREATE INDEX APPLICATIONS_TYPE_INDEX
  ON applications (type);

CREATE TABLE applications_configs (
  application_id BIGINT NOT NULL,
  file_id        BIGINT NOT NULL,
  PRIMARY KEY (application_id, file_id),
  CONSTRAINT APPLICATIONS_CONFIGS_APPLICATION_ID_FK FOREIGN KEY (application_id) REFERENCES applications (id)
    ON DELETE CASCADE,
  CONSTRAINT APPLICATIONS_CONFIGS_FILE_ID_FK FOREIGN KEY (file_id) REFERENCES files (id)
    ON DELETE RESTRICT
);

CREATE INDEX APPLICATIONS_CONFIGS_APPLICATION_ID_INDEX
  ON applications_configs (application_id);
CREATE INDEX APPLICATIONS_CONFIGS_FILE_ID_INDEX
  ON applications_configs (file_id);

CREATE TABLE applications_dependencies (
  application_id BIGINT NOT NULL,
  file_id        BIGINT NOT NULL,
  PRIMARY KEY (application_id, file_id),
  CONSTRAINT APPLICATIONS_DEPENDENCIES_APPLICATION_ID_FK FOREIGN KEY (application_id) REFERENCES applications (id)
    ON DELETE CASCADE,
  CONSTRAINT APPLICATIONS_DEPENDENCIES_FILE_ID_FK FOREIGN KEY (file_id) REFERENCES files (id)
    ON DELETE RESTRICT
);

CREATE INDEX APPLICATIONS_DEPENDENCIES_APPLICATION_ID_INDEX
  ON applications_dependencies (application_id);
CREATE INDEX APPLICATIONS_DEPENDENCIES_FILE_ID_INDEX
  ON applications_dependencies (file_id);

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

CREATE TABLE clusters (
  id             BIGINT IDENTITY                                      NOT NULL,
  created        TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)            NOT NULL,
  updated        TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)            NOT NULL,
  entity_version INT DEFAULT '0'                                      NOT NULL,
  unique_id      VARCHAR(255)                                         NOT NULL,
  name           VARCHAR(255)                                         NOT NULL,
  genie_user     VARCHAR(255)                                         NOT NULL,
  version        VARCHAR(255)                                         NOT NULL,
  description    LONGVARCHAR DEFAULT NULL,
  setup_file     BIGINT      DEFAULT NULL,
  status         VARCHAR(20) DEFAULT 'OUT_OF_SERVICE'                 NOT NULL,
  CONSTRAINT CLUSTERS_UNIQUE_ID_UNIQUE_INDEX UNIQUE (unique_id),
  CONSTRAINT CLUSTERS_SETUP_FILE_FK FOREIGN KEY (setup_file) REFERENCES files (id)
    ON DELETE RESTRICT
);

CREATE INDEX CLUSTERS_NAME_INDEX
  ON clusters (name);
CREATE INDEX CLUSTERS_SETUP_FILE_INDEX
  ON clusters (setup_file);
CREATE INDEX CLUSTERS_STATUS_INDEX
  ON clusters (status);

CREATE TABLE clusters_configs (
  cluster_id BIGINT NOT NULL,
  file_id    BIGINT NOT NULL,
  PRIMARY KEY (cluster_id, file_id),
  CONSTRAINT CLUSTERS_CONFIGS_CLUSTER_ID_FK FOREIGN KEY (cluster_id) REFERENCES clusters (id)
    ON DELETE CASCADE,
  CONSTRAINT CLUSTERS_CONFIGS_FILE_ID_FK FOREIGN KEY (file_id) REFERENCES files (id)
    ON DELETE RESTRICT
);

CREATE INDEX CLUSTERS_CONFIGS_CLUSTER_ID_INDEX
  ON clusters_configs (cluster_id);
CREATE INDEX CLUSTERS_CONFIGS_FILE_ID_INDEX
  ON clusters_configs (file_id);

CREATE TABLE clusters_dependencies (
  cluster_id BIGINT NOT NULL,
  file_id    BIGINT NOT NULL,
  PRIMARY KEY (cluster_id, file_id),
  CONSTRAINT CLUSTERS_DEPENDENCIES_CLUSTER_ID_FK FOREIGN KEY (cluster_id) REFERENCES clusters (id)
    ON DELETE CASCADE,
  CONSTRAINT CLUSTERS_DEPENDENCIES_FILE_ID_FK FOREIGN KEY (file_id) REFERENCES files (id)
    ON DELETE RESTRICT
);

CREATE INDEX CLUSTERS_DEPENDENCIES_CLUSTER_ID_INDEX
  ON clusters_dependencies (cluster_id);
CREATE INDEX CLUSTERS_DEPENDENCIES_FILE_ID_INDEX
  ON clusters_dependencies (file_id);

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

CREATE TABLE commands (
  id             BIGINT IDENTITY                                       NOT NULL,
  created        TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)             NOT NULL,
  updated        TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)             NOT NULL,
  entity_version INT DEFAULT '0'                                       NOT NULL,
  unique_id      VARCHAR(255)                                          NOT NULL,
  name           VARCHAR(255)                                          NOT NULL,
  genie_user     VARCHAR(255)                                          NOT NULL,
  version        VARCHAR(255)                                          NOT NULL,
  description    LONGVARCHAR DEFAULT NULL,
  setup_file     BIGINT      DEFAULT NULL,
  executable     VARCHAR(255)                                          NOT NULL,
  check_delay    BIGINT DEFAULT '10000'                                NOT NULL,
  memory         INT         DEFAULT NULL,
  status         VARCHAR(20) DEFAULT 'INACTIVE'                        NOT NULL,
  CONSTRAINT COMMANDS_UNIQUE_ID_UNIQUE_INDEX UNIQUE (unique_id),
  CONSTRAINT COMMANDS_SETUP_FILE_FK FOREIGN KEY (setup_file) REFERENCES files (id)
    ON DELETE RESTRICT
);

CREATE INDEX COMMANDS_NAME_INDEX
  ON commands (name);
CREATE INDEX COMMANDS_SETUP_FILE_INDEX
  ON commands (setup_file);
CREATE INDEX COMMANDS_STATUS_INDEX
  ON commands (status);

CREATE TABLE commands_configs (
  command_id BIGINT NOT NULL,
  file_id    BIGINT NOT NULL,
  PRIMARY KEY (command_id, file_id),
  CONSTRAINT COMMANDS_CONFIGS_COMMAND_ID_FK FOREIGN KEY (command_id) REFERENCES commands (id)
    ON DELETE CASCADE,
  CONSTRAINT COMMANDS_CONFIGS_FILE_ID_FK FOREIGN KEY (file_id) REFERENCES files (id)
    ON DELETE RESTRICT
);

CREATE INDEX COMMANDS_CONFIGS_COMMAND_ID_INDEX
  ON commands_configs (command_id);
CREATE INDEX COMMANDS_CONFIGS_FILE_ID_INDEX
  ON commands_configs (file_id);

CREATE TABLE commands_dependencies (
  command_id BIGINT NOT NULL,
  file_id    BIGINT NOT NULL,
  PRIMARY KEY (command_id, file_id),
  CONSTRAINT COMMANDS_DEPENDENCIES_COMMAND_ID_FK FOREIGN KEY (command_id) REFERENCES commands (id)
    ON DELETE CASCADE,
  CONSTRAINT COMMANDS_DEPENDENCIES_FILE_ID_FK FOREIGN KEY (file_id) REFERENCES files (id)
    ON DELETE RESTRICT
);

CREATE INDEX COMMANDS_DEPENDENCIES_COMMAND_ID_INDEX
  ON commands_dependencies (command_id);
CREATE INDEX COMMANDS_DEPENDENCIES_FILE_ID_INDEX
  ON commands_dependencies (file_id);

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

CREATE TABLE jobs (
  -- common
  id                        BIGINT IDENTITY                                         NOT NULL,
  created                   TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)               NOT NULL,
  updated                   TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)               NOT NULL,
  entity_version            INT DEFAULT '0'                                         NOT NULL,

  -- Job Request
  unique_id                 VARCHAR(255)                                            NOT NULL,
  name                      VARCHAR(255)                                            NOT NULL,
  genie_user                VARCHAR(255)                                            NOT NULL,
  version                   VARCHAR(255)                                            NOT NULL,
  command_criterion         BIGINT        DEFAULT NULL,
  description               LONGVARCHAR   DEFAULT NULL,
  setup_file                BIGINT        DEFAULT NULL,
  tags                      VARCHAR(1024) DEFAULT NULL,
  genie_user_group          VARCHAR(255)  DEFAULT NULL,
  disable_log_archival      BOOLEAN DEFAULT FALSE                                   NOT NULL,
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
  finished                  DATETIME(3)   DEFAULT NULL,
  started                   DATETIME(3)   DEFAULT NULL,
  status                    VARCHAR(20) DEFAULT 'INIT'                              NOT NULL,
  status_msg                VARCHAR(255)  DEFAULT NULL,

  -- Job Execution
  host_name                 VARCHAR(255)                                            NOT NULL,
  process_id                INT           DEFAULT NULL,
  exit_code                 INT           DEFAULT NULL,
  check_delay               BIGINT        DEFAULT NULL,
  timeout                   TIMESTAMP(3)  DEFAULT NULL,
  memory_used               INT           DEFAULT NULL,
  archive_location          VARCHAR(1024) DEFAULT NULL,
  CONSTRAINT JOBS_UNIQUE_ID_UNIQUE_INDEX UNIQUE (unique_id),
  CONSTRAINT JOBS_COMMAND_CRITERION_FK FOREIGN KEY (command_criterion) REFERENCES criteria (id)
    ON DELETE RESTRICT,
  CONSTRAINT JOBS_CLUSTER_ID_FK FOREIGN KEY (cluster_id) REFERENCES clusters (id)
    ON DELETE RESTRICT,
  CONSTRAINT JOBS_COMMAND_ID_FK FOREIGN KEY (command_id) REFERENCES commands (id)
    ON DELETE RESTRICT,
  CONSTRAINT JOBS_SETUP_FILE_FK FOREIGN KEY (setup_file) REFERENCES files (id)
);

CREATE INDEX JOBS_CLUSTER_ID_INDEX
  ON jobs (cluster_id);
CREATE INDEX JOBS_CLUSTER_NAME_INDEX
  ON jobs (cluster_name);
CREATE INDEX JOBS_COMMAND_CRITERION_INDEX
  ON jobs (command_criterion);
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

CREATE TABLE job_command_arguments (
  job_id         BIGINT         NOT NULL,
  argument       VARCHAR(10000) NOT NULL,
  argument_order INT            NOT NULL,
  PRIMARY KEY (job_id, argument_order),
  CONSTRAINT JOB_COMMAND_ARGUMENTS_JOB_ID FOREIGN KEY (job_id) REFERENCES jobs (id)
    ON DELETE CASCADE
);

CREATE INDEX JOB_COMMAND_ARGUMENTS_JOB_ID_INDEX
  ON job_command_arguments (job_id);

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

CREATE TABLE jobs_cluster_criteria (
  job_id         BIGINT NOT NULL,
  criterion_id   BIGINT NOT NULL,
  priority_order INT    NOT NULL,
  PRIMARY KEY (job_id, criterion_id, priority_order),
  CONSTRAINT JOBS_CLUSTER_CRITERIA_JOB_ID FOREIGN KEY (job_id) REFERENCES jobs (id)
    ON DELETE CASCADE,
  CONSTRAINT JOBS_CLUSTER_CRITERIA_CRITERION_ID_FK FOREIGN KEY (criterion_id) REFERENCES criteria (id)
    ON DELETE RESTRICT
);

CREATE INDEX JOBS_CLUSTER_CRITERIA_JOB_ID_INDEX
  ON jobs_cluster_criteria (job_id);
CREATE INDEX JOBS_CLUSTER_CRITERIA_CRITERION_ID_INDEX
  ON jobs_cluster_criteria (criterion_id);

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

CREATE TABLE jobs_configs (
  job_id  BIGINT NOT NULL,
  file_id BIGINT NOT NULL,
  PRIMARY KEY (job_id, file_id),
  CONSTRAINT JOBS_CONFIGS_JOB_ID_FK FOREIGN KEY (job_id) REFERENCES jobs (id)
    ON DELETE CASCADE,
  CONSTRAINT JOBS_CONFIGS_FILE_ID_FK FOREIGN KEY (file_id) REFERENCES files (id)
    ON DELETE RESTRICT
);

CREATE INDEX JOBS_CONFIGS_JOB_ID_INDEX
  ON jobs_configs (job_id);
CREATE INDEX JOBS_CONFIGS_FILE_ID_INDEX
  ON jobs_configs (file_id);

CREATE TABLE jobs_dependencies (
  job_id  BIGINT NOT NULL,
  file_id BIGINT NOT NULL,
  PRIMARY KEY (job_id, file_id),
  CONSTRAINT JOBS_DEPENDENCIES_JOB_ID_FK FOREIGN KEY (job_id) REFERENCES jobs (id)
    ON DELETE CASCADE,
  CONSTRAINT JOBS_DEPENDENCIES_FILE_ID_FK FOREIGN KEY (file_id) REFERENCES files (id)
    ON DELETE RESTRICT
);

CREATE INDEX JOBS_DEPENDENCIES_JOB_ID_INDEX
  ON jobs_dependencies (job_id);
CREATE INDEX JOBS_DEPENDENCIES_FILE_ID_INDEX
  ON jobs_dependencies (file_id);
