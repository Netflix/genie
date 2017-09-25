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

-- SET DATABASE SQL SYNTAX ORA TRUE;

--
-- Table structure for table applications
--

CREATE TABLE applications (
  id             VARCHAR(255)                              NOT NULL,
  created        TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3) NOT NULL,
  updated        TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3) NOT NULL,
  name           VARCHAR(255)                              NOT NULL,
  genie_user     VARCHAR(255)                              NOT NULL,
  version        VARCHAR(255)                              NOT NULL,
  description    LONGVARCHAR    DEFAULT NULL,
  tags           VARCHAR(10000) DEFAULT NULL,
  setup_file     VARCHAR(1024)  DEFAULT NULL,
  status         VARCHAR(20) DEFAULT 'INACTIVE'            NOT NULL,
  type           VARCHAR(255)   DEFAULT NULL,
  entity_version INT DEFAULT '0'                           NOT NULL,
  PRIMARY KEY (id)
);

CREATE INDEX APPLICATIONS_NAME_INDEX
  ON applications (name);
CREATE INDEX APPLICATIONS_STATUS_INDEX
  ON applications (status);
CREATE INDEX APPLICATIONS_TAGS_INDEX
  ON applications (tags);
CREATE INDEX APPLICATIONS_TYPE_INDEX
  ON applications (type);

--
-- Table structure for table clusters
--

CREATE TABLE clusters (
  id             VARCHAR(255)                              NOT NULL,
  created        TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3) NOT NULL,
  updated        TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3) NOT NULL,
  name           VARCHAR(255)                              NOT NULL,
  genie_user     VARCHAR(255)                              NOT NULL,
  version        VARCHAR(255)                              NOT NULL,
  description    LONGVARCHAR    DEFAULT NULL,
  tags           VARCHAR(10000) DEFAULT NULL,
  setup_file     VARCHAR(1024)  DEFAULT NULL,
  status         VARCHAR(20) DEFAULT 'OUT_OF_SERVICE'      NOT NULL,
  entity_version INT            DEFAULT '0',
  PRIMARY KEY (id)
);

CREATE INDEX CLUSTERS_NAME_INDEX
  ON clusters (name);
CREATE INDEX CLUSTERS_STATUS_INDEX
  ON clusters (status);
CREATE INDEX CLUSTERS_TAGS_INDEX
  ON clusters (tags);

--
-- Table structure for table commands
--

CREATE TABLE commands (
  id             VARCHAR(255)                              NOT NULL,
  created        TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3) NOT NULL,
  updated        TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3) NOT NULL,
  name           VARCHAR(255)                              NOT NULL,
  genie_user     VARCHAR(255)                              NOT NULL,
  version        VARCHAR(255)                              NOT NULL,
  description    LONGVARCHAR    DEFAULT NULL,
  tags           VARCHAR(10000) DEFAULT NULL,
  setup_file     VARCHAR(1024)  DEFAULT NULL,
  executable     VARCHAR(255)                              NOT NULL,
  check_delay    BIGINT DEFAULT '10000'                    NOT NULL,
  memory         INT            DEFAULT NULL,
  status         VARCHAR(20) DEFAULT 'INACTIVE'            NOT NULL,
  entity_version INT DEFAULT '0'                           NOT NULL,
  PRIMARY KEY (id)
);

CREATE INDEX COMMANDS_NAME_INDEX
  ON commands (name);
CREATE INDEX COMMANDS_STATUS_INDEX
  ON commands (status);
CREATE INDEX COMMANDS_TAGS_INDEX
  ON commands (tags);

--
-- Table structure for table job_requests
--

CREATE TABLE job_requests (
  id                   VARCHAR(255)                                NOT NULL,
  created              TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)   NOT NULL,
  updated              TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)   NOT NULL,
  name                 VARCHAR(255)                                NOT NULL,
  genie_user           VARCHAR(255)                                NOT NULL,
  version              VARCHAR(255)                                NOT NULL,
  description          LONGVARCHAR    DEFAULT NULL,
  entity_version       INT DEFAULT '0'                             NOT NULL,
  command_args         VARCHAR(10000)                              NOT NULL,
  group_name           VARCHAR(255)   DEFAULT NULL,
  setup_file           VARCHAR(1024)  DEFAULT NULL,
  cluster_criterias    LONGVARCHAR                                 NOT NULL,
  command_criteria     LONGVARCHAR                                 NOT NULL,
  dependencies         LONGVARCHAR                                 NOT NULL,
  disable_log_archival BOOLEAN DEFAULT FALSE                       NOT NULL,
  email                VARCHAR(255)   DEFAULT NULL,
  tags                 VARCHAR(10000) DEFAULT NULL,
  cpu                  INT            DEFAULT NULL,
  memory               INT            DEFAULT NULL,
  applications         VARCHAR(2048) DEFAULT '[]'                  NOT NULL,
  timeout              INT            DEFAULT NULL,
  configs              LONGVARCHAR                                 NOT NULL,
  PRIMARY KEY (id)
);

CREATE INDEX JOB_REQUESTS_CREATED_INDEX
  ON job_requests (created);

--
-- Table structure for table jobs
--

CREATE TABLE jobs (
  id               VARCHAR(255)                              NOT NULL,
  created          TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3) NOT NULL,
  updated          TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3) NOT NULL,
  name             VARCHAR(255)                              NOT NULL,
  genie_user       VARCHAR(255)                              NOT NULL,
  version          VARCHAR(255)                              NOT NULL,
  archive_location VARCHAR(1024)  DEFAULT NULL,
  command_args     VARCHAR(10000)                            NOT NULL,
  command_id       VARCHAR(255)   DEFAULT NULL,
  command_name     VARCHAR(255)   DEFAULT NULL,
  description      LONGVARCHAR    DEFAULT NULL,
  cluster_id       VARCHAR(255)   DEFAULT NULL,
  cluster_name     VARCHAR(255)   DEFAULT NULL,
  finished         DATETIME(3)    DEFAULT NULL,
  started          DATETIME(3)    DEFAULT NULL,
  status           VARCHAR(20) DEFAULT 'INIT'                NOT NULL,
  status_msg       VARCHAR(255)   DEFAULT NULL,
  entity_version   INT DEFAULT '0'                           NOT NULL,
  tags             VARCHAR(10000) DEFAULT NULL,
  PRIMARY KEY (id),
  CONSTRAINT JOBS_CLUSTER_ID_FK FOREIGN KEY (cluster_id) REFERENCES clusters (id),
  CONSTRAINT JOBS_COMMAND_ID_FK FOREIGN KEY (command_id) REFERENCES commands (id),
  CONSTRAINT JOBS_ID_FK FOREIGN KEY (id) REFERENCES job_requests (id)
    ON DELETE CASCADE
);

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
CREATE INDEX JOBS_NAME_INDEX
  ON jobs (name);
CREATE INDEX JOBS_STARTED_INDEX
  ON jobs (started);
CREATE INDEX JOBS_STATUS_INDEX
  ON jobs (status);
CREATE INDEX JOBS_TAGS_INDEX
  ON jobs (tags);
CREATE INDEX JOBS_USER_INDEX
  ON jobs (genie_user);

--
-- Table structure for table job_executions
--

CREATE TABLE job_executions (
  id             VARCHAR(255)                              NOT NULL,
  created        TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3) NOT NULL,
  updated        TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3) NOT NULL,
  entity_version INT DEFAULT '0'                           NOT NULL,
  host_name      VARCHAR(255)                              NOT NULL,
  process_id     INT          DEFAULT NULL,
  exit_code      INT          DEFAULT NULL,
  check_delay    BIGINT       DEFAULT NULL,
  timeout        TIMESTAMP(3) DEFAULT NULL,
  memory         INT          DEFAULT NULL,
  PRIMARY KEY (id),
  CONSTRAINT JOB_EXECUTIONS_ID_FK FOREIGN KEY (id) REFERENCES jobs (id)
    ON DELETE CASCADE
);

CREATE INDEX JOB_EXECUTIONS_HOSTNAME_INDEX
  ON job_executions (host_name);

--
-- Table structure for table job_metadata
--

CREATE TABLE job_metadata (
  id                        VARCHAR(255)                              NOT NULL,
  created                   TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3) NOT NULL,
  updated                   TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3) NOT NULL,
  entity_version            INT DEFAULT '0'                           NOT NULL,
  client_host               VARCHAR(255)  DEFAULT NULL,
  user_agent                VARCHAR(2048) DEFAULT NULL,
  num_attachments           INT           DEFAULT NULL,
  total_size_of_attachments BIGINT        DEFAULT NULL,
  std_out_size              BIGINT        DEFAULT NULL,
  std_err_size              BIGINT        DEFAULT NULL,
  PRIMARY KEY (id),
  CONSTRAINT JOB_METADATA_ID_FK FOREIGN KEY (id) REFERENCES jobs (id)
    ON DELETE CASCADE
);

--
-- Table structure for table application_configs
--

CREATE TABLE application_configs (
  application_id VARCHAR(255)  NOT NULL,
  config         VARCHAR(2048) NOT NULL,
  PRIMARY KEY (application_id, config),
  CONSTRAINT APPLICATION_CONFIGS_APPLICATION_ID_FK FOREIGN KEY (application_id) REFERENCES applications (id)
    ON DELETE CASCADE
);

CREATE INDEX APPLICATION_CONFIGS_APPLICATION_ID_INDEX
  ON application_configs (application_id);

--
-- Table structure for table application_dependencies
--

CREATE TABLE application_dependencies (
  application_id VARCHAR(255)  NOT NULL,
  dependency     VARCHAR(2048) NOT NULL,
  PRIMARY KEY (application_id, dependency),
  CONSTRAINT APPLICATION_DEPENDENCIES_APPLICATION_ID_FK FOREIGN KEY (application_id) REFERENCES applications (id)
    ON DELETE CASCADE
);

CREATE INDEX APPLICATION_DEPENDENCIES_APPLICATION_ID_INDEX
  ON application_dependencies (application_id);

--
-- Table structure for table cluster_configs
--

CREATE TABLE cluster_configs (
  cluster_id VARCHAR(255)  NOT NULL,
  config     VARCHAR(2048) NOT NULL,
  PRIMARY KEY (cluster_id, config),
  CONSTRAINT CLUSTER_CONFIGS_CLUSTER_ID_FK FOREIGN KEY (cluster_id) REFERENCES clusters (id)
    ON DELETE CASCADE
);

CREATE INDEX CLUSTER_CONFIGS_CLUSTER_ID_INDEX
  ON cluster_configs (cluster_id);

--
-- Table structure for table cluster_dependencies
--

CREATE TABLE cluster_dependencies (
  cluster_id VARCHAR(255)  NOT NULL,
  dependency VARCHAR(2048) NOT NULL,
  PRIMARY KEY (cluster_id, dependency),
  CONSTRAINT CLUSTER_DEPENDENCIES_CLUSTER_ID_FK FOREIGN KEY (cluster_id) REFERENCES clusters (id)
    ON DELETE CASCADE
);

CREATE INDEX CLUSTER_DEPENDENCIES_CLUSTER_ID_INDEX
  ON cluster_dependencies (cluster_id);

--
-- Table structure for table clusters_commands
--
CREATE TABLE clusters_commands (
  cluster_id    VARCHAR(255) NOT NULL,
  command_id    VARCHAR(255) NOT NULL,
  command_order INT          NOT NULL,
  PRIMARY KEY (cluster_id, command_id, command_order),
  CONSTRAINT CLUSTERS_COMMANDS_CLUSTER_ID_FK FOREIGN KEY (cluster_id) REFERENCES clusters (id)
    ON DELETE CASCADE,
  CONSTRAINT CLUSTERS_COMMANDS_COMMAND_ID_FK FOREIGN KEY (command_id) REFERENCES commands (id)
);

CREATE INDEX CLUSTERS_COMMANDS_CLUSTER_ID_INDEX
  ON clusters_commands (cluster_id);
CREATE INDEX CLUSTERS_COMMANDS_COMMAND_ID_INDEX
  ON clusters_commands (command_id);

--
-- Table structure for table command_configs
--

CREATE TABLE command_configs (
  command_id VARCHAR(255)  NOT NULL,
  config     VARCHAR(2048) NOT NULL,
  PRIMARY KEY (command_id, config),
  CONSTRAINT COMMAND_CONFIGS_COMMAND_ID_FK FOREIGN KEY (command_id) REFERENCES commands (id)
    ON DELETE CASCADE
);

CREATE INDEX COMMAND_CONFIGS_COMMAND_ID_INDEX
  ON command_configs (command_id);

--
-- Table structure for table command_dependencies
--

CREATE TABLE command_dependencies (
  command_id VARCHAR(255)  NOT NULL,
  dependency VARCHAR(2048) NOT NULL,
  PRIMARY KEY (command_id, dependency),
  CONSTRAINT COMMAND_DEPENDENCIES_COMMAND_ID_FK FOREIGN KEY (command_id) REFERENCES commands (id)
    ON DELETE CASCADE
);

CREATE INDEX COMMAND_DEPENDENCIES_COMMAND_ID_INDEX
  ON command_dependencies (command_id);

--
-- Table structure for table commands_applications
--

CREATE TABLE commands_applications (
  command_id        VARCHAR(255) NOT NULL,
  application_id    VARCHAR(255) NOT NULL,
  application_order INT          NOT NULL,
  PRIMARY KEY (command_id, application_id, application_order),
  CONSTRAINT COMMANDS_APPLICATIONS_APPLICATION_ID_FK FOREIGN KEY (application_id) REFERENCES applications (id),
  CONSTRAINT COMMANDS_APPLICATIONS_COMMAND_ID_FK FOREIGN KEY (command_id) REFERENCES commands (id)
    ON DELETE CASCADE
);

CREATE INDEX COMMANDS_APPLICATIONS_APPLICATION_ID_INDEX
  ON commands_applications (application_id);
CREATE INDEX COMMANDS_APPLICATIONS_COMMAND_ID_INDEX
  ON commands_applications (command_id);

--
-- Table structure for table jobs_applications
--

CREATE TABLE jobs_applications (
  job_id            VARCHAR(255) NOT NULL,
  application_id    VARCHAR(255) NOT NULL,
  application_order INT          NOT NULL,
  PRIMARY KEY (job_id, application_id, application_order),
  CONSTRAINT JOBS_APPLICATIONS_APPLICATION_ID_FK FOREIGN KEY (application_id) REFERENCES applications (id),
  CONSTRAINT JOBS_APPLICATIONS_JOB_ID_FK FOREIGN KEY (job_id) REFERENCES jobs (id)
    ON DELETE CASCADE
);

CREATE INDEX JOBS_APPLICATIONS_APPLICATION_ID_INDEX
  ON jobs_applications (application_id);
CREATE INDEX JOBS_APPLICATIONS_JOB_ID_INDEX
  ON jobs_applications (job_id);
