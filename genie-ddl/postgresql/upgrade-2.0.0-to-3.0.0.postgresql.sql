BEGIN;
SELECT CURRENT_TIMESTAMP, 'Fixing https://github.com/Netflix/genie/issues/148';
-- Fix referential integrity problem described in https://github.com/Netflix/genie/issues/148
DELETE FROM application_configs WHERE application_id NOT IN (SELECT id from application);
DELETE FROM application_jars WHERE application_id NOT IN (SELECT id from application);
DELETE FROM application_tags WHERE application_id NOT IN (SELECT id from application);
DELETE FROM cluster_command WHERE clusters_id NOT IN (SELECT id FROM cluster);
DELETE FROM cluster_tags WHERE cluster_id NOT IN (SELECT id from cluster);
DELETE FROM cluster_configs WHERE cluster_id NOT IN (SELECT id FROM cluster);
DELETE FROM cluster_command WHERE commands_id NOT IN (SELECT id FROM command);
DELETE FROM command_tags WHERE command_id NOT IN (SELECT id from command);
DELETE FROM command_configs WHERE command_id NOT IN (SELECT id FROM command);
DELETE FROM job_tags WHERE job_id NOT IN (SELECT id FROM job);
SELECT CURRENT_TIMESTAMP, 'Finished applying fix for https://github.com/Netflix/genie/issues/148';
COMMIT;

BEGIN;
SELECT CURRENT_TIMESTAMP, 'Beginning upgrade of Genie schema from version 2.0.0 to 3.0.0';

-- Rename the tables to be a little bit nicer
SELECT CURRENT_TIMESTAMP, 'Renaming all the tables to be more friendly...';
ALTER TABLE application RENAME TO applications;
ALTER TABLE application_jars RENAME TO application_dependencies;
ALTER TABLE cluster RENAME TO clusters;
ALTER TABLE cluster_command RENAME TO clusters_commands;
ALTER TABLE command RENAME TO commands;
ALTER TABLE job RENAME TO jobs;
SELECT CURRENT_TIMESTAMP, 'Successfully renamed all tables';

SELECT CURRENT_TIMESTAMP, 'Truncating job description field for new size limit of 10000 characters...';
UPDATE jobs SET description = SUBSTRING(description FROM 1 FOR 10000) WHERE LENGTH(description) > 10000;
SELECT CURRENT_TIMESTAMP, 'Finished truncating job description field to 10000 characters.';

SELECT CURRENT_TIMESTAMP, 'Truncating job command args field for new size limit of 10000 characters...';
UPDATE jobs SET commandargs = SUBSTRING(commandargs FROM 1 FOR 10000) WHERE LENGTH(commandargs) > 10000;
SELECT CURRENT_TIMESTAMP, 'Finished truncating job command args field to 10000 characters.';

-- Really truncate to 29500 to allow for conversion to JSON
SELECT CURRENT_TIMESTAMP, 'Truncating job dependencies field for new size limit of 30000 characters...';
UPDATE jobs SET filedependencies = SUBSTRING(filedependencies FROM 1 FOR 29500) WHERE LENGTH(filedependencies) > 30000;
SELECT CURRENT_TIMESTAMP, 'Finished truncating job dependencies field to 30000 characters.';

-- Create a new Many to Many table for commands to applications
SELECT CURRENT_TIMESTAMP, 'Creating commands_applications table...';
CREATE TABLE commands_applications (
  command_id VARCHAR(255) NOT NULL,
  application_id VARCHAR(255) NOT NULL,
  application_order INT NOT NULL,
  FOREIGN KEY (command_id) REFERENCES commands (id) ON DELETE CASCADE,
  FOREIGN KEY (application_id) REFERENCES applications (id) ON DELETE RESTRICT
);
SELECT CURRENT_TIMESTAMP, 'Successfully created commands_applications table.';

-- Save the values of the current command to application relationship for the new table
SELECT CURRENT_TIMESTAMP, 'Adding existing applications to commands...';
INSERT INTO commands_applications (command_id, application_id, application_order)
  SELECT id, application_id, 0 FROM commands WHERE application_id IS NOT NULL;
SELECT CURRENT_TIMESTAMP, 'Successfully added existing applications to commands.';

-- Create a new Many to Many table for commands to applications
SELECT CURRENT_TIMESTAMP, 'Creating jobs_applications table...';
CREATE TABLE jobs_applications (
  job_id VARCHAR(255) NOT NULL,
  application_id VARCHAR(255) NOT NULL,
  application_order INT NOT NULL,
  FOREIGN KEY (job_id) REFERENCES jobs (id) ON DELETE CASCADE,
  FOREIGN KEY (application_id) REFERENCES applications (id) ON DELETE RESTRICT
);
SELECT CURRENT_TIMESTAMP, 'Successfully created jobs_applications table.';

-- Save the values of the current job to application relationship for the new table
SELECT CURRENT_TIMESTAMP, 'Adding existing applications to jobs...';
INSERT INTO jobs_applications (job_id, application_id, application_order)
  SELECT id, applicationid, 0 FROM jobs WHERE applicationid IS NOT NULL AND applicationid IN (
    SELECT id FROM applications
  );
SELECT CURRENT_TIMESTAMP, 'Successfully added existing applications to jobs.';

-- Modify the applications and the associated children tables
SELECT CURRENT_TIMESTAMP, 'Altering the applications table for 3.0...';
ALTER TABLE applications ALTER COLUMN created TYPE TIMESTAMP(3) WITHOUT TIME ZONE;
ALTER TABLE applications ALTER COLUMN created SET NOT NULL;
ALTER TABLE applications ALTER COLUMN created SET DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE applications ALTER COLUMN updated TYPE TIMESTAMP(3) WITHOUT TIME ZONE;
ALTER TABLE applications ALTER COLUMN updated SET NOT NULL;
ALTER TABLE applications ALTER COLUMN updated SET DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE applications ALTER COLUMN name SET NOT NULL;
ALTER TABLE applications ALTER COLUMN user0 SET NOT NULL;
ALTER TABLE applications RENAME COLUMN user0 TO "user";
ALTER TABLE applications ALTER COLUMN version SET NOT NULL;
ALTER TABLE applications ADD COLUMN description VARCHAR(10000) DEFAULT NULL;
ALTER TABLE applications ADD COLUMN tags VARCHAR(2048) DEFAULT NULL;
ALTER TABLE applications ALTER COLUMN status SET NOT NULL;
ALTER TABLE applications ALTER COLUMN status SET DEFAULT 'INACTIVE';
ALTER TABLE applications ADD COLUMN type VARCHAR(255) DEFAULT NULL;
ALTER TABLE applications ALTER COLUMN envpropfile TYPE VARCHAR(1024);
ALTER TABLE applications ALTER COLUMN envpropfile SET DEFAULT NULL;
ALTER TABLE applications RENAME COLUMN envPropFile TO setup_file;
ALTER TABLE applications ALTER COLUMN entityversion SET NOT NULL;
ALTER TABLE applications ALTER COLUMN entityversion SET DEFAULT 0;
ALTER TABLE applications ALTER COLUMN entityversion TYPE INT;
ALTER TABLE applications RENAME COLUMN entityversion TO entity_version;
CREATE INDEX APPLICATIONS_NAME_INDEX ON applications (name);
CREATE INDEX APPLICATIONS_TAGS_INDEX ON applications (tags);
CREATE INDEX APPLICATIONS_STATUS_INDEX ON applications (status);
CREATE INDEX APPLICATIONS_TYPE_INDEX ON applications (type);
SELECT CURRENT_TIMESTAMP, 'Successfully updated the applications table.';

SELECT CURRENT_TIMESTAMP, 'De-normalizing application tags for 3.0...';
UPDATE applications SET tags = CONCAT(
  '|',
  REPLACE(
    (
      SELECT string_agg(DISTINCT element, '|' ORDER BY element)
      FROM application_tags
      WHERE applications.id = application_tags.application_id
      GROUP BY application_id
    ),
    '|',
    '||'
  ),
  '|'
);
SELECT CURRENT_TIMESTAMP, 'Finished de-normalizing application tags for 3.0.';

SELECT CURRENT_TIMESTAMP, 'Altering the application_configs table for 3.0...';
DROP INDEX i_pplcfgs_application_id;
ALTER TABLE application_configs ALTER COLUMN application_id SET NOT NULL;
ALTER TABLE application_configs ALTER COLUMN element SET NOT NULL;
ALTER TABLE application_configs ALTER COLUMN element TYPE VARCHAR(1024);
ALTER TABLE application_configs RENAME COLUMN element TO config;
ALTER TABLE application_configs ADD FOREIGN KEY (application_id) REFERENCES applications (id) ON DELETE CASCADE;
SELECT CURRENT_TIMESTAMP, 'Successfully updated the application_configs table.';

SELECT CURRENT_TIMESTAMP, 'Altering the application_dependencies table for 3.0...';
DROP INDEX i_pplcjrs_application_id;
ALTER TABLE application_dependencies ALTER COLUMN application_id SET NOT NULL;
ALTER TABLE application_dependencies ALTER COLUMN element SET NOT NULL;
ALTER TABLE application_dependencies ALTER COLUMN element TYPE VARCHAR(1024);
ALTER TABLE application_dependencies RENAME COLUMN element TO dependency;
ALTER TABLE application_dependencies ADD FOREIGN KEY (application_id) REFERENCES applications (id) ON DELETE CASCADE;
SELECT CURRENT_TIMESTAMP, 'Successfully updated the application_dependencies table.';

SELECT CURRENT_TIMESTAMP, 'Dropping the application_tags table from 3.0...';
DROP TABLE application_tags;
SELECT CURRENT_TIMESTAMP, 'Successfully dropped the application_tags table.';

-- Modify the clusters and associated children tables
SELECT CURRENT_TIMESTAMP, 'Updating the clusters table for 3.0...';
ALTER TABLE clusters ALTER COLUMN created TYPE TIMESTAMP(3) WITHOUT TIME ZONE;
ALTER TABLE clusters ALTER COLUMN created SET NOT NULL;
ALTER TABLE clusters ALTER COLUMN created SET DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE clusters ALTER COLUMN updated TYPE TIMESTAMP(3) WITHOUT TIME ZONE;
ALTER TABLE clusters ALTER COLUMN updated SET NOT NULL;
ALTER TABLE clusters ALTER COLUMN updated SET DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE clusters ALTER COLUMN name SET NOT NULL;
ALTER TABLE clusters ALTER COLUMN user0 SET NOT NULL;
ALTER TABLE clusters RENAME COLUMN user0 TO "user";
ALTER TABLE clusters ALTER COLUMN version SET NOT NULL;
ALTER TABLE clusters ADD COLUMN description VARCHAR(10000) DEFAULT NULL;
ALTER TABLE clusters ADD COLUMN tags VARCHAR(2048) DEFAULT NULL;
ALTER TABLE clusters ADD COLUMN setup_file VARCHAR(1024) DEFAULT NULL;
ALTER TABLE clusters ALTER COLUMN status SET NOT NULL;
ALTER TABLE clusters ALTER COLUMN status SET DEFAULT 'OUT_OF_SERVICE';
ALTER TABLE clusters ALTER COLUMN entityversion SET NOT NULL;
ALTER TABLE clusters ALTER COLUMN entityversion SET DEFAULT 0;
ALTER TABLE clusters ALTER COLUMN entityversion TYPE INT;
ALTER TABLE clusters RENAME COLUMN entityversion TO entity_version;
ALTER TABLE clusters DROP COLUMN clusterType;

CREATE INDEX CLUSTERS_NAME_INDEX ON clusters (name);
CREATE INDEX CLUSTERS_TAG_INDEX ON clusters (tags);
CREATE INDEX CLUSTERS_STATUS_INDEX ON clusters (status);
SELECT CURRENT_TIMESTAMP, 'Successfully updated the clusters table.';

SELECT CURRENT_TIMESTAMP, 'De-normalizing cluster tags for 3.0...';
UPDATE clusters SET tags = CONCAT(
  '|',
  REPLACE(
    (
      SELECT string_agg(DISTINCT element, '|' ORDER BY element)
      FROM cluster_tags
      WHERE clusters.id = cluster_tags.cluster_id
      GROUP BY cluster_id
    ),
    '|',
    '||'
  ),
  '|'
);
SELECT CURRENT_TIMESTAMP, 'Finished de-normalizing cluster tags for 3.0.';

SELECT CURRENT_TIMESTAMP, 'Updating the clusters_commands table for 3.0...';
DROP INDEX i_clstmnd_clusters_id;
DROP INDEX i_clstmnd_element;

ALTER TABLE clusters_commands ALTER COLUMN clusters_id SET NOT NULL;
ALTER TABLE clusters_commands RENAME COLUMN clusters_id TO cluster_id;
ALTER TABLE clusters_commands ALTER COLUMN commands_id SET NOT NULL;
ALTER TABLE clusters_commands RENAME COLUMN commands_id TO command_id;
ALTER TABLE clusters_commands ALTER COLUMN commands_order SET NOT NULL;
ALTER TABLE clusters_commands RENAME COLUMN commands_order TO command_order;
ALTER TABLE clusters_commands ADD FOREIGN KEY (cluster_id) REFERENCES clusters (id) ON DELETE CASCADE;
ALTER TABLE clusters_commands ADD FOREIGN KEY (command_id) REFERENCES commands (id) ON DELETE RESTRICT;
SELECT CURRENT_TIMESTAMP, 'Successfully updated the clusters_commands table.';

SELECT CURRENT_TIMESTAMP, 'Updating the cluster_configs table for 3.0...';
DROP INDEX i_clstfgs_cluster_id;
ALTER TABLE cluster_configs ALTER COLUMN cluster_id SET NOT NULL;
ALTER TABLE cluster_configs ALTER COLUMN element TYPE VARCHAR(1024);
ALTER TABLE cluster_configs ALTER COLUMN element SET NOT NULL;
ALTER TABLE cluster_configs RENAME COLUMN element TO config;
ALTER TABLE cluster_configs ADD FOREIGN KEY (cluster_id) REFERENCES clusters (id) ON DELETE CASCADE;
SELECT CURRENT_TIMESTAMP, 'Successfully updated the cluster_configs table.';

SELECT CURRENT_TIMESTAMP, 'Dropping the cluster_tags table for 3.0...';
DROP TABLE cluster_tags;
SELECT CURRENT_TIMESTAMP, 'Dropped the cluster_tags table.';

-- Modify the commands and associated children tables
SELECT CURRENT_TIMESTAMP, 'Updating the commands table for 3.0...';
ALTER TABLE commands ALTER COLUMN created TYPE TIMESTAMP(3) WITHOUT TIME ZONE;
ALTER TABLE commands ALTER COLUMN created SET NOT NULL;
ALTER TABLE commands ALTER COLUMN created SET DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE commands ALTER COLUMN updated TYPE TIMESTAMP(3) WITHOUT TIME ZONE;
ALTER TABLE commands ALTER COLUMN updated SET NOT NULL;
ALTER TABLE commands ALTER COLUMN updated SET DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE commands ALTER COLUMN name SET NOT NULL;
ALTER TABLE commands ALTER COLUMN user0 SET NOT NULL;
ALTER TABLE commands RENAME COLUMN user0 TO "user";
ALTER TABLE commands ALTER COLUMN version SET NOT NULL;
ALTER TABLE commands ADD COLUMN description VARCHAR(10000) DEFAULT NULL;
ALTER TABLE commands ADD COLUMN tags VARCHAR(2048) DEFAULT NULL;
ALTER TABLE commands ADD COLUMN check_delay BIGINT NOT NULL DEFAULT 10000;
ALTER TABLE commands ADD COLUMN memory INT DEFAULT NULL;
ALTER TABLE commands ALTER COLUMN status SET NOT NULL;
ALTER TABLE commands ALTER COLUMN status SET DEFAULT 'INACTIVE';
ALTER TABLE commands ALTER COLUMN executable SET NOT NULL;
ALTER TABLE commands ALTER COLUMN envpropfile TYPE VARCHAR(1024);
ALTER TABLE commands ALTER COLUMN envpropfile SET DEFAULT NULL;
ALTER TABLE commands RENAME COLUMN envPropFile TO setup_file;
ALTER TABLE commands ALTER COLUMN entityversion SET NOT NULL;
ALTER TABLE commands ALTER COLUMN entityversion SET DEFAULT 0;
ALTER TABLE commands ALTER COLUMN entityversion TYPE INT;
ALTER TABLE commands DROP application_id;
ALTER TABLE commands DROP jobType;

CREATE INDEX COMMANDS_NAME_INDEX ON commands (name);
CREATE INDEX COMMANDS_TAGS_INDEX ON commands (tags);
CREATE INDEX COMMANDS_STATUS_INDEX on commands (status);
SELECT CURRENT_TIMESTAMP, 'Successfully updated the commands table.';

SELECT CURRENT_TIMESTAMP, 'De-normalizing command tags for 3.0...';
UPDATE commands SET tags = CONCAT(
  '|',
  REPLACE(
    (
      SELECT string_agg(DISTINCT element, '|' ORDER BY element)
      FROM command_tags
      WHERE commands.id = command_tags.command_id
      GROUP BY command_id
    ),
    '|',
    '||'
  ),
  '|'
);
UPDATE commands SET tags = REPLACE(tags, '|', '||');
UPDATE commands SET tags = CONCAT('|', tags, '|');
SELECT CURRENT_TIMESTAMP, 'Finished de-normalizing command tags for 3.0.';

SELECT CURRENT_TIMESTAMP, 'Updating the command_configs table for 3.0...';
DROP INDEX i_cmmnfgs_command_id;

ALTER TABLE command_configs ALTER COLUMN command_id SET NOT NULL;
ALTER TABLE command_configs ALTER COLUMN element TYPE VARCHAR(1024);
ALTER TABLE command_configs ALTER COLUMN element SET NOT NULL;
ALTER TABLE command_configs RENAME COLUMN element TO config;
ALTER TABLE command_configs ADD FOREIGN KEY (command_id) REFERENCES commands (id) ON DELETE CASCADE;
SELECT CURRENT_TIMESTAMP, 'Successfully updated the command_configs table.';

SELECT CURRENT_TIMESTAMP, 'Dropping the command_tags table for 3.0...';
DROP TABLE command_tags;
SELECT CURRENT_TIMESTAMP, 'Successfully dropped the command_tags table.';

SELECT CURRENT_TIMESTAMP, 'Creating the job_requests table...';
CREATE TABLE job_requests (
  id VARCHAR(255) NOT NULL,
  created TIMESTAMP(3) WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated TIMESTAMP(3) WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  name VARCHAR(255) NOT NULL,
  "user" VARCHAR(255) NOT NULL,
  version VARCHAR(255) NOT NULL,
  description VARCHAR(10000) DEFAULT NULL,
  entity_version INT NOT NULL DEFAULT 0,
  command_args VARCHAR(10000) NOT NULL,
  group_name VARCHAR(255) DEFAULT NULL,
  setup_file VARCHAR(1024) DEFAULT NULL,
  cluster_criterias VARCHAR(2048) NOT NULL DEFAULT '[]',
  command_criteria VARCHAR(1024) NOT NULL DEFAULT '[]',
  dependencies VARCHAR(30000) DEFAULT NULL,
  disable_log_archival BOOLEAN NOT NULL DEFAULT FALSE,
  email VARCHAR(255) DEFAULT NULL,
  tags VARCHAR(2048) DEFAULT NULL,
  cpu INT DEFAULT NULL,
  memory INT DEFAULT NULL,
  applications VARCHAR(2048) NOT NULL DEFAULT '[]',
  timeout INT DEFAULT NULL,
  PRIMARY KEY (id)
);
SELECT CURRENT_TIMESTAMP, 'Successfully created the job_requests table.';

CREATE INDEX JOB_REQUESTS_CREATED_INDEX ON job_requests (created);

SELECT CURRENT_TIMESTAMP, 'Inserting values into job_requests table from the jobs table...';
INSERT INTO job_requests (
  id,
  created,
  updated,
  name,
  "user",
  version,
  description,
  entity_version,
  command_args,
  group_name,
  setup_file,
  cluster_criterias,
  command_criteria,
  dependencies,
  disable_log_archival,
  email,
  tags
) SELECT
  j.id,
  j.created,
  j.updated,
  j.name,
  j.user0,
  j.version,
  j.description,
  1,
  j.commandargs,
  j.groupname,
  j.envpropfile,
  j.clustercriteriasstring,
  j.commandcriteriastring,
  j.filedependencies,
  j.disablelogarchival,
  j.email,
  CONCAT(
    '|',
    REPLACE(
      (
        SELECT string_agg(DISTINCT element, '|' ORDER BY element)
        FROM job_tags
        WHERE j.id = job_tags.job_id
        GROUP BY job_id
      ),
      '|',
      '||'
    ),
    '|'
  )
  FROM jobs j;
SELECT CURRENT_TIMESTAMP, 'Successfully inserted values into job_requests table.';

SELECT CURRENT_TIMESTAMP, 'Attempting to convert command_criteria to JSON in job_requests table...';
UPDATE job_requests
  SET command_criteria = RPAD(command_criteria, LENGTH(command_criteria) + 2, '"]')
  WHERE command_criteria IS NOT NULL;
UPDATE job_requests
  SET command_criteria = LPAD(command_criteria, LENGTH(command_criteria) + 2, '["')
  WHERE command_criteria IS NOT NULL;
UPDATE job_requests
  SET command_criteria = REPLACE(command_criteria, ',', '","')
  WHERE command_criteria IS NOT NULL;
UPDATE job_requests
  SET command_criteria = '[]'
  WHERE command_criteria = '[""]' OR command_criteria IS NULL;
SELECT CURRENT_TIMESTAMP, 'Successfully converted command_criteria to JSON in job_requests table.';

SELECT CURRENT_TIMESTAMP, 'Attempting to convert cluster_criterias to JSON in job_requests table...';
UPDATE job_requests
  SET cluster_criterias = RPAD(cluster_criterias, LENGTH(cluster_criterias) + 4, '"]}]')
  WHERE cluster_criterias IS NOT NULL;
UPDATE job_requests
  SET cluster_criterias = LPAD(cluster_criterias, LENGTH(cluster_criterias) + 11, '[{"tags":["')
  WHERE cluster_criterias IS NOT NULL;
UPDATE job_requests
  SET cluster_criterias = REPLACE(cluster_criterias, ',', '","')
  WHERE cluster_criterias IS NOT NULL;
UPDATE job_requests
  SET cluster_criterias = REPLACE(cluster_criterias, '|', '"]},{"tags":["')
  WHERE cluster_criterias IS NOT NULL;
UPDATE job_requests
  SET cluster_criterias = '[]'
  WHERE cluster_criterias = '[""]' OR cluster_criterias IS NULL;
SELECT CURRENT_TIMESTAMP, 'Successfully converted to cluster_criterias to JSON in job_requests table.';

SELECT CURRENT_TIMESTAMP, 'Attempting to convert dependencies to JSON in job_requests table...';
UPDATE job_requests
  SET dependencies = RPAD(dependencies, LENGTH(dependencies) + 2, '"]')
  WHERE dependencies IS NOT NULL;
UPDATE job_requests
  SET dependencies = LPAD(dependencies, LENGTH(dependencies) + 2, '["')
  WHERE dependencies IS NOT NULL;
UPDATE job_requests
  SET dependencies = REPLACE(dependencies, ',', '","')
  WHERE dependencies IS NOT NULL;
UPDATE job_requests
  SET dependencies = '[]'
  WHERE dependencies = '[""]' OR dependencies IS NULL;
SELECT CURRENT_TIMESTAMP, 'Successfully converted dependencies to JSON in job_requests table...';

SELECT CURRENT_TIMESTAMP, 'Attempting to make dependencies field not null in job_requests table...';
ALTER TABLE job_requests ALTER COLUMN dependencies SET NOT NULL;;
SELECT CURRENT_TIMESTAMP, 'Successfully made dependencies field not null in job_requests table.';

SELECT CURRENT_TIMESTAMP, 'Creating the job_metadata table...';
CREATE TABLE job_metadata (
  id VARCHAR(255) NOT NULL,
  created TIMESTAMP(3) WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated TIMESTAMP(3) WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  entity_version INT NOT NULL DEFAULT 0,
  client_host VARCHAR(255) DEFAULT NULL,
  user_agent VARCHAR(2048) DEFAULT NULL,
  num_attachments INT DEFAULT NULL,
  total_size_of_attachments BIGINT DEFAULT NULL,
  std_out_size BIGINT DEFAULT NULL,
  std_err_size BIGINT DEFAULT NULL,
  FOREIGN KEY (id) REFERENCES job_requests (id) ON DELETE CASCADE
);
SELECT CURRENT_TIMESTAMP, 'Successfully created the job_metadata table.';

SELECT CURRENT_TIMESTAMP, 'Inserting values into job_metadata from the jobs table...';
INSERT INTO job_metadata (
  id,
  created,
  updated,
  entity_version,
  client_host
) SELECT id, created, updated, entityVersion, clientHost FROM jobs;
SELECT CURRENT_TIMESTAMP, 'Successfully inserted values into the job_metadata table.';

SELECT CURRENT_TIMESTAMP, 'Creating the job_executions table...';
CREATE TABLE job_executions (
  id VARCHAR(255) NOT NULL,
  created TIMESTAMP(3) WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated TIMESTAMP(3) WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  entity_version INT NOT NULL DEFAULT 0,
  host_name VARCHAR(255) NOT NULL,
  process_id INT DEFAULT NULL,
  exit_code INT DEFAULT NULL,
  check_delay BIGINT DEFAULT NULL,
  timeout TIMESTAMP WITHOUT TIME ZONE DEFAULT NULL,
  memory INT,
  FOREIGN KEY (id) REFERENCES jobs (id) ON DELETE CASCADE
);

CREATE INDEX JOB_EXECUTIONS_HOSTNAME_INDEX ON job_executions (host_name);
CREATE INDEX JOB_EXECUTIONS_EXIT_CODE_INDEX ON job_executions (exit_code);
SELECT CURRENT_TIMESTAMP, 'Successfully created the job_executions table.';

SELECT CURRENT_TIMESTAMP, 'Inserting values into job_executions from the jobs table...';
INSERT INTO job_executions (
  id,
  created,
  updated,
  entity_version,
  host_name,
  process_id,
  exit_code
) SELECT
  id,
  created,
  updated,
  entityversion,
  hostname,
  processhandle,
  exitcode
  FROM jobs;
SELECT CURRENT_TIMESTAMP, 'Successfully inserted values into the job_executions table.';

-- Modify the job table to remove the cluster id if cluster doesn't exist to prepare for foreign key constraints
SELECT CURRENT_TIMESTAMP, 'Setting executionClusterId in jobs table to NULL if cluster no longer exists...';
UPDATE jobs SET executionclusterid = NULL WHERE executionclusterid NOT IN (SELECT id FROM clusters);
SELECT CURRENT_TIMESTAMP, 'Successfully updated executionClusterId.';

-- Modify the job table to remove the command id if the command doesn't exist to prepare for foreign key constraints
SELECT CURRENT_TIMESTAMP, 'Setting commandId in Job table to NULL if command no longer exists...';
UPDATE jobs SET commandid = NULL WHERE commandid NOT IN (SELECT id FROM commands);
SELECT CURRENT_TIMESTAMP, 'Successfully updated commandId.';

-- Modify the jobs and associated children tables
SELECT CURRENT_TIMESTAMP, 'Updating the jobs table for 3.0...';
ALTER TABLE jobs ALTER COLUMN created TYPE TIMESTAMP(3) WITHOUT TIME ZONE;
ALTER TABLE jobs ALTER COLUMN created SET NOT NULL;
ALTER TABLE jobs ALTER COLUMN created SET DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE jobs ALTER COLUMN updated TYPE TIMESTAMP(3) WITHOUT TIME ZONE;
ALTER TABLE jobs ALTER COLUMN updated SET NOT NULL;
ALTER TABLE jobs ALTER COLUMN updated SET DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE jobs ALTER COLUMN name SET NOT NULL;
ALTER TABLE jobs ALTER COLUMN user0 SET NOT NULL;
ALTER TABLE jobs RENAME COLUMN user0 TO "user";
ALTER TABLE jobs ALTER COLUMN version SET NOT NULL;
ALTER TABLE jobs ALTER COLUMN description SET DEFAULT NULL;
ALTER TABLE jobs ALTER COLUMN description TYPE VARCHAR(10000);
ALTER TABLE jobs ALTER COLUMN entityversion SET NOT NULL;
ALTER TABLE jobs ALTER COLUMN entityversion SET DEFAULT 0;
ALTER TABLE jobs ALTER COLUMN entityversion TYPE INT;
ALTER TABLE jobs ALTER COLUMN status SET NOT NULL;
ALTER TABLE jobs ALTER COLUMN status SET DEFAULT 'INIT';
ALTER TABLE jobs ALTER COLUMN statusmsg SET NOT NULL;
ALTER TABLE jobs RENAME COLUMN statusmsg TO status_msg;
ALTER TABLE jobs ALTER COLUMN archivelocation TYPE VARCHAR(1024);
ALTER TABLE jobs ALTER COLUMN archivelocation SET DEFAULT NULL;
ALTER TABLE jobs RENAME COLUMN archivelocation TO archive_location;
ALTER TABLE jobs ALTER COLUMN executionclusterid SET DEFAULT NULL;
ALTER TABLE jobs RENAME COLUMN executionclusterid TO cluster_id;
ALTER TABLE jobs ALTER COLUMN executionclustername SET DEFAULT NULL;
ALTER TABLE jobs RENAME COLUMN executionclustername TO cluster_name;
ALTER TABLE jobs ALTER COLUMN commandid SET DEFAULT NULL;
ALTER TABLE jobs RENAME COLUMN commandid TO command_id;
ALTER TABLE jobs ALTER COLUMN commandname SET DEFAULT NULL;
ALTER TABLE jobs RENAME COLUMN commandname TO command_name;
ALTER TABLE jobs ALTER COLUMN commandargs TYPE VARCHAR(10000);
ALTER TABLE jobs ALTER COLUMN commandargs SET NOT NULL;
ALTER TABLE jobs RENAME COLUMN commandargs TO command_args;
ALTER TABLE jobs ALTER COLUMN started TYPE TIMESTAMP(3) WITHOUT TIME ZONE;
ALTER TABLE jobs ALTER COLUMN started SET DEFAULT NULL;
ALTER TABLE jobs ALTER COLUMN finished TYPE TIMESTAMP(3) WITHOUT TIME ZONE;
ALTER TABLE jobs ALTER COLUMN finished SET DEFAULT NULL;
ALTER TABLE jobs ADD COLUMN tags VARCHAR(2048) DEFAULT NULL;
ALTER TABLE jobs DROP forwarded;
ALTER TABLE jobs DROP applicationid;
ALTER TABLE jobs DROP applicationname;
ALTER TABLE jobs DROP hostname;
ALTER TABLE jobs DROP clienthost;
ALTER TABLE jobs DROP filedependencies;
ALTER TABLE jobs DROP envpropfile;
ALTER TABLE jobs DROP exitcode;
ALTER TABLE jobs DROP disablelogarchival;
ALTER TABLE jobs DROP clustercriteriasstring;
ALTER TABLE jobs DROP commandcriteriastring;
ALTER TABLE jobs DROP chosenclustercriteriastring;
ALTER TABLE jobs DROP processhandle;
ALTER TABLE jobs DROP email;
ALTER TABLE jobs DROP groupname;
ALTER TABLE jobs DROP killuri;
ALTER TABLE jobs DROP outputuri;
ALTER TABLE jobs ADD FOREIGN KEY (id) REFERENCES job_requests (id) ON DELETE CASCADE;
ALTER TABLE jobs ADD FOREIGN KEY (cluster_id) REFERENCES clusters (id) ON DELETE RESTRICT;
ALTER TABLE jobs ADD FOREIGN KEY (command_id) REFERENCES commands (id) ON DELETE RESTRICT;

CREATE INDEX JOBS_STARTED_INDEX ON jobs (started);
CREATE INDEX JOBS_FINISHED_INDEX ON jobs (finished);
CREATE INDEX JOBS_STATUS_INDEX ON jobs (status);
CREATE INDEX JOBS_USER_INDEX ON jobs ("user");
CREATE INDEX JOBS_CREATED_INDEX ON jobs (created);
CREATE INDEX JOBS_CLUSTER_NAME_INDEX ON jobs (cluster_name);
CREATE INDEX JOBS_COMMAND_NAME_INDEX ON jobs (command_name);
CREATE INDEX JOBS_TAGS_INDEX ON jobs (tags);
SELECT CURRENT_TIMESTAMP, 'Successfully updated the jobs table.';

SELECT CURRENT_TIMESTAMP, 'De-normalizing jobs tags for 3.0...';
UPDATE jobs SET tags = CONCAT(
  '|',
  REPLACE(
    (
      SELECT string_agg(DISTINCT element, '|' ORDER BY element)
      FROM job_tags
      WHERE jobs.id = job_tags.job_id
      GROUP BY job_id
    ),
    '|',
    '||'
  ),
  '|'
);
SELECT CURRENT_TIMESTAMP, 'Finished de-normalizing job tags for 3.0.';

SELECT CURRENT_TIMESTAMP, 'Dropping the job_tags table for 3.0...';
DROP TABLE job_tags;
SELECT CURRENT_TIMESTAMP, 'Successfully dropped the job_tags table.';

SELECT CURRENT_TIMESTAMP, 'Finished upgrading Genie schema from version 2.0.0 to 3.0.0';

COMMIT;
