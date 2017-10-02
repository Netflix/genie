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
  'Inserting column data into jobs table';

INSERT INTO jobs (
  created,
  updated,
  entity_version,
  unique_id,
  name,
  genie_user,
  version,
  command_args,
  description,
  genie_user_group,
  disable_log_archival,
  email,
  cpu_requested,
  memory_requested,
  timeout_requested,
  client_host,
  user_agent,
  num_attachments,
  total_size_of_attachments,
  std_out_size,
  std_err_size,
  command_name,
  cluster_name,
  started,
  finished,
  status,
  status_msg,
  host_name,
  process_id,
  exit_code,
  check_delay,
  timeout,
  memory_used,
  archive_location
) SELECT
    j.created,
    j.updated,
    j.entity_version,
    j.id,
    j.name,
    j.genie_user,
    j.version,
    j.command_args,
    j.description,
    r.group_name,
    r.disable_log_archival,
    r.email,
    r.cpu,
    r.memory,
    r.timeout,
    m.client_host,
    m.user_agent,
    m.num_attachments,
    m.total_size_of_attachments,
    m.std_out_size,
    m.std_err_size,
    j.command_name,
    j.cluster_name,
    j.started,
    j.finished,
    j.status,
    j.status_msg,
    e.host_name,
    e.process_id,
    e.exit_code,
    e.check_delay,
    e.timeout,
    e.memory,
    j.archive_location
  FROM jobs_320 j
    JOIN job_requests_320 r ON j.id = r.id
    JOIN job_executions_320 e ON j.id = e.id
    JOIN job_metadata_320 m ON j.id = m.id;

SELECT
  CURRENT_TIMESTAMP,
  'Finished inserting column data into jobs table';

SELECT
  CURRENT_TIMESTAMP,
  'Splitting fields from 3.2.0 jobs table into new jobs table';

CREATE OR REPLACE FUNCTION genie_split_jobs_320()
  RETURNS VOID AS $$
DECLARE
  job_record     RECORD;
  new_command_id BIGINT;
  new_cluster_id BIGINT;
BEGIN

  << JOBS_LOOP >>
  FOR job_record IN
  SELECT
    id,
    cluster_id,
    command_id
  FROM jobs_320
  LOOP
    START TRANSACTION;

    SELECT cl.id
    INTO new_cluster_id
    FROM clusters cl
    WHERE cl.unique_id = job_record.cluster_id;

    SELECT co.id
    INTO new_command_id
    FROM commands co
    WHERE co.unique_id = job_record.command_id;

    UPDATE jobs j
    SET j.cluster_id = new_cluster_id, j.command_id = new_command_id
    WHERE j.unique_id = job_record.id;

    COMMIT;
  END LOOP JOBS_LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT genie_split_jobs_320();
DROP FUNCTION genie_split_jobs_320();

SELECT
  CURRENT_TIMESTAMP,
  'Finished splitting fields from 3.2.0 jobs table into new jobs table';

SELECT
  CURRENT_TIMESTAMP,
  'Splitting fields from 3.2.0 job requests table into new jobs table';

CREATE OR REPLACE FUNCTION genie_split_job_requests_320()
  RETURNS VOID AS $$
DECLARE
  job_request_record           RECORD;
  applications_requested_local VARCHAR(2048);
  application_order_local      INT;
  application_requested        VARCHAR(255);
  cluster_criterias_local      TEXT;
  cluster_criteria_order_local INT;
  cluster_criteria             TEXT;
  cluster_tag                  VARCHAR(255);
  cluster_criteria_id_local    BIGINT;
  command_criteria_local       TEXT;
  command_tag                  VARCHAR(255);
  configs_local                TEXT;
  config                       VARCHAR(1024);
  dependencies_local           TEXT;
  dependency                   VARCHAR(1024);
  tags_local                   VARCHAR(10000);
  new_tags_local               VARCHAR(1024);
  job_tag                      VARCHAR(255);
  new_job_id                   BIGINT;
  found_tag_id                 BIGINT;
  found_file_id                BIGINT;
BEGIN

  << JOB_REQUESTS_LOOP >>
  FOR job_request_record IN
  SELECT
    id,
    applications,
    cluster_criterias,
    command_criteria,
    configs,
    dependencies,
    tags,
    setup_file
  FROM job_requests_320
  LOOP

    START TRANSACTION;

    SELECT j.id
    INTO new_job_id
    FROM jobs j
    WHERE j.unique_id = job_request_record.id;

    IF job_request_record.setup_file IS NOT NULL
    THEN
      INSERT INTO files (file) VALUES (job_request_record.setup_file)
      ON CONFLICT DO NOTHING;

      SELECT f.id
      INTO found_file_id
      FROM files f
      WHERE f.file = job_request_record.setup_file;

      UPDATE jobs j
      SET j.setup_file = found_file_id
      WHERE j.id = new_job_id;
    END IF;

    /*
     * APPLICATIONS REQUESTED FOR A GIVEN JOB
     */

    applications_requested_local = job_request_record.applications;
    -- Pull off the brackets
    applications_requested_local = TRIM(LEADING '[' FROM applications_requested_local);
    applications_requested_local = TRIM(TRAILING ']' FROM applications_requested_local);

    -- LOOP while nothing left
    application_order_local = 0;
    << APPLICATIONS_REQUESTED_LOOP >> WHILE LENGTH(applications_requested_local) > 0 LOOP
      application_requested = SPLIT_PART(applications_requested_local, '",', 1);
      applications_requested_local = TRIM(LEADING application_requested FROM applications_requested_local);
      applications_requested_local = TRIM(LEADING '"' FROM applications_requested_local);
      applications_requested_local = TRIM(LEADING ',' FROM applications_requested_local);
      application_requested = TRIM(application_requested);
      application_requested = TRIM(BOTH '"' FROM application_requested);
      INSERT INTO job_applications_requested
      VALUES (new_job_id, application_requested, application_order_local);
      application_order_local = application_order_local + 1;
    END LOOP APPLICATIONS_REQUESTED_LOOP;

    /*
     * CLUSTER CRITERIA (desired cluster tags) FOR A GIVEN JOB
     */

    -- Rip off array brackets []
    cluster_criterias_local = job_request_record.cluster_criterias;
    cluster_criterias_local = TRIM(LEADING '[' FROM cluster_criterias_local);
    cluster_criterias_local = TRIM(TRAILING ']' FROM cluster_criterias_local);
    -- Loop through array (keep variable for order starting at 0)
    cluster_criteria_order_local = 0;
    << CLUSTER_CRITERIAS_LOOP >> WHILE LENGTH(cluster_criterias_local) > 0 LOOP
      -- Create cluster_criterias entry (save ID)
      INSERT INTO cluster_criterias (job_id, priority_order)
      VALUES (new_job_id, cluster_criteria_order_local)
      RETURNING id
        INTO cluster_criteria_id_local;
      -- Rip off JSON Object tags
      cluster_criterias_local = TRIM(LEADING '{' FROM cluster_criterias_local);
      cluster_criteria = SPLIT_PART(cluster_criterias_local, '}', 1);
      cluster_criterias_local = TRIM(LEADING cluster_criteria FROM cluster_criterias_local);
      cluster_criterias_local = TRIM(LEADING '}' FROM cluster_criterias_local);
      cluster_criterias_local = TRIM(LEADING ',' FROM cluster_criterias_local);
      cluster_criteria = TRIM(cluster_criteria);
      -- Rip off "{tags:["
      cluster_criteria = TRIM(LEADING '"tags":[' FROM cluster_criteria);
      -- Rip off bracket ]
      cluster_criteria = TRIM(TRAILING ']' FROM cluster_criteria);
      -- Loop through array
      << CLUSTER_CRITERIA_LOOP >> WHILE LENGTH(cluster_criteria) > 0 LOOP
        -- Create entry in cluster_criteria_tags using saved id
        cluster_tag = SPLIT_PART(cluster_criteria, '",', 1);
        cluster_criteria = TRIM(LEADING cluster_tag FROM cluster_criteria);
        cluster_criteria = TRIM(LEADING '"' FROM cluster_criteria);
        cluster_criteria = TRIM(LEADING ',' FROM cluster_criteria);
        cluster_tag = TRIM(cluster_tag);
        cluster_tag = TRIM(BOTH '"' FROM cluster_tag);

        INSERT INTO tags (tag) VALUES (cluster_tag)
        ON CONFLICT DO NOTHING;

        SELECT t.id
        INTO found_tag_id
        FROM tags t
        WHERE t.tag = cluster_tag;

        INSERT INTO cluster_criterias_tags VALUES (cluster_criteria_id_local, found_tag_id);
      END LOOP CLUSTER_CRITERIA_LOOP;
      -- Increment order
      cluster_criteria_order_local = cluster_criteria_order_local + 1;
    END LOOP CLUSTER_CRITERIAS_LOOP;

    /*
     * COMMAND CRITERIA (desired command tags) FOR A GIVEN JOB
     */

    command_criteria_local = job_request_record.command_criteria;
    -- Pull off the brackets
    command_criteria_local = TRIM(LEADING '[' FROM command_criteria_local);
    command_criteria_local = TRIM(TRAILING ']' FROM command_criteria_local);

    -- LOOP while nothing left
    << COMMAND_CRITERIA_LOOP >> WHILE LENGTH(command_criteria_local) > 0 LOOP
      command_tag = SPLIT_PART(command_criteria_local, '",', 1);
      command_criteria_local = TRIM(LEADING command_tag FROM command_criteria_local);
      command_criteria_local = TRIM(LEADING '"' FROM command_criteria_local);
      command_criteria_local = TRIM(LEADING ',' FROM command_criteria_local);
      command_tag = TRIM(command_tag);
      command_tag = TRIM(BOTH '"' FROM command_tag);

      INSERT INTO tags (tag) VALUES (command_tag)
      ON CONFLICT DO NOTHING;

      SELECT t.id
      INTO found_tag_id
      FROM tags t
      WHERE t.tag = command_tag;

      INSERT INTO job_command_criteria_tags VALUES (new_job_id, found_tag_id);
    END LOOP COMMAND_CRITERIA_LOOP;

    /*
     * CONFIG FILES FOR A GIVEN JOB
     */

    configs_local = job_request_record.configs;
    -- Pull off the brackets
    configs_local = TRIM(LEADING '[' FROM configs_local);
    configs_local = TRIM(TRAILING ']' FROM configs_local);

    -- LOOP while nothing left
    << CONFIGS_LOOP >> WHILE LENGTH(configs_local) > 0 LOOP
      config = SPLIT_PART(configs_local, '",', 1);
      configs_local = TRIM(LEADING config FROM configs_local);
      configs_local = TRIM(LEADING '"' FROM configs_local);
      configs_local = TRIM(LEADING ',' FROM configs_local);
      config = TRIM(config);
      config = TRIM(BOTH '"' FROM config);

      INSERT INTO files (file) VALUES (config)
      ON CONFLICT DO NOTHING;

      SELECT f.id
      INTO found_file_id
      FROM files f
      WHERE f.file = config;

      INSERT INTO jobs_configs VALUES (new_job_id, found_file_id);
    END LOOP CONFIGS_LOOP;

    /*
     * DEPENDENCY FILES FOR A GIVEN JOB
     */

    dependencies_local = job_request_record.dependencies;
    -- Pull off the brackets
    dependencies_local = TRIM(LEADING '[' FROM dependencies_local);
    dependencies_local = TRIM(TRAILING ']' FROM dependencies_local);

    -- LOOP while nothing left
    << DEPENDENCIES_LOOP >> WHILE LENGTH(dependencies_local) > 0 LOOP
      dependency = SPLIT_PART(dependencies_local, '",', 1);
      dependencies_local = TRIM(LEADING dependency FROM dependencies_local);
      dependencies_local = TRIM(LEADING '"' FROM dependencies_local);
      dependencies_local = TRIM(LEADING ',' FROM dependencies_local);
      dependency = TRIM(dependency);
      dependency = TRIM(BOTH '"' FROM dependency);

      INSERT INTO files (file) VALUES (dependency)
      ON CONFLICT DO NOTHING;

      SELECT f.id
      INTO found_file_id
      FROM files f
      WHERE f.file = dependency;

      INSERT INTO jobs_dependencies VALUES (new_job_id, found_file_id);
    END LOOP DEPENDENCIES_LOOP;

    /*
     * TAGS FOR A GIVEN JOB
     */

    tags_local = job_request_record.tags;
    IF LENGTH(tags_local) > 0
    THEN
      new_tags_local = '';
    ELSE
      new_tags_local = NULL;
    END IF;
    << TAGS_LOOP >> WHILE LENGTH(tags_local) > 0 LOOP
      -- Tear OFF the LEADING |
      tags_local = TRIM(LEADING '|' FROM tags_local);
      job_tag = SPLIT_PART(tags_local, '|', 1);
      tags_local = TRIM(LEADING job_tag FROM tags_local);
      tags_local = TRIM(LEADING '|' FROM tags_local);

      IF job_tag LIKE 'scheduler.job_name:%'
      THEN
        UPDATE jobs j
        SET j.grouping = LTRIM(job_tag, 'scheduler.job_name:')
        WHERE j.id = new_job_id;
      ELSEIF job_tag LIKE 'scheduler.run_id%'
        THEN
          UPDATE jobs j
          SET j.grouping_instance = LTRIM(job_tag, 'bdp.test.build.name:')
          WHERE j.id = new_job_id;
      ELSEIF job_tag LIKE 'bdp.test.build.name:%'
        THEN
          UPDATE jobs j
          SET j.grouping = LTRIM(job_tag, 'bdp.test.build.name:')
          WHERE j.id = new_job_id;
      ELSEIF job_tag LIKE 'bdp.test.build.number:%'
        THEN
          UPDATE jobs j
          SET j.grouping_instance = LTRIM(job_tag, 'bdp.test.build.number:')
          WHERE j.id = new_job_id;
      ELSE
        INSERT INTO tags (tag) VALUES (job_tag)
        ON CONFLICT DO NOTHING;

        SELECT t.id
        INTO found_tag_id
        FROM tags t
        WHERE t.tag = job_tag;

        INSERT INTO jobs_tags VALUES (new_job_id, found_tag_id);

        new_tags_local = CONCAT(new_tags_local, '|', job_tag, '|');
      END IF;
    END LOOP TAGS_LOOP;

    -- FOR SEARCH
    UPDATE jobs j
    SET j.tags = new_tags_local
    WHERE j.id = new_job_id;

    COMMIT;

  END LOOP JOB_REQUESTS_LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT genie_split_job_requests_320();
DROP FUNCTION genie_split_job_requests_320();

SELECT
  CURRENT_TIMESTAMP,
  'Finished splitting fields from 3.2.0 job requests table into new jobs table';

SELECT
  CURRENT_TIMESTAMP,
  'Loading data into jobs_applications table';

CREATE OR REPLACE FUNCTION genie_split_jobs_applications_320()
  RETURNS VOID AS $$
DECLARE
  application_record RECORD;
  new_application_id BIGINT;
  new_job_id         BIGINT;
BEGIN

  << READ_LOOP >>
  FOR application_record IN
  SELECT
    job_id,
    application_id,
    application_order
  FROM jobs_applications_320
  LOOP
    START TRANSACTION;

    SELECT j.id
    INTO new_job_id
    FROM jobs j
    WHERE j.unique_id = application_record.job_id;

    SELECT a.id
    INTO new_application_id
    FROM applications a
    WHERE a.unique_id = application_record.application_id;

    INSERT INTO jobs_applications VALUES (new_job_id, new_application_id, application_record.application_order);

    COMMIT;
  END LOOP READ_LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT genie_split_jobs_applications_320();
DROP FUNCTION genie_split_jobs_applications_320();

SELECT
  CURRENT_TIMESTAMP,
  'Finished loading data into jobs_applications table';

SELECT
  CURRENT_TIMESTAMP,
  'Dropping old tables';

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

SELECT
  CURRENT_TIMESTAMP,
  'Finished dropping old tables';

SELECT
  CURRENT_TIMESTAMP,
  'Finished loading data from old 3.2.0 jobs tables to 3.3.0 job table';
