BEGIN;
SELECT CURRENT_TIMESTAMP, 'Upgrading from 3.0.1 schema to 3.1.0 schema';

CREATE TABLE cluster_dependencies (
    cluster_id character varying(255) NOT NULL,
    dependency character varying(1024) NOT NULL
);

ALTER TABLE cluster_dependencies
  ADD CONSTRAINT cluster_dependencies_cluster_id_fkey FOREIGN KEY (cluster_id) REFERENCES clusters(id) ON DELETE CASCADE;

CREATE INDEX JOBS_NAME_INDEX ON jobs (name);

ALTER TABLE job_requests
  ALTER COLUMN cluster_criterias TYPE TEXT,
  ALTER COLUMN cluster_criterias SET DEFAULT ''::character varying,
  ALTER COLUMN cluster_criterias SET NOT NULL,
  ALTER COLUMN command_criteria TYPE TEXT,
  ALTER COLUMN command_criteria SET DEFAULT ''::character varying,
  ALTER COLUMN command_criteria SET NOT NULL;

SELECT CURRENT_TIMESTAMP, 'Finished upgrading from 3.0.1 schema to 3.1.0 schema';
COMMIT;
