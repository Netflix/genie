BEGIN;
SELECT CURRENT_TIMESTAMP, 'Upgrading from 3.0.1 schema to 3.1.0 schema';

CREATE TABLE cluster_dependencies (
    cluster_id character varying(255) NOT NULL,
    dependency character varying(1024) NOT NULL
);

ALTER TABLE cluster_dependencies
  ADD CONSTRAINT cluster_dependencies_cluster_id_fkey FOREIGN KEY (cluster_id) REFERENCES clusters(id) ON DELETE CASCADE;

CREATE TABLE command_dependencies (
    command_id character varying(255) NOT NULL,
    dependency character varying(1024) NOT NULL
);

ALTER TABLE command_dependencies
  ADD CONSTRAINT command_dependencies_command_id_fkey FOREIGN KEY (command_id) REFERENCES commands(id) ON DELETE CASCADE;

CREATE INDEX JOBS_NAME_INDEX ON jobs (name);

ALTER TABLE job_requests
  ALTER COLUMN cluster_criterias TYPE TEXT,
  ALTER COLUMN cluster_criterias SET DEFAULT ''::character varying,
  ALTER COLUMN cluster_criterias SET NOT NULL,
  ALTER COLUMN command_criteria TYPE TEXT,
  ALTER COLUMN command_criteria SET DEFAULT ''::character varying,
  ALTER COLUMN command_criteria SET NOT NULL,
  ALTER COLUMN dependencies TYPE TEXT,
  ALTER COLUMN dependencies SET NOT NULL,
  ALTER COLUMN dependencies SET DEFAULT ''::character varying,
  ADD COLUMN configs TYPE TEXT,
  ALTER COLUMN configs SET NOT NULL,
  ALTER COLUMN configs SET DEFAULT ''::character varying;

ALTER TABLE applications
  ALTER COLUMN description TYPE text,
  ALTER COLUMN description SET DEFAULT NULL;

ALTER TABLE clusters
  ALTER COLUMN description TYPE text,
  ALTER COLUMN description SET DEFAULT NULL;

ALTER TABLE commands
  ALTER COLUMN description TYPE text,
  ALTER COLUMN description SET DEFAULT NULL;

ALTER TABLE jobs
  ALTER COLUMN description TYPE text,
  ALTER COLUMN description SET DEFAULT NULL;

ALTER TABLE job_requests
  ALTER COLUMN description TYPE text,
  ALTER COLUMN description SET DEFAULT NULL;

ALTER TABLE applications
  ALTER COLUMN tags TYPE character varying(10000),
  ALTER COLUMN tags SET DEFAULT NULL;

ALTER TABLE clusters
  ALTER COLUMN tags TYPE character varying(10000),
  ALTER COLUMN tags SET DEFAULT NULL;

ALTER TABLE commands
  ALTER COLUMN tags TYPE character varying(10000),
  ALTER COLUMN tags SET DEFAULT NULL;

ALTER TABLE jobs
  ALTER COLUMN tags TYPE character varying(10000);

ALTER TABLE job_requests
  ALTER COLUMN tags TYPE character varying(10000);

SELECT CURRENT_TIMESTAMP, 'Finished upgrading from 3.0.1 schema to 3.1.0 schema';
COMMIT;
