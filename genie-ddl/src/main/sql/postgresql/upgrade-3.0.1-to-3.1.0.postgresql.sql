BEGIN;
SELECT CURRENT_TIMESTAMP, 'Upgrading from 3.0.1 schema to 3.1.0 schema';

SELECT CURRENT_TIMESTAMP, 'Upgrading applications table';

ALTER TABLE applications
  ALTER COLUMN description TYPE text,
  ALTER COLUMN description SET DEFAULT NULL,
  ALTER COLUMN tags TYPE character varying(10000),
  ALTER COLUMN tags SET DEFAULT NULL;

SELECT CURRENT_TIMESTAMP, 'Upgrading clusters table';

ALTER TABLE clusters
  ALTER COLUMN description TYPE text,
  ALTER COLUMN description SET DEFAULT NULL,
  ALTER COLUMN tags TYPE character varying(10000),
  ALTER COLUMN tags SET DEFAULT NULL;

SELECT CURRENT_TIMESTAMP, 'Upgrading commands table';

ALTER TABLE commands
  ALTER COLUMN description TYPE text,
  ALTER COLUMN description SET DEFAULT NULL,
  ALTER COLUMN tags TYPE character varying(10000),
  ALTER COLUMN tags SET DEFAULT NULL;

SELECT CURRENT_TIMESTAMP, 'Upgrading jobs table';

ALTER TABLE jobs
  ALTER COLUMN description TYPE text,
  ALTER COLUMN description SET DEFAULT NULL,
  ALTER COLUMN tags TYPE character varying(10000);

SELECT CURRENT_TIMESTAMP, 'Creating index for name in jobs table';

CREATE INDEX JOBS_NAME_INDEX ON jobs (name);

SELECT CURRENT_TIMESTAMP, 'Upgrading job_requests table';

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
  ADD COLUMN configs text DEFAULT ''::character varying NOT NULL,
  ALTER COLUMN description TYPE text,
  ALTER COLUMN description SET DEFAULT NULL,
  ALTER COLUMN tags TYPE character varying(10000);

SELECT CURRENT_TIMESTAMP, 'Creating cluster_dependencies table';

CREATE TABLE cluster_dependencies (
    cluster_id character varying(255) NOT NULL,
    dependency character varying(2048) NOT NULL
);

ALTER TABLE cluster_dependencies
  ADD CONSTRAINT cluster_dependencies_cluster_id_fkey FOREIGN KEY (cluster_id) REFERENCES clusters(id) ON DELETE CASCADE;

SELECT CURRENT_TIMESTAMP, 'Creating command_dependencies table';

CREATE TABLE command_dependencies (
    command_id character varying(255) NOT NULL,
    dependency character varying(2048) NOT NULL
);

ALTER TABLE command_dependencies
  ADD CONSTRAINT command_dependencies_command_id_fkey FOREIGN KEY (command_id) REFERENCES commands(id) ON DELETE CASCADE;

SELECT CURRENT_TIMESTAMP, 'Upgrading application_dependencies table';

ALTER TABLE application_dependencies
  ALTER COLUMN dependency TYPE character varying(2048),
  ALTER COLUMN dependency SET NOT NULL;

SELECT CURRENT_TIMESTAMP, 'Upgrading application_configs table';

ALTER TABLE application_configs
  ALTER COLUMN config TYPE character varying(2048),
  ALTER COLUMN config SET NOT NULL;

SELECT CURRENT_TIMESTAMP, 'Upgrading cluster_configs table';

ALTER TABLE cluster_configs
  ALTER COLUMN config TYPE character varying(2048),
  ALTER COLUMN config SET NOT NULL;

SELECT CURRENT_TIMESTAMP, 'Upgrading command_configs table';

ALTER TABLE command_configs
  ALTER COLUMN config TYPE character varying(2048),
  ALTER COLUMN config SET NOT NULL;

SELECT CURRENT_TIMESTAMP, 'Finished upgrading from 3.0.1 schema to 3.1.0 schema';
COMMIT;
