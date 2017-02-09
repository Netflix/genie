BEGIN;
SELECT CURRENT_TIMESTAMP, 'Fixing https://github.com/Netflix/genie/issues/463';

ALTER TABLE applications RENAME COLUMN "user" TO genie_user;
ALTER TABLE clusters RENAME COLUMN "user" TO genie_user;
ALTER TABLE commands RENAME COLUMN "user" TO genie_user;
ALTER TABLE commands RENAME COLUMN entityversion TO entity_version;
ALTER TABLE job_requests RENAME COLUMN "user" TO genie_user;
ALTER TABLE jobs RENAME COLUMN "user" TO genie_user;
ALTER TABLE jobs RENAME COLUMN entityversion TO entity_version;

SELECT CURRENT_TIMESTAMP, 'Finished fixing https://github.com/Netflix/genie/issues/463';
COMMIT;
