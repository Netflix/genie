BEGIN;
SELECT CURRENT_TIMESTAMP AS '', 'Fixing https://github.com/Netflix/genie/issues/463' AS '';

ALTER TABLE `applications` CHANGE `user` `genie_user` VARCHAR(255) NOT NULL;
ALTER TABLE `clusters` CHANGE `user` `genie_user` VARCHAR(255) NOT NULL;
ALTER TABLE `commands` CHANGE `user` `genie_user` VARCHAR(255) NOT NULL;
ALTER TABLE `job_requests` CHANGE `user` `genie_user` VARCHAR(255) NOT NULL;
ALTER TABLE `jobs` CHANGE `user` `genie_user` VARCHAR(255) NOT NULL;

SELECT CURRENT_TIMESTAMP AS '', 'Finished fixing https://github.com/Netflix/genie/issues/463' AS '';
COMMIT;
