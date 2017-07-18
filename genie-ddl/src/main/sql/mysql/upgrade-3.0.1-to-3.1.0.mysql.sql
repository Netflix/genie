BEGIN;
SELECT CURRENT_TIMESTAMP AS '', 'Upgrading from 3.0.1 schema to 3.1.0 schema' AS '';

CREATE TABLE `cluster_dependencies` (
  `cluster_id` varchar(255) NOT NULL,
  `dependency` varchar(1024) NOT NULL,
  KEY `cluster_id` (`cluster_id`),
  CONSTRAINT `cluster_dependencies_ibfk_1` FOREIGN KEY (`cluster_id`) REFERENCES `clusters` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE `command_dependencies` (
  `command_id` varchar(255) NOT NULL,
  `dependency` varchar(1024) NOT NULL,
  KEY `command_id` (`command_id`),
  CONSTRAINT `command_dependencies_ibfk_1` FOREIGN KEY (`command_id`) REFERENCES `commands` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

ALTER TABLE `jobs`
  ADD INDEX `JOBS_NAME_INDEX` (`name`);

ALTER TABLE `job_requests`
  MODIFY COLUMN `cluster_criterias` TEXT NOT NULL,
  MODIFY COLUMN `command_criteria` TEXT NOT NULL,
  MODIFY COLUMN `dependencies` TEXT NOT NULL,
  ADD COLUMN `configs` TEXT NOT NULL;

ALTER TABLE `clusters`
  MODIFY COLUMN `description` TEXT;

ALTER TABLE `applications`
  MODIFY COLUMN `description` TEXT;

ALTER TABLE `commands`
  MODIFY COLUMN `description` TEXT;

ALTER TABLE `jobs`
  MODIFY COLUMN `description` TEXT;

ALTER TABLE `job_requests`
  MODIFY COLUMN `description` TEXT;

ALTER TABLE `clusters`
  MODIFY COLUMN `tags` VARCHAR(10000) DEFAULT NULL;

ALTER TABLE `applications`
  MODIFY COLUMN `tags` VARCHAR(10000) DEFAULT NULL;

ALTER TABLE `commands`
  MODIFY COLUMN `tags` VARCHAR(10000) DEFAULT NULL;

ALTER TABLE `jobs`
  MODIFY COLUMN `tags` VARCHAR(10000) DEFAULT NULL;

ALTER TABLE `job_requests`
  MODIFY COLUMN `tags` VARCHAR(10000) DEFAULT NULL;

ALTER TABLE `application_configs`
  MODIFY COLUMN `config` VARCHAR(2048) NOT NULL;

ALTER TABLE `cluster_configs`
  MODIFY COLUMN `config` VARCHAR(2048) NOT NULL;

ALTER TABLE `command_configs`
  MODIFY COLUMN `config` VARCHAR(2048) NOT NULL;

ALTER TABLE `application_dependencies`
  MODIFY COLUMN `dependency` VARCHAR(2048) NOT NULL;

ALTER TABLE `cluster_dependencies`
  MODIFY COLUMN `dependency` VARCHAR(2048) NOT NULL;

ALTER TABLE `command_dependencies`
  MODIFY COLUMN `dependency` VARCHAR(2048) NOT NULL;

SELECT CURRENT_TIMESTAMP AS '', 'Finished upgrading from 3.0.1 schema to 3.1.0 schema' AS '';
COMMIT;
