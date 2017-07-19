BEGIN;
SELECT CURRENT_TIMESTAMP AS '', 'Upgrading from 3.0.1 schema to 3.1.0 schema' AS '';

SELECT CURRENT_TIMESTAMP AS '', 'Upgrading applications table' AS '';

ALTER TABLE `applications`
  MODIFY COLUMN `description` TEXT,
  MODIFY COLUMN `tags` VARCHAR(10000) DEFAULT NULL;

SELECT CURRENT_TIMESTAMP AS '', 'Upgrading clusters table' AS '';

ALTER TABLE `clusters`
  MODIFY COLUMN `description` TEXT,
  MODIFY COLUMN `tags` VARCHAR(10000) DEFAULT NULL;

SELECT CURRENT_TIMESTAMP AS '', 'Upgrading commands table' AS '';

ALTER TABLE `commands`
  MODIFY COLUMN `description` TEXT,
  MODIFY COLUMN `tags` VARCHAR(10000) DEFAULT NULL;

SELECT CURRENT_TIMESTAMP AS '', 'Upgrading jobs table' AS '';

ALTER TABLE `jobs`
  MODIFY COLUMN `description` TEXT,
  MODIFY COLUMN `tags` VARCHAR(10000) DEFAULT NULL,
  ADD INDEX `JOBS_NAME_INDEX` (`name`);

SELECT CURRENT_TIMESTAMP AS '', 'Upgrading job_requests table' AS '';

ALTER TABLE `job_requests`
  MODIFY COLUMN `description` TEXT,
  MODIFY COLUMN `cluster_criterias` TEXT NOT NULL,
  MODIFY COLUMN `command_criteria` TEXT NOT NULL,
  MODIFY COLUMN `dependencies` TEXT NOT NULL,
  ADD COLUMN `configs` TEXT NOT NULL,
  MODIFY COLUMN `tags` VARCHAR(10000) DEFAULT NULL;

SELECT CURRENT_TIMESTAMP AS '', 'Upgrading application_dependencies table' AS '';

ALTER TABLE `application_dependencies`
  MODIFY COLUMN `dependency` VARCHAR(2048) NOT NULL;

SELECT CURRENT_TIMESTAMP AS '', 'Creating cluster_dependencies table' AS '';

CREATE TABLE `cluster_dependencies` (
  `cluster_id` varchar(255) NOT NULL,
  `dependency` varchar(2048) NOT NULL,
  KEY `cluster_id` (`cluster_id`),
  CONSTRAINT `cluster_dependencies_ibfk_1` FOREIGN KEY (`cluster_id`) REFERENCES `clusters` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

SELECT CURRENT_TIMESTAMP AS '', 'Creating command_dependencies table' AS '';

CREATE TABLE `command_dependencies` (
  `command_id` varchar(255) NOT NULL,
  `dependency` varchar(2048) NOT NULL,
  KEY `command_id` (`command_id`),
  CONSTRAINT `command_dependencies_ibfk_1` FOREIGN KEY (`command_id`) REFERENCES `commands` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

SELECT CURRENT_TIMESTAMP AS '', 'Upgrading application_configs table' AS '';

ALTER TABLE `application_configs`
  MODIFY COLUMN `config` VARCHAR(2048) NOT NULL;

SELECT CURRENT_TIMESTAMP AS '', 'Upgrading cluster_configs table' AS '';

ALTER TABLE `cluster_configs`
  MODIFY COLUMN `config` VARCHAR(2048) NOT NULL;

SELECT CURRENT_TIMESTAMP AS '', 'Upgrading command_configs table' AS '';

ALTER TABLE `command_configs`
  MODIFY COLUMN `config` VARCHAR(2048) NOT NULL;

SELECT CURRENT_TIMESTAMP AS '', 'Finished upgrading from 3.0.1 schema to 3.1.0 schema' AS '';
COMMIT;
