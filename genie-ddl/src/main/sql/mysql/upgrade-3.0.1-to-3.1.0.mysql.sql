BEGIN;
SELECT CURRENT_TIMESTAMP AS '', 'Upgrading from 3.0.1 schema to 3.1.0 schema' AS '';

CREATE TABLE `cluster_dependencies` (
  `cluster_id` varchar(255) NOT NULL,
  `dependency` varchar(1024) NOT NULL,
  KEY `cluster_id` (`cluster_id`),
  CONSTRAINT `cluster_dependencies_ibfk_1` FOREIGN KEY (`cluster_id`) REFERENCES `clusters` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

ALTER TABLE `jobs`
  ADD INDEX `JOBS_NAME_INDEX` (`name`);

SELECT CURRENT_TIMESTAMP AS '', 'Finished upgrading from 3.0.1 schema to 3.1.0 schema' AS '';
COMMIT;
