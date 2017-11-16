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
  CURRENT_TIMESTAMP                       AS '',
  'Inserting column data into jobs table' AS '';

INSERT INTO `jobs` (
  `created`,
  `updated`,
  `entity_version`,
  `unique_id`,
  `name`,
  `genie_user`,
  `version`,
  `command_args`,
  `description`,
  `genie_user_group`,
  `disable_log_archival`,
  `email`,
  `cpu_requested`,
  `memory_requested`,
  `timeout_requested`,
  `client_host`,
  `user_agent`,
  `num_attachments`,
  `total_size_of_attachments`,
  `std_out_size`,
  `std_err_size`,
  `command_name`,
  `cluster_name`,
  `started`,
  `finished`,
  `status`,
  `status_msg`,
  `host_name`,
  `process_id`,
  `exit_code`,
  `check_delay`,
  `timeout`,
  `memory_used`,
  `archive_location`
) SELECT
    `j`.`created`,
    `j`.`updated`,
    `j`.`entity_version`,
    `j`.`id`,
    `j`.`name`,
    `j`.`genie_user`,
    `j`.`version`,
    `j`.`command_args`,
    `j`.`description`,
    `r`.`group_name`,
    `r`.`disable_log_archival`,
    `r`.`email`,
    `r`.`cpu`,
    `r`.`memory`,
    `r`.`timeout`,
    `m`.`client_host`,
    `m`.`user_agent`,
    `m`.`num_attachments`,
    `m`.`total_size_of_attachments`,
    `m`.`std_out_size`,
    `m`.`std_err_size`,
    `j`.`command_name`,
    `j`.`cluster_name`,
    `j`.`started`,
    `j`.`finished`,
    `j`.`status`,
    `j`.`status_msg`,
    `e`.`host_name`,
    `e`.`process_id`,
    `e`.`exit_code`,
    `e`.`check_delay`,
    `e`.`timeout`,
    `e`.`memory`,
    `j`.`archive_location`
  FROM `jobs_320` `j`
    JOIN `job_requests_320` `r` ON `j`.`id` = `r`.`id`
    JOIN `job_executions_320` e ON `j`.`id` = `e`.`id`
    JOIN `job_metadata_320` `m` ON `j`.`id` = `m`.`id`;

SELECT
  CURRENT_TIMESTAMP                                AS '',
  'Finished inserting column data into jobs table' AS '';

SELECT
  CURRENT_TIMESTAMP                                            AS '',
  'Splitting fields from 3.2.0 jobs table into new jobs table' AS '';

DELIMITER $$
CREATE PROCEDURE GENIE_SPLIT_JOBS_320()
  BEGIN
    DECLARE `done` INT DEFAULT FALSE;
    DECLARE `old_cluster_id` VARCHAR(255)
    CHARSET utf8;
    DECLARE `old_command_id` VARCHAR(255)
    CHARSET utf8;
    DECLARE `old_job_id` VARCHAR(255)
    CHARSET utf8;
    DECLARE `new_command_id` BIGINT(20);
    DECLARE `new_cluster_id` BIGINT(20);

    DECLARE `jobs_cursor` CURSOR FOR
      SELECT
        `id`,
        `cluster_id`,
        `command_id`
      FROM `jobs_320`;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN `jobs_cursor`;
    READ_LOOP: LOOP
      SET `done` = FALSE;

      FETCH `jobs_cursor`
      INTO `old_job_id`, `old_cluster_id`, `old_command_id`;

      IF `done`
      THEN
        LEAVE READ_LOOP;
      END IF;

      START TRANSACTION;

      SELECT `cl`.`id`
      INTO `new_cluster_id`
      FROM `clusters` `cl`
      WHERE `cl`.`unique_id` = `old_cluster_id` COLLATE utf8_bin;

      SELECT `co`.`id`
      INTO `new_command_id`
      FROM `commands` `co`
      WHERE `co`.`unique_id` = `old_command_id` COLLATE utf8_bin;

      UPDATE `jobs` `j`
      SET `j`.`cluster_id` = `new_cluster_id`, `j`.`command_id` = `new_command_id`
      WHERE `j`.`unique_id` = `old_job_id`;

      COMMIT;
    END LOOP READ_LOOP;
  END $$

DELIMITER ;

CALL GENIE_SPLIT_JOBS_320();
DROP PROCEDURE GENIE_SPLIT_JOBS_320;

SELECT
  CURRENT_TIMESTAMP                                                     AS '',
  'Finished splitting fields from 3.2.0 jobs table into new jobs table' AS '';

SELECT
  CURRENT_TIMESTAMP                                                    AS '',
  'Splitting fields from 3.2.0 job requests table into new jobs table' AS '';

DELIMITER $$
CREATE PROCEDURE GENIE_SPLIT_JOB_REQUESTS_320()
  BEGIN
    DECLARE `done` INT DEFAULT FALSE;
    DECLARE `old_job_id` VARCHAR(255)
    CHARSET utf8;
    DECLARE `applications_json` VARCHAR(2048)
    CHARSET utf8;
    DECLARE `cluster_criterias_json` TEXT CHARSET utf8;
    DECLARE `command_criteria_json` TEXT CHARSET utf8;
    DECLARE `configs_json` TEXT CHARSET utf8;
    DECLARE `dependencies_json` TEXT CHARSET utf8;
    DECLARE `old_job_tags` VARCHAR(10000)
    CHARSET utf8;
    DECLARE `old_setup_file` VARCHAR(1024)
    CHARSET utf8;
    DECLARE `new_job_id` BIGINT(20);
    DECLARE `found_tag_id` BIGINT(20);

    DECLARE `job_request_cursor` CURSOR FOR
      SELECT
        `id`,
        `applications`,
        `cluster_criterias`,
        `command_criteria`,
        `configs`,
        `dependencies`,
        `tags`,
        `setup_file`
      FROM `job_requests_320`;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN `job_request_cursor`;
    READ_LOOP: LOOP
      SET `done` = FALSE;

      FETCH `job_request_cursor`
      INTO
        `old_job_id`,
        `applications_json`,
        `cluster_criterias_json`,
        `command_criteria_json`,
        `configs_json`,
        `dependencies_json`,
        `old_job_tags`,
        `old_setup_file`;

      IF `done`
      THEN
        LEAVE READ_LOOP;
      END IF;

      START TRANSACTION;

      SELECT `j`.`id`
      INTO `new_job_id`
      FROM `jobs` `j`
      WHERE `j`.`unique_id` = `old_job_id` COLLATE utf8_bin;

      IF `old_setup_file` IS NOT NULL
      THEN
        INSERT IGNORE INTO `files` (`file`) VALUES (`old_setup_file`);

        SELECT `f`.`id`
        INTO @file_id
        FROM `files` `f`
        WHERE `f`.`file` = `old_setup_file`;

        UPDATE `jobs`
        SET `setup_file` = @file_id
        WHERE `id` = `new_job_id`;
      END IF;

      /*
       * APPLICATIONS REQUESTED FOR A GIVEN JOB
       */

      SET @applications_requested_local = `applications_json`;
      -- Pull off the brackets
      SET @applications_requested_local = TRIM(LEADING '[' FROM @applications_requested_local);
      SET @applications_requested_local = TRIM(TRAILING ']' FROM @applications_requested_local);

      -- LOOP while nothing left
      SET @application_order = 0;
      APPLICATIONS_REQUESTED_LOOP: WHILE LENGTH(@applications_requested_local) > 0 DO
        SET @application_requested = SUBSTRING_INDEX(@applications_requested_local, '",', 1);
        SET @applications_requested_local = TRIM(LEADING @application_requested FROM @applications_requested_local);
        SET @applications_requested_local = TRIM(LEADING '"' FROM @applications_requested_local);
        SET @applications_requested_local = TRIM(LEADING ',' FROM @applications_requested_local);
        SET @application_requested = TRIM(@application_requested);
        SET @application_requested = TRIM(BOTH '"' FROM @application_requested);
        INSERT INTO `job_applications_requested`
        VALUES (`new_job_id`, @application_requested, @application_order);
        SET @application_order = @application_order + 1;
      END WHILE APPLICATIONS_REQUESTED_LOOP;

      /*
       * CLUSTER CRITERIA (desired cluster tags) FOR A GIVEN JOB
       */

      -- Rip off array brackets []
      SET @cluster_criteria = `cluster_criterias_json`;
      SET @cluster_criteria = TRIM(LEADING '[' FROM @cluster_criteria);
      SET @cluster_criteria = TRIM(TRAILING ']' FROM @cluster_criteria);
      IF LENGTH(@cluster_criteria > 0)
      THEN
        -- Loop through array (keep variable for order starting at 0)
        SET @cluster_criteria_order = 0;
        CLUSTER_CRITERIA_LOOP: WHILE LENGTH(@cluster_criteria) > 0 DO
          -- Create cluster_criterias entry (save ID)
          INSERT INTO `criteria` (`created`) VALUES (CURRENT_TIMESTAMP(3));
          SET @criteria_id = LAST_INSERT_ID();

          INSERT INTO `jobs_cluster_criteria` (`job_id`, `criterion_id`, `priority_order`)
          VALUES (`new_job_id`, @criteria_id, @cluster_criteria_order);

          -- Rip off JSON Object tags
          SET @cluster_criteria = TRIM( LEADING '{' FROM @cluster_criteria);
          SET @cluster_criterion = SUBSTRING_INDEX(@cluster_criteria, '}', 1);
          SET @cluster_criteria = TRIM(LEADING @cluster_criterion FROM @cluster_criteria);
          SET @cluster_criteria = TRIM(LEADING '}' FROM @cluster_criteria);
          SET @cluster_criteria = TRIM(LEADING ',' FROM @cluster_criteria);
          SET @cluster_criterion = TRIM(@cluster_criterion);
          -- Rip off "{tags:["
          SET @cluster_criterion = TRIM(LEADING '"tags":[' FROM @cluster_criterion);
          -- Rip off bracket ]
          SET @cluster_criterion = TRIM(TRAILING ']' FROM @cluster_criterion);
          -- Loop through array
          CLUSTER_CRITERION_LOOP: WHILE LENGTH(@cluster_criterion) > 0 DO
            -- Create entry in cluster_criteria_tags using saved id
            SET @criterion_tag = SUBSTRING_INDEX(@cluster_criterion, '",', 1);
            SET @cluster_criterion = TRIM(LEADING @criterion_tag FROM @cluster_criterion);
            SET @cluster_criterion = TRIM(LEADING '"' FROM @cluster_criterion);
            SET @cluster_criterion = TRIM(LEADING ',' FROM @cluster_criterion);
            SET @criterion_tag = TRIM(@criterion_tag);
            SET @criterion_tag = TRIM(BOTH '"' FROM @criterion_tag);

            INSERT IGNORE INTO `tags` (`tag`) VALUES (@criterion_tag);

            SELECT `t`.`id`
            INTO `found_tag_id`
            FROM `tags` `t`
            WHERE `t`.`tag` = @criterion_tag COLLATE utf8_bin;

            INSERT INTO `criteria_tags` (`criterion_id`, `tag_id`) VALUES (@criteria_id, `found_tag_id`);
          END WHILE CLUSTER_CRITERION_LOOP;

          -- Increment order
          SET @cluster_criteria_order = @cluster_criteria_order + 1;
        END WHILE CLUSTER_CRITERIA_LOOP;
      END IF;

      /*
       * COMMAND CRITERIA (desired command tags) FOR A GIVEN JOB
       */

      SET @command_criterion = `command_criteria_json`;
      -- Pull off the brackets
      SET @command_criterion = TRIM(LEADING '[' FROM @command_criterion);
      SET @command_criterion = TRIM(TRAILING ']' FROM @command_criterion);

      IF LENGTH(@command_criterion) > 0
      THEN
        -- Create cluster_criterias entry (save ID)
        INSERT INTO `criteria` (`created`) VALUES (CURRENT_TIMESTAMP(3));
        SET @criteria_id = LAST_INSERT_ID();

        UPDATE `jobs` `j`
        SET `j`.`command_criterion` = @criteria_id
        WHERE `j`.`id` = `new_job_id`;

        -- LOOP while nothing left
        COMMAND_CRITERION_LOOP: WHILE LENGTH(@command_criterion) > 0 DO
          SET @criterion_tag = SUBSTRING_INDEX(@command_criterion, '",', 1);
          SET @command_criterion = TRIM(LEADING @criterion_tag FROM @command_criterion);
          SET @command_criterion = TRIM(LEADING '"' FROM @command_criterion);
          SET @command_criterion = TRIM(LEADING ',' FROM @command_criterion);
          SET @criterion_tag = TRIM(@criterion_tag);
          SET @criterion_tag = TRIM(BOTH '"' FROM @criterion_tag);

          INSERT IGNORE INTO `tags` (`tag`) VALUES (@criterion_tag);

          SELECT `t`.`id`
          INTO `found_tag_id`
          FROM `tags` `t`
          WHERE `t`.`tag` = @criterion_tag COLLATE utf8_bin;

          -- Save tag data for later
          INSERT INTO `criteria_tags` (`criterion_id`, `tag_id`) VALUES (@criteria_id, `found_tag_id`);
        END WHILE COMMAND_CRITERION_LOOP;
      END IF;

      /*
       * CONFIG FILES FOR A GIVEN JOB
       */

      SET @configs_local = `configs_json`;
      -- Pull off the brackets
      SET @configs_local = TRIM(LEADING '[' FROM @configs_local);
      SET @configs_local = TRIM(TRAILING ']' FROM @configs_local);

      -- LOOP while nothing left
      CONFIGS_LOOP: WHILE LENGTH(@configs_local) > 0 DO
        SET @config = SUBSTRING_INDEX(@configs_local, '",', 1);
        SET @configs_local = TRIM(LEADING @config FROM @configs_local);
        SET @configs_local = TRIM(LEADING '"' FROM @configs_local);
        SET @configs_local = TRIM(LEADING ',' FROM @configs_local);
        SET @config = TRIM(@config);
        SET @config = TRIM(BOTH '"' FROM @config);

        INSERT IGNORE INTO `files` (`file`) VALUES (@config);

        SELECT `f`.`id`
        INTO @file_id
        FROM `files` `f`
        WHERE `f`.`file` = @config;

        INSERT INTO `jobs_configs` VALUES (`new_job_id`, @file_id);
      END WHILE CONFIGS_LOOP;

      /*
       * DEPENDENCY FILES FOR A GIVEN JOB
       */

      SET @dependencies_local = `dependencies_json`;
      -- Pull off the brackets
      SET @dependencies_local = TRIM(LEADING '[' FROM @dependencies_local);
      SET @dependencies_local = TRIM(TRAILING ']' FROM @dependencies_local);

      -- LOOP while nothing left
      DEPENDENCIES_LOOP: WHILE LENGTH(@dependencies_local) > 0 DO
        SET @dependency = SUBSTRING_INDEX(@dependencies_local, '",', 1);
        SET @dependencies_local = TRIM(LEADING @dependency FROM @dependencies_local);
        SET @dependencies_local = TRIM(LEADING '"' FROM @dependencies_local);
        SET @dependencies_local = TRIM(LEADING ',' FROM @dependencies_local);
        SET @dependency = TRIM(@dependency);
        SET @dependency = TRIM(BOTH '"' FROM @dependency);

        INSERT IGNORE INTO `files` (`file`) VALUES (@dependency);

        SELECT `f`.`id`
        INTO @file_id
        FROM `files` `f`
        WHERE `f`.`file` = @dependency;

        INSERT INTO `jobs_dependencies` VALUES (`new_job_id`, @file_id);
      END WHILE DEPENDENCIES_LOOP;

      /*
       * TAGS FOR A GIVEN JOB
       */

      SET @tags_local = `old_job_tags`;
      IF LENGTH(@tags_local) > 0
      THEN
        SET @new_tags_local = '';
      ELSE
        SET @new_tags_local = NULL;
      END IF;
      TAGS_LOOP: WHILE LENGTH(@tags_local) > 0 DO
        # Tear off the leading |
        SET @tags_local = TRIM(LEADING '|' FROM @tags_local);
        SET @job_tag = SUBSTRING_INDEX(@tags_local, '|', 1);
        SET @tags_local = TRIM(LEADING @job_tag FROM @tags_local);
        SET @tags_local = TRIM(LEADING '|' FROM @tags_local);

        IF @job_tag LIKE 'scheduler.job_name:%'
        THEN
          UPDATE `jobs` `j`
          SET `j`.`grouping` = SUBSTRING_INDEX(@job_tag, 'scheduler.job_name:', -1)
          WHERE `j`.`id` = `new_job_id`;
        ELSEIF @job_tag LIKE 'scheduler.run_id%'
          THEN
            UPDATE `jobs` `j`
            SET `j`.`grouping_instance` = SUBSTRING_INDEX(@job_tag, 'bdp.test.build.name:', -1)
            WHERE `j`.`id` = `new_job_id`;
        ELSEIF @job_tag LIKE 'bdp.test.build.name:%'
          THEN
            UPDATE `jobs` `j`
            SET `j`.`grouping` = SUBSTRING_INDEX(@job_tag, 'bdp.test.build.name:', -1)
            WHERE `j`.`id` = `new_job_id`;
        ELSEIF @job_tag LIKE 'bdp.test.build.number:%'
          THEN
            UPDATE `jobs` `j`
            SET `j`.`grouping_instance` = SUBSTRING_INDEX(@job_tag, 'bdp.test.build.number:', -1)
            WHERE `j`.`id` = `new_job_id`;
        ELSE
          INSERT IGNORE INTO `tags` (`tag`) VALUES (@job_tag);

          SELECT `t`.`id`
          INTO `found_tag_id`
          FROM `tags` `t`
          WHERE `t`.`tag` = @job_tag COLLATE utf8_bin;

          INSERT INTO `jobs_tags` VALUES (`new_job_id`, `found_tag_id`);

          SET @new_tags_local = CONCAT(@new_tags_local, '|', @job_tag, '|');
        END IF;
      END WHILE TAGS_LOOP;

      # For search
      UPDATE `jobs` `j`
      SET `j`.`tags` = @new_tags_local
      WHERE `j`.`id` = `new_job_id`;

      COMMIT;

    END LOOP READ_LOOP;
    CLOSE `job_request_cursor`;
  END $$

DELIMITER ;

CALL GENIE_SPLIT_JOB_REQUESTS_320();
DROP PROCEDURE GENIE_SPLIT_JOB_REQUESTS_320;

SELECT
  CURRENT_TIMESTAMP                                                             AS '',
  'Finished splitting fields from 3.2.0 job requests table into new jobs table' AS '';

SELECT
  CURRENT_TIMESTAMP                           AS '',
  'Loading data into jobs_applications table' AS '';

DELIMITER $$
CREATE PROCEDURE GENIE_LOAD_JOBS_APPLICATIONS_320()
  BEGIN
    DECLARE `done` INT DEFAULT FALSE;
    DECLARE `old_application_id` VARCHAR(255)
    CHARSET utf8;
    DECLARE `old_job_id` VARCHAR(255)
    CHARSET utf8;
    DECLARE `new_application_id` BIGINT(20);
    DECLARE `new_job_id` BIGINT(20);
    DECLARE `old_application_order` INT(11);

    DECLARE `jobs_applications_cursor` CURSOR FOR
      SELECT
        `job_id`,
        `application_id`,
        `application_order`
      FROM `jobs_applications_320`;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN `jobs_applications_cursor`;
    READ_LOOP: LOOP
      SET `done` = FALSE;

      FETCH `jobs_applications_cursor`
      INTO `old_job_id`, `old_application_id`, `old_application_order`;

      IF `done`
      THEN
        LEAVE READ_LOOP;
      END IF;

      START TRANSACTION;

      SELECT `j`.`id`
      INTO `new_job_id`
      FROM `jobs` `j`
      WHERE `j`.`unique_id` = `old_job_id` COLLATE utf8_bin;

      SELECT `a`.`id`
      INTO `new_application_id`
      FROM `applications` `a`
      WHERE `a`.`unique_id` = `old_application_id` COLLATE utf8_bin;

      INSERT INTO `jobs_applications` VALUES (`new_job_id`, `new_application_id`, `old_application_order`);

      COMMIT;
    END LOOP READ_LOOP;
  END $$

DELIMITER ;

CALL GENIE_LOAD_JOBS_APPLICATIONS_320();
DROP PROCEDURE GENIE_LOAD_JOBS_APPLICATIONS_320;

SELECT
  CURRENT_TIMESTAMP                                    AS '',
  'Finished loading data into jobs_applications table' AS '';

SELECT
  CURRENT_TIMESTAMP     AS '',
  'Dropping old tables' AS '';

DROP TABLE IF EXISTS
`job_metadata_320`,
`job_executions_320`,
`jobs_applications_320`,
`jobs_320`,
`job_requests_320`,
`application_configs_320`,
`application_dependencies_320`,
`cluster_configs_320`,
`cluster_dependencies_320`,
`command_configs_320`,
`command_dependencies_320`,
`commands_applications_320`,
`clusters_commands_320`,
`applications_320`,
`clusters_320`,
`commands_320`;

SELECT
  CURRENT_TIMESTAMP              AS '',
  'Finished dropping old tables' AS '';

DROP TABLE IF EXISTS `criteria_tags_tmp`;

SELECT
  CURRENT_TIMESTAMP                                                     AS '',
  'Finished loading data from old 3.2.0 jobs tables to 3.3.0 job table' AS '';
