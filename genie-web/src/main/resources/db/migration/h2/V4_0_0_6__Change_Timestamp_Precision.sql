/*
 *
 *  Copyright 2020 Netflix, Inc.
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

ALTER TABLE `applications`
    ALTER COLUMN `created` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6);
ALTER TABLE `applications`
    ALTER COLUMN `updated` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6);

ALTER TABLE `clusters`
    ALTER COLUMN `created` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6);
ALTER TABLE `clusters`
    ALTER COLUMN `updated` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6);

ALTER TABLE `commands`
    ALTER COLUMN `created` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6);
ALTER TABLE `commands`
    ALTER COLUMN `updated` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6);
ALTER TABLE `commands`
    ADD COLUMN `launcher_ext` TEXT DEFAULT NULL;

ALTER TABLE `files`
    ALTER COLUMN `created` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6);
ALTER TABLE `files`
    ALTER COLUMN `updated` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6);

ALTER TABLE `jobs`
    ALTER COLUMN `created` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6);
ALTER TABLE `jobs`
    ALTER COLUMN `updated` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6);
ALTER TABLE `jobs`
    ALTER COLUMN `started` DATETIME(6) DEFAULT NULL;
ALTER TABLE `jobs`
    ALTER COLUMN `finished` DATETIME(6) DEFAULT NULL;
ALTER TABLE `jobs`
    ALTER COLUMN `timeout` DATETIME(6) DEFAULT NULL;
ALTER TABLE `jobs`
    DROP COLUMN `requested_archive_location_prefix`;
ALTER TABLE `jobs`
    ADD COLUMN `archive_status` VARCHAR(20) DEFAULT NULL;
ALTER TABLE `jobs`
    ADD COLUMN `requested_launcher_ext` TEXT DEFAULT NULL;
ALTER TABLE `jobs`
    ADD COLUMN `launcher_ext` TEXT DEFAULT NULL;

CREATE INDEX `JOBS_ARCHIVE_STATUS_INDEX` ON `jobs` (`archive_status`);

ALTER TABLE `tags`
    ALTER COLUMN `created` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6);
ALTER TABLE `tags`
    ALTER COLUMN `updated` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6);
