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
    MODIFY COLUMN `created` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    MODIFY COLUMN `updated` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6);

ALTER TABLE `clusters`
    MODIFY COLUMN `created` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    MODIFY COLUMN `updated` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6);

ALTER TABLE `commands`
    MODIFY COLUMN `created` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    MODIFY COLUMN `updated` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    ADD COLUMN `launcher_ext` TEXT DEFAULT NULL;

ALTER TABLE `files`
    MODIFY COLUMN `created` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    MODIFY COLUMN `updated` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6);

ALTER TABLE `jobs`
    MODIFY COLUMN `created` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    MODIFY COLUMN `updated` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    MODIFY COLUMN `started` DATETIME(6) DEFAULT NULL,
    MODIFY COLUMN `finished` DATETIME(6) DEFAULT NULL,
    MODIFY COLUMN `timeout` DATETIME(6) DEFAULT NULL,
    DROP COLUMN `requested_archive_location_prefix`,
    ADD COLUMN `archive_status`         VARCHAR(20) DEFAULT NULL,
    ADD COLUMN `requested_launcher_ext` TEXT        DEFAULT NULL,
    ADD COLUMN `launcher_ext`           TEXT        DEFAULT NULL,
    ADD KEY `JOBS_ARCHIVE_STATUS_INDEX` (`archive_status`);

ALTER TABLE `tags`
    MODIFY COLUMN `created` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    MODIFY COLUMN `updated` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6);
