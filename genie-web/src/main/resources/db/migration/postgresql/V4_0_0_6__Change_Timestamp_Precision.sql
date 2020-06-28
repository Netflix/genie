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

ALTER TABLE applications
    ALTER COLUMN created TYPE TIMESTAMP(6) WITHOUT TIME ZONE,
    ALTER COLUMN updated TYPE TIMESTAMP(6) WITHOUT TIME ZONE;

ALTER TABLE clusters
    ALTER COLUMN created TYPE TIMESTAMP(6) WITHOUT TIME ZONE,
    ALTER COLUMN updated TYPE TIMESTAMP(6) WITHOUT TIME ZONE;

ALTER TABLE commands
    ALTER COLUMN created TYPE TIMESTAMP(6) WITHOUT TIME ZONE,
    ALTER COLUMN updated TYPE TIMESTAMP(6) WITHOUT TIME ZONE,
    ADD COLUMN launcher_ext TEXT DEFAULT NULL;

ALTER TABLE files
    ALTER COLUMN created TYPE TIMESTAMP(6) WITHOUT TIME ZONE,
    ALTER COLUMN updated TYPE TIMESTAMP(6) WITHOUT TIME ZONE;

ALTER TABLE jobs
    ALTER COLUMN created TYPE TIMESTAMP(6) WITHOUT TIME ZONE,
    ALTER COLUMN updated TYPE TIMESTAMP(6) WITHOUT TIME ZONE,
    ALTER COLUMN started TYPE TIMESTAMP(6) WITHOUT TIME ZONE,
    ALTER COLUMN finished TYPE TIMESTAMP(6) WITHOUT TIME ZONE,
    ALTER COLUMN timeout TYPE TIMESTAMP(6) WITHOUT TIME ZONE,
    DROP COLUMN requested_archive_location_prefix,
    ADD COLUMN archive_status         VARCHAR(20) DEFAULT NULL,
    ADD COLUMN requested_launcher_ext TEXT        DEFAULT NULL,
    ADD COLUMN launcher_ext           TEXT        DEFAULT NULL;

CREATE INDEX IF NOT EXISTS jobs_archive_status_index ON jobs (archive_status);

ALTER TABLE tags
    ALTER COLUMN created TYPE TIMESTAMP(6) WITHOUT TIME ZONE,
    ALTER COLUMN updated TYPE TIMESTAMP(6) WITHOUT TIME ZONE;
