/*
 *
 *  Copyright 2022 Netflix, Inc.
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

ALTER TABLE jobs
  ADD COLUMN cpu_used               INT           DEFAULT NULL,
  ADD COLUMN requested_image_name   VARCHAR(1024) DEFAULT NULL,
  ADD COLUMN image_name_used        VARCHAR(1024) DEFAULT NULL,
  ADD COLUMN requested_image_tag    VARCHAR(1024) DEFAULT NULL,
  ADD COLUMN image_tag_used         VARCHAR(1024) DEFAULT NULL,
  ADD COLUMN requested_gpu          INT           DEFAULT NULL,
  ADD COLUMN gpu_used               INT           DEFAULT NULL,
  ADD COLUMN requested_disk_mb      BIGINT        DEFAULT NULL,
  ADD COLUMN disk_mb_used           BIGINT        DEFAULT NULL,
  ADD COLUMN requested_network_mbps BIGINT        DEFAULT NULL,
  ADD COLUMN network_mbps_used      BIGINT        DEFAULT NULL,
  DROP COLUMN check_delay,
  DROP COLUMN v4,
  ALTER COLUMN requested_memory TYPE BIGINT,
  ALTER COLUMN memory_used TYPE BIGINT;

ALTER TABLE commands
  DROP COLUMN check_delay,
  ADD COLUMN cpu          INT           DEFAULT NULL,
  ADD COLUMN gpu          INT           DEFAULT NULL,
  ADD COLUMN disk_mb      INT           DEFAULT NULL,
  ADD COLUMN network_mbps INT           DEFAULT NULL,
  ADD COLUMN image_name   VARCHAR(1024) DEFAULT NULL,
  ADD COLUMN image_tag    VARCHAR(1024) DEFAULT NULL,
  ALTER COLUMN memory TYPE BIGINT;
