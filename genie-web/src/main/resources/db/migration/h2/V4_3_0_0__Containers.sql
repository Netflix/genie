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

ALTER TABLE `jobs`
  ADD COLUMN `cpu_used` INT(11) DEFAULT NULL;
ALTER TABLE `jobs`
  ADD COLUMN `requested_image_name` VARCHAR(1024) DEFAULT NULL;
ALTER TABLE `jobs`
  ADD COLUMN `image_name_used` VARCHAR(1024) DEFAULT NULL;
ALTER TABLE `jobs`
  ADD COLUMN `requested_image_tag` VARCHAR(1024) DEFAULT NULL;
ALTER TABLE `jobs`
  ADD COLUMN `image_tag_used` VARCHAR(1024) DEFAULT NULL;
ALTER TABLE `jobs`
  ADD COLUMN `requested_gpu` INT(11) DEFAULT NULL;
ALTER TABLE `jobs`
  ADD COLUMN `gpu_used` INT(11) DEFAULT NULL;
ALTER TABLE `jobs`
  ADD COLUMN `requested_disk_mb` BIGINT(20) DEFAULT NULL;
ALTER TABLE `jobs`
  ADD COLUMN `disk_mb_used` BIGINT(20) DEFAULT NULL;
ALTER TABLE `jobs`
  ADD COLUMN `requested_network_mbps` BIGINT(20) DEFAULT NULL;
ALTER TABLE `jobs`
  ADD COLUMN `network_mbps_used` BIGINT(20) DEFAULT NULL;
ALTER TABLE `jobs`
  DROP COLUMN `check_delay`;
ALTER TABLE `jobs`
  DROP COLUMN `v4`;
ALTER TABLE `jobs`
  ALTER COLUMN `requested_memory` BIGINT(20) DEFAULT NULL;
ALTER TABLE `jobs`
  ALTER COLUMN `memory_used` BIGINT(20) DEFAULT NULL;

ALTER TABLE `commands`
  DROP COLUMN `check_delay`;
ALTER TABLE `commands`
  ADD COLUMN `cpu` INT(11) DEFAULT NULL;
ALTER TABLE `commands`
  ADD COLUMN `gpu` INT(11) DEFAULT NULL;
ALTER TABLE `commands`
  ADD COLUMN `disk_mb` BIGINT(20) DEFAULT NULL;
ALTER TABLE `commands`
  ADD COLUMN `network_mbps` BIGINT(20) DEFAULT NULL;
ALTER TABLE `commands`
  ADD COLUMN `image_name` VARCHAR(1024) DEFAULT NULL;
ALTER TABLE `commands`
  ADD COLUMN `image_tag` VARCHAR(1024) DEFAULT NULL;
ALTER TABLE `commands`
  ALTER COLUMN `memory` BIGINT(20) DEFAULT NULL;
