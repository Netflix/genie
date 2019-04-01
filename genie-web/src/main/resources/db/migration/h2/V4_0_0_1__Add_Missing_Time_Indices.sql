/*
 *
 *  Copyright 2019 Netflix, Inc.
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

CREATE INDEX IF NOT EXISTS `APPLICATIONS_CREATED_INDEX`
  ON `applications` (`created`);
CREATE INDEX IF NOT EXISTS `APPLICATIONS_UPDATED_INDEX`
  ON `applications` (`updated`);

CREATE INDEX IF NOT EXISTS `CLUSTERS_CREATED_INDEX`
  ON `clusters` (`created`);
CREATE INDEX IF NOT EXISTS `CLUSTERS_UPDATED_INDEX`
  ON `clusters` (`updated`);

CREATE INDEX IF NOT EXISTS `COMMANDS_CREATED_INDEX`
  ON `commands` (`created`);
CREATE INDEX IF NOT EXISTS `COMMANDS_UPDATED_INDEX`
  ON `commands` (`updated`);

CREATE INDEX IF NOT EXISTS `JOBS_CREATED_INDEX`
  ON `jobs` (`created`);
CREATE INDEX IF NOT EXISTS `JOBS_UPDATED_INDEX`
  ON `jobs` (`updated`);
