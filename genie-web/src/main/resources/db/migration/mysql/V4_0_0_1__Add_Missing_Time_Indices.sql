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

ALTER TABLE `applications`
  ADD KEY `APPLICATIONS_CREATED_INDEX` (`created`),
  ADD KEY `APPLICATIONS_UPDATED_INDEX` (`updated`);

ALTER TABLE `clusters`
  ADD KEY `CLUSTERS_CREATED_INDEX` (`created`),
  ADD KEY `CLUSTERS_UPDATED_INDEX` (`updated`);

ALTER TABLE `commands`
  ADD KEY `COMMANDS_CREATED_INDEX` (`created`),
  ADD KEY `COMMANDS_UPDATED_INDEX` (`updated`);

ALTER TABLE `jobs`
  ADD KEY `JOBS_UPDATED_INDEX` (`updated`);
