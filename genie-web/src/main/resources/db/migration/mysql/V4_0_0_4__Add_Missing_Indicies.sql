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

ALTER TABLE `agent_connections`
    ADD KEY AGENT_CONNECTIONS_SERVER_HOSTNAME_INDEX (`server_hostname`);

ALTER TABLE `applications`
    ADD KEY `APPLICATIONS_VERSION_INDEX` (`version`);

ALTER TABLE `clusters`
    ADD KEY `CLUSTERS_VERSION_INDEX` (`version`);

ALTER TABLE `commands`
    ADD KEY `COMMANDS_VERSION_INDEX` (`version`);

ALTER TABLE `files`
    ADD KEY `FILES_CREATED_INDEX` (`created`),
    ADD KEY `FILES_UPDATED_INDEX` (`updated`);

ALTER TABLE `jobs`
    ADD KEY `JOBS_AGENT_HOSTNAME_INDEX` (`agent_hostname`),
    ADD KEY `JOBS_API_INDEX` (`api`),
    ADD KEY `JOBS_V4_INDEX` (`v4`),
    ADD KEY `JOBS_VERSION_INDEX` (`version`);

ALTER TABLE `tags`
    ADD KEY `TAGS_CREATED_INDEX` (`created`),
    ADD KEY `TAGS_UPDATED_INDEX` (`updated`);
