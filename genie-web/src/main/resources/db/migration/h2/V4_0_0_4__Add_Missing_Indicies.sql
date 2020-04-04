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

CREATE INDEX `AGENT_CONNECTIONS_SERVER_HOSTNAME_INDEX` ON `agent_connections` (`server_hostname`);

CREATE INDEX `APPLICATIONS_VERSION_INDEX` ON `applications` (`version`);

CREATE INDEX `CLUSTERS_VERSION_INDEX` ON `clusters` (`version`);

CREATE INDEX `COMMANDS_VERSION_INDEX` ON `commands` (`version`);

CREATE INDEX `FILES_CREATED_INDEX` ON `files` (`created`);
CREATE INDEX `FILES_UPDATED_INDEX` ON `files` (`updated`);

CREATE INDEX `JOBS_AGENT_HOSTNAME_INDEX` ON `jobs` (`agent_hostname`);
CREATE INDEX `JOBS_API_INDEX` ON `jobs` (`api`);
CREATE INDEX `JOBS_V4_INDEX` ON `jobs` (`v4`);
CREATE INDEX `JOBS_VERSION_INDEX` ON `jobs` (`version`);

CREATE INDEX `TAGS_CREATED_INDEX` ON `tags` (`created`);
CREATE INDEX `TAGS_UPDATED_INDEX` ON `tags` (`updated`);
