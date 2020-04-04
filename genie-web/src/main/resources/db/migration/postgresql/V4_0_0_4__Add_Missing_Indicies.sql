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

CREATE INDEX IF NOT EXISTS agent_connections_server_hostname_index ON agent_connections (server_hostname);

CREATE INDEX IF NOT EXISTS applications_version_index ON applications (version);

CREATE INDEX IF NOT EXISTS clusters_version_index ON clusters (version);

CREATE INDEX IF NOT EXISTS commands_version_index ON commands (version);

CREATE INDEX IF NOT EXISTS files_created_index ON files (created);
CREATE INDEX IF NOT EXISTS files_updated_index ON files (updated);

CREATE INDEX IF NOT EXISTS jobs_agent_hostname_index ON jobs (agent_hostname);
CREATE INDEX IF NOT EXISTS jobs_api_index ON jobs (api);
CREATE INDEX IF NOT EXISTS jobs_v4_index ON jobs (v4);
CREATE INDEX IF NOT EXISTS jobs_version_index ON jobs (version);

CREATE INDEX IF NOT EXISTS tags_created_index ON tags (created);
CREATE INDEX IF NOT EXISTS tags_updated_index ON tags (updated);
