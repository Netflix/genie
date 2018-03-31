/*
 *
 *  Copyright 2018 Netflix, Inc.
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

DROP TABLE IF EXISTS
job_metadata_320,
job_executions_320,
jobs_applications_320,
jobs_320,
job_requests_320,
application_configs_320,
application_dependencies_320,
cluster_configs_320,
cluster_dependencies_320,
command_configs_320,
command_dependencies_320,
commands_applications_320,
clusters_commands_320,
applications_320,
clusters_320,
commands_320;

ALTER TABLE applications
  ALTER COLUMN version DROP NOT NULL,
  ALTER COLUMN version SET DEFAULT NULL;

ALTER TABLE clusters
  ALTER COLUMN version DROP NOT NULL,
  ALTER COLUMN version SET DEFAULT NULL;

ALTER TABLE commands
  ALTER COLUMN version DROP NOT NULL,
  ALTER COLUMN version SET DEFAULT NULL;

ALTER TABLE jobs
  ALTER COLUMN version DROP NOT NULL,
  ALTER COLUMN version SET DEFAULT NULL;
