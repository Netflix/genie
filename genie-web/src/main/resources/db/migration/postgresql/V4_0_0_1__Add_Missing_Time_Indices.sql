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

CREATE INDEX IF NOT EXISTS applications_created_index
  ON applications (created);
CREATE INDEX IF NOT EXISTS applications_updated_index
  ON applications (updated);

CREATE INDEX IF NOT EXISTS clusters_created_index
  ON clusters (created);
CREATE INDEX IF NOT EXISTS clusters_updated_index
  ON clusters (updated);

CREATE INDEX IF NOT EXISTS commands_created_index
  ON commands (created);
CREATE INDEX IF NOT EXISTS commands_updated_index
  ON commands (updated);

CREATE INDEX IF NOT EXISTS jobs_created_index
  ON jobs (created);
CREATE INDEX IF NOT EXISTS jobs_updated_index
  ON jobs (updated);
