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

CREATE TABLE commands_cluster_criteria
(
    command_id     BIGINT NOT NULL,
    criterion_id   BIGINT NOT NULL,
    priority_order INT    NOT NULL,
    PRIMARY KEY (command_id, criterion_id, priority_order),
    CONSTRAINT commands_cluster_criteria_command_id_fkey FOREIGN KEY (command_id) REFERENCES commands (id)
        ON DELETE CASCADE,
    CONSTRAINT commands_cluster_criteria_criterion_id_fkey FOREIGN KEY (criterion_id) REFERENCES criteria (id)
        ON DELETE RESTRICT
);

CREATE INDEX commands_cluster_criteria_command_id_index
    ON commands_cluster_criteria (command_id);
CREATE INDEX commands_cluster_criteria_criterion_id_index
    ON commands_cluster_criteria (criterion_id);
