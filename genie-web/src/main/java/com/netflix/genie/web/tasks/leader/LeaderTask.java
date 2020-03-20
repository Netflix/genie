/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.genie.web.tasks.leader;

import com.netflix.genie.web.tasks.GenieTask;
import lombok.extern.slf4j.Slf4j;

/**
 * Interface for any task that a node elected as the leader of a Genie cluster should run.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Slf4j
public abstract class LeaderTask extends GenieTask {

    /**
     * Any cleanup that needs to be performed when this task is stopped due to leadership being revoked.
     */
    public void cleanup() {
        log.info("Task cleanup called. Nothing to do.");
    }
}
