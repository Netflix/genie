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

package com.netflix.genie.agent.execution.services;

/**
 * Service responsible for killing the job.
 *
 * @author standon
 * @since 4.0.0
 */
public interface KillService {
    /**
     * Perform all operations associated with killing the job.
     *
     * @param killSource the source of kill
     */
    void kill(final KillSource killSource);

    /**
     * Enumeration for the source of a request to kill the job.
     */
    enum KillSource {
        /**
         * A system signal (SIGINT), for example user ctrl-c or system shutdown.
         */
        SYSTEM_SIGNAL,
        /**
         * A request to the server, forwarded to the agent.
         */
        API_KILL_REQUEST,
    }
}
