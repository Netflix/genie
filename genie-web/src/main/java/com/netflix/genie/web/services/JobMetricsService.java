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
package com.netflix.genie.web.services;

/**
 * A service which returns metrics for active jobs.
 *
 * @author tgianos
 * @since 3.0.0
 */
public interface JobMetricsService {
    /**
     * Get the number of jobs active on this node.
     *
     * @return The number of jobs currently active on this node
     */
    int getNumActiveJobs();

    /**
     * Get the amount of memory currently used by jobs in MB.
     *
     * @return The total memory used by jobs in megabytes
     */
    int getUsedMemory();

    /**
     * Get the number of agents currently connected to this server.
     *
     * @return The number of agents connected to the server
     */
    default long getNumActiveAgents() {
        return 0L;
    }
}
