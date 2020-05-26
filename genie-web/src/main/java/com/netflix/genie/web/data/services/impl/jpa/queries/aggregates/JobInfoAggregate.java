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
package com.netflix.genie.web.data.services.impl.jpa.queries.aggregates;

/**
 * An object to return aggregate data selected with regards to memory usage on a given Genie host.
 *
 * @author tgianos
 * @since 4.0.0
 */
public interface JobInfoAggregate {

    /**
     * Get the total amount of memory (in MB) that was allocated by jobs on this host.
     *
     * @return The total memory allocated
     */
    long getTotalMemoryAllocated();

    /**
     * Get the total amount of memory (in MB) that is actively in use by jobs on this host.
     *
     * @return The total memory used
     */
    long getTotalMemoryUsed();

    /**
     * Get the number of jobs in any of the active states on this host.
     *
     * @return The number of active jobs
     */
    long getNumberOfActiveJobs();
}
