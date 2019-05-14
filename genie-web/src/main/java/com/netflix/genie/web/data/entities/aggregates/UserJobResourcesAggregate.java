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
package com.netflix.genie.web.data.entities.aggregates;

/**
 * An aggregate of running jobs and memory used for a given user.
 *
 * @author mprimi
 * @since 4.0.0
 */
public interface UserJobResourcesAggregate {

    /**
     * Get the user name.
     *
     * @return the user name
     */
    String getUser();

    /**
     * Get the number of running jobs for the user.
     *
     * @return count of jobs
     */
    Long getRunningJobsCount();

    /**
     * Get the total amount of memory used by all running jobs for the user.
     *
     * @return amount of memory (in megabytes)
     */
    Long getUsedMemory();
}
