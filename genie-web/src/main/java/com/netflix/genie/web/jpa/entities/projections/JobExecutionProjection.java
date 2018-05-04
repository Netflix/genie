/*
 *
 *  Copyright 2017 Netflix, Inc.
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
package com.netflix.genie.web.jpa.entities.projections;

import java.time.Instant;
import java.util.Optional;

/**
 * Projection with the data present in a Genie JobExecutionEntity from pre-3.3.0.
 *
 * @author tgianos
 * @since 3.3.0
 */
public interface JobExecutionProjection extends AuditProjection, AgentHostnameProjection {

    /**
     * Get the unique identifier of this job execution.
     *
     * @return The unique id
     */
    String getUniqueId();

    /**
     * Get the process id of the job.
     *
     * @return the process id
     */
    Optional<Integer> getProcessId();

    /**
     * Get the amount of time (in milliseconds) to delay the check for the job status.
     *
     * @return Could be null so return optional of the Long
     */
    Optional<Long> getCheckDelay();

    /**
     * Get the exit code from the process that ran the job.
     *
     * @return The exit code or -1 if the job hasn't finished yet
     */
    Optional<Integer> getExitCode();

    /**
     * Get the amount of memory (in MB) that this job is/was run with.
     *
     * @return The memory as an optional as it could be null
     */
    Optional<Integer> getMemoryUsed();

    /**
     * Get the date this job will be killed due to exceeding its set timeout duration.
     *
     * @return The timeout date
     */
    Optional<Instant> getTimeout();
}
