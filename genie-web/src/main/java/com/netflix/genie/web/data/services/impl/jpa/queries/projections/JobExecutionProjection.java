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
package com.netflix.genie.web.data.services.impl.jpa.queries.projections;

import com.fasterxml.jackson.databind.JsonNode;

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
     * Get when the job was started.
     *
     * @return The start date
     */
    Optional<Instant> getStarted();

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
     * Get the exit code from the process that ran the job.
     *
     * @return The exit code or -1 if the job hasn't finished yet
     */
    Optional<Integer> getExitCode();

    /**
     * Get the number of CPU's used by the job.
     *
     * @return The number of CPU's used or {@link Optional#empty()}
     */
    Optional<Integer> getCpuUsed();

    /**
     * Get the number of GPUs used by the job.
     *
     * @return The number of GPUs used or {@link Optional#empty()}
     */
    Optional<Integer> getGpuUsed();

    /**
     * Get the amount of memory (in MB) that this job is/was run with.
     *
     * @return The memory as an optional as it could be null
     */
    Optional<Integer> getMemoryUsed();

    /**
     * Get the amount of disk space used for a job.
     *
     * @return The amount of disk space in MB or {@link Optional#empty()}
     */
    Optional<Long> getDiskMbUsed();

    /**
     * Get network bandwidth used for a job if any.
     *
     * @return The network bandwidth in mbps or {@link Optional#empty()}
     */
    Optional<Long> getNetworkMbpsUsed();

    /**
     * Get the final resolved timeout duration (in seconds) if there was one for this job.
     *
     * @return The timeout value wrapped in an {@link Optional}
     */
    Optional<Integer> getTimeoutUsed();

    /**
     * Get the archive status for this job.
     *
     * @return An Optional wrapping the string representation of the archive status, if is present.
     */
    Optional<String> getArchiveStatus();

    /**
     * Get the launcher metadata for this job.
     *
     * @return An Optional wrapping the JSON representation of the launcher metadata, if present.
     */
    Optional<JsonNode> getLauncherExt();

    /**
     * Get the set of image configuration used for this job.
     *
     * @return The images used or {@link Optional#empty()}
     */
    Optional<JsonNode> getImagesUsed();
}
