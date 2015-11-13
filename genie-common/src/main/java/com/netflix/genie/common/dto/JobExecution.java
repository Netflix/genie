/*
 *
 *  Copyright 2015 Netflix, Inc.
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
package com.netflix.genie.common.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import javax.validation.constraints.Size;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * All information needed to show state of a running job.
 *
 * @author tgianos
 * @since 3.0.0
 */
@JsonDeserialize(builder = JobExecution.Builder.class)
public class JobExecution extends BaseDTO {

    @Size(min = 1, max = 1024, message = "Host name is required but no longer than 1024 characters")
    private String hostName;

    private int processId;

    private int exitCode;

    private Set<String> clusterCriteria = new HashSet<>();

    /**
     * Constructor used by the builder build() method.
     *
     * @param builder The builder to use
     */
    protected JobExecution(final Builder builder) {
        super(builder);
        this.hostName = builder.bHostName;
        this.processId = builder.bProcessId;
        this.exitCode = builder.bExitCode;
        this.clusterCriteria.addAll(builder.bClusterCriteria);
    }

    /**
     * Get the host name this job is running on.
     *
     * @return The host name
     */
    public String getHostName() {
        return this.hostName;
    }

    /**
     * Get the process id on the system.
     *
     * @return the process id
     */
    public int getProcessId() {
        return this.processId;
    }

    /**
     * Get the process exit code. Defaults to -1 before the job is complete.
     *
     * @return The process exit code
     */
    public int getExitCode() {
        return this.exitCode;
    }

    /**
     * Get the cluster criteria that was used to chose the cluster to run this job as unmodifiable set.
     *
     * @return The criteria. Any attempt to modify will throw runtime exception.
     */
    public Set<String> getClusterCriteria() {
        return Collections.unmodifiableSet(this.clusterCriteria);
    }

    /**
     * A builder to create job requests.
     *
     * @author tgianos
     * @since 3.0.0
     */
    public static class Builder extends BaseDTO.Builder<Builder> {

        private final String bHostName;
        private final int bProcessId;
        private int bExitCode = -1;
        private final Set<String> bClusterCriteria = new HashSet<>();

        /**
         * Constructor which has required fields.
         *
         * @param hostName  The hostname where the job is running
         * @param processId The id of the process running the job
         */
        public Builder(
                @JsonProperty("hostName")
                final String hostName,
                @JsonProperty("processId")
                final int processId
        ) {
            super();
            this.bHostName = hostName;
            this.bProcessId = processId;
        }

        /**
         * Set the exit code for the jobs' execution. If not set will default to -1.
         *
         * @param exitCode The exit code.
         * @return The builder
         */
        public Builder withExitCode(final int exitCode) {
            this.bExitCode = exitCode;
            return this;
        }

        /**
         * Set the cluster criteria used to select the cluster this job ran on.
         *
         * @param clusterCriteria The cluster criteria
         * @return The builder
         */
        public Builder withClusterCriteria(final Set<String> clusterCriteria) {
            if (clusterCriteria != null) {
                this.bClusterCriteria.addAll(clusterCriteria);
            }
            return this;
        }

        /**
         * Build the job request.
         *
         * @return Create the final read-only JobRequest instance
         */
        public JobExecution build() {
            return new JobExecution(this);
        }
    }
}
