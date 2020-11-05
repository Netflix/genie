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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.netflix.genie.common.external.dtos.v4.ArchiveStatus;
import lombok.Getter;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.validation.constraints.Size;
import java.time.Instant;
import java.util.Optional;

/**
 * All information needed to show state of a running job.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Getter
@JsonDeserialize(builder = JobExecution.Builder.class)
public class JobExecution extends BaseDTO {

    /**
     * The exit code that will be set to indicate a job is killed.
     */
    public static final int KILLED_EXIT_CODE = 999;

    /**
     * The exit code that will be set to indicate a job is has been lost by Genie.
     */
    public static final int LOST_EXIT_CODE = 666;

    /**
     * The exit code that will be set to indicate a job has succeeded.
     */
    public static final int SUCCESS_EXIT_CODE = 0;

    private static final long serialVersionUID = 5005391660522052211L;

    @Size(min = 1, max = 1024, message = "Host name is required but no longer than 1024 characters")
    private final String hostName;
    private final Integer processId;
    @Min(
        value = 1,
        message = "The delay between checks must be at least 1 millisecond. Probably should be much more than that"
    )
    private final Long checkDelay;
    private final Instant timeout;
    private final Integer exitCode;
    @Min(
        value = 1,
        message = "The amount of memory this job is set to use on the system"
    )
    private final Integer memory;
    private final ArchiveStatus archiveStatus;
    private final JsonNode launcherExt;

    /**
     * Constructor used by the builder build() method.
     *
     * @param builder The builder to use
     */
    protected JobExecution(final Builder builder) {
        super(builder);
        this.hostName = builder.bHostName;
        this.processId = builder.bProcessId;
        this.checkDelay = builder.bCheckDelay;
        this.exitCode = builder.bExitCode;
        this.memory = builder.bMemory;
        this.timeout = builder.bTimeout;
        this.archiveStatus = builder.bArchiveStatus;
        this.launcherExt = builder.bLauncherExt;
    }

    /**
     * Get the process id for this job execution as Optional.
     *
     * @return The process id
     */
    public Optional<Integer> getProcessId() {
        return Optional.ofNullable(this.processId);
    }

    /**
     * Get the amount of time (in milliseconds) to delay between checks of status of the job process.
     *
     * @return The time to delay as an Optional as it could be null
     */
    public Optional<Long> getCheckDelay() {
        return Optional.ofNullable(this.checkDelay);
    }

    /**
     * Get the timeout date for this job after which if it is still running the system will attempt to kill it.
     *
     * @return The timeout date
     */
    public Optional<Instant> getTimeout() {
        return Optional.ofNullable(this.timeout);
    }

    /**
     * Get the exit code of the process.
     *
     * @return The exit code as an Optional as it could be null
     */
    public Optional<Integer> getExitCode() {
        return Optional.ofNullable(this.exitCode);
    }

    /**
     * Get the amount of memory (in MB) of the job.
     *
     * @return The amount of memory the job is set to use as an Optional as it could be null
     */
    public Optional<Integer> getMemory() {
        return Optional.ofNullable(this.memory);
    }

    /**
     * Get the archival status of job files.
     *
     * @return the archival status as Optional as it could be null
     */
    public Optional<ArchiveStatus> getArchiveStatus() {
        return Optional.ofNullable(this.archiveStatus);
    }

    /**
     * Get the launcher extension.
     *
     * @return the launcher extension as Optional as it could be null
     */
    public Optional<JsonNode> getLauncherExt() {
        return Optional.ofNullable(this.launcherExt);
    }

    /**
     * A builder to create job requests.
     *
     * @author tgianos
     * @since 3.0.0
     */
    public static class Builder extends BaseDTO.Builder<Builder> {

        private final String bHostName;
        private Integer bProcessId;
        private Long bCheckDelay;
        private Instant bTimeout;
        private Integer bExitCode;
        private Integer bMemory;
        private ArchiveStatus bArchiveStatus;
        private JsonNode bLauncherExt;

        /**
         * Constructor which has required fields.
         *
         * @param hostName The hostname where the job is running
         */
        public Builder(
            @JsonProperty(value = "hostName", required = true) final String hostName
        ) {
            super();
            this.bHostName = hostName;
        }

        /**
         * Set the process id for the jobs' execution.
         *
         * @param processId The process id
         * @return The builder
         */
        public Builder withProcessId(@Nullable final Integer processId) {
            this.bProcessId = processId;
            return this;
        }

        /**
         * Set the amount of time (in milliseconds) to delay between checks of the process.
         *
         * @param checkDelay The check delay to use
         * @return The builder
         */
        public Builder withCheckDelay(@Nullable final Long checkDelay) {
            this.bCheckDelay = checkDelay;
            return this;
        }

        /**
         * Set the timeout date when the job will be failed if it hasn't completed by.
         *
         * @param timeout The timeout date
         * @return The builder
         */
        public Builder withTimeout(@Nullable final Instant timeout) {
            this.bTimeout = timeout;
            return this;
        }

        /**
         * Set the exit code for the jobs' execution. If not set will default to -1.
         *
         * @param exitCode The exit code.
         * @return The builder
         */
        public Builder withExitCode(@Nullable final Integer exitCode) {
            this.bExitCode = exitCode;
            return this;
        }

        /**
         * Set the amount of memory (in MB) to use for this job execution.
         *
         * @param memory The amount of memory in megabytes
         * @return The builder
         */
        public Builder withMemory(@Nullable final Integer memory) {
            this.bMemory = memory;
            return this;
        }

        /**
         * Set the archive status for this job.
         *
         * @param archiveStatus The archive status
         * @return The builder
         */
        public Builder withArchiveStatus(final ArchiveStatus archiveStatus) {
            this.bArchiveStatus = archiveStatus;
            return this;
        }

        /**
         * Set the launcher extension for this job.
         *
         * @param launcherExt The launcher extension
         * @return The builder
         */
        public Builder withLauncherExt(@Nullable final JsonNode launcherExt) {
            this.bLauncherExt = launcherExt;
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
