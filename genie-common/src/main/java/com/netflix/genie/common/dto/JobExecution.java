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
import lombok.Getter;

import javax.validation.constraints.Min;
import javax.validation.constraints.Size;

/**
 * All information needed to show state of a running job.
 *
 * @author tgianos
 * @since 3.0.0
 */
@JsonDeserialize(builder = JobExecution.Builder.class)
@Getter
public class JobExecution extends BaseDTO {
    /**
     * The exit code that will be set to indicate a job is currently executing.
     */
    public static final int DEFAULT_EXIT_CODE = -1;

    /**
     * The exit code that will be set to indicate a job is killed.
     */
    public static final int KILLED_EXIT_CODE = 777;

    /**
     * The exit code that will be set to indicate a job is marked as zombie.
     */
    public static final int ZOMBIE_EXIT_CODE = 666;

    /**
     * The exit code that will be set to indicate a job has succeeded.
     */
    public static final int SUCCESS_EXIT_CODE = 0;

    private static final long serialVersionUID = 5005391660522052211L;

    @Size(min = 1, max = 1024, message = "Host name is required but no longer than 1024 characters")
    private final String hostname;
    private final int processId;
    @Min(
        value = 1,
        message = "The delay between checks must be at least 1 millisecond. Probably should be much more than that"
    )
    private final long checkDelay;
    private final int exitCode;

    /**
     * Constructor used by the builder build() method.
     *
     * @param builder The builder to use
     */
    protected JobExecution(final Builder builder) {
        super(builder);
        this.hostname = builder.bHostname;
        this.processId = builder.bProcessId;
        this.checkDelay = builder.bCheckDelay;
        this.exitCode = builder.bExitCode;
    }

    /**
     * A builder to create job requests.
     *
     * @author tgianos
     * @since 3.0.0
     */
    public static class Builder extends BaseDTO.Builder<Builder> {

        private final String bHostname;
        private final int bProcessId;
        private final long bCheckDelay;
        private int bExitCode = -1;

        /**
         * Constructor which has required fields.
         *
         * @param hostname   The hostname where the job is running
         * @param processId  The id of the process running the job
         * @param checkDelay How long, in milliseconds, to wait between checks for job status
         */
        public Builder(
            @JsonProperty("hostname")
            final String hostname,
            @JsonProperty("processId")
            final int processId,
            @JsonProperty("checkDelay")
            final long checkDelay
        ) {
            super();
            this.bHostname = hostname;
            this.bProcessId = processId;
            this.bCheckDelay = checkDelay;
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
         * Build the job request.
         *
         * @return Create the final read-only JobRequest instance
         */
        public JobExecution build() {
            return new JobExecution(this);
        }
    }
}
