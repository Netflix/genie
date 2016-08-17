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
import com.netflix.genie.common.util.JsonDateDeserializer;
import lombok.Getter;

import javax.validation.constraints.Min;
import javax.validation.constraints.Size;
import java.util.Date;
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
    private final Date timeout;
    private final Integer exitCode;

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
        if (builder.bTimeout != null) {
            this.timeout = new Date(builder.bTimeout.getTime());
        } else {
            this.timeout = null;
        }
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
    public Optional<Date> getTimeout() {
        if (this.timeout == null) {
            return Optional.empty();
        } else {
            return Optional.of(new Date(this.timeout.getTime()));
        }
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
     * A builder to create job requests.
     *
     * @author tgianos
     * @since 3.0.0
     */
    public static class Builder extends BaseDTO.Builder<Builder> {

        private final String bHostName;
        private Integer bProcessId;
        private Long bCheckDelay;
        @JsonDeserialize(using = JsonDateDeserializer.class)
        private Date bTimeout;
        private Integer bExitCode;

        /**
         * Constructor which has required fields.
         *
         * @param hostName The hostname where the job is running
         */
        public Builder(
            @JsonProperty("hostName")
            final String hostName
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
        public Builder withProcessId(final Integer processId) {
            this.bProcessId = processId;
            return this;
        }

        /**
         * Set the amount of time (in milliseconds) to delay between checks of the process.
         *
         * @param checkDelay The check delay to use
         * @return The builder
         */
        public Builder withCheckDelay(final Long checkDelay) {
            this.bCheckDelay = checkDelay;
            return this;
        }

        /**
         * Set the timeout date when the job will be failed if it hasn't completed by.
         *
         * @param timeout The timeout date
         * @return The builder
         */
        public Builder withTimeout(final Date timeout) {
            if (timeout != null) {
                this.bTimeout = new Date(timeout.getTime());
            } else {
                this.bTimeout = null;
            }
            return this;
        }

        /**
         * Set the exit code for the jobs' execution. If not set will default to -1.
         *
         * @param exitCode The exit code.
         * @return The builder
         */
        public Builder withExitCode(final Integer exitCode) {
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
