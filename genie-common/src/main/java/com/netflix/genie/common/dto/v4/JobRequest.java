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
package com.netflix.genie.common.dto.v4;

import com.google.common.collect.ImmutableList;
import lombok.Value;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.Size;
import java.util.List;
import java.util.Optional;

/**
 * All details a user will provide to Genie in order to run a job.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Value
public final class JobRequest {

    @Size(max = 255, message = "Max length for the ID is 255 characters")
    private final String requestedId;
    private final ImmutableList<
        @Size(
            max = 10_000,
            message = "Max length of an individual command line argument is 10,000 characters"
        ) String> commandArgs;
    private final boolean disableArchiving;
    @Min(value = 1, message = "The timeout must be at least 1 second, preferably much more.")
    private final Integer timeout;
    private final boolean interactive;
    @Valid
    private final ExecutionEnvironment resources;
    @Valid
    private final JobMetadata metadata;
    @Valid
    private final ExecutionResourceCriteria criteria;

    @SuppressWarnings("unchecked")
    private JobRequest(final Builder builder) {
        this.requestedId = builder.bRequestedId;
        this.commandArgs = builder.bCommandArgs == null
            ? ImmutableList.of()
            : ImmutableList.copyOf(builder.bCommandArgs);
        this.disableArchiving = builder.bDisableArchival;
        this.timeout = builder.bTimeout;
        this.interactive = builder.bInteractive;
        this.metadata = builder.bMetadata;
        this.criteria = builder.bCriteria;
        this.resources = builder.bResources == null
            ? new ExecutionEnvironment(null, null, null)
            : builder.bResources;
    }

    /**
     * Get the ID the user has requested for this Job if one was.
     *
     * @return The ID wrapped in an {@link Optional}
     */
    public Optional<String> getRequestedId() {
        return Optional.ofNullable(this.requestedId);
    }

    /**
     * Get the timeout a user has requested for this job as number of seconds from job start that the server will
     * kill it.
     *
     * @return The timeout duration (in seconds) if one was requested wrapped in an {@link Optional}
     */
    public Optional<Integer> getTimeout() {
        return Optional.ofNullable(this.timeout);
    }

    /**
     * Get the command arguments a user has requested be appended to a command executable for their job.
     *
     * @return The command arguments as an immutable list. Any attempt to modify will throw exception
     */
    public List<String> getCommandArgs() {
        return this.commandArgs;
    }

    /**
     * Builder for a V4 Job Request.
     *
     * @author tgianos
     * @since 4.0.0
     */
    public static class Builder {

        private final JobMetadata bMetadata;
        private final ExecutionResourceCriteria bCriteria;
        private String bRequestedId;
        private ImmutableList<String> bCommandArgs;
        private Integer bTimeout;
        private boolean bDisableArchival;
        private boolean bInteractive;
        private ExecutionEnvironment bResources;

        /**
         * Constructor with required parameters.
         *
         * @param metadata All user supplied metadata
         * @param criteria All user supplied execution criteria
         */
        public Builder(final JobMetadata metadata, final ExecutionResourceCriteria criteria) {
            this.bMetadata = metadata;
            this.bCriteria = criteria;
        }

        /**
         * Set the id being requested for the job. Will be rejected if the ID is already used by another job. If not
         * included a GUID will be supplied.
         *
         * @param requestedId The requested id. Max of 255 characters.
         * @return The builder
         */
        public Builder withRequestedId(@Nullable final String requestedId) {
            this.bRequestedId = requestedId;
            return this;
        }

        /**
         * Set the ordered list of command line arguments to append to the command executable at runtime.
         *
         * @param commandArgs The arguments in the order they should be placed on the command line. Maximum of 10,000
         *                    characters per argument
         * @return The builder
         */
        public Builder withCommandArgs(@Nullable final List<String> commandArgs) {
            this.bCommandArgs = commandArgs == null ? ImmutableList.of() : ImmutableList.copyOf(commandArgs);
            return this;
        }

        /**
         * Set the timeout (in seconds) that the job should be killed after by the service after it has started.
         *
         * @param timeout The timeout. Must be greater >= 1 but preferably much higher
         * @return The builder
         */
        public Builder withTimeout(@Nullable final Integer timeout) {
            this.bTimeout = timeout;
            return this;
        }

        /**
         * Set whether to disable log archive for the job.
         *
         * @param disableArchival true if you want to disable log archival
         * @return The builder
         */
        public Builder withDisableArchival(final boolean disableArchival) {
            this.bDisableArchival = disableArchival;
            return this;
        }

        /**
         * Set whether the job should be treated as an interactive job or not.
         *
         * @param interactive true if the job is interactive
         * @return The builder
         */
        public Builder withInteractive(final boolean interactive) {
            this.bInteractive = interactive;
            return this;
        }

        /**
         * Set the execution resources for this job. e.g. setup file or configuration files etc
         *
         * @param resources The job resources to use
         * @return The builder
         */
        public Builder withResources(@Nullable final ExecutionEnvironment resources) {
            this.bResources = resources;
            return this;
        }

        /**
         * Build an immutable job request instance.
         *
         * @return An immutable representation of the user supplied information for a job request
         */
        public JobRequest build() {
            return new JobRequest(this);
        }
    }
}
