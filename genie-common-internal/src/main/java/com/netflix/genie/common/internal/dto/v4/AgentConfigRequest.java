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
package com.netflix.genie.common.internal.dto.v4;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import java.io.File;
import java.util.Optional;

/**
 * Configuration options for the Genie agent.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Getter
@ToString(doNotUseGetters = true)
@EqualsAndHashCode(doNotUseGetters = true)
@SuppressWarnings("checkstyle:finalclass")
public class AgentConfigRequest {
    private final boolean archivingDisabled;
    @Min(value = 1, message = "The timeout must be at least 1 second, preferably much more.")
    private final Integer timeoutRequested;
    private final boolean interactive;
    private final File requestedJobDirectoryLocation;
    private final JsonNode ext;

    private AgentConfigRequest(final Builder builder) {
        this.archivingDisabled = builder.bArchivingDisabled;
        this.timeoutRequested = builder.bTimeoutRequested;
        this.interactive = builder.bInteractive;
        this.requestedJobDirectoryLocation = builder.bRequestedJobDirectoryLocation;
        this.ext = builder.bExt;
    }

    /**
     * Get the amount of time (in seconds) after the job starts that the agent should timeout and kill the job.
     *
     * @return The time in seconds if one was requested wrapped in an {@link Optional}
     */
    public Optional<Integer> getTimeoutRequested() {
        return Optional.ofNullable(this.timeoutRequested);
    }

    /**
     * Get the location where the Agent should place the job directory when it runs.
     *
     * @return The directory location wrapped in an {@link Optional}
     */
    public Optional<File> getRequestedJobDirectoryLocation() {
        return Optional.ofNullable(this.requestedJobDirectoryLocation);
    }

    /**
     * Get the extension variables to the agent configuration as a JSON blob.
     *
     * @return The extension variables wrapped in an {@link Optional}
     */
    public Optional<JsonNode> getExt() {
        return Optional.ofNullable(this.ext);
    }

    /**
     * Builder to create an immutable {@link AgentConfigRequest} instance.
     *
     * @author tgianos
     * @since 4.0.0
     */
    public static class Builder {
        private boolean bArchivingDisabled;
        private Integer bTimeoutRequested;
        private boolean bInteractive;
        private File bRequestedJobDirectoryLocation;
        private JsonNode bExt;

        /**
         * Set whether the agent should or should not archive the job directory.
         *
         * @param archivingDisabled True if archiving should be disabled (not done)
         * @return The builder
         */
        public Builder withArchivingDisabled(final boolean archivingDisabled) {
            this.bArchivingDisabled = archivingDisabled;
            return this;
        }

        /**
         * Set whether the agent should be run as an interactive job or not.
         *
         * @param interactive True if the agent should be configured to run an interactive job
         * @return The builder
         */
        public Builder withInteractive(final boolean interactive) {
            this.bInteractive = interactive;
            return this;
        }

        /**
         * Set the amount of time (in seconds) from the job start time that the job should be killed by the agent due
         * to timeout.
         *
         * @param timeoutRequested The requested amount of time in seconds
         * @return the builder
         */
        public Builder withTimeoutRequested(@Nullable final Integer timeoutRequested) {
            this.bTimeoutRequested = timeoutRequested;
            return this;
        }

        /**
         * Set the directory where the agent should put the job working directory.
         *
         * @param requestedJobDirectoryLocation The location
         * @return The builder
         */
        @JsonSetter
        public Builder withRequestedJobDirectoryLocation(@Nullable final String requestedJobDirectoryLocation) {
            this.bRequestedJobDirectoryLocation = requestedJobDirectoryLocation == null
                ? null
                : new File(requestedJobDirectoryLocation);
            return this;
        }

        /**
         * Set the directory where the agent should put the job working directory.
         *
         * @param requestedJobDirectoryLocation The location
         * @return The builder
         */
        public Builder withRequestedJobDirectoryLocation(@Nullable final File requestedJobDirectoryLocation) {
            this.bRequestedJobDirectoryLocation = requestedJobDirectoryLocation;
            return this;
        }

        /**
         * Set the extension configuration for the agent.
         *
         * @param ext The extension configuration which is effectively a DSL per Agent implementation
         * @return The builder
         */
        public Builder withExt(@Nullable final JsonNode ext) {
            this.bExt = ext;
            return this;
        }

        /**
         * Build a new immutable instance of an {@link AgentConfigRequest}.
         *
         * @return An instance containing the fields set in this builder
         */
        public AgentConfigRequest build() {
            return new AgentConfigRequest(this);
        }
    }
}
