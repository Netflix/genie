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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.Map;
import java.util.Optional;

/**
 * Fields that allow manipulation of the Genie Agent execution container environment.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Getter
@EqualsAndHashCode(doNotUseGetters = true)
@ToString(doNotUseGetters = true)
@JsonDeserialize(builder = AgentEnvironmentRequest.Builder.class)
@SuppressWarnings("checkstyle:finalclass")
public class AgentEnvironmentRequest {
    @Min(value = 1, message = "Number of CPU's requested can't be less than 1")
    private final Integer requestedJobCpu;
    @Min(value = 1, message = "Amount of memory requested has to be greater than 1 MB and preferably much more")
    private final Integer requestedJobMemory;
    private final ImmutableMap<
        @NotBlank(message = "Environment variable key can't be blank")
        @Size(max = 255, message = "Max environment variable name length is 255 characters") String,
        @NotNull(message = "Environment variable value can't be null")
        @Size(max = 1024, message = "Max environment variable value length is 1024 characters") String>
        requestedEnvironmentVariables;
    private final JsonNode ext;

    private AgentEnvironmentRequest(final Builder builder) {
        this.requestedJobCpu = builder.bRequestedJobCpu;
        this.requestedJobMemory = builder.bRequestedJobMemory;
        this.requestedEnvironmentVariables = ImmutableMap.copyOf(builder.bRequestedEnvironmentVariables);
        this.ext = builder.bExt;
    }

    /**
     * Get the number of CPU cores requested by the user for the job process.
     *
     * @return The number of CPU cores requested wrapped in an {@link Optional}
     */
    public Optional<Integer> getRequestedJobCpu() {
        return Optional.ofNullable(this.requestedJobCpu);
    }

    /**
     * Get the amount of memory requested by the user to launch the job process with.
     *
     * @return The memory requested wrapped in an {@link Optional}
     */
    public Optional<Integer> getRequestedJobMemory() {
        return Optional.ofNullable(this.requestedJobMemory);
    }

    /**
     * Get the environment variables requested by the user to be added to the job runtime.
     *
     * @return The environment variables backed by an immutable map. Any attempt to modify with throw exception
     */
    public Map<String, String> getRequestedEnvironmentVariables() {
        return this.requestedEnvironmentVariables;
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
     * Builder to create an immutable {@link AgentEnvironmentRequest} instance.
     *
     * @author tgianos
     * @since 4.0.0
     */
    public static class Builder {
        private final Map<String, String> bRequestedEnvironmentVariables = Maps.newHashMap();
        private Integer bRequestedJobCpu;
        private Integer bRequestedJobMemory;
        private JsonNode bExt;

        /**
         * Set the number of CPU cores that should be allocated to run the associated job.
         *
         * @param requestedJobCpu The number of CPU's. Must be greater than or equal to 1.
         * @return The builder
         */
        public Builder withRequestedJobCpu(@Nullable final Integer requestedJobCpu) {
            this.bRequestedJobCpu = requestedJobCpu;
            return this;
        }

        /**
         * Set the amount of memory (in MB) that should be allocated for the job processes.
         *
         * @param requestedJobMemory The requested memory. Must be greater than or equal to 1 but preferably much more
         * @return The builder
         */
        public Builder withRequestedJobMemory(@Nullable final Integer requestedJobMemory) {
            this.bRequestedJobMemory = requestedJobMemory;
            return this;
        }

        /**
         * Set any environment variables that the agent should add to the job runtime.
         *
         * @param requestedEnvironmentVariables Additional environment variables
         * @return The builder
         */
        public Builder withRequestedEnvironmentVariables(
            @Nullable final Map<String, String> requestedEnvironmentVariables
        ) {
            this.bRequestedEnvironmentVariables.clear();
            if (requestedEnvironmentVariables != null) {
                this.bRequestedEnvironmentVariables.putAll(requestedEnvironmentVariables);
            }
            return this;
        }

        /**
         * Set the extension configuration for the agent. This is generally used for specific implementations of the
         * job launcher e.g. on Titus or local docker etc.
         *
         * @param ext The extension configuration which is effectively a DSL per job launch implementation
         * @return The builder
         */
        public Builder withExt(@Nullable final JsonNode ext) {
            this.bExt = ext;
            return this;
        }

        /**
         * Build a new immutable instance of an {@link AgentEnvironmentRequest}.
         *
         * @return An instance containing the fields set in this builder
         */
        public AgentEnvironmentRequest build() {
            return new AgentEnvironmentRequest(this);
        }
    }
}
