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
package com.netflix.genie.common.external.dtos.v4;

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
import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

/**
 * Final values for settings of the Genie job execution environment.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Getter
@EqualsAndHashCode(doNotUseGetters = true)
@ToString(doNotUseGetters = true)
@JsonDeserialize(builder = JobEnvironment.Builder.class)
@SuppressWarnings("checkstyle:finalclass")
public class JobEnvironment implements Serializable {

    private static final long serialVersionUID = 8478136461571895069L;
    private static final int DEFAULT_NUM_CPU = 1;
    @Min(value = 1, message = "Number of CPU's can't be less than 1")
    private final int cpu;
    @Min(value = 1, message = "Amount of memory has to be greater than 1 MB and preferably much more")
    private final int memory;
    private final ImmutableMap<
        @NotBlank(message = "Environment variable key can't be blank")
        @Size(max = 255, message = "Max environment variable name length is 255 characters") String,
        @NotNull(message = "Environment variable value can't be null")
        @Size(max = 1024, message = "Max environment variable value length is 1024 characters") String>
        environmentVariables;
    // TODO: Remove transient once Jackson 2.10 is picked up as dependency:
    //       https://github.com/FasterXML/jackson-databind/issues/18
    private final transient JsonNode ext;

    private JobEnvironment(final Builder builder) {
        this.cpu = builder.bCpu == null ? DEFAULT_NUM_CPU : builder.bCpu;
        this.memory = builder.bMemory;
        this.environmentVariables = ImmutableMap.copyOf(builder.bEnvironmentVariables);
        this.ext = builder.bExt;
    }

    /**
     * Get the environment variables requested by the user to be added to the job runtime.
     *
     * @return The environment variables backed by an immutable map. Any attempt to modify with throw exception
     */
    public Map<String, String> getEnvironmentVariables() {
        return this.environmentVariables;
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
     * Builder to create an immutable {@link JobEnvironment} instance.
     *
     * @author tgianos
     * @since 4.0.0
     */
    public static class Builder {
        private final Map<String, String> bEnvironmentVariables = Maps.newHashMap();
        private Integer bCpu;
        private int bMemory;
        private JsonNode bExt;

        /**
         * Constructor.
         *
         * @param memory The amount of memory (in MB) to allocate for the job
         */
        public Builder(final int memory) {
            this.bMemory = memory;
        }

        /**
         * Set the number of CPU cores that should be allocated to run the associated job.
         *
         * @param cpu The number of CPU's. Must be greater than or equal to 1.
         * @return The builder
         */
        public Builder withCpu(@Nullable final Integer cpu) {
            this.bCpu = cpu;
            return this;
        }

        /**
         * Set the amount of memory (in MB) that should be allocated for the job processes.
         *
         * @param memory The memory. Must be greater than or equal to 1 but preferably much more
         * @return The builder
         */
        public Builder withMemory(final int memory) {
            this.bMemory = memory;
            return this;
        }

        /**
         * Set any environment variables that the agent should add to the job runtime.
         *
         * @param environmentVariables Additional environment variables
         * @return The builder
         */
        public Builder withEnvironmentVariables(@Nullable final Map<String, String> environmentVariables) {
            this.bEnvironmentVariables.clear();
            if (environmentVariables != null) {
                this.bEnvironmentVariables.putAll(environmentVariables);
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
         * Build a new immutable instance of an {@link JobEnvironment}.
         *
         * @return An instance containing the fields set in this builder
         */
        public JobEnvironment build() {
            return new JobEnvironment(this);
        }
    }
}
