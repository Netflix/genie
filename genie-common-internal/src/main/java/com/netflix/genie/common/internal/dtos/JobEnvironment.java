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
package com.netflix.genie.common.internal.dtos;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import jakarta.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
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
public class JobEnvironment implements Serializable {

    private static final long serialVersionUID = 8478136461571895069L;
    private final Map<
        @NotBlank(message = "Environment variable key can't be blank")
        @Size(max = 255, message = "Max environment variable name length is 255 characters") String,
        @NotNull(message = "Environment variable value can't be null")
        @Size(max = 1024, message = "Max environment variable value length is 1024 characters") String>
        environmentVariables;
    private final JsonNode ext;
    private final ComputeResources computeResources;
    private final Map<String, Image> images;

    private JobEnvironment(final Builder builder) {
        this.environmentVariables = Collections.unmodifiableMap(new HashMap<>(builder.bEnvironmentVariables));
        this.ext = builder.bExt;
        this.computeResources = builder.bComputeResources;
        this.images = Collections.unmodifiableMap(new HashMap<>(builder.bImages));
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
     * Get the computation resources for the job if any were defined.
     *
     * @return The {@link ComputeResources}
     */
    public ComputeResources getComputeResources() {
        return this.computeResources;
    }

    /**
     * Get the images for the job if any were defined.
     *
     * @return The {@link Image}
     */
    public Map<String, Image> getImages() {
        return this.images;
    }

    /**
     * Builder to create an immutable {@link JobEnvironment} instance.
     *
     * @author tgianos
     * @since 4.0.0
     */
    public static class Builder {
        private final Map<String, String> bEnvironmentVariables;
        private JsonNode bExt;
        private ComputeResources bComputeResources;
        private final Map<String, Image> bImages;

        /**
         * Constructor.
         */
        public Builder() {
            this.bEnvironmentVariables = new HashMap<>();
            this.bComputeResources = new ComputeResources.Builder().build();
            this.bImages = new HashMap<>();
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
         * Set the computation resources for the job.
         *
         * @param computeResources The {@link ComputeResources}
         * @return This {@link Builder} instance
         */
        public Builder withComputeResources(final ComputeResources computeResources) {
            this.bComputeResources = computeResources;
            return this;
        }

        /**
         * Set the images the job should use.
         *
         * @param images The {@link Image} set to use
         * @return This {@link Builder} instance
         */
        public Builder withImages(final Map<String, Image> images) {
            this.bImages.clear();
            this.bImages.putAll(images);
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
