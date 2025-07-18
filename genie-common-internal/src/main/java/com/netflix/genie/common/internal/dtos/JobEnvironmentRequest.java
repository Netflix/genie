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
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Fields that allow manipulation of the Genie job execution container environment.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Getter
@EqualsAndHashCode(doNotUseGetters = true)
@ToString(doNotUseGetters = true)
@JsonDeserialize(builder = JobEnvironmentRequest.Builder.class)
public class JobEnvironmentRequest implements Serializable {

    private static final long serialVersionUID = -1782447793634908168L;

    private final Map<
        @NotBlank(message = "Environment variable key can't be blank")
        @Size(max = 255, message = "Max environment variable name length is 255 characters") String,
        @NotNull(message = "Environment variable value can't be null")
        @Size(max = 1024, message = "Max environment variable value length is 1024 characters") String>
        requestedEnvironmentVariables;
    private final JsonNode ext;
    private final ComputeResources requestedComputeResources;
    private final Map<String, Image> requestedImages;

    private JobEnvironmentRequest(final Builder builder) {
        this.requestedEnvironmentVariables = Collections.unmodifiableMap(
            new HashMap<>(builder.bRequestedEnvironmentVariables)
        );
        this.ext = builder.bExt;
        this.requestedComputeResources = builder.bRequestedComputeResources;
        this.requestedImages = Collections.unmodifiableMap(new HashMap<>(builder.bRequestedImages));
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
     * Get the execution environment the user requested.
     *
     * @return The {@link ComputeResources} that were requested or {@link Optional#empty()}
     */
    public ComputeResources getRequestedComputeResources() {
        return this.requestedComputeResources;
    }

    /**
     * Get the requested image metadata the user entered.
     *
     * @return The {@link Image} data in an immutable map.
     */
    public Map<String, Image> getRequestedImages() {
        return this.requestedImages;
    }

    /**
     * Builder to create an immutable {@link JobEnvironmentRequest} instance.
     *
     * @author tgianos
     * @since 4.0.0
     */
    public static class Builder {
        private final Map<String, String> bRequestedEnvironmentVariables;
        private JsonNode bExt;
        private ComputeResources bRequestedComputeResources;
        private Map<String, Image> bRequestedImages;

        /**
         * Constructor.
         */
        public Builder() {
            this.bRequestedEnvironmentVariables = new HashMap<>();
            this.bRequestedComputeResources = new ComputeResources.Builder().build();
            this.bRequestedImages = new HashMap<>();
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
         * Set the computation resources the job should run with.
         *
         * @param requestedComputeResources The {@link ComputeResources}
         * @return This {@link Builder} instance
         */
        public Builder withRequestedComputeResources(final ComputeResources requestedComputeResources) {
            this.bRequestedComputeResources = requestedComputeResources;
            return this;
        }

        /**
         * Set the images the job should run with.
         *
         * @param requestedImages The {@link Image} map
         * @return This {@link Builder} instance
         */
        public Builder withRequestedImages(final Map<String, Image> requestedImages) {
            this.bRequestedImages.clear();
            this.bRequestedImages.putAll(requestedImages);
            return this;
        }

        /**
         * Build a new immutable instance of an {@link JobEnvironmentRequest}.
         *
         * @return An instance containing the fields set in this builder
         */
        public JobEnvironmentRequest build() {
            return new JobEnvironmentRequest(this);
        }
    }
}
