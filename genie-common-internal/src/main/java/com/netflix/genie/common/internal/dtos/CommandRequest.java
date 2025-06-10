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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import jakarta.annotation.Nullable;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Fields representing all the values users can set when creating a new Command resource.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Getter
@EqualsAndHashCode(callSuper = true, doNotUseGetters = true)
@ToString(callSuper = true, doNotUseGetters = true)
@JsonDeserialize(builder = CommandRequest.Builder.class)
@SuppressWarnings("checkstyle:finalclass")
public class CommandRequest extends CommonRequestImpl {

    @Valid
    private final CommandMetadata metadata;
    @NotEmpty(message = "At least one executable entry is required")
    private final List<@Size(max = 255, message = "Executable elements can only be 255 characters") String> executable;
    private final List<Criterion> clusterCriteria;
    private final ComputeResources computeResources;
    private final Map<String, Image> images;

    private CommandRequest(final Builder builder) {
        super(builder);
        this.metadata = builder.bMetadata;
        this.executable = Collections.unmodifiableList(new ArrayList<>(builder.bExecutable));
        this.clusterCriteria = Collections.unmodifiableList(new ArrayList<>(builder.bClusterCriteria));
        this.computeResources = builder.bComputeResources;
        this.images = Collections.unmodifiableMap(new HashMap<>(builder.bImages));
    }

    /**
     * Get the executable elements (generally a binary followed optionally by default arguments) that should be used
     * with this command when executing a job.
     *
     * @return The executable arguments as an immutable list. Any attempt to modify will throw an exception
     */
    public List<String> getExecutable() {
        return this.executable;
    }

    /**
     * Get the ordered list of {@link Criterion} for this command to resolve clusters are runtime.
     *
     * @return The ordered list of {@link Criterion} as an immutable list. Any attempt to modify will throw an exception
     */
    public List<Criterion> getClusterCriteria() {
        return this.clusterCriteria;
    }

    /**
     * Get any default compute resources that were requested for this command.
     *
     * @return The {@link ComputeResources} or {@link Optional#empty()}
     */
    public Optional<ComputeResources> getComputeResources() {
        return Optional.ofNullable(this.computeResources);
    }

    /**
     * Get any image information associated by default with this command.
     *
     * @return The map of image domain name to {@link Image} metadata as unmodifiable map. Any attempt to modify
     * will throw an exception
     */
    public Map<String, Image> getImages() {
        return this.images;
    }

    /**
     * Builder for a V4 Command Request.
     *
     * @author tgianos
     * @since 4.0.0
     */
    public static class Builder extends CommonRequestImpl.Builder<Builder> {

        private final CommandMetadata bMetadata;
        private final List<String> bExecutable;
        private final List<Criterion> bClusterCriteria;
        private ComputeResources bComputeResources;
        private Map<String, Image> bImages;

        /**
         * Constructor which has required fields.
         *
         * @param metadata   The user supplied metadata about a command resource
         * @param executable The executable arguments to use on job process launch. Typically the binary path followed
         *                   by optional default parameters for that given binary. Must have at least one. Blanks will
         *                   be removed
         */
        @JsonCreator
        public Builder(
            @JsonProperty(value = "metadata", required = true) final CommandMetadata metadata,
            @JsonProperty(value = "executable", required = true) final List<String> executable
        ) {
            super();
            this.bClusterCriteria = new ArrayList<>();
            this.bMetadata = metadata;
            this.bExecutable = executable
                .stream()
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toList());
            this.bImages = new HashMap<>();
        }

        /**
         * Set the ordered list of {@link Criterion} that should be used to resolve which clusters this command
         * can run on at any given time.
         *
         * @param clusterCriteria The {@link Criterion} in priority order
         * @return The builder
         */
        public Builder withClusterCriteria(@Nullable final List<Criterion> clusterCriteria) {
            this.bClusterCriteria.clear();
            if (clusterCriteria != null) {
                this.bClusterCriteria.addAll(clusterCriteria);
            }
            return this;
        }

        /**
         * Set any default compute resources that should be used if this command is selected.
         *
         * @param computeResources The {@link ComputeResources} or {@link Optional#empty()}
         * @return This {@link Builder} instance
         */
        public Builder withComputeResources(@Nullable final ComputeResources computeResources) {
            this.bComputeResources = computeResources;
            return this;
        }

        /**
         * Set any default image metadata that should be used if this command is selected.
         *
         * @param images The {@link Image}'s or {@literal null}
         * @return This {@link Builder} instance
         */
        public Builder withImages(@Nullable final Map<String, Image> images) {
            this.bImages.clear();
            if (images != null) {
                this.bImages.putAll(images);
            }
            return this;
        }

        /**
         * Build a new CommandRequest instance.
         *
         * @return The immutable command request
         */
        public CommandRequest build() {
            return new CommandRequest(this);
        }
    }
}
