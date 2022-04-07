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
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.Size;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * An immutable V4 Command resource.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Getter
@EqualsAndHashCode(callSuper = true, doNotUseGetters = true)
@ToString(callSuper = true, doNotUseGetters = true)
public class Command extends CommonResource {
    @Valid
    private final CommandMetadata metadata;
    @NotEmpty(message = "At least one executable entry is required")
    private final ImmutableList<
        @NotEmpty(message = "A default executable element shouldn't be an empty string")
        @Size(max = 1024, message = "Executable elements can only be 1024 characters") String> executable;
    private final ImmutableList<Criterion> clusterCriteria;
    private final ComputeResources computeResources;
    private final Image image;

    /**
     * Constructor.
     *
     * @param id               The unique identifier of this command
     * @param created          The time this command was created in the system
     * @param updated          The last time this command was updated in the system
     * @param resources        The execution resources associated with this command
     * @param metadata         The metadata associated with this command
     * @param executable       The executable command that will be used when a job is run with this command. Generally
     *                         this will start with the binary and be followed optionally by default arguments. Must
     *                         have at least one. Blanks will be removed
     * @param clusterCriteria  The ordered list of cluster {@link Criterion} that should be used to resolve which
     *                         clusters this command can run on at job execution time
     * @param computeResources The default computational resources a job should have if this command is selected
     * @param image            The default image the job should launch an image in if this command is selected
     */
    @JsonCreator
    public Command(
        @JsonProperty(value = "id", required = true) final String id,
        @JsonProperty(value = "created", required = true) final Instant created,
        @JsonProperty(value = "updated", required = true) final Instant updated,
        @JsonProperty(value = "resources") @Nullable final ExecutionEnvironment resources,
        @JsonProperty(value = "metadata", required = true) final CommandMetadata metadata,
        @JsonProperty(value = "executable", required = true) final List<String> executable,
        @JsonProperty(value = "clusterCriteria") @Nullable final List<Criterion> clusterCriteria,
        @JsonProperty(value = "computeResources") @Nullable final ComputeResources computeResources,
        @JsonProperty(value = "image") @Nullable final Image image
    ) {
        super(id, created, updated, resources);
        this.metadata = metadata;
        this.executable = ImmutableList.copyOf(
            executable
                .stream()
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toList())
        );
        this.clusterCriteria = clusterCriteria != null ? ImmutableList.copyOf(clusterCriteria) : ImmutableList.of();
        this.computeResources = computeResources != null ? computeResources : new ComputeResources.Builder().build();
        this.image = image != null ? image : new Image.Builder().build();
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
    public ComputeResources getComputeResources() {
        return this.computeResources;
    }

    /**
     * Get any image information associated by default with this command.
     *
     * @return The {@link Image} metadata
     */
    public Image getImage() {
        return this.image;
    }
}
