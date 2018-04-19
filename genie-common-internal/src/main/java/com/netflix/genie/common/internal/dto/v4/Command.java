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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.Min;
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
    @Min(
        value = 1,
        message = "The minimum amount of memory if desired is 1 MB. Probably should be much more than that"
    )
    private final Integer memory;
    @Min(
        value = 1,
        message = "The delay between checks must be at least 1 millisecond. Probably should be much more than that"
    )
    // TODO: This is here for Genie 3 backwards compatibility while Genie 4 agent is still in development
    private final long checkDelay;

    /**
     * Constructor.
     *
     * @param id         The unique identifier of this command
     * @param created    The time this command was created in the system
     * @param updated    The last time this command was updated in the system
     * @param resources  The execution resources associated with this command
     * @param metadata   The metadata associated with this command
     * @param executable The executable command that will be used when a job is run with this command. Generally
     *                   this will start with the binary and be followed optionally by default arguments. Must have
     *                   at least one. Blanks will be removed
     * @param memory     The default memory that should be used to run a job with this command
     * @param checkDelay The amount of time (in milliseconds) to delay between checks of job status for jobs run using
     *                   this command. Min 1 but preferably much more
     */
    @JsonCreator
    public Command(
        @JsonProperty(value = "id", required = true) final String id,
        @JsonProperty(value = "created", required = true) final Instant created,
        @JsonProperty(value = "updated", required = true) final Instant updated,
        @JsonProperty(value = "resources") @Nullable final ExecutionEnvironment resources,
        @JsonProperty(value = "metadata", required = true) final CommandMetadata metadata,
        @JsonProperty(value = "executable", required = true) final List<String> executable,
        @JsonProperty(value = "memory") @Nullable final Integer memory,
        @JsonProperty(value = "checkDelay", required = true) final long checkDelay
    ) {
        super(id, created, updated, resources);
        this.metadata = metadata;
        this.executable = ImmutableList.copyOf(
            executable
                .stream()
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toList())
        );
        this.memory = memory;
        this.checkDelay = checkDelay;
    }

    /**
     * Get the default amount of memory (in MB) to use for jobs which use this command.
     *
     * @return {@link Optional} wrapper of the amount of memory to use for a job
     */
    public Optional<Integer> getMemory() {
        return Optional.ofNullable(this.memory);
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
}
