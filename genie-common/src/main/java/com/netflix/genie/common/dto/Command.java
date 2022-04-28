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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A command data transfer object. After creation, it is read-only.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Getter
@JsonDeserialize(builder = Command.Builder.class)
public class Command extends ExecutionEnvironmentDTO {

    /**
     * The default amount of time to wait between job process checks.
     */
    public static final long DEFAULT_CHECK_DELAY = 10000L;

    private static final long serialVersionUID = -3559641165667609041L;

    @NotNull(message = "A valid command status is required")
    private final CommandStatus status;
    @NotEmpty(message = "An executable is required")
    @Size(max = 1024, message = "Executable path can't be longer than 1024 characters")
    private final String executable;
    @NotEmpty(message = "An executable is required")
    private final List<@NotEmpty @Size(max = 1024) String> executableAndArguments;
    @Deprecated
    private final long checkDelay;
    @Min(
        value = 1,
        message = "The minimum amount of memory if desired is 1 MB. Probably should be much more than that"
    )
    @Deprecated
    private final Integer memory;
    private final List<Criterion> clusterCriteria;
    private final Runtime runtime;

    /**
     * Constructor used by the builder.
     *
     * @param builder The builder to get data from
     */
    protected Command(@Valid final Builder builder) {
        super(builder);
        this.status = builder.bStatus;
        this.checkDelay = DEFAULT_CHECK_DELAY;
        if (!builder.bExecutableAndArguments.isEmpty()) {
            this.executableAndArguments = Collections.unmodifiableList(
                new ArrayList<>(builder.bExecutableAndArguments)
            );
            this.executable = StringUtils.join(builder.bExecutableAndArguments, ' ');
        } else if (builder.bExecutable != null && !builder.bExecutable.isEmpty()) {
            this.executable = builder.bExecutable;
            this.executableAndArguments = Collections.unmodifiableList(
                Arrays.asList(StringUtils.split(builder.bExecutable, ' '))
            );
        } else {
            throw new IllegalArgumentException("Cannot build command without 'executable' OR 'executableAndArguments'");
        }
        this.clusterCriteria = Collections.unmodifiableList(new ArrayList<>(builder.bClusterCriteria));
        this.runtime = new Runtime.Builder()
            .withResources(builder.bRuntimeResources.build())
            .withImages(builder.bImages)
            .build();
        this.memory = this.runtime
            .getResources()
            .getMemoryMb()
            .flatMap(mem -> Optional.of(mem.intValue()))
            .orElse(null);
    }

    /**
     * Get the default amount of memory (in MB) to use for jobs which use this command.
     *
     * @return Optional of the amount of memory as it could be null if none set
     * @deprecated Use runtime instead
     */
    @Deprecated
    public Optional<Integer> getMemory() {
        return Optional.ofNullable(this.memory);
    }

    /**
     * A builder to create commands.
     *
     * @author tgianos
     * @since 3.0.0
     */
    public static class Builder extends ExecutionEnvironmentDTO.Builder<Builder> {

        private final CommandStatus bStatus;
        private final List<String> bExecutableAndArguments = new ArrayList<>();
        private final List<Criterion> bClusterCriteria = new ArrayList<>();
        private String bExecutable;
        private final RuntimeResources.Builder bRuntimeResources = new RuntimeResources.Builder();
        private final Map<String, ContainerImage> bImages = new HashMap<>();

        /**
         * Constructor which has required fields.
         *
         * @param name              The name to use for the Command
         * @param user              The user to use for the Command
         * @param version           The version to use for the Command
         * @param status            The status of the Command
         * @param executable        The executable for the command
         * @param ignoredCheckDelay Ignored. Here for backwards compatibility.
         * @deprecated Use {@link #Builder(String, String, String, CommandStatus, List)}
         */
        @Deprecated
        public Builder(
            final String name,
            final String user,
            final String version,
            final CommandStatus status,
            final String executable,
            final long ignoredCheckDelay
        ) {
            super(name, user, version);
            this.bStatus = status;
            this.bExecutable = executable;
        }

        /**
         * Constructor with required fields.
         *
         * @param name                   The name to use for the Command
         * @param user                   The user to use for the Command
         * @param version                The version to use for the Command
         * @param status                 The status of the Command
         * @param executableAndArguments The executable for the command and its fixed arguments
         * @param ignoredCheckDelay      Ignored. Here for backwards compatibility.
         * @deprecated Use {@link #Builder(String, String, String, CommandStatus, List)}
         */
        @Deprecated
        public Builder(
            final String name,
            final String user,
            final String version,
            final CommandStatus status,
            final List<String> executableAndArguments,
            final long ignoredCheckDelay
        ) {
            super(name, user, version);
            this.bStatus = status;
            this.bExecutableAndArguments.addAll(executableAndArguments);
        }

        /**
         * Constructor with required fields.
         *
         * @param name                   The name to use for the Command
         * @param user                   The user to use for the Command
         * @param version                The version to use for the Command
         * @param status                 The status of the Command
         * @param executableAndArguments The executable for the command and its fixed arguments
         */
        public Builder(
            final String name,
            final String user,
            final String version,
            final CommandStatus status,
            final List<String> executableAndArguments
        ) {
            super(name, user, version);
            this.bStatus = status;
            this.bExecutableAndArguments.addAll(executableAndArguments);
        }

        /*
         * This constructor is provided just for JSON deserialization and considers both the old 'executable' and the
         * new 'executableAndArguments' fields optional for API backward compatibility.
         */
        @JsonCreator
        Builder(
            @JsonProperty(value = "name", required = true) final String name,
            @JsonProperty(value = "user", required = true) final String user,
            @JsonProperty(value = "version", required = true) final String version,
            @JsonProperty(value = "status", required = true) final CommandStatus status
        ) {
            super(name, user, version);
            this.bStatus = status;
        }

        /**
         * Set the amount of memory (in MB) to default jobs run with this command to use.
         *
         * @param memory The default amount of memory (in MB) for jobs to use
         * @return The builder
         * @deprecated Use {@link #withRuntime(Runtime)} instead
         */
        @Deprecated
        public Builder withMemory(@Nullable final Integer memory) {
            this.bRuntimeResources.withMemoryMb(memory == null ? null : memory.longValue());
            return this;
        }

        /**
         * Set the executable and its fixed arguments for this command.
         * Note that this string is tokenized with a naive strategy (split on space) to break apart the executable from
         * its fixed arguments. Escape characters and quotes are ignored in this process. To avoid this tokenization,
         * use {@link #withExecutableAndArguments(List)}
         *
         * @param executable the executable, possibly followed by arguments
         * @return The builder
         * @deprecated this setter is provided transitionally to make both 'executable' and 'executableAndArguments'
         * optional for API backward compatibility. The proper way to construct a Command is via the constructor
         * {@link #Builder(String, String, String, CommandStatus, List, long)}.
         */
        @Deprecated
        public Builder withExecutable(final String executable) {
            this.bExecutable = executable;
            return this;
        }

        /**
         * Set the executable and its fixed arguments for this command.
         *
         * @param executableAndArguments the executable and its argument
         * @return The builder
         * @deprecated this setter is provided transitionally to make both 'executable' and 'executableAndArguments'
         * optional for API backward compatibility. The proper way to construct a Command is via the constructor
         * {@link #Builder(String, String, String, CommandStatus, List, long)}.
         */
        @Deprecated
        public Builder withExecutableAndArguments(@Nullable final List<String> executableAndArguments) {
            this.bExecutableAndArguments.clear();
            if (executableAndArguments != null) {
                this.bExecutableAndArguments.addAll(executableAndArguments);
            }
            return this;
        }

        /**
         * The check delay for pre-4.x.x Genie backends. This no longer has any impact on the system and is here
         * to maintain backwards compatibility. Will always be set to -1.
         *
         * @param ignoredCheckDelay The check delay but this value is ignored
         * @return The current {@link Builder} instance
         */
        @Deprecated
        public Builder withCheckDelay(@Nullable final Long ignoredCheckDelay) {
            // No-Op for backwards compatibility
            return this;
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
         * Set the runtime for any jobs that use this command.
         *
         * @param runtime The {@link Runtime} or {@literal null}
         * @return The builder instance
         */
        public Builder withRuntime(@Nullable final Runtime runtime) {
            this.bImages.clear();
            if (runtime == null) {
                this.bRuntimeResources
                    .withCpu(null)
                    .withGpu(null)
                    .withMemoryMb(null)
                    .withDiskMb(null)
                    .withNetworkMbps(null);
            } else {
                final RuntimeResources resources = runtime.getResources();
                this.bRuntimeResources
                    .withCpu(resources.getCpu().orElse(null))
                    .withGpu(resources.getGpu().orElse(null))
                    .withMemoryMb(resources.getMemoryMb().orElse(null))
                    .withDiskMb(resources.getDiskMb().orElse(null))
                    .withNetworkMbps(resources.getNetworkMbps().orElse(null));
                this.bImages.putAll(runtime.getImages());
            }
            return this;
        }

        /**
         * Build the command.
         *
         * @return Create the final read-only Command instance
         */
        public Command build() {
            return new Command(this);
        }
    }
}
