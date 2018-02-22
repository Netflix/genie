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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Getter;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.Optional;

/**
 * A command data transfer object. After creation it is read-only.
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
    @Size(max = 255, message = "Executable path can't be longer than 255 characters")
    private final String executable;
    @Min(
        value = 1,
        message = "The delay between checks must be at least 1 millisecond. Probably should be much more than that"
    )
    private final long checkDelay;
    @Min(
        value = 1,
        message = "The minimum amount of memory if desired is 1 MB. Probably should be much more than that"
    )
    private final Integer memory;

    /**
     * Constructor used by the builder.
     *
     * @param builder The builder to get data from
     */
    protected Command(@Valid final Builder builder) {
        super(builder);
        this.status = builder.bStatus;
        this.executable = builder.bExecutable;
        this.checkDelay = builder.bCheckDelay;
        this.memory = builder.bMemory;
    }

    /**
     * Get the default amount of memory (in MB) to use for jobs which use this command.
     *
     * @return Optional of the amount of memory as it could be null if none set
     */
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
        private final String bExecutable;
        private final long bCheckDelay;
        private Integer bMemory;

        /**
         * Constructor which has required fields.
         *
         * @param name       The name to use for the Command
         * @param user       The user to use for the Command
         * @param version    The version to use for the Command
         * @param status     The status of the Command
         * @param executable The executable for the command
         * @param checkDelay How long the system should go between checking the status of jobs run with this command.
         *                   In milliseconds.
         */
        public Builder(
            @JsonProperty("name") final String name,
            @JsonProperty("user") final String user,
            @JsonProperty("version") final String version,
            @JsonProperty("status") final CommandStatus status,
            @JsonProperty("executable") final String executable,
            @JsonProperty("checkDelay") final long checkDelay
        ) {
            super(name, user, version);
            this.bStatus = status;
            this.bExecutable = executable;
            this.bCheckDelay = checkDelay;
        }

        /**
         * Set the amount of memory (in MB) to default jobs run with this command to use.
         *
         * @param memory The default amount of memory (in MB) for jobs to use
         * @return The builder
         */
        public Builder withMemory(@Nullable final Integer memory) {
            this.bMemory = memory;
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
