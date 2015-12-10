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

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

/**
 * A command data transfer object. After creation it is read-only.
 *
 * @author tgianos
 * @since 3.0.0
 */
@JsonDeserialize(builder = Command.Builder.class)
public class Command extends ConfigDTO {

    @NotNull(message = "No command status entered and is required.")
    private CommandStatus status;

    @Size(min = 1, max = 255, message = "Executable path can't be longer than 255 characters")
    private String executable;

    private String setupFile;

    /**
     * Constructor used by the builder.
     *
     * @param builder The builder to get data from
     */
    protected Command(final Builder builder) {
        super(builder);
        this.status = builder.bStatus;
        this.executable = builder.bExecutable;
        this.setupFile = builder.bSetupFile;
    }

    /**
     * Get the status of the command.
     *
     * @return The command status
     */
    public CommandStatus getStatus() {
        return this.status;
    }

    /**
     * Get the executable of the command.
     *
     * @return the executable
     */
    public String getExecutable() {
        return this.executable;
    }

    /**
     * Get the setup file for command.
     *
     * @return The setup file for the command
     */
    public String getSetupFile() {
        return this.setupFile;
    }

    /**
     * A builder to create commands.
     *
     * @author tgianos
     * @since 3.0.0
     */
    public static class Builder extends ConfigDTO.Builder<Builder> {

        private final CommandStatus bStatus;
        private final String bExecutable;
        private String bSetupFile;

        /**
         * Constructor which has required fields.
         *
         * @param name       The name to use for the Command
         * @param user       The user to use for the Command
         * @param version    The version to use for the Command
         * @param status     The status of the Command
         * @param executable The executable for the command
         */
        public Builder(
            @JsonProperty("name")
            final String name,
            @JsonProperty("user")
            final String user,
            @JsonProperty("version")
            final String version,
            @JsonProperty("status")
            final CommandStatus status,
            @JsonProperty("executable")
            final String executable
        ) {
            super(name, user, version);
            this.bStatus = status;
            this.bExecutable = executable;
        }

        /**
         * Set the setup file for the command.
         *
         * @param setupFile The location of the setup file to use
         * @return The builder
         */
        public Builder withSetupFile(final String setupFile) {
            this.bSetupFile = setupFile;
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
