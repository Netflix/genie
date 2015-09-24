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
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.hibernate.validator.constraints.Length;
import org.hibernate.validator.constraints.NotBlank;

import javax.validation.constraints.NotNull;

/**
 * A command data transfer object. After creation it is read-only.
 *
 * @author tgianos
 * @since 3.0.0
 */
@ApiModel(description = "A resource for a command in Genie.")
@JsonDeserialize(builder = Command.Builder.class)
public class Command extends ConfigDTO {

    @ApiModelProperty(
            value = "The status of the command",
            required = true
    )
    @NotNull(message = "No command status entered and is required.")
    private CommandStatus status;

    @ApiModelProperty(
            value = "Location of the executable for this command",
            required = true
    )
    @NotBlank(message = "No executable entered for command and is required.")
    @Length(max = 255, message = "Max length is 255 characters")
    private String executable;

    @ApiModelProperty(
            value = "Location of a setup file which will be downloaded and run before command execution"
    )
    private String setupFile;

    //TODO: this doesn't seem useful we don't use it for anything other than human data...remove
    @ApiModelProperty(
            value = "Job type of the command. eg: hive, pig , hadoop etc"
    )
    @Length(max = 255, message = "Max length is 255 characters")
    private String jobType;

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
        this.jobType = builder.bJobType;
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
     * Get the job type of the command.
     *
     * @return The job type
     */
    public String getJobType() {
        return this.jobType;
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
        private String bJobType;

        /**
         * Constructor which has required fields.
         *
         * @param name        The name to use for the Command
         * @param user        The user to use for the Command
         * @param version     The version to use for the Command
         * @param status      The status of the Command
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
         * Set the job type for the command.
         *
         * @param jobType The type of job this command will run
         * @return The builder
         */
        public Builder withJobType(final String jobType) {
            this.bJobType = jobType;
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
