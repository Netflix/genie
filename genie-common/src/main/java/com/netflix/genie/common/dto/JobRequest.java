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
import io.swagger.annotations.ApiModelProperty;
import org.hibernate.validator.constraints.Email;
import org.hibernate.validator.constraints.Length;
import org.hibernate.validator.constraints.NotBlank;
import org.hibernate.validator.constraints.NotEmpty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * All information needed to make a request to run a new job.
 *
 * @author tgianos
 * @since 3.0.0
 */
@JsonDeserialize(builder = JobRequest.Builder.class)
public class JobRequest extends BaseDTO {

    @ApiModelProperty(value = "Command line arguments for the job.", example = "-f hive.q", required = true)
    @NotBlank(message = "Command arguments are required.")
    private String commandArgs;

    @ApiModelProperty(
            value = "List of criteria containing tags to use to pick a cluster to run this job, evaluated in order",
            required = true
    )
    @NotEmpty(message = "No cluster criteria entered. At least one required.")
    private List<ClusterCriteria> clusterCriterias = new ArrayList<>();

    @ApiModelProperty(
            value = "List of criteria containing tags to use to pick a command to run this job",
            required = true
    )
    @NotEmpty(message = "No command criteria entered. At least one required.")
    private Set<String> commandCriteria = new HashSet<>();

    @ApiModelProperty(value = "Group name of the user who submitted this job")
    @Length(max = 255, message = "Max length is 255 characters")
    private String group;

    @ApiModelProperty(value = "Location of a file which is sourced before job is run where properties can be set")
    private String setupFile;

    @ApiModelProperty(value = "Dependent files for this job to run. Will be downloaded before job starts")
    private Set<String> fileDependencies = new HashSet<>();

    @ApiModelProperty(
            value = "Boolean variable to decide whether job should be archived after it finishes defaults to true"
    )
    private boolean disableLogArchival;

    @ApiModelProperty(value = "Email address to send notifications to on job completion")
    @Length(max = 255, message = "Max length is 255 characters")
    @Email(message = "Must be a valid email address")
    private String email;

    /**
     * Constructor used by the builder build() method.
     *
     * @param builder The builder to use
     */
    protected JobRequest(final Builder builder) {
        super(builder);
        this.commandArgs = builder.bCommandArgs;
        if (builder.bClusterCriterias != null) {
            this.clusterCriterias.addAll(builder.bClusterCriterias);
        }
        if (builder.bCommandCriteria != null) {
            this.commandCriteria.addAll(builder.bCommandCriteria);
        }
        this.group = builder.bGroup;
        this.setupFile = builder.bSetupFile;
        if (builder.bFileDependencies != null) {
            this.fileDependencies.addAll(builder.bFileDependencies);
        }
        this.disableLogArchival = builder.bDisableLogArchival;
        this.email = builder.bEmail;
    }

    /**
     * Get the command arguments for this job.
     *
     * @return The command arguments
     */
    public String getCommandArgs() {
        return this.commandArgs;
    }

    /**
     * Get the list of cluster criterias.
     *
     * @return Read-only version of the cluster criterias. Attempts to modify will throw exception
     */
    public List<ClusterCriteria> getClusterCriterias() {
        return Collections.unmodifiableList(this.clusterCriterias);
    }

    /**
     * Get the set of command criteria.
     *
     * @return Read-only version of the command criteria. Attempts to modify will throw exception
     */
    public Set<String> getCommandCriteria() {
        return Collections.unmodifiableSet(this.commandCriteria);
    }

    /**
     * Get the group of the user running the job.
     *
     * @return The group
     */
    public String getGroup() {
        return this.group;
    }

    /**
     * Get the setup file that should be run before the job is executed.
     *
     * @return The setup file
     */
    public String getSetupFile() {
        return this.setupFile;
    }

    /**
     * Get the dependencies that should be downloaded for this job.
     *
     * @return The file dependencies as a read-only set. Attempts to modify will throw exception
     */
    public Set<String> getFileDependencies() {
        return Collections.unmodifiableSet(this.fileDependencies);
    }

    /**
     * Get whether to disable log archival or not.
     *
     * @return true if it should be disabled
     */
    public boolean getDisableLogArchival() {
        return this.disableLogArchival;
    }

    /**
     * Get the email to use for alerts.
     *
     * @return The email address
     */
    public String getEmail() {
        return this.email;
    }

    /**
     * A builder to create job requests.
     *
     * @param <T> The type of builder that extends this builder for final implementation
     * @author tgianos
     * @since 3.0.0
     */
    public static class Builder<T extends Builder> extends BaseDTO.Builder<T> {

        private final String bCommandArgs;
        private final List<ClusterCriteria> bClusterCriterias = new ArrayList<>();
        private final Set<String> bCommandCriteria = new HashSet<>();
        private String bGroup;
        private String bSetupFile;
        private Set<String> bFileDependencies = new HashSet<>();
        private boolean bDisableLogArchival;
        private String bEmail;

        /**
         * Constructor which has required fields.
         *
         * @param name             The name to use for the Job
         * @param user             The user to use for the Job
         * @param version          The version to use for the Job
         * @param commandArgs      The command line arguments for the Job
         * @param clusterCriterias The list of cluster criteria for the Job
         * @param commandCriteria  The list of command criteria for the Job
         */
        public Builder(
                @JsonProperty("name")
                final String name,
                @JsonProperty("user")
                final String user,
                @JsonProperty("version")
                final String version,
                @JsonProperty("commandArgs")
                final String commandArgs,
                @JsonProperty("clusterCriterias")
                final List<ClusterCriteria> clusterCriterias,
                @JsonProperty("commandCriteria")
                final Set<String> commandCriteria
        ) {
            super(name, user, version);
            this.bCommandArgs = commandArgs;
            if (clusterCriterias != null) {
                this.bClusterCriterias.addAll(clusterCriterias);
            }
            if (commandCriteria != null) {
                this.bCommandCriteria.addAll(commandCriteria);
            }
        }

        /**
         * Set the group for the job.
         *
         * @param group The group
         * @return The builder
         */
        public T withGroup(final String group) {
            this.bGroup = group;
            return (T) this;
        }

        /**
         * Set the setup file to execute before running the job.
         *
         * @param setupFile The setup file to use
         * @return The builder
         */
        public T withSetupFile(final String setupFile) {
            this.bSetupFile = setupFile;
            return (T) this;
        }

        /**
         * Set the file dependencies needed to run the job.
         *
         * @param fileDependencies The file dependencies
         * @return The builder
         */
        public T withFileDependencies(final Set<String> fileDependencies) {
            if (fileDependencies != null) {
                this.bFileDependencies.addAll(fileDependencies);
            }
            return (T) this;
        }

        /**
         * Set whether to disable log archive for the job.
         *
         * @param disableLogArchival true if you want to disable log archival
         * @return The builder
         */
        public T withDisableLogArchival(final boolean disableLogArchival) {
            this.bDisableLogArchival = disableLogArchival;
            return (T) this;
        }

        /**
         * Set the email to use for alerting of job completion. If no alert desired leave blank.
         *
         * @param email the email address to use
         * @return The builder
         */
        public T withEmail(final String email) {
            this.bEmail = email;
            return (T) this;
        }

        /**
         * Build the job request.
         *
         * @return Create the final read-only JobRequest instance
         */
        public JobRequest build() {
            return new JobRequest(this);
        }
    }
}
