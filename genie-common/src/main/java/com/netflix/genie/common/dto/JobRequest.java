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
import org.hibernate.validator.constraints.Email;

import javax.validation.constraints.Min;
import javax.validation.constraints.Size;
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
public class JobRequest extends CommonDTO {

    @Size(min = 1, max = 1024, message = "Command arguments are required but no longer than 1024 characters")
    private String commandArgs;

    @Size(min = 1, message = "No cluster criteria entered. At least one required.")
    private List<ClusterCriteria> clusterCriterias = new ArrayList<>();

    @Size(min = 1, message = "No command criteria entered. At least one required.")
    private Set<String> commandCriteria = new HashSet<>();

    @Size(max = 255, message = "Max length is 255 characters")
    private String group;

    @Size(max = 1024, message = "Max length is 1024 characters")
    private String setupFile;

    private Set<String> fileDependencies = new HashSet<>();

    private boolean disableLogArchival;

    @Size(max = 255, message = "Max length is 255 characters")
    @Email(message = "Must be a valid email address")
    private String email;

    @Min(value = 1, message = "Minimum value for the number of cpu's is 1")
    private int cpu;

    @Min(value = 1, message = "Minimum value for the amount of memory is 1 MB")
    private int memory;

    /**
     * Constructor used by the builder build() method.
     *
     * @param builder The builder to use
     */
    @SuppressWarnings("unchecked")
    protected JobRequest(final Builder builder) {
        super(builder);
        this.commandArgs = builder.bCommandArgs;
        this.clusterCriterias.addAll(builder.bClusterCriterias);
        this.commandCriteria.addAll(builder.bCommandCriteria);
        this.group = builder.bGroup;
        this.setupFile = builder.bSetupFile;
        this.fileDependencies.addAll(builder.bFileDependencies);
        this.disableLogArchival = builder.bDisableLogArchival;
        this.email = builder.bEmail;
        this.cpu = builder.bCpu;
        this.memory = builder.bMemory;
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
     * Get the email to use for alerts.
     *
     * @return The email address
     */
    public String getEmail() {
        return this.email;
    }

    /**
     * Get whether log archival should be disabled or not.
     *
     * @return true if archival should be disabled
     */
    public boolean isDisableLogArchival() {
        return this.disableLogArchival;
    }

    /**
     * Get how many CPU's should be assigned to this job. Defaults to 1.
     *
     * @return The number of CPU's the user is requesting for this job
     */
    public int getCpu() {
        return this.cpu;
    }

    /**
     * Get the amount of memory the user is requesting for the job. Defaults to 1.5 GB (1536 MB).
     *
     * @return The amount of memory the user is requesting in MB's.
     */
    public int getMemory() {
        return this.memory;
    }

    /**
     * A builder to create job requests.
     *
     * @author tgianos
     * @since 3.0.0
     */
    public static class Builder extends CommonDTO.Builder<Builder> {

        private final String bCommandArgs;
        private final List<ClusterCriteria> bClusterCriterias = new ArrayList<>();
        private final Set<String> bCommandCriteria = new HashSet<>();
        private String bGroup;
        private String bSetupFile;
        private Set<String> bFileDependencies = new HashSet<>();
        private boolean bDisableLogArchival;
        private String bEmail;
        private int bCpu = 1;
        private int bMemory = 1536;

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
        public Builder withGroup(final String group) {
            this.bGroup = group;
            return this;
        }

        /**
         * Set the setup file to execute before running the job.
         *
         * @param setupFile The setup file to use
         * @return The builder
         */
        public Builder withSetupFile(final String setupFile) {
            this.bSetupFile = setupFile;
            return this;
        }

        /**
         * Set the file dependencies needed to run the job.
         *
         * @param fileDependencies The file dependencies
         * @return The builder
         */
        public Builder withFileDependencies(final Set<String> fileDependencies) {
            if (fileDependencies != null) {
                this.bFileDependencies.addAll(fileDependencies);
            }
            return this;
        }

        /**
         * Set whether to disable log archive for the job.
         *
         * @param disableLogArchival true if you want to disable log archival
         * @return The builder
         */
        public Builder withDisableLogArchival(final boolean disableLogArchival) {
            this.bDisableLogArchival = disableLogArchival;
            return this;
        }

        /**
         * Set the email to use for alerting of job completion. If no alert desired leave blank.
         *
         * @param email the email address to use
         * @return The builder
         */
        public Builder withEmail(final String email) {
            this.bEmail = email;
            return this;
        }

        /**
         * Set the number of cpu's being requested to run the job. Defaults to 1 if not set.
         *
         * @param cpu The number of cpu's. Must be greater than 0.
         * @return The builder
         */
        public Builder withCpu(@Min(1) final int cpu) {
            this.bCpu = cpu;
            return this;
        }

        /**
         * Set the amount of memory being requested to run the job. Defaults to 1536 MB if not set.
         *
         * @param memory The amount of memory in terms of MB's. Must be greater than     0.
         * @return The builder
         */
        public Builder withMemory(@Min(1) final int memory) {
            this.bMemory = memory;
            return this;
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
