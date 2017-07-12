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
import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.Email;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * All information needed to make a request to run a new job.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Getter
@JsonDeserialize(builder = JobRequest.Builder.class)
public class JobRequest extends SetupFileDTO {

    /**
     * The default number of seconds from start before a job times out.
     */
    public static final int DEFAULT_TIMEOUT_DURATION = 604800;

    private static final long serialVersionUID = 3163971970144435277L;

    @NotNull
    @Size(min = 1, max = 10000, message = "Command arguments are required and max at 10000 characters")
    private final String commandArgs;
    @Valid
    @NotEmpty(message = "At least one cluster criteria is required")
    private final List<ClusterCriteria> clusterCriterias = new ArrayList<>();
    @NotEmpty(message = "At least one valid (e.g. non-blank) command criteria is required")
    private final Set<String> commandCriteria = new HashSet<>();
    @Size(max = 255, message = "Max length of the group is 255 characters")
    private final String group;
    private final boolean disableLogArchival;
    @Size(max = 255, message = "Max length of the email 255 characters")
    @Email(message = "Must be a valid email address")
    private final String email;
    @Min(value = 1, message = "Must have at least 1 CPU")
    private final Integer cpu;
    @Min(value = 1, message = "Must have at least 1 MB of memory. Preferably much more.")
    private final Integer memory;
    @Min(value = 1, message = "The timeout must be at least 1 second, preferably much more.")
    private final Integer timeout;
    private final Set<String> dependencies = new HashSet<>();
    private final List<String> applications = new ArrayList<>();

    /**
     * Constructor used by the builder build() method.
     *
     * @param builder The builder to use
     */
    @SuppressWarnings("unchecked")
    protected JobRequest(@Valid final Builder builder) {
        super(builder);
        this.commandArgs = builder.bCommandArgs;
        this.clusterCriterias.addAll(builder.bClusterCriterias);
        this.commandCriteria.addAll(builder.bCommandCriteria);
        this.group = builder.bGroup;
        this.dependencies.addAll(builder.bDependencies);
        this.disableLogArchival = builder.bDisableLogArchival;
        this.email = builder.bEmail;
        this.cpu = builder.bCpu;
        this.memory = builder.bMemory;
        this.timeout = builder.bTimeout;
        this.applications.addAll(builder.bApplications);
    }

    /**
     * Get the group the user should be a member of.
     *
     * @return The group as an optional
     */
    public Optional<String> getGroup() {
        return Optional.ofNullable(this.group);
    }

    /**
     * Get the email for the user.
     *
     * @return The email address as an Optional
     */
    public Optional<String> getEmail() {
        return Optional.ofNullable(this.email);
    }

    /**
     * Get the number of CPU's requested to run this job.
     *
     * @return The number of CPU's as an Optional
     */
    public Optional<Integer> getCpu() {
        return Optional.ofNullable(this.cpu);
    }

    /**
     * Get the amount of memory (in MB) requested to run this job with.
     *
     * @return The amount of memory as an Optional
     */
    public Optional<Integer> getMemory() {
        return Optional.ofNullable(this.memory);
    }

    /**
     * Get the amount of time requested (in seconds) before this job is timed out on the server.
     *
     * @return The timeout as an Optional
     */
    public Optional<Integer> getTimeout() {
        return Optional.ofNullable(this.timeout);
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
     * Get the dependencies that should be downloaded for this job.
     *
     * @return The file dependencies as a read-only set or null if none. Attempts to modify will throw exception
     */
    public Set<String> getDependencies() {
        return Collections.unmodifiableSet(this.dependencies);
    }

    /**
     * Get the list of application id's this job is requesting to override the default applications with.
     *
     * @return The application ids as an read-only list. Attempts to modify with throw runtime exception.
     */
    public List<String> getApplications() {
        return Collections.unmodifiableList(this.applications);
    }

    /**
     * A builder to create job requests.
     *
     * @author tgianos
     * @since 3.0.0
     */
    public static class Builder extends SetupFileDTO.Builder<Builder> {

        private final String bCommandArgs;
        private final List<ClusterCriteria> bClusterCriterias = new ArrayList<>();
        private final Set<String> bCommandCriteria = new HashSet<>();
        private final Set<String> bDependencies = new HashSet<>();
        private final List<String> bApplications = new ArrayList<>();
        private String bGroup;
        private boolean bDisableLogArchival;
        private String bEmail;
        private Integer bCpu;
        private Integer bMemory;
        private Integer bTimeout;

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
                commandCriteria.forEach(
                    criteria -> {
                        if (StringUtils.isNotBlank(criteria)) {
                            this.bCommandCriteria.add(criteria);
                        }
                    }
                );
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
         * Set the file dependencies needed to run the job.
         *
         * @param dependencies The file dependencies
         * @return The builder
         */
        public Builder withDependencies(final Set<String> dependencies) {
            this.bDependencies.clear();
            if (dependencies != null) {
                this.bDependencies.addAll(dependencies);
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
        public Builder withCpu(final Integer cpu) {
            this.bCpu = cpu;
            return this;
        }

        /**
         * Set the amount of memory being requested to run the job..
         *
         * @param memory The amount of memory in terms of MB's. Must be greater than 0.
         * @return The builder
         */
        public Builder withMemory(final Integer memory) {
            this.bMemory = memory;
            return this;
        }

        /**
         * Set the ids of applications to override the default applications from the command with.
         *
         * @param applications The ids of applications to override
         * @return The builder
         */
        public Builder withApplications(final List<String> applications) {
            this.bApplications.clear();
            if (applications != null) {
                this.bApplications.addAll(applications);
            }
            return this;
        }

        /**
         * Set the length of the job timeout in seconds after which Genie will kill the client process.
         *
         * @param timeout The timeout to use
         * @return The builder
         */
        public Builder withTimeout(final Integer timeout) {
            this.bTimeout = timeout;
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
