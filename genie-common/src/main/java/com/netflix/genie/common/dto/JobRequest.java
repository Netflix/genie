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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.Email;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.Size;
import java.util.ArrayList;
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
public class JobRequest extends ExecutionEnvironmentDTO {

    /**
     * The default number of seconds from start before a job times out.
     */
    public static final int DEFAULT_TIMEOUT_DURATION = 604800;

    private static final long serialVersionUID = 3163971970144435277L;

    private final String commandArgs;
    @Valid
    @NotEmpty(message = "At least one cluster criteria is required")
    private final List<ClusterCriteria> clusterCriterias;
    @NotEmpty(message = "At least one valid (e.g. non-blank) command criteria is required")
    private final Set<String> commandCriteria;
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
    private final List<String> applications;
    private String grouping;
    private String groupingInstance;

    /**
     * Constructor used by the builder build() method.
     *
     * @param builder The builder to use
     */
    @SuppressWarnings("unchecked")
    JobRequest(@Valid final Builder builder) {
        super(builder);
        this.commandArgs = builder.bCommandArgs.isEmpty()
            ? null
            : StringUtils.join(builder.bCommandArgs, StringUtils.SPACE);
        this.clusterCriterias = ImmutableList.copyOf(builder.bClusterCriterias);
        this.commandCriteria = ImmutableSet.copyOf(builder.bCommandCriteria);
        this.group = builder.bGroup;
        this.disableLogArchival = builder.bDisableLogArchival;
        this.email = builder.bEmail;
        this.cpu = builder.bCpu;
        this.memory = builder.bMemory;
        this.timeout = builder.bTimeout;
        this.applications = ImmutableList.copyOf(builder.bApplications);
        this.grouping = builder.bGrouping;
        this.groupingInstance = builder.bGroupingInstance;
    }

    /**
     * Get the arguments to be put on the command line along with the command executable.
     *
     * @return The command arguments
     */
    public Optional<String> getCommandArgs() {
        return Optional.ofNullable(this.commandArgs);
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
     * Get the grouping for this job if there currently is one.
     *
     * @return The grouping
     * @since 3.3.0
     */
    public Optional<String> getGrouping() {
        return Optional.ofNullable(this.grouping);
    }

    /**
     * Get the grouping instance for this job if there currently is one.
     *
     * @return The grouping instance
     * @since 3.3.0
     */
    public Optional<String> getGroupingInstance() {
        return Optional.ofNullable(this.groupingInstance);
    }

    /**
     * A builder to create job requests.
     *
     * @author tgianos
     * @since 3.0.0
     */
    public static class Builder extends ExecutionEnvironmentDTO.Builder<Builder> {

        private final List<String> bCommandArgs;
        private final List<ClusterCriteria> bClusterCriterias = new ArrayList<>();
        private final Set<String> bCommandCriteria = new HashSet<>();
        private final List<String> bApplications = new ArrayList<>();
        private String bGroup;
        private boolean bDisableLogArchival;
        private String bEmail;
        private Integer bCpu;
        private Integer bMemory;
        private Integer bTimeout;
        private String bGrouping;
        private String bGroupingInstance;

        /**
         * Constructor which has required fields.
         *
         * @param name             The name to use for the Job
         * @param user             The user to use for the Job
         * @param version          The version to use for the Job
         * @param clusterCriterias The list of cluster criteria for the Job
         * @param commandCriteria  The list of command criteria for the Job
         * @since 3.3.0
         */
        @JsonCreator
        public Builder(
            @JsonProperty("name") final String name,
            @JsonProperty("user") final String user,
            @JsonProperty("version") final String version,
            @JsonProperty("clusterCriterias") final List<ClusterCriteria> clusterCriterias,
            @JsonProperty("commandCriteria") final Set<String> commandCriteria
        ) {
            super(name, user, version);
            this.bCommandArgs = Lists.newArrayList();
            this.bClusterCriterias.addAll(clusterCriterias);
            commandCriteria.forEach(
                criteria -> {
                    if (StringUtils.isNotBlank(criteria)) {
                        this.bCommandCriteria.add(criteria);
                    }
                }
            );
        }

        /**
         * Constructor which has required fields.
         * <p>
         * DEPRECATED: Will be removed in 4.0.0 as command args are optional and should be a List now
         *
         * @param name             The name to use for the Job
         * @param user             The user to use for the Job
         * @param version          The version to use for the Job
         * @param commandArgs      The command line arguments for the Job
         * @param clusterCriterias The list of cluster criteria for the Job
         * @param commandCriteria  The list of command criteria for the Job
         * @see #Builder(String, String, String, List, Set)
         */
        @Deprecated
        public Builder(
            final String name,
            final String user,
            final String version,
            @Nullable final String commandArgs,
            final List<ClusterCriteria> clusterCriterias,
            final Set<String> commandCriteria
        ) {
            super(name, user, version);
            this.bCommandArgs = commandArgs == null
                ? Lists.newArrayList()
                : Lists.newArrayList(StringUtils.splitByWholeSeparator(commandArgs, StringUtils.SPACE));
            this.bClusterCriterias.addAll(clusterCriterias);
            commandCriteria.forEach(
                criteria -> {
                    if (StringUtils.isNotBlank(criteria)) {
                        this.bCommandCriteria.add(criteria);
                    }
                }
            );
        }

        /**
         * The command arguments to use in conjunction with the command executable selected for this job.
         * <p>
         * DEPRECATED: This API will be removed in 4.0.0 in favor of the List based method for improved control over
         * escaping of arguments.
         *
         * @param commandArgs The command args
         * @return The builder
         * @see #withCommandArgs(List)
         * @since 3.3.0
         */
        @Deprecated
        public Builder withCommandArgs(@Nullable final String commandArgs) {
            this.bCommandArgs.clear();
            if (commandArgs != null) {
                this.bCommandArgs.addAll(
                    Lists.newArrayList(StringUtils.splitByWholeSeparator(commandArgs, StringUtils.SPACE))
                );
            }
            return this;
        }

        /**
         * The command arguments to use in conjunction with the command executable selected for this job.
         *
         * @param commandArgs The command args
         * @return The builder
         * @since 3.3.0
         */
        public Builder withCommandArgs(@Nullable final List<String> commandArgs) {
            this.bCommandArgs.clear();
            if (commandArgs != null) {
                this.bCommandArgs.addAll(commandArgs);
            }
            return this;
        }

        /**
         * Set the group for the job.
         *
         * @param group The group
         * @return The builder
         */
        public Builder withGroup(@Nullable final String group) {
            this.bGroup = group;
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
        public Builder withEmail(@Nullable final String email) {
            this.bEmail = email;
            return this;
        }

        /**
         * Set the number of cpu's being requested to run the job. Defaults to 1 if not set.
         *
         * @param cpu The number of cpu's. Must be greater than 0.
         * @return The builder
         */
        public Builder withCpu(@Nullable final Integer cpu) {
            this.bCpu = cpu;
            return this;
        }

        /**
         * Set the amount of memory being requested to run the job..
         *
         * @param memory The amount of memory in terms of MB's. Must be greater than 0.
         * @return The builder
         */
        public Builder withMemory(@Nullable final Integer memory) {
            this.bMemory = memory;
            return this;
        }

        /**
         * Set the ids of applications to override the default applications from the command with.
         *
         * @param applications The ids of applications to override
         * @return The builder
         */
        public Builder withApplications(@Nullable final List<String> applications) {
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
        public Builder withTimeout(@Nullable final Integer timeout) {
            this.bTimeout = timeout;
            return this;
        }

        /**
         * Set the grouping to use for this job.
         *
         * @param grouping The grouping
         * @return The builder
         * @since 3.3.0
         */
        public Builder withGrouping(@Nullable final String grouping) {
            this.bGrouping = grouping;
            return this;
        }

        /**
         * Set the grouping instance to use for this job.
         *
         * @param groupingInstance The grouping instance
         * @return The builder
         * @since 3.3.0
         */
        public Builder withGroupingInstance(@Nullable final String groupingInstance) {
            this.bGroupingInstance = groupingInstance;
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
