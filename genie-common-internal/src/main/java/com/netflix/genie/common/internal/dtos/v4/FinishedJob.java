/*
 *
 *  Copyright 2019 Netflix, Inc.
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
package com.netflix.genie.common.internal.dtos.v4;

import com.google.common.collect.ImmutableList;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.external.dtos.v4.Criterion;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieInvalidStatusException;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.Size;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * DTO for a job that reached a final state.
 *
 * @author mprimi
 * @since 4.0.0
 */
//@Getter
@ToString(callSuper = true, doNotUseGetters = true)
@EqualsAndHashCode(callSuper = true, doNotUseGetters = true)
@SuppressWarnings("checkstyle:finalclass")
public class FinishedJob extends CommonMetadata {

    @Getter
    @Size(max = 255, message = "Max length for the ID is 255 characters")
    private final String uniqueId;
    @Getter
    private final Instant created;
    @Getter
    private final JobStatus status;
    @Getter
    private final List<String> commandArgs;
    @Getter
    @Valid
    private final Criterion commandCriterion;
    @Getter
    private final List<@Valid Criterion> clusterCriteria;
    private final Instant started;
    private final Instant finished;
    private final String grouping;
    private final String groupingInstance;
    private final String statusMessage;
    private final Integer requestedMemory;
    private final String requestApiClientHostname;
    private final String requestApiClientUserAgent;
    private final String requestAgentClientHostname;
    private final String requestAgentClientVersion;
    private final Integer numAttachments;
    private final Integer exitCode;
    private final String archiveLocation;
    private final Integer memoryUsed;
    private final Command command;
    private final Cluster cluster;
    @Getter
    private final List<Application> applications;

    /**
     * Constructor.
     *
     * @param builder The builder containing the values to use.
     */
    FinishedJob(final Builder builder) {
        super(builder);
        this.uniqueId = builder.bUniqueId;
        this.created = builder.bCreated;
        this.status = builder.bStatus;
        this.commandArgs = builder.bCommandArgs;
        this.commandCriterion = builder.bCommandCriterion;
        this.clusterCriteria = builder.bClusterCriteria;
        this.started = builder.bStarted;
        this.finished = builder.bFinished;
        this.grouping = builder.bGrouping;
        this.groupingInstance = builder.bGroupingInstance;
        this.statusMessage = builder.bStatusMessage;
        this.requestedMemory = builder.bRequestedMemory;
        this.requestApiClientHostname = builder.bRequestApiClientHostname;
        this.requestApiClientUserAgent = builder.bRequestApiClientUserAgent;
        this.requestAgentClientHostname = builder.bRequestAgentClientHostname;
        this.requestAgentClientVersion = builder.bRequestAgentClientVersion;
        this.numAttachments = builder.bNumAttachments;
        this.exitCode = builder.bExitCode;
        this.archiveLocation = builder.bArchiveLocation;
        this.memoryUsed = builder.bMemoryUsed;
        this.command = builder.bCommand;
        this.cluster = builder.bCluster;
        this.applications = builder.bApplications;
    }

    /**
     * Getter for optional field.
     *
     * @return the job start timestamp, if present
     */
    public Optional<Instant> getStarted() {
        return Optional.ofNullable(started);
    }

    /**
     * Getter for optional field.
     *
     * @return the job completion timestamp, if present
     */
    public Optional<Instant> getFinished() {
        return Optional.ofNullable(finished);
    }

    /**
     * Getter for optional field.
     *
     * @return the job grouping, if present
     */
    public Optional<String> getGrouping() {
        return Optional.ofNullable(grouping);
    }

    /**
     * Getter for optional field.
     *
     * @return the job grouping instance, if present
     */
    public Optional<String> getGroupingInstance() {
        return Optional.ofNullable(groupingInstance);
    }

    /**
     * Getter for optional field.
     *
     * @return the job final status message, if present
     */
    public Optional<String> getStatusMessage() {
        return Optional.ofNullable(statusMessage);
    }

    /**
     * Getter for optional field.
     *
     * @return the job requested memory, if present
     */
    public Optional<Integer> getRequestedMemory() {
        return Optional.ofNullable(requestedMemory);
    }

    /**
     * Getter for optional field.
     *
     * @return the requesting client hostname, if present
     */
    public Optional<String> getRequestApiClientHostname() {
        return Optional.ofNullable(requestApiClientHostname);
    }

    /**
     * Getter for optional field.
     *
     * @return the requesting client user-agent, if present
     */
    public Optional<String> getRequestApiClientUserAgent() {
        return Optional.ofNullable(requestApiClientUserAgent);
    }

    /**
     * Getter for optional field.
     *
     * @return the executing agent hostname, if present
     */
    public Optional<String> getRequestAgentClientHostname() {
        return Optional.ofNullable(requestAgentClientHostname);
    }

    /**
     * Getter for optional field.
     *
     * @return the executing agent version, if present
     */
    public Optional<String> getRequestAgentClientVersion() {
        return Optional.ofNullable(requestAgentClientVersion);
    }

    /**
     * Getter for optional field.
     *
     * @return the number of job attachments, if present
     */
    public Optional<Integer> getNumAttachments() {
        return Optional.ofNullable(numAttachments);
    }

    /**
     * Getter for optional field.
     *
     * @return the job process exit code, if present
     */
    public Optional<Integer> getExitCode() {
        return Optional.ofNullable(exitCode);
    }

    /**
     * Getter for optional field.
     *
     * @return the job outputs archive location, if present
     */
    public Optional<String> getArchiveLocation() {
        return Optional.ofNullable(archiveLocation);
    }

    /**
     * Getter for optional field.
     *
     * @return the job used memory, if present
     */
    public Optional<Integer> getMemoryUsed() {
        return Optional.ofNullable(memoryUsed);
    }

    /**
     * Getter for optional field.
     *
     * @return the job resolved command, if present
     */
    public Optional<Command> getCommand() {
        return Optional.ofNullable(command);
    }

    /**
     * Getter for optional field.
     *
     * @return the job resolved cluster, if present
     */
    public Optional<Cluster> getCluster() {
        return Optional.ofNullable(cluster);
    }

    /**
     * Builder.
     */
    public static class Builder extends CommonMetadata.Builder<Builder> {

        private final String bUniqueId;
        private final Instant bCreated;
        private final JobStatus bStatus;
        private final List<String> bCommandArgs;
        private final Criterion bCommandCriterion;
        private final List<Criterion> bClusterCriteria;
        private Instant bStarted;
        private Instant bFinished;
        private String bGrouping;
        private String bGroupingInstance;
        private String bStatusMessage;
        private Integer bRequestedMemory;
        private String bRequestApiClientHostname;
        private String bRequestApiClientUserAgent;
        private String bRequestAgentClientHostname;
        private String bRequestAgentClientVersion;
        private Integer bNumAttachments;
        private Integer bExitCode;
        private String bArchiveLocation;
        private Integer bMemoryUsed;
        private Command bCommand;
        private Cluster bCluster;
        private List<Application> bApplications;

        /**
         * Constructor with required fields.
         *
         * @param uniqueId         job id
         * @param name             job name
         * @param user             job user
         * @param version          job version
         * @param created          job creation timestamp
         * @param status           job status
         * @param commandArgs      job command arguments
         * @param commandCriterion job command criterion
         * @param clusterCriteria  job cluster criteria
         */
        public Builder(
            final String uniqueId,
            final String name,
            final String user,
            final String version,
            final Instant created,
            final JobStatus status,
            final List<String> commandArgs,
            final Criterion commandCriterion,
            final List<Criterion> clusterCriteria
        ) {
            super(name, user, version);
            this.bUniqueId = uniqueId;
            this.bCreated = created;
            this.bStatus = status;
            this.bCommandArgs = ImmutableList.copyOf(commandArgs);
            this.bCommandCriterion = commandCriterion;
            this.bClusterCriteria = ImmutableList.copyOf(clusterCriteria);
        }

        /**
         * Build the DTO.
         *
         * @return Create the final read-only FinishedJob instance.
         * @throws GenieInvalidStatusException if the status is not final
         */
        public FinishedJob build() {
            if (!this.bStatus.isFinished()) {
                throw new GenieInvalidStatusException("Status is not final: " + bStatus.name());
            }
            return new FinishedJob(this);
        }

        /**
         * Setter for optional field.
         *
         * @param started start timestamp
         * @return the builder
         */
        public Builder withStarted(@Nullable final Instant started) {
            this.bStarted = started;
            return this;
        }

        /**
         * Setter for optional field.
         *
         * @param finished the finish timestamp
         * @return the builder
         */
        public Builder withFinished(@Nullable final Instant finished) {
            this.bFinished = finished;
            return this;
        }

        /**
         * Setter for optional field.
         *
         * @param grouping the job grouping
         * @return the builder
         */
        public Builder withGrouping(@Nullable final String grouping) {
            this.bGrouping = grouping;
            return this;
        }

        /**
         * Setter for optional field.
         *
         * @param groupingInstance the job grouping instance
         * @return the builder
         */
        public Builder withGroupingInstance(@Nullable final String groupingInstance) {
            this.bGroupingInstance = groupingInstance;
            return this;
        }

        /**
         * Setter for optional field.
         *
         * @param statusMessage the job final status message
         * @return the builder
         */
        public Builder withStatusMessage(@Nullable final String statusMessage) {
            this.bStatusMessage = statusMessage;
            return this;
        }

        /**
         * Setter for optional field.
         *
         * @param requestedMemory the amount of memory requested
         * @return the builder
         */
        public Builder withRequestedMemory(@Nullable final Integer requestedMemory) {
            this.bRequestedMemory = requestedMemory;
            return this;
        }

        /**
         * Setter for optional field.
         *
         * @param requestApiClientHostname the hostname of the client submitting the job via API
         * @return the builder
         */
        public Builder withRequestApiClientHostname(@Nullable final String requestApiClientHostname) {
            this.bRequestApiClientHostname = requestApiClientHostname;
            return this;
        }

        /**
         * Setter for optional field.
         *
         * @param requestApiClientUserAgent the user-agent string of the client submitting the job via API
         * @return the builder
         */
        public Builder withRequestApiClientUserAgent(@Nullable final String requestApiClientUserAgent) {
            this.bRequestApiClientUserAgent = requestApiClientUserAgent;
            return this;
        }

        /**
         * Setter for optional field.
         *
         * @param requestAgentClientHostname the hostname where the agent executing the job is running
         * @return the builder
         */
        public Builder withRequestAgentClientHostname(@Nullable final String requestAgentClientHostname) {
            this.bRequestAgentClientHostname = requestAgentClientHostname;
            return this;
        }

        /**
         * Setter for optional field.
         *
         * @param requestAgentClientVersion the version of the agent executing the job
         * @return the builder
         */
        public Builder withRequestAgentClientVersion(@Nullable final String requestAgentClientVersion) {
            this.bRequestAgentClientVersion = requestAgentClientVersion;
            return this;
        }

        /**
         * Setter for optional field.
         *
         * @param numAttachments the number of attachments
         * @return the builder
         */
        public Builder withNumAttachments(@Nullable final Integer numAttachments) {
            this.bNumAttachments = numAttachments;
            return this;
        }

        /**
         * Setter for optional field.
         *
         * @param exitCode the exit code
         * @return the builder
         */
        public Builder withExitCode(@Nullable final Integer exitCode) {
            this.bExitCode = exitCode;
            return this;
        }

        /**
         * Setter for optional field.
         *
         * @param archiveLocation the archive location
         * @return the builder
         */
        public Builder withArchiveLocation(@Nullable final String archiveLocation) {
            this.bArchiveLocation = archiveLocation;
            return this;
        }

        /**
         * Setter for optional field.
         *
         * @param memoryUsed the memory allocated to the job
         * @return the builder
         */
        public Builder withMemoryUsed(@Nullable final Integer memoryUsed) {
            this.bMemoryUsed = memoryUsed;
            return this;
        }

        /**
         * Setter for optional field.
         *
         * @param command the resolved command for this job
         * @return the builder
         */
        public Builder withCommand(@Nullable final Command command) {
            this.bCommand = command;
            return this;
        }

        /**
         * Setter for optional field.
         *
         * @param cluster the resolved cluster for this job
         * @return the builder
         */
        public Builder withCluster(@Nullable final Cluster cluster) {
            this.bCluster = cluster;
            return this;
        }

        /**
         * Setter for optional field.
         *
         * @param applications the resolved list of applications for this command
         * @return the builder
         */
        public Builder withApplications(@Nullable final List<Application> applications) {
            this.bApplications = applications == null ? ImmutableList.of() : ImmutableList.copyOf(applications);
            return this;
        }
    }
}
