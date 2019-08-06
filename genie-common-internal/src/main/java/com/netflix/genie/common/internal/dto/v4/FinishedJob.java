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
package com.netflix.genie.common.internal.dto.v4;

import com.netflix.genie.common.dto.JobStatus;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * @author mprimi
 * @since 4.0.0
 */
@Getter
@ToString(callSuper = true, doNotUseGetters = true)
@EqualsAndHashCode(callSuper = true, doNotUseGetters = true)
@SuppressWarnings("checkstyle:finalclass")
public class FinishedJob extends CommonMetadata {

    private final String uniqueId;
    private final Instant created;
    private final JobStatus status;
    private final List<String> commandArgs;
    private final Criterion commandCriterion;
    private final List<Criterion> clusterCriteria;
    private final Optional<Instant> started;
    private final Optional<Instant> finished;
    private final Optional<String> grouping;
    private final Optional<String> groupingInstance;
    private final Optional<String> statusMessage;
    private final Optional<Integer> requestedMemory;
    private final Optional<String> requestApiClientHostname;
    private final Optional<String> requestApiClientUserAgent;
    private final Optional<String> requestAgentClientHostname;
    private final Optional<String> requestAgentClientVersion;
    private final Optional<Integer> numAttachments;
    private final Optional<Integer> exitCode;
    private final Optional<String> archiveLocation;
    private final Optional<Integer> memoryUsed;
    private final Optional<Command> command;
    private final Optional<Cluster> cluster;
    private final Optional<List<Application>> applications;

    /**
     * Constructor.
     *
     * @param builder The builder containing the values to use.
     */
    FinishedJob(final Builder builder) {
        super(builder);
        this.uniqueId = builder.uniqueId;
        this.created = builder.created;
        this.status = builder.status;
        this.commandArgs = builder.commandArgs;
        this.commandCriterion = builder.commandCriterion;
        this.clusterCriteria = builder.clusterCriteria;
        this.started = Optional.ofNullable(builder.started);
        this.finished = Optional.ofNullable(builder.finished);
        this.grouping = Optional.ofNullable(builder.grouping);
        this.groupingInstance = Optional.ofNullable(builder.groupingInstance);
        this.statusMessage = Optional.ofNullable(builder.statusMessage);
        this.requestedMemory = Optional.ofNullable(builder.requestedMemory);
        this.requestApiClientHostname = Optional.ofNullable(builder.requestApiClientHostname);
        this.requestApiClientUserAgent = Optional.ofNullable(builder.requestApiClientUserAgent);
        this.requestAgentClientHostname = Optional.ofNullable(builder.requestAgentClientHostname);
        this.requestAgentClientVersion = Optional.ofNullable(builder.requestAgentClientVersion);
        this.numAttachments = Optional.ofNullable(builder.numAttachments);
        this.exitCode = Optional.ofNullable(builder.exitCode);
        this.archiveLocation = Optional.ofNullable(builder.archiveLocation);
        this.memoryUsed = Optional.ofNullable(builder.memoryUsed);
        this.command = Optional.ofNullable(builder.command);
        this.cluster = Optional.ofNullable(builder.cluster);
        this.applications = Optional.ofNullable(builder.applications);
    }

    public static class Builder extends CommonMetadata.Builder<Builder> {

        private final String uniqueId;
        private final Instant created;
        private final JobStatus status;
        private final List<String> commandArgs;
        private final Criterion commandCriterion;
        private final List<Criterion> clusterCriteria;
        private Instant started;
        private Instant finished;
        private String grouping;
        private String groupingInstance;
        private String statusMessage;
        private Integer requestedMemory;
        private String requestApiClientHostname;
        private String requestApiClientUserAgent;
        private String requestAgentClientHostname;
        private String requestAgentClientVersion;
        private Integer numAttachments;
        private Integer exitCode;
        private String archiveLocation;
        private Integer memoryUsed;
        private Command command;
        private Cluster cluster;
        private List<Application> applications;

        /**
         * Constructor with required fields.
         *
         * @param name    The name of the resource
         * @param user    The user owning the resource
         * @param version The version of hte resource
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
            this.uniqueId = uniqueId;
            this.created = created;
            this.status = status;
            this.commandArgs = commandArgs;
            this.commandCriterion = commandCriterion;
            this.clusterCriteria = clusterCriteria;
        }

        /**
         * Build the DTO.
         *
         * @return Create the final read-only FinishedJob instance
         */
        public FinishedJob build() {
            if (!this.status.isFinished()) {
                throw new IllegalArgumentException("Status is not final: " + status.name());
            }
            return new FinishedJob(this);
        }

        /**
         * Setter for optional field.
         *
         * @param started start timestamp
         * @return the builder
         */
        public Builder withStarted(final Instant started) {
            this.started = started;
            return this;
        }

        /**
         * Setter for optional field.
         *
         * @param finished the finish timestamp
         * @return the builder
         */
        public Builder withFinished(final Instant finished) {
            this.finished = finished;
            return this;
        }

        /**
         * Setter for optional field.
         *
         * @param grouping the job grouping
         * @return the builder
         */
        public Builder withGrouping(final String grouping) {
            this.grouping = grouping;
            return this;
        }

        /**
         * Setter for optional field.
         *
         * @param groupingInstance the job grouping instance
         * @return the builder
         */
        public Builder withGroupingInstance(final String groupingInstance) {
            this.groupingInstance = groupingInstance;
            return this;
        }

        /**
         * Setter for optional field.
         *
         * @param statusMessage the job final status message
         * @return the builder
         */
        public Builder withStatusMessage(final String statusMessage) {
            this.statusMessage = statusMessage;
            return this;
        }

        /**
         * Setter for optional field.
         *
         * @param requestedMemory the amount of memory requested
         * @return the builder
         */
        public Builder withRequestedMemory(final Integer requestedMemory) {
            this.requestedMemory = requestedMemory;
            return this;
        }

        /**
         * Setter for optional field.
         *
         * @param requestApiClientHostname the hostname of the client submitting the job via API
         * @return the builder
         */
        public Builder withRequestApiClientHostname(final String requestApiClientHostname) {
            this.requestApiClientHostname = requestApiClientHostname;
            return this;
        }

        /**
         * Setter for optional field.
         *
         * @param requestApiClientUserAgent the user-agent string of the client submitting the job via API
         * @return the builder
         */
        public Builder withRequestApiClientUserAgent(final String requestApiClientUserAgent) {
            this.requestApiClientUserAgent = requestApiClientUserAgent;
            return this;
        }

        /**
         * Setter for optional field.
         *
         * @param requestAgentClientHostname the hostname where the agent executing the job is running
         * @return the builder
         */
        public Builder withRequestAgentClientHostname(final String requestAgentClientHostname) {
            this.requestAgentClientHostname = requestAgentClientHostname;
            return this;
        }

        /**
         * Setter for optional field.
         *
         * @param requestApiClientVersion the version of the agent executing the job
         * @return the builder
         */
        public Builder withRequestAgentClientVersion(final String requestAgentClientVersion) {
            this.requestAgentClientVersion = requestAgentClientVersion;
            return this;
        }

        /**
         * Setter for optional field.
         *
         * @param numAttachments the number of attachments
         * @return the builder
         */
        public Builder withNumAttachments(final Integer numAttachments) {
            this.numAttachments = numAttachments;
            return this;
        }

        /**
         * Setter for optional field.
         *
         * @param exitCode the exit code
         * @return the builder
         */
        public Builder withExitCode(final Integer exitCode) {
            this.exitCode = exitCode;
            return this;
        }

        /**
         * Setter for optional field.
         *
         * @param archiveLocation the archive location
         * @return the builder
         */
        public Builder withArchiveLocation(final String archiveLocation) {
            this.archiveLocation = archiveLocation;
            return this;
        }

        /**
         * Setter for optional field.
         *
         * @param memoryUsed the memory allocated to the job
         * @return the builder
         */
        public Builder withMemoryUsed(final Integer memoryUsed) {
            this.memoryUsed = memoryUsed;
            return this;
        }

        /**
         * Setter for optional field.
         *
         * @param command the resolved command for this job
         * @return the builder
         */
        public Builder withCommand(final Command command) {
            this.command = command;
            return this;
        }

        /**
         * Setter for optional field.
         *
         * @param cluster the resolved cluster for this job
         * @return the builder
         */
        public Builder withCluster(final Cluster cluster) {
            this.cluster = cluster;
            return this;
        }

        /**
         * Setter for optional field.
         *
         * @param applications the resolved list of applicatons for this command
         * @return the builder
         */
        public Builder withApplications(final List<Application> applications) {
            this.applications = applications;
            return this;
        }
    }
}
