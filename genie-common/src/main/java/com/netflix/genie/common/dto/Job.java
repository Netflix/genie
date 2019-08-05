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
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.netflix.genie.common.util.JsonUtils;
import com.netflix.genie.common.util.TimeUtils;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Read only data transfer object representing a Job in the Genie system.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Getter
@JsonDeserialize(builder = Job.Builder.class)
public class Job extends CommonDTO {

    private static final long serialVersionUID = -4218933066048954819L;
    @NotNull(message = "A valid job status is required")
    private final JobStatus status;
    @Size(max = 255, message = "Max length of the status message is 255 characters")
    private final String statusMsg;
    private final Instant started;
    private final Instant finished;
    @Size(max = 1024, message = "Max character length is 1024 characters for the archive location")
    private final String archiveLocation;
    @Size(max = 255, message = "Max character length is 255 characters for the cluster name")
    private final String clusterName;
    @Size(max = 255, message = "Max character length is 255 characters for the command name")
    private final String commandName;
    @NotNull
    @JsonSerialize(using = ToStringSerializer.class)
    private final Duration runtime;
    private final List<
        @Size(
            max = 10_000,
            message = "The maximum number of characters for a single command argument is 10,000"
        ) String> commandArgs;
    private final String grouping;
    private final String groupingInstance;

    // Used to prevent serializing the List multiple times per get request
    private final String commandArgsString;

    /**
     * Constructor used by the builder.
     *
     * @param builder The builder to use
     */
    protected Job(@Valid final Builder builder) {
        super(builder);
        this.commandArgs = ImmutableList.copyOf(builder.bCommandArgs);
        this.commandArgsString = JsonUtils.joinArguments(this.commandArgs);
        this.status = builder.bStatus;
        this.statusMsg = builder.bStatusMsg;
        this.started = builder.bStarted;
        this.finished = builder.bFinished;
        this.archiveLocation = builder.bArchiveLocation;
        this.clusterName = builder.bClusterName;
        this.commandName = builder.bCommandName;
        this.grouping = builder.bGrouping;
        this.groupingInstance = builder.bGroupingInstance;

        this.runtime = TimeUtils.getDuration(this.started, this.finished);
    }

    /**
     * Get the arguments to be put on the command line along with the command executable.
     *
     * @return The command arguments
     * @deprecated See {@link #getCommandArgsList()}
     */
    @JsonGetter("commandArgs")
    @Deprecated
    public Optional<String> getCommandArgsString() {
        return Optional.ofNullable(this.commandArgsString);
    }

    /**
     * Get the command arguments for this job as an immutable list.
     *
     * @return Any command args that were in the job request
     */
    @JsonIgnore
    public List<String> getCommandArgs() {
        return this.commandArgs;
    }

    /**
     * Get the current status message.
     *
     * @return The status message as an optional
     */
    public Optional<String> getStatusMsg() {
        return Optional.ofNullable(this.statusMsg);
    }

    /**
     * Get the archive location for the job if there is one.
     *
     * @return The archive location
     */
    public Optional<String> getArchiveLocation() {
        return Optional.ofNullable(this.archiveLocation);
    }

    /**
     * Get the name of the cluster running the job if there currently is one.
     *
     * @return The name of the cluster where the job is running
     */
    public Optional<String> getClusterName() {
        return Optional.ofNullable(this.clusterName);
    }

    /**
     * Get the name of the command running this job if there currently is one.
     *
     * @return The name of the command
     */
    public Optional<String> getCommandName() {
        return Optional.ofNullable(this.commandName);
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
     * Get the time the job started.
     *
     * @return The started time or null if not set
     */
    public Optional<Instant> getStarted() {
        return Optional.ofNullable(this.started);
    }

    /**
     * Get the time the job finished.
     *
     * @return The finished time or null if not set
     */
    public Optional<Instant> getFinished() {
        return Optional.ofNullable(this.finished);
    }

    /**
     * A builder to create jobs.
     *
     * @author tgianos
     * @since 3.0.0
     */
    public static class Builder extends CommonDTO.Builder<Builder> {

        private final List<String> bCommandArgs = Lists.newArrayList();
        private JobStatus bStatus = JobStatus.INIT;
        private String bStatusMsg;
        private Instant bStarted;
        private Instant bFinished;
        private String bArchiveLocation;
        private String bClusterName;
        private String bCommandName;
        private String bGrouping;
        private String bGroupingInstance;

        /**
         * Constructor which has required fields.
         *
         * @param name    The name to use for the Job
         * @param user    The user to use for the Job
         * @param version The version to use for the Job
         * @since 3.3.0
         */
        @JsonCreator
        public Builder(
            @JsonProperty("name") final String name,
            @JsonProperty("user") final String user,
            @JsonProperty("version") final String version
        ) {
            super(name, user, version);
        }

        /**
         * Constructor which has required fields.
         * <p>
         * Deprecated: Command args is optional. Use new constructor. Will be removed in 4.0.0
         *
         * @param name        The name to use for the Job
         * @param user        The user to use for the Job
         * @param version     The version to use for the Job
         * @param commandArgs The command arguments used for this job. Max length 10,000 characters
         * @see #Builder(String, String, String)
         */
        @Deprecated
        public Builder(
            final String name,
            final String user,
            final String version,
            @Nullable final String commandArgs
        ) {
            super(name, user, version);
            if (StringUtils.isNotBlank(commandArgs)) {
                this.bCommandArgs.addAll(JsonUtils.splitArguments(commandArgs));
            }
        }

        /**
         * The command arguments to use in conjunction with the command executable selected for this job.
         * <p>
         * DEPRECATED: This API will be removed in 4.0.0 in favor of the List based method for improved control over
         * escaping of arguments.
         *
         * @param commandArgs The command args. The max length is 10,000 characters
         * @return The builder
         * @since 3.3.0
         * @deprecated See {@link #withCommandArgs(List)}
         */
        @Deprecated
        public Builder withCommandArgs(@Nullable final String commandArgs) {
            this.bCommandArgs.clear();
            if (StringUtils.isNotBlank(commandArgs)) {
                this.bCommandArgs.addAll(JsonUtils.splitArguments(commandArgs));
            }
            return this;
        }

        /**
         * The command arguments to use in conjunction with the command executable selected for this job.
         *
         * @param commandArgs The command args. The maximum combined size of the command args plus 1 space character
         *                    between each argument must be less than or equal to 10,000 characters
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
         * Set the execution cluster name for this job.
         *
         * @param clusterName The execution cluster name
         * @return The builder
         */
        public Builder withClusterName(@Nullable final String clusterName) {
            this.bClusterName = clusterName;
            return this;
        }

        /**
         * Set the name of the command used to run this job.
         *
         * @param commandName The name of the command
         * @return The builder
         */
        public Builder withCommandName(@Nullable final String commandName) {
            this.bCommandName = commandName;
            return this;
        }

        /**
         * Set the status of the job.
         *
         * @param status The status
         * @return The builder
         * @see JobStatus
         */
        public Builder withStatus(final JobStatus status) {
            this.bStatus = status;
            return this;
        }

        /**
         * Set the detailed status message of the job.
         *
         * @param statusMsg The status message
         * @return The builder
         */
        public Builder withStatusMsg(@Nullable final String statusMsg) {
            this.bStatusMsg = statusMsg;
            return this;
        }

        /**
         * Set the started time of the job.
         *
         * @param started The started time of the job
         * @return The builder
         */
        public Builder withStarted(@Nullable final Instant started) {
            this.bStarted = started;
            return this;
        }

        /**
         * Set the finished time of the job.
         *
         * @param finished The time the job finished
         * @return The builder
         */
        public Builder withFinished(@Nullable final Instant finished) {
            this.bFinished = finished;
            return this;
        }

        /**
         * Set the archive location of the job.
         *
         * @param archiveLocation The location where the job results are archived
         * @return The builder
         */
        public Builder withArchiveLocation(@Nullable final String archiveLocation) {
            this.bArchiveLocation = archiveLocation;
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
         * Build the job.
         *
         * @return Create the final read-only Job instance
         */
        public Job build() {
            return new Job(this);
        }
    }
}
