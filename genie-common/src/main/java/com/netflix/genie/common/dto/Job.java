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
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.netflix.genie.common.util.JsonDateDeserializer;
import com.netflix.genie.common.util.JsonDateSerializer;
import com.netflix.genie.common.util.TimeUtils;
import lombok.Getter;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.time.Duration;
import java.util.Date;

/**
 * Read only data transfer object representing a Job in the Genie system.
 *
 * @author tgianos
 * @since 3.0.0
 */
@JsonDeserialize(builder = Job.Builder.class)
@Getter
public class Job extends CommonDTO {

    private static final long serialVersionUID = -4218933066048954819L;

    @Size(min = 1, max = 10000, message = "Command arguments are required and max at 10000 characters")
    private String commandArgs;
    @NotNull
    private final JobStatus status;
    @Size(max = 255, message = "Max length is 255 characters")
    private final String statusMsg;
    @JsonSerialize(using = JsonDateSerializer.class)
    private final Date started;
    @JsonSerialize(using = JsonDateSerializer.class)
    private final Date finished;
    @Size(max = 1024, message = "Max character length is 1024 characters")
    private final String archiveLocation;
    @Size(max = 255, message = "Max character length is 255 characters")
    private final String clusterName;
    @Size(max = 255, message = "Max character length is 255 characters")
    private final String commandName;
    @NotNull
    @JsonSerialize(using = ToStringSerializer.class)
    private final Duration runtime;

    /**
     * Constructor used by the builder.
     *
     * @param builder The builder to use
     */
    protected Job(final Builder builder) {
        super(builder);
        this.commandArgs = builder.bCommandArgs;
        this.status = builder.bStatus;
        this.statusMsg = builder.bStatusMsg;
        this.started = builder.bStarted == null ? null : new Date(builder.bStarted.getTime());
        this.finished = builder.bFinished == null ? null : new Date(builder.bFinished.getTime());
        this.archiveLocation = builder.bArchiveLocation;
        this.clusterName = builder.bClusterName;
        this.commandName = builder.bCommandName;

        this.runtime = TimeUtils.getDuration(this.started, this.finished);
    }

    /**
     * Get the time the job started.
     *
     * @return The started time or null if not set
     */
    public Date getStarted() {
        return this.started == null ? null : new Date(this.started.getTime());
    }

    /**
     * Get the time the job finished.
     *
     * @return The finished time or null if not set
     */
    public Date getFinished() {
        return this.finished == null ? null : new Date(this.finished.getTime());
    }

    /**
     * A builder to create jobs.
     *
     * @author tgianos
     * @since 3.0.0
     */
    public static class Builder extends CommonDTO.Builder<Builder> {

        private final String bCommandArgs;
        private JobStatus bStatus = JobStatus.INIT;
        private String bStatusMsg;
        @JsonDeserialize(using = JsonDateDeserializer.class)
        private Date bStarted;
        @JsonDeserialize(using = JsonDateDeserializer.class)
        private Date bFinished;
        private String bArchiveLocation;
        private String bClusterName;
        private String bCommandName;

        /**
         * Constructor which has required fields.
         *
         * @param name        The name to use for the Job
         * @param user        The user to use for the Job
         * @param version     The version to use for the Job
         * @param commandArgs The command arguments used for this job
         */
        public Builder(
            @JsonProperty("name")
            final String name,
            @JsonProperty("user")
            final String user,
            @JsonProperty("version")
            final String version,
            @JsonProperty("commandArgs")
            final String commandArgs
        ) {
            super(name, user, version);
            this.bCommandArgs = commandArgs;
        }

        /**
         * Set the execution cluster name for this job.
         *
         * @param clusterName The execution cluster name
         * @return The builder
         */
        public Builder withClusterName(final String clusterName) {
            this.bClusterName = clusterName;
            return this;
        }

        /**
         * Set the name of the command used to run this job.
         *
         * @param commandName The name of the command
         * @return The builder
         */
        public Builder withCommandName(final String commandName) {
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
            if (status != null) {
                this.bStatus = status;
            }
            return this;
        }

        /**
         * Set the detailed status message of the job.
         *
         * @param statusMsg The status message
         * @return The builder
         */
        public Builder withStatusMsg(final String statusMsg) {
            this.bStatusMsg = statusMsg;
            return this;
        }

        /**
         * Set the started time of the job.
         *
         * @param started The started time of the job
         * @return The builder
         */
        public Builder withStarted(final Date started) {
            if (started != null) {
                this.bStarted = new Date(started.getTime());
            }
            return this;
        }

        /**
         * Set the finished time of the job.
         *
         * @param finished The time the job finished
         * @return The builder
         */
        public Builder withFinished(final Date finished) {
            if (finished != null) {
                this.bFinished = new Date(finished.getTime());
            }
            return this;
        }

        /**
         * Set the archive location of the job.
         *
         * @param archiveLocation The location where the job results are archived
         * @return The builder
         */
        public Builder withArchiveLocation(final String archiveLocation) {
            this.bArchiveLocation = archiveLocation;
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
