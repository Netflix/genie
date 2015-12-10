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
import com.netflix.genie.common.util.JsonDateDeserializer;
import com.netflix.genie.common.util.JsonDateSerializer;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.Date;

/**
 * Read only data transfer object representing a Job in the Genie system.
 *
 * @author tgianos
 * @since 3.0.0
 */
@JsonDeserialize(builder = Job.Builder.class)
public class Job extends CommonDTO {

    @NotNull(message = "A job must have a status")
    private JobStatus status;

    @Size(max = 255, message = "Max length is 255 characters")
    private String statusMsg;

    @JsonSerialize(using = JsonDateSerializer.class)
    private Date started;

    @JsonSerialize(using = JsonDateSerializer.class)
    private Date finished;

    @Size(max = 1024, message = "Max character length is 1024 characters")
    private String archiveLocation;

    @Size(max = 255, message = "Max character length is 255 characters")
    private String clusterName;

    @Size(max = 255, message = "Max character length is 255 characters")
    private String commandName;

    /**
     * Constructor used by the builder.
     *
     * @param builder The builder to use
     */
    protected Job(final Builder builder) {
        super(builder);
        this.status = builder.bStatus;
        this.statusMsg = builder.bStatusMsg;
        if (builder.bStarted != null) {
            this.started = new Date(builder.bStarted.getTime());
        }
        if (builder.bFinished != null) {
            this.finished = new Date(builder.bFinished.getTime());
        }
        this.archiveLocation = builder.bArchiveLocation;
        this.clusterName = builder.bClusterName;
        this.commandName = builder.bCommandName;
    }

    /**
     * Get the status of the job.
     *
     * @return The status
     */
    public JobStatus getStatus() {
        return this.status;
    }

    /**
     * Get the detailed status message for the job.
     *
     * @return The detailed status message
     */
    public String getStatusMsg() {
        return statusMsg;
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
     * Get the location where the jobs' results were archived to.
     *
     * @return The archive location
     */
    public String getArchiveLocation() {
        return archiveLocation;
    }

    /**
     * Get the name of the cluster this job executed on.
     *
     * @return The name
     */
    public String getClusterName() {
        return this.clusterName;
    }

    /**
     * Get the name of the command used to run this job.
     *
     * @return The command name
     */
    public String getCommandName() {
        return this.commandName;
    }

    /**
     * A builder to create jobs.
     *
     * @author tgianos
     * @since 3.0.0
     */
    public static class Builder extends CommonDTO.Builder<Builder> {

        private JobStatus bStatus = JobStatus.INIT;
        private String bStatusMsg;
        private Date bStarted;
        private Date bFinished;
        private String bArchiveLocation;
        private String bClusterName;
        private String bCommandName;

        /**
         * Constructor which has required fields.
         *
         * @param name    The name to use for the Job
         * @param user    The user to use for the Job
         * @param version The version to use for the Job
         */
        public Builder(
            @JsonProperty("name")
            final String name,
            @JsonProperty("user")
            final String user,
            @JsonProperty("version")
            final String version
        ) {
            super(name, user, version);
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
        @JsonDeserialize(using = JsonDateDeserializer.class)
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
        @JsonDeserialize(using = JsonDateDeserializer.class)
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
