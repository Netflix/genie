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
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.hibernate.validator.constraints.Length;

import javax.validation.constraints.NotNull;
import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * Read only data transfer object representing a Job in the Genie system.
 *
 * @author tgianos
 * @since 3.0.0
 */
@ApiModel(description = "A resource for a job in Genie.")
@JsonDeserialize(builder = Job.Builder.class)
public class Job extends JobRequest {

    @ApiModelProperty(value = "Id of the cluster where the job is running or was run.", readOnly = true)
    @Length(max = 255, message = "Max length is 255 characters")
    private String executionClusterId;

    @ApiModelProperty(value = "Id of the command that this job is using to run or ran with.", readOnly = true)
    @Length(max = 255, message = "Max length in database is 255 characters")
    private String commandId;

    @ApiModelProperty(value = "The current status of the job.", readOnly = true)
    @NotNull(message = "A job must have a status")
    private JobStatus status;

    @ApiModelProperty(value = "A status message about the job.", readOnly = true)
    @Length(max = 255, message = "Max length is 255 characters")
    private String statusMsg;

    @ApiModelProperty(value = "The start time of the job.", dataType = "dateTime", readOnly = true)
    @JsonSerialize(using = JsonDateSerializer.class)
    private Date started;

    @ApiModelProperty(value = "The end time of the job.", dataType = "dateTime", readOnly = true)
    @JsonSerialize(using = JsonDateSerializer.class)
    private Date finished;

    @ApiModelProperty(value = "The genie host where the job is being run or was run.", readOnly = true)
    @Length(max = 255, message = "Max length in database is 255 characters")
    private String hostName;

    @ApiModelProperty(value = "The URI to use to kill the job.", readOnly = true)
    private String killURI;

    @ApiModelProperty(value = "The URI where to find job output.", readOnly = true)
    private String outputURI;

    @ApiModelProperty(value = "The exit code of the job.", readOnly = true)
    private int exitCode = -1;

    @ApiModelProperty(value = "Where the logs were archived.", readOnly = true)
    private String archiveLocation;

    //TODO: Redesign should get rid of this and a lot of other fields
    @ApiModelProperty(value = "The process id.", readOnly = true)
    private int processId = -1;

    /**
     * Constructor used by the builder.
     *
     * @param builder The builder to use
     */
    protected Job(final Builder builder) {
        super(builder);
        this.executionClusterId = builder.bExecutionClusterId;
        this.commandId = builder.bCommandId;
        this.status = builder.bStatus;
        this.statusMsg = builder.bStatusMsg;
        if (builder.bStarted != null) {
            this.started = new Date(builder.bStarted.getTime());
        }
        if (builder.bFinished != null) {
            this.finished = new Date(builder.bFinished.getTime());
        }
        this.hostName = builder.bHostName;
        this.killURI = builder.bKillURI;
        this.outputURI = builder.bOutputURI;
        this.exitCode = builder.bExitCode;
        this.archiveLocation = builder.bArchiveLocation;
        this.processId = builder.bProcessId;
    }

    /**
     * Get the id of the cluster this job executed on.
     *
     * @return The id
     */
    public String getExecutionClusterId() {
        return this.executionClusterId;
    }

    /**
     * Get the id of the command used to run this job.
     *
     * @return The command id
     */
    public String getCommandId() {
        return this.commandId;
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
     * Get the host name of the Genie node the job ran on.
     *
     * @return The host name
     */
    public String getHostName() {
        return hostName;
    }

    /**
     * Get the URI used to kill the job.
     *
     * @return The kill URI
     */
    public String getKillURI() {
        return killURI;
    }

    /**
     * Get the URI where the job is putting its output.
     *
     * @return The output URI
     */
    public String getOutputURI() {
        return outputURI;
    }

    /**
     * Get the code the job exitted with.
     *
     * @return The exit code
     */
    public int getExitCode() {
        return exitCode;
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
     * Get the id of the process that ran or is running the job.
     *
     * @return the process id
     */
    public int getProcessId() {
        return this.processId;
    }

    /**
     * A builder to create jobs.
     *
     * @author tgianos
     * @since 3.0.0
     */
    public static class Builder extends JobRequest.Builder<Builder> {

        private String bExecutionClusterId;
        private String bCommandId;
        private JobStatus bStatus;
        private String bStatusMsg;
        private Date bStarted;
        private Date bFinished;
        private String bHostName;
        private String bKillURI;
        private String bOutputURI;
        private int bExitCode;
        private String bArchiveLocation;
        private int bProcessId;

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
            super(name, user, version, commandArgs, clusterCriterias, commandCriteria);
        }

        /**
         * Set the execution cluster id for this job.
         *
         * @param executionClusterId The execution cluster id
         * @return The builder
         */
        public Builder withExecutionClusterId(final String executionClusterId) {
            this.bExecutionClusterId = executionClusterId;
            return this;
        }

        /**
         * Set the id of the command used to run this job.
         *
         * @param commandId The id of the command
         * @return The builder
         */
        public Builder withCommandId(final String commandId) {
            this.bCommandId = commandId;
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
         * Set the Genie host name the job ran on.
         *
         * @param hostName The host name
         * @return The builder
         */
        public Builder withHostName(final String hostName) {
            this.bHostName = hostName;
            return this;
        }

        /**
         * Set the kill URI for the job.
         *
         * @param killURI The kill uri
         * @return The builder
         */
        public Builder withKillURI(final String killURI) {
            this.bKillURI = killURI;
            return this;
        }

        /**
         * Set the output URI for the job.
         *
         * @param outputURI The output uri
         * @return The builder
         */
        public Builder withOutputURI(final String outputURI) {
            this.bOutputURI = outputURI;
            return this;
        }

        /**
         * Set the exit code of the job.
         *
         * @param exitCode The exit code
         * @return The builder
         */
        public Builder withExitCode(final int exitCode) {
            this.bExitCode = exitCode;
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
         * Set the process id of the job.
         *
         * @param processId The id of the process being used to run the job client
         * @return The builder
         */
        public Builder withProcessId(final int processId) {
            this.bProcessId = processId;
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
