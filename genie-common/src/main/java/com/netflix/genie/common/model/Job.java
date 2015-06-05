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
package com.netflix.genie.common.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.netflix.config.ConfigurationManager;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.util.JsonDateDeserializer;
import com.netflix.genie.common.util.JsonDateSerializer;
import com.wordnik.swagger.annotations.ApiModel;
import com.wordnik.swagger.annotations.ApiModelProperty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.persistence.Basic;
import javax.persistence.Cacheable;
import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.Lob;
import javax.persistence.PostLoad;
import javax.persistence.PrePersist;
import javax.persistence.PreUpdate;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Transient;

import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.NotBlank;
import org.hibernate.validator.constraints.NotEmpty;

/**
 * Representation of the state of a Genie 2.0 job.
 *
 * @author amsharma
 * @author tgianos
 */
@Entity
@Cacheable(false)
@ApiModel(description = "An entity for submitting and monitoring a job in Genie.")
public class Job extends CommonEntityFields {
    /**
     * Used to split between cluster criteria sets.
     */
    protected static final char CRITERIA_SET_DELIMITER = '|';
    /**
     * Used to split between criteria.
     */
    protected static final char CRITERIA_DELIMITER = ',';
    /**
     * Used as default version when one not entered.
     */
    protected static final String DEFAULT_VERSION = "NA";

    // ------------------------------------------------------------------------
    // GENERAL COMMON PARAMS FOR ALL JOBS - TO BE SPECIFIED BY CLIENTS
    // ------------------------------------------------------------------------
    /**
     * Command line arguments (REQUIRED).
     */
    @Lob
    @Basic(optional = false)
    @ApiModelProperty(
            value = "Command line arguments for the job.",
            example = "-f hive.q",
            required = true
    )
    @NotBlank(message = "Command arguments are required.")
    private String commandArgs;

    /**
     * Human readable description.
     */
    @Basic
    @ApiModelProperty(
            value = "Description specified for the job"
    )
    private String description;

    /**
     * The group user belongs.
     */
    @Basic
    @Column(name = "groupName")
    @ApiModelProperty(
            value = "Group name of the user who submitted this job"
    )
    private String group;

    /**
     * Users can specify a property file location with environment variables.
     */
    @Basic
    @ApiModelProperty(
            value = "Path to a shell file which is sourced before job is run where properties can be set"
    )
    private String envPropFile;

    /**
     * Set of tags to use for scheduling (REQUIRED).
     */
    @Transient
    @ApiModelProperty(
            value = "List of criteria containing tags to use to pick a cluster to run this job, evaluated in order",
            required = true
    )
    @NotEmpty(message = "No cluster criteria entered. At least one required.")
    private List<ClusterCriteria> clusterCriterias;

    /**
     * Set of tags to use for selecting command (REQUIRED).
     */
    @Transient
    @ApiModelProperty(
            value = "List of criteria containing tags to use to pick a command to run this job",
            required = true
    )
    @NotEmpty(message = "No command criteria entered. At least one required.")
    private Set<String> commandCriteria;

    /**
     * File dependencies.
     */
    @Lob
    @ApiModelProperty(
            value = "Dependent files for this job to run. Will be downloaded from s3/hdfs before job starts"
    )
    private String fileDependencies;

    /**
     * Set of file dependencies, sent as MIME attachments. This is not persisted
     * in the DB for space reasons.
     */
    @Transient
    @ApiModelProperty(
            value = "Attachments sent as a part of job request. Can be used as command line arguments"
    )
    private Set<FileAttachment> attachments;

    /**
     * Whether to disable archive logs or not - default is false.
     */
    @Basic
    @ApiModelProperty(
            value = "Boolean variable to decide whether job should be archived after it finishes defaults to true"
    )
    private boolean disableLogArchival;

    /**
     * Email address of the user where they expects an email. This is sent once
     * the Genie job completes.
     */
    @Basic
    @ApiModelProperty(
            value = "Email address to send notifications to on job completion"
    )
    private String email;

    /**
     * Set of tags for a job.
     */
    @ElementCollection(fetch = FetchType.EAGER)
    @ApiModelProperty(
            value = "Any tags a user wants to add to the job to help with discovery of job later"
    )
    private Set<String> tags;

    // ------------------------------------------------------------------------
    // GENERAL COMMON STUFF FOR ALL JOBS
    // TO BE GENERATED/USED BY SERVER
    // ------------------------------------------------------------------------

    /**
     * String representation of the the cluster criteria array list object
     * above.
     */
    @JsonIgnore
    @Lob
    @Basic(optional = false)
    private String clusterCriteriasString;

    /**
     * String representation of the the command criteria set object above.
     */
    @JsonIgnore
    @Lob
    @Basic(optional = false)
    private String commandCriteriaString;

    /**
     * String representation of the criteria that was successfully used to
     * select a cluster.
     */
    @JsonIgnore
    @Lob
    @Basic
    private String chosenClusterCriteriaString;

    /**
     * Cluster Name of the cluster selected to run the job.
     */
    @Basic
    @ApiModelProperty(
            value = "Name of the cluster where the job is running or was run. Set automatically by system",
            readOnly = true
    )
    private String executionClusterName;

    /**
     * ID for the cluster that was selected to run the job .
     */
    @Basic
    @ApiModelProperty(
            value = "Id of the cluster where the job is running or was run. Set automatically by system",
            readOnly = true
    )
    private String executionClusterId;

    /**
     * Application name - e.g. mapreduce, tez
     */
    @Basic
    @ApiModelProperty(
            value = "Name of the application that this job is using to run or ran with. Set automatically by system",
            readOnly = true
    )
    private String applicationName;

    /**
     * Application Id to pin to specific application id e.g. mr1
     */
    @Basic
    @ApiModelProperty(
            value = "Id of the application that this job is using to run or ran with. Set automatically by system",
            readOnly = true
    )
    private String applicationId;

    /**
     * Command name to run - e.g. prodhive, testhive, prodpig, testpig.
     */
    @Basic
    @ApiModelProperty(
            value = "Name of the command that this job is using to run or ran with. Set automatically by system",
            readOnly = true
    )
    private String commandName;

    /**
     * Command Id to run - Used to pin to a particular command e.g.
     * prodhive11_mr1
     */
    @Basic
    @ApiModelProperty(
            value = "Id of the command that this job is using to run or ran with. Set automatically by system",
            readOnly = true
    )
    private String commandId;

    /**
     * PID for job - updated by the server.
     */
    @Basic
    @ApiModelProperty(
            value = "The process handle. Set by system",
            readOnly = true
    )
    private int processHandle = -1;

    /**
     * Job status - INIT, RUNNING, SUCCEEDED, KILLED, FAILED (upper case in DB).
     */
    @Basic
    @Enumerated(EnumType.STRING)
    @ApiModelProperty(
            value = "The current status of the job. Set automatically by system",
            readOnly = true
    )
    private JobStatus status;

    /**
     * More verbose status message.
     */
    @Basic
    @ApiModelProperty(
            value = "A status message about the job. Set automatically by system",
            readOnly = true
    )
    private String statusMsg;

    /**
     * Start time for job - initialized to null.
     */
    @Basic
    @Temporal(TemporalType.TIMESTAMP)
    @ApiModelProperty(
            value = "The start time of the job. Set automatically by system",
            dataType = "dateTime",
            readOnly = true
    )
    @JsonSerialize(using = JsonDateSerializer.class)
    @JsonDeserialize(using = JsonDateDeserializer.class)
    private Date started = new Date(0);

    /**
     * Finish time for job - initialized to zero (for historic reasons).
     */
    @Basic
    @Temporal(TemporalType.TIMESTAMP)
    @ApiModelProperty(
            value = "The end time of the job. Initialized at 0. Set automatically by system",
            dataType = "dateTime",
            readOnly = true
    )
    @JsonSerialize(using = JsonDateSerializer.class)
    @JsonDeserialize(using = JsonDateDeserializer.class)
    private Date finished = new Date(0);

    /**
     * The host/IP address of the client submitting job.
     */
    @Basic
    @ApiModelProperty(
            value = "The hostname of the client submitting the job. Set automatically by system",
            readOnly = true
    )
    private String clientHost;

    /**
     * The genie host name on which the job is being run.
     */
    @Basic
    @ApiModelProperty(
            value = "The genie host where the job is being run or was run. Set automatically by system",
            readOnly = true
    )
    private String hostName;

    /**
     * REST URI to do a HTTP DEL on to kill this job - points to running
     * instance.
     */
    @Basic
    @ApiModelProperty(
            value = "The URI to use to kill the job. Set automatically by system",
            readOnly = true
    )
    private String killURI;

    /**
     * URI to fetch the stdout/err and logs.
     */
    @Basic
    @ApiModelProperty(
            value = "The URI where to find job output. Set automatically by system",
            readOnly = true
    )
    private String outputURI;

    /**
     * Job exit code.
     */
    @Basic
    @ApiModelProperty(
            value = "The exit code of the job. Set automatically by system",
            readOnly = true
    )
    private int exitCode = -1;

    /**
     * Whether this job was forwarded to new instance or not.
     */
    @Basic
    @ApiModelProperty(
            value = "Whether this job was forwarded or not. Set automatically by system",
            readOnly = true
    )
    private boolean forwarded = false;

    /**
     * Location of logs being archived to s3.
     */
    @Lob
    @ApiModelProperty(
            value = "Where the logs were archived. Set automatically by system",
            readOnly = true
    )
    private String archiveLocation;

    /**
     * Default Constructor.
     */
    public Job() {
        super();
        // Set version to default if not specified
        if (StringUtils.isBlank(this.getVersion())) {
            this.setVersion(DEFAULT_VERSION);
        }
    }

    /**
     * Construct a new Job.
     *
     * @param user             The name of the user running the job. Not null/empty/blank.
     * @param name             The name specified for the job. Not null/empty/blank.
     * @param version          The version of this job. Not null/empty/blank.
     * @param commandArgs      The command line arguments for the job. Not
     *                         null/empty/blank.
     * @param commandCriteria  The criteria for the command. Not null/empty.
     * @param clusterCriterias The cluster criteria for the job. Not null/empty.
     */
    public Job(
            final String user,
            final String name,
            final String version,
            final String commandArgs,
            final Set<String> commandCriteria,
            final List<ClusterCriteria> clusterCriterias) {
        super(name, user, version);

        this.commandArgs = commandArgs;
        this.clusterCriterias = clusterCriterias;
        this.commandCriteria = commandCriteria;

        // Set version to default if not specified
        if (StringUtils.isBlank(this.getVersion())) {
            this.setVersion(DEFAULT_VERSION);
        }
    }

    /**
     * Makes sure non-transient fields are set from transient fields.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @PrePersist
    @PreUpdate
    protected void onCreateOrUpdateJob() throws GeniePreconditionException {
        this.clusterCriteriasString = clusterCriteriasToString(this.clusterCriterias);
        this.commandCriteriaString = commandCriteriaToString(this.commandCriteria);
        // Add the id to the tags
        if (this.tags == null) {
            this.tags = new HashSet<>();
        }
        if (ConfigurationManager.getConfigInstance().getBoolean("com.netflix.genie.server.jobs.tags.default", false)) {
            this.addAndValidateSystemTags(this.tags);
        }
    }

    /**
     * On any update to the entity will add id to tags.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @PostLoad
    protected void onLoadJob() throws GeniePreconditionException {
        this.clusterCriterias = this.stringToClusterCriterias(this.clusterCriteriasString);
        this.commandCriteria = this.stringToCommandCriteria(this.commandCriteriaString);
    }

    /**
     * Gets the description of this job.
     *
     * @return description
     */
    public String getDescription() {
        return this.description;
    }

    /**
     * Sets the description for this job.
     *
     * @param description description for the job
     */
    public void setDescription(final String description) {
        this.description = description;
    }

    /**
     * Gets the group name of the user who submitted the job.
     *
     * @return group
     */
    public String getGroup() {
        return this.group;
    }

    /**
     * Sets the group of the user who submits the job.
     *
     * @param group group of the user submitting the job
     */
    public void setGroup(final String group) {
        this.group = group;
    }

    /**
     * Gets the name of the cluster on which this job was run.
     *
     * @return executionClusterName
     */
    public String getExecutionClusterName() {
        return this.executionClusterName;
    }

    /**
     * Sets the name of the cluster on which this job is run.
     *
     * @param executionClusterName Name of the cluster on which job is executed.
     *                             Populated by the server.
     */
    public void setExecutionClusterName(final String executionClusterName) {
        this.executionClusterName = executionClusterName;
    }

    /**
     * Gets the id of the cluster on which this job was run.
     *
     * @return executionClusterId
     */
    public String getExecutionClusterId() {
        return this.executionClusterId;
    }

    /**
     * Sets the id of the cluster on which this job is run.
     *
     * @param executionClusterId Id of the cluster on which job is executed.
     *                           Populated by the server.
     */
    public void setExecutionClusterId(final String executionClusterId) {
        this.executionClusterId = executionClusterId;
    }

    /**
     * Gets the cluster criteria which was specified to pick a cluster to run
     * the job.
     *
     * @return clusterCriterias
     */
    public List<ClusterCriteria> getClusterCriterias() {
        return this.clusterCriterias;
    }

    /**
     * Sets the list of cluster criteria specified to pick a cluster.
     *
     * @param clusterCriterias The criteria list. Not null or empty.
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    public void setClusterCriterias(final List<ClusterCriteria> clusterCriterias) throws GeniePreconditionException {
        if (clusterCriterias == null || clusterCriterias.isEmpty()) {
            throw new GeniePreconditionException("No cluster criteria entered.");
        }
        this.clusterCriterias = clusterCriterias;
        this.clusterCriteriasString = clusterCriteriasToString(clusterCriterias);
    }

    /**
     * Gets the commandArgs specified to run the job.
     *
     * @return commandArgs
     */
    public String getCommandArgs() {
        return this.commandArgs;
    }

    /**
     * Parameters specified to be run and fed as command line arguments to the
     * job run.
     *
     * @param commandArgs Arguments to be used to run the command with. Not
     *                    null/empty/blank.
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    public void setCommandArgs(final String commandArgs) throws GeniePreconditionException {
        if (StringUtils.isBlank(commandArgs)) {
            throw new GeniePreconditionException("No command args entered.");
        }
        this.commandArgs = commandArgs;
    }

    /**
     * Gets the fileDependencies for the job.
     *
     * @return fileDependencies
     */
    public String getFileDependencies() {
        return this.fileDependencies;
    }

    /**
     * Sets the fileDependencies for the job.
     *
     * @param fileDependencies Dependent files for the job in csv format
     */
    public void setFileDependencies(final String fileDependencies) {
        this.fileDependencies = fileDependencies;
    }

    /**
     * Get the attachments for this job.
     *
     * @return The attachments
     */
    public Set<FileAttachment> getAttachments() {
        return this.attachments;
    }

    /**
     * Set the attachments for this job.
     *
     * @param attachments The attachments to set
     */
    public void setAttachments(final Set<FileAttachment> attachments) {
        this.attachments = attachments;
    }

    /**
     * Is the log archival disabled.
     *
     * @return true if it's disabled
     */
    public boolean isDisableLogArchival() {
        return this.disableLogArchival;
    }

    /**
     * Set whether the log archival is disabled or not.
     *
     * @param disableLogArchival True if disabling is desired
     */
    public void setDisableLogArchival(final boolean disableLogArchival) {
        this.disableLogArchival = disableLogArchival;
    }

    /**
     * Gets the commandArgs specified to run the job.
     *
     * @return commandArgs
     */
    public String getEmail() {
        return this.email;
    }

    /**
     * Set user Email address for the job.
     *
     * @param email user email address
     */
    public void setEmail(final String email) {
        this.email = email;
    }

    /**
     * Gets the application name specified to run the job.
     *
     * @return applicationName
     */
    public String getApplicationName() {
        return this.applicationName;
    }

    /**
     * Set application Name with which this job is run, if not null.
     *
     * @param applicationName Name of the application if specified on which the
     *                        job is run
     */
    public void setApplicationName(final String applicationName) {
        this.applicationName = applicationName;
    }

    /**
     * Gets the application id specified to run the job.
     *
     * @return applicationId
     */
    public String getApplicationId() {
        return this.applicationId;
    }

    /**
     * Set application Id with which this job is run, if not null.
     *
     * @param applicationId Id of the application if specified on which the job
     *                      is run
     */
    public void setApplicationId(final String applicationId) {
        this.applicationId = applicationId;
    }

    /**
     * Gets the command name for this job.
     *
     * @return commandName
     */
    public String getCommandName() {
        return this.commandName;
    }

    /**
     * Set command Name with which this job is run.
     *
     * @param commandName Name of the command if specified on which the job is
     *                    run
     */
    public void setCommandName(final String commandName) {
        this.commandName = commandName;
    }

    /**
     * Gets the command id for this job.
     *
     * @return commandId
     */
    public String getCommandId() {
        return this.commandId;
    }

    /**
     * Set command Id with which this job is run.
     *
     * @param commandId Id of the command if specified on which the job is run
     */
    public void setCommandId(final String commandId) {
        this.commandId = commandId;
    }

    /**
     * Get the process handle for the job.
     *
     * @return processHandle
     */
    public int getProcessHandle() {
        return this.processHandle;
    }

    /**
     * Set the process handle for the job.
     *
     * @param processHandle the process handle
     */
    public void setProcessHandle(final int processHandle) {
        this.processHandle = processHandle;
    }

    /**
     * Gets the status for this job.
     *
     * @return status
     * @see JobStatus
     */
    public JobStatus getStatus() {
        return this.status;
    }

    /**
     * Set the status for the job.
     *
     * @param status The new status
     */
    public void setStatus(final JobStatus status) {
        this.status = status;
    }

    /**
     * Gets the status message or this job.
     *
     * @return statusMsg
     */
    public String getStatusMsg() {
        return this.statusMsg;
    }

    /**
     * Set the status message for the job.
     *
     * @param statusMsg The status message.
     */
    public void setStatusMsg(final String statusMsg) {
        this.statusMsg = statusMsg;
    }

    /**
     * Gets the start time for this job.
     *
     * @return startTime : start time in ms
     */
    public Date getStarted() {
        return new Date(this.started.getTime());
    }

    /**
     * Set the startTime for the job.
     *
     * @param started epoch time in ms
     */
    public void setStarted(final Date started) {
        this.started = started;
    }

    /**
     * Gets the finish time for this job.
     *
     * @return finished. The job finish timestamp.
     */
    public Date getFinished() {
        return new Date(this.finished.getTime());
    }

    /**
     * Set the finishTime for the job.
     *
     * @param finished The finished time.
     */
    public void setFinished(final Date finished) {
        this.finished = finished;
    }

    /**
     * Gets client hostname from which this job is run.
     *
     * @return clientHost
     */
    public String getClientHost() {
        return this.clientHost;
    }

    /**
     * Set the client host for the job.
     *
     * @param clientHost The client host anme.
     */
    public void setClientHost(final String clientHost) {
        this.clientHost = clientHost;
    }

    /**
     * Gets genie hostname on which this job is run.
     *
     * @return hostName
     */
    public String getHostName() {
        return this.hostName;
    }

    /**
     * Set the genie hostname on which the job is run.
     *
     * @param hostName The host name.
     */
    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    /**
     * Get the kill URI for this job.
     *
     * @return killURI The kill uri.
     */
    public String getKillURI() {
        return this.killURI;
    }

    /**
     * Set the kill URI for this job.
     *
     * @param killURI The kill URI
     */
    public void setKillURI(final String killURI) {
        this.killURI = killURI;
    }

    /**
     * Get the output URI for this job.
     *
     * @return outputURI the output uri.
     */
    public String getOutputURI() {
        return this.outputURI;
    }

    /**
     * Set the output URI for this job.
     *
     * @param outputURI The output URI.
     */
    public void setOutputURI(final String outputURI) {
        this.outputURI = outputURI;
    }

    /**
     * Get the exit code for this job.
     *
     * @return exitCode The exit code. 0 for Success.
     */
    public int getExitCode() {
        return this.exitCode;
    }

    /**
     * Set the exit code for this job.
     *
     * @param exitCode The exit code of the job.
     */
    public void setExitCode(final int exitCode) {
        this.exitCode = exitCode;
    }

    /**
     * Has the job been forwarded to another instance.
     *
     * @return true, if forwarded
     */
    public boolean isForwarded() {
        return this.forwarded;
    }

    /**
     * Has the job been forwarded to another instance.
     *
     * @param forwarded true, if forwarded
     */
    public void setForwarded(final boolean forwarded) {
        this.forwarded = forwarded;
    }

    /**
     * Get location where logs are archived.
     *
     * @return s3/hdfs location where logs are archived
     */
    public String getArchiveLocation() {
        return this.archiveLocation;
    }

    /**
     * Set location where logs are archived.
     *
     * @param archiveLocation s3/hdfs location where logs are archived
     */
    public void setArchiveLocation(final String archiveLocation) {
        this.archiveLocation = archiveLocation;
    }

    /**
     * Get the cluster criteria specified to run this job in string format.
     *
     * @return clusterCriteriasString
     */
    protected String getClusterCriteriasString() {
        return this.clusterCriteriasString;
    }

    /**
     * Set the cluster criteria string.
     *
     * @param clusterCriteriasString A list of cluster criteria objects
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    protected void setClusterCriteriasString(final String clusterCriteriasString) throws GeniePreconditionException {
        this.clusterCriteriasString = clusterCriteriasString;
        this.clusterCriterias = stringToClusterCriterias(clusterCriteriasString);
    }

    /**
     * Gets the command criteria which was specified to pick a command to run
     * the job.
     *
     * @return commandCriteria
     */
    public Set<String> getCommandCriteria() {
        return this.commandCriteria;
    }

    /**
     * Sets the set of command criteria specified to pick a command.
     *
     * @param commandCriteria The criteria list
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    public void setCommandCriteria(Set<String> commandCriteria) throws GeniePreconditionException {
        this.commandCriteria = commandCriteria;
        this.commandCriteriaString = commandCriteriaToString(commandCriteria);
    }

    /**
     * Get the command criteria specified to run this job in string format.
     *
     * @return commandCriteriaString
     */
    public String getCommandCriteriaString() {
        return this.commandCriteriaString;
    }

    /**
     * Set the command criteria string.
     *
     * @param commandCriteriaString A set of command criteria tags
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    public void setCommandCriteriaString(String commandCriteriaString) throws GeniePreconditionException {
        this.commandCriteriaString = commandCriteriaString;
        this.commandCriteria = stringToCommandCriteria(commandCriteriaString);
    }

    /**
     * Set job status, and update start/update/finish times, if needed.
     *
     * @param jobStatus status for job
     */
    public void setJobStatus(final JobStatus jobStatus) {
        this.status = jobStatus;

        if (jobStatus == JobStatus.INIT) {
            this.setStarted(new Date());
        } else if (jobStatus == JobStatus.SUCCEEDED
                || jobStatus == JobStatus.KILLED
                || jobStatus == JobStatus.FAILED) {
            setFinished(new Date());
        }
    }

    /**
     * Sets job status and human-readable message.
     *
     * @param status predefined status
     * @param msg    human-readable message
     */
    public void setJobStatus(final JobStatus status, final String msg) {
        setJobStatus(status);
        setStatusMsg(msg);
    }

    /**
     * Gets the envPropFile name.
     *
     * @return envPropFile - file name containing environment variables.
     */
    public String getEnvPropFile() {
        return this.envPropFile;
    }

    /**
     * Sets the env property file name in string form.
     *
     * @param envPropFile contains the list of env variables to set while
     *                    running this job.
     */
    public void setEnvPropFile(final String envPropFile) {
        this.envPropFile = envPropFile;
    }

    /**
     * Gets the tags allocated to this job.
     *
     * @return the tags
     */
    public Set<String> getTags() {
        return this.tags;
    }

    /**
     * Sets the tags allocated to this job.
     *
     * @param tags the tags to set. Not Null.
     */
    public void setTags(final Set<String> tags) {
        this.tags = tags;
    }

    /**
     * Gets the criteria used to select a cluster for this job.
     *
     * @return the criteria containing tags which was chosen to select a cluster
     * to run this job.
     */
    public String getChosenClusterCriteriaString() {
        return chosenClusterCriteriaString;
    }

    /**
     * Sets the criteria used to select cluster to run this job.
     *
     * @param chosenClusterCriteriaString he criteria used to select cluster to
     *                                    run this job.
     */
    public void setChosenClusterCriteriaString(String chosenClusterCriteriaString) {
        this.chosenClusterCriteriaString = chosenClusterCriteriaString;
    }

    /**
     * Helper method for building the cluster criteria string.
     *
     * @param clusterCriteria The criteria to build up from
     * @return The cluster criteria string
     */
    protected String clusterCriteriasToString(final List<ClusterCriteria> clusterCriteria) {
        if (clusterCriteria == null || clusterCriteria.isEmpty()) {
            return null;
        }
        final StringBuilder builder = new StringBuilder();
        for (final ClusterCriteria cc : clusterCriteria) {
            if (builder.length() != 0) {
                builder.append(CRITERIA_SET_DELIMITER);
            }
            builder.append(StringUtils.join(cc.getTags(), CRITERIA_DELIMITER));
        }
        return builder.toString();
    }

    /**
     * Helper method for building the cluster criteria string.
     *
     * @param commandCriteria The criteria to build up from
     * @return The cluster criteria string
     */
    protected String commandCriteriaToString(final Set<String> commandCriteria) {
        if (commandCriteria == null || commandCriteria.isEmpty()) {
            return null;
        } else {
            return StringUtils.join(this.commandCriteria, CRITERIA_DELIMITER);
        }
    }

    /**
     * Convert a string to cluster criteria objects.
     *
     * @param criteriaString The string to convert
     * @return The set of ClusterCriteria
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    protected Set<String> stringToCommandCriteria(final String criteriaString) throws GeniePreconditionException {
        final String[] criterias = StringUtils.split(criteriaString, CRITERIA_DELIMITER);
        if (criterias == null || criterias.length == 0) {
            throw new GeniePreconditionException("No command criteria found. Unable to continue.");
        }
        final Set<String> c = new HashSet<>();
        c.addAll(Arrays.asList(criterias));
        return c;
    }

    /**
     * Convert a string to cluster criteria objects.
     *
     * @param criteriaString The string to convert
     * @return The set of ClusterCriteria
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    protected List<ClusterCriteria> stringToClusterCriterias(final String criteriaString)
            throws GeniePreconditionException {
        //Rebuild the cluster criteria objects
        final String[] criteriaSets = StringUtils.split(criteriaString, CRITERIA_SET_DELIMITER);
        if (criteriaSets == null || criteriaSets.length == 0) {
            throw new GeniePreconditionException("No cluster criteria found. Unable to continue.");
        }
        final List<ClusterCriteria> cc = new ArrayList<>();
        for (final String criteriaSet : criteriaSets) {
            final String[] criterias = StringUtils.split(criteriaSet, CRITERIA_DELIMITER);
            if (criterias == null || criterias.length == 0) {
                continue;
            }
            final Set<String> c = new HashSet<>();
            c.addAll(Arrays.asList(criterias));
            cc.add(new ClusterCriteria(c));
        }
        if (cc.isEmpty()) {
            throw new GeniePreconditionException("No Cluster Criteria found. Unable to continue");
        }
        return cc;
    }
}
