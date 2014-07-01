/*
 *
 *  Copyright 2014 Netflix, Inc.
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

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.model.Types.JobStatus;
import com.wordnik.swagger.annotations.ApiModelProperty;

import java.io.Serializable;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.persistence.Basic;
import javax.persistence.Cacheable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Lob;
import javax.persistence.PrePersist;
import javax.persistence.Table;
import javax.persistence.Transient;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Representation of the state of a Genie 2.0 job.
 *
 * @author amsharma
 * @author tgianos
 */
@Entity
@Table(schema = "genie")
@Cacheable(false)
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class Job extends Auditable implements Serializable {

    private static final long serialVersionUID = 2979506788441089067L;
    private static final Logger LOG = LoggerFactory.getLogger(Job.class);
    private static final char CRITERIA_SET_DELIMITER = '|';
    private static final char CRITERIA_DELIMITER = ',';

    // ------------------------------------------------------------------------
    // GENERAL COMMON PARAMS FOR ALL JOBS - TO BE SPECIFIED BY CLIENTS
    // ------------------------------------------------------------------------
    /**
     * User who submitted the job (REQUIRED).
     */
    @Basic(optional = false)
    @ApiModelProperty(
            value = "User who submitted this job",
            required = true)
    private String user;

    /**
     * Command line arguments (REQUIRED).
     */
    @Lob
    @Basic(optional = false)
    @ApiModelProperty(
            value = "Command line arguments for the job",
            required = true)
    private String commandArgs;

    /**
     * User-specified or system-generated job name.
     */
    @Basic
    @ApiModelProperty(
            value = "Name specified for the job")
    private String name;

    /**
     * Human readable description.
     */
    @Basic
    @ApiModelProperty(
            value = "Description specified for the job")
    private String description;

    /**
     * The group user belongs.
     */
    @Basic
    @Column(name = "groupName")
    @ApiModelProperty(
            value = "group name of the user who submitted this job")
    private String group;

    /**
     * Client - UC4, Ab Initio, Search.
     */
    @Basic
    @ApiModelProperty(
            value = "Client from where the job was submitted")
    private String client;

    /**
     * Alias - Cluster Name of the cluster selected to run the job.
     */
    @Basic
    @ApiModelProperty(
            value = "Name of the cluster where the job is run")
    private String executionClusterName;

    /**
     * ID for the cluster that was selected to run the job .
     */
    @Basic
    @ApiModelProperty(
            value = "Id of the cluster where the job is run")
    private String executionClusterId;

    /**
     * Users can specify a property file location with environment variables.
     */
    @Basic
    @ApiModelProperty(
            value = "Path to a shell file which is sourced before job is run.")
    private String envPropFile;

    /**
     * Set of tags to use for scheduling (REQUIRED).
     */
    @Transient
    @ApiModelProperty(
            value = "List of criteria containing tags to use to pick a cluster to run this job")
    private List<ClusterCriteria> clusterCriteria;

    /**
     * String representation of the the cluster criteria array list object
     * above.
     */
    @XmlTransient
    @JsonIgnore
    @Lob
    @Basic(optional = false)
    private String clusterCriteriaString;

    /**
     * File dependencies.
     */
    @Lob
    @ApiModelProperty(
            value = "Dependent files for this job to run.")
    private String fileDependencies;

    /**
     * Set of file dependencies, sent as MIME attachments. This is not persisted
     * in the DB for space reasons.
     */
    @Transient
    @ApiModelProperty(
            value = "Attachments sent as a part of job request.")
    private Set<FileAttachment> attachments;

    /**
     * Whether to disable archive logs or not - default is false.
     */
    @Basic
    @ApiModelProperty(
            value = "Boolean variable to decide whether job should be archived after it finishes.")
    private boolean disableLogArchival;

    /**
     * Email address of the user where they expects an email. This is sent once
     * the Genie job completes.
     */
    @Basic
    @ApiModelProperty(
            value = "Email address to send notifications to on job completion.")
    private String email;

    // ------------------------------------------------------------------------
    // Genie2 command and application combinations to be specified by the user while running jobs.
    // ------------------------------------------------------------------------
    /**
     * Application name - e.g. mapreduce, tez
     */
    @Basic
    @ApiModelProperty(
            value = "Name of the application that this job should use to run.")
    private String applicationName;

    /**
     * Application Id to pin to specific application id e.g. mr1
     */
    @Basic
    @ApiModelProperty(
            value = "Id of the application that this job should use to run.")
    private String applicationId;

    /**
     * Command name to run - e.g. prodhive, testhive, prodpig, testpig.
     */
    @Basic
    @ApiModelProperty(
            value = "Name of the command that this job should run.")
    private String commandName;

    /**
     * Command Id to run - Used to pin to a particular command e.g.
     * prodhive11_mr1
     */
    @Basic
    @ApiModelProperty(
            value = "Id of the command that this job should run.")
    private String commandId;

    // ------------------------------------------------------------------------
    // GENERAL COMMON STUFF FOR ALL JOBS
    // TO BE GENERATED/USED BY SERVER
    // ------------------------------------------------------------------------
    /**
     * PID for job - updated by the server.
     */
    @Basic
    private int processHandle = -1;

    /**
     * Job status - INIT, RUNNING, SUCCEEDED, KILLED, FAILED (upper case in DB).
     */
    @Basic
    @Enumerated(EnumType.STRING)
    private JobStatus status;

    /**
     * More verbose status message.
     */
    @Basic
    private String statusMsg;

    /**
     * Start time for job - initialized to null.
     */
    @Basic
    private Long startTime;

    /**
     * Finish time for job - initialized to zero (for historic reasons).
     */
    @Basic
    private Long finishTime = 0L;

    /**
     * The host/ip address of the client submitting job.
     */
    @Basic
    private String clientHost;

    /**
     * The genie host name on which the job is being run.
     */
    @Basic
    private String hostName;

    /**
     * REST URI to do a HTTP DEL on to kill this job - points to running
     * instance.
     */
    @Basic
    private String killURI;

    /**
     * URI to fetch the stdout/err and logs.
     */
    @Basic
    private String outputURI;

    /**
     * Job exit code.
     */
    @Basic
    private Integer exitCode;

    /**
     * Whether this job was forwarded to new instance or not.
     */
    @Basic
    private boolean forwarded;

    /**
     * Location of logs being archived to s3.
     */
    @Lob
    private String archiveLocation;

    /**
     * Default Constructor.
     */
    public Job() {
        super();
    }

    /**
     * Construct a new Job.
     *
     * @param user The name of the user running the job. Not null/empty/blank.
     * @param commandId The id of the command to run for the job. Not
     * null/empty/blank if no commandName entered.
     * @param commandName The name of the command to run for the job. Not
     * null/empty/blank if no commandId entered.
     * @param commandArgs The command line arguments for the job. Not
     * null/empty/blank.
     * @param clusterCriteria The cluster criteria for the job. Not null/empty.
     * @throws CloudServiceException
     */
    public Job(
            final String user,
            final String commandId,
            final String commandName,
            final String commandArgs,
            final List<ClusterCriteria> clusterCriteria) throws CloudServiceException {
        this.user = user;
        this.commandId = commandId;
        this.commandName = commandName;
        this.commandArgs = commandArgs;
        this.clusterCriteria = clusterCriteria;
    }

    /**
     * Makes sure non-transient fields are set from transient fields.
     *
     * @throws CloudServiceException
     */
    @PrePersist
    protected void onCreateJob() throws CloudServiceException {
        validate(this.user, this.commandId, this.commandName, this.commandArgs, this.clusterCriteria);
        this.clusterCriteriaString = criteriaToString(this.clusterCriteria);
    }

    /**
     * Gets the name for this job.
     *
     * @return name
     */
    public String getName() {
        return this.name;
    }

    /**
     * Sets the name for this job.
     *
     * @param name name for the job
     */
    public void setName(final String name) {
        this.name = name;
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
     * Gets the user who submit the job.
     *
     * @return the user
     */
    public String getUser() {
        return this.user;
    }

    /**
     * Sets the user who submits the job.
     *
     * @param user user submitting the job
     * @throws CloudServiceException
     */
    public void setUser(final String user) throws CloudServiceException {
        if (StringUtils.isBlank(user)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No user entered.");
        }
        this.user = user;
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
     * Get the client from which this job is submitted. used for
     * grouping/identifying the source.
     *
     * @return client
     */
    public String getClient() {
        return this.client;
    }

    /**
     * Sets the client from which the job is submitted.
     *
     * @param client client from which the job is submitted. Used for book
     * keeping/grouping.
     */
    public void setClient(final String client) {
        this.client = client;
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
     * Populated by the server.
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
     * Populated by the server.
     */
    public void setExecutionClusterId(final String executionClusterId) {
        this.executionClusterId = executionClusterId;
    }

    /**
     * Gets the criteria which was specified to pick a cluster to run the job.
     *
     * @return clusterCriteria
     */
    public List<ClusterCriteria> getClusterCriteria() {
        return this.clusterCriteria;
    }

    /**
     * Sets the list of cluster criteria specified to pick a cluster.
     *
     * @param clusterCriteria The criteria list
     * @throws CloudServiceException
     */
    public void setClusterCriteria(final List<ClusterCriteria> clusterCriteria) throws CloudServiceException {
        if (clusterCriteria == null || clusterCriteria.isEmpty()) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No user entered.");
        }
        this.clusterCriteria = clusterCriteria;
        this.clusterCriteriaString = criteriaToString(clusterCriteria);
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
     * null/empty/blank.
     * @throws CloudServiceException
     */
    public void setCommandArgs(final String commandArgs) throws CloudServiceException {
        if (StringUtils.isBlank(commandArgs)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No command args entered.");
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
     * @throws CloudServiceException
     */
    public void setAttachments(final Set<FileAttachment> attachments) throws CloudServiceException {
        if (attachments != null) {
            this.attachments = attachments;
        } else {
            final String msg = "No attachments passed in to set. Unable to continue.";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }
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
     * job is run
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
     * is run
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
     * run
     * @throws CloudServiceException
     */
    public void setCommandName(final String commandName) throws CloudServiceException {
        if (StringUtils.isBlank(commandName)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No command name entered.");
        }
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
     * @throws CloudServiceException
     */
    public void setCommandId(final String commandId) throws CloudServiceException {
        if (StringUtils.isBlank(commandId)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No command id entered.");
        }
        this.commandId = commandId;
    }

    /**
     * Get the process handle for the job.
     *
     * @return processHandle
     *
     */
    public int getProcessHandle() {
        return this.processHandle;
    }

    /**
     * Set the process handle for the job.
     *
     * @param processHandle
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
     * @param statusMsg
     */
    public void setStatusMsg(final String statusMsg) {
        this.statusMsg = statusMsg;
    }

    /**
     * Gets the start time for this job.
     *
     * @return startTime : start time in ms
     */
    public Long getStartTime() {
        return this.startTime;
    }

    /**
     * Set the startTime for the job.
     *
     * @param startTime epoch time in ms
     */
    public void setStartTime(final Long startTime) {
        this.startTime = startTime;
    }

    /**
     * Gets the finish time for this job.
     *
     * @return finishTime
     */
    public Long getFinishTime() {
        return this.finishTime;
    }

    /**
     * Set the finishTime for the job.
     *
     * @param finishTime epoch time in ms
     */
    public void setFinishTime(final Long finishTime) {
        this.finishTime = finishTime;
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
     * @param clientHost
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
     * @param hostName
     */
    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    /**
     * Get the kill URI for this job.
     *
     * @return killURI
     */
    public String getKillURI() {
        return this.killURI;
    }

    /**
     * Set the kill URI for this job.
     *
     * @param killURI
     */
    public void setKillURI(final String killURI) {
        this.killURI = killURI;
    }

    /**
     * Get the output URI for this job.
     *
     * @return outputURI
     */
    public String getOutputURI() {
        return this.outputURI;
    }

    /**
     * Set the output URI for this job.
     *
     * @param outputURI
     */
    public void setOutputURI(final String outputURI) {
        this.outputURI = outputURI;
    }

    /**
     * Get the exit code for this job.
     *
     * @return exitCode
     */
    public Integer getExitCode() {
        return this.exitCode;
    }

    /**
     * Set the exit code for this job.
     *
     * @param exitCode
     */
    public void setExitCode(final Integer exitCode) {
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
     * Get the criteria specified to run this job in string format.
     *
     * @return clusterCriteriaString
     */
    protected String getClusterCriteriaString() {
        return this.clusterCriteriaString;
    }

    /**
     * Set the cluster criteria string.
     *
     * @param clusterCriteriaString A list of cluster criteria objects
     * @throws CloudServiceException
     */
    protected void setClusterCriteriaString(final String clusterCriteriaString) throws CloudServiceException {
        if (StringUtils.isBlank(clusterCriteriaString)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No clusterCriteriaString passed in to set. Unable to continue.");
        }
        this.clusterCriteriaString = clusterCriteriaString;
        this.clusterCriteria = stringToCriteria(clusterCriteriaString);
    }

    /**
     * Set job status, and update start/update/finish times, if needed.
     *
     * @param jobStatus status for job
     */
    public void setJobStatus(final JobStatus jobStatus) {
        this.status = jobStatus;

        if (jobStatus == Types.JobStatus.INIT) {
            setStartTime(System.currentTimeMillis());
        } else if (jobStatus == Types.JobStatus.SUCCEEDED
                || jobStatus == Types.JobStatus.KILLED
                || jobStatus == Types.JobStatus.FAILED) {
            setFinishTime(System.currentTimeMillis());
        }
    }

    /**
     * Sets job status and human-readable message.
     *
     * @param status predefined status
     * @param msg human-readable message
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
     * running this job.
     */
    public void setEnvPropFile(final String envPropFile) {
        this.envPropFile = envPropFile;
    }

    /**
     * Check to make sure that the required parameters exist.
     *
     * @param job The configuration to check
     * @throws CloudServiceException
     */
    public static void validate(final Job job) throws CloudServiceException {
        if (job == null) {
            final String msg = "Required parameter job can't be NULL";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }
        validate(
                job.getUser(),
                job.getCommandId(),
                job.getCommandName(),
                job.getCommandArgs(),
                job.getClusterCriteria());
    }

    /**
     * Validate that required parameters are present for a Job.
     *
     * @param user The name of the user running the job
     * @param commandId The id of the command to run for the job
     * @param commandName The name of the command to run for the job
     * @param commandArgs The command line arguments for the job
     * @param criteria The cluster criteria for the job
     * @throws CloudServiceException
     */
    private static void validate(
            final String user,
            final String commandId,
            final String commandName,
            final String commandArgs,
            final List<ClusterCriteria> criteria) throws CloudServiceException {
        final StringBuilder builder = new StringBuilder();
        if (StringUtils.isBlank(user)) {
            builder.append("User name is missing.\n");
        }
        if (StringUtils.isBlank(commandId) && StringUtils.isBlank(commandName)) {
            builder.append("Need one of command id or command name in order to run a job\n");
        }
        if (StringUtils.isEmpty(commandArgs)) {
            builder.append("Command arguments are required\n");
        }
        if (criteria == null || criteria.isEmpty()) {
            builder.append("At least one cluster criteria is required in order to figure out where to run this job.\n");
        }

        if (builder.length() != 0) {
            builder.insert(0, "Job configuration errors:\n");
            final String msg = builder.toString();
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }
    }

    /**
     * Helper method for building the cluster criteria string.
     *
     * @param clusterCriteria2 The criteria to build up from
     * @return The cluster criteria string
     */
    private String criteriaToString(final List<ClusterCriteria> clusterCriteria2) throws CloudServiceException {
        if (clusterCriteria2 == null || clusterCriteria2.isEmpty()) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No cluster criteria entered unable to create string");
        }
        final StringBuilder builder = new StringBuilder();
        for (final ClusterCriteria cc : clusterCriteria2) {
            if (builder.length() != 0) {
                builder.append(CRITERIA_SET_DELIMITER);
            }
            builder.append(StringUtils.join(cc.getTags(), CRITERIA_DELIMITER));
        }
        return builder.toString();
    }

    /**
     * Convert a string to cluster criteria objects.
     *
     * @param criteriaString The string to convert
     * @return The set of ClusterCriteria
     * @throws CloudServiceException
     */
    private List<ClusterCriteria> stringToCriteria(final String criteriaString) throws CloudServiceException {
        //Rebuild the cluster criteria objects
        final List<ClusterCriteria> cc = new ArrayList<ClusterCriteria>();
        final String[] criteriaSets = StringUtils.split(criteriaString, CRITERIA_SET_DELIMITER);
        if (criteriaSets == null || criteriaSets.length == 0) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No cluster criteria found. Unable to continue.");
        }
        for (final String criteriaSet : criteriaSets) {
            final String[] criterias = StringUtils.split(criteriaSet, CRITERIA_DELIMITER);
            if (criterias == null || criterias.length == 0) {
                continue;
            }
            final Set<String> c = new HashSet<String>();
            for (final String criteria : criterias) {
                c.add(criteria);
            }
            cc.add(new ClusterCriteria(c));
        }
        if (cc.isEmpty()) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No Cluster Criteria found. Unable to continue");
        }
        return cc;
    }
}
