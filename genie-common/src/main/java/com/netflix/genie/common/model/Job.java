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
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.persistence.Basic;
import javax.persistence.Cacheable;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Lob;
import javax.persistence.Table;
import javax.persistence.Transient;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Representation of the state of a Genie 2.0 job.
 *
 * @author amsharma
 * @author tgianos
 */
//TODO: Should we check all string parameters for null/empty?
@Entity
@Table(schema = "genie")
@Cacheable(false)
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class Job extends Auditable implements Serializable {

    private static final long serialVersionUID = 2979506788441089067L;
    private static final Logger LOG = LoggerFactory.getLogger(Command.class);

    // ------------------------------------------------------------------------
    // GENERAL COMMON PARAMS FOR ALL JOBS - TO BE SPECIFIED BY CLIENTS
    // ------------------------------------------------------------------------
    /**
     * User-specified or system-generated job name.
     */
    @Basic
    private String jobName;

    /**
     * Human readable description.
     */
    @Basic
    private String description;

    /**
     * User who submitted the job (REQUIRED).
     */
    @Basic(optional = false)
    private String userName;

    /**
     * The group user belongs.
     */
    @Basic
    private String groupName;

    /**
     * Client - UC4, Ab Initio, Search.
     */
    @Basic
    private String client;

    /**
     * Alias - Cluster Name of the cluster selected to run the job.
     */
    @Basic
    private String executionClusterName;

    /**
     * ID for the cluster that was selected to run the job .
     */
    @Basic
    private String executionClusterId;

    /**
     * Users can specify a property file location with environment variables.
     */
    @Basic
    private String envPropFile;

    /**
     * Set of tags to use for scheduling (REQUIRED).
     */
    @Transient
    private Set<ClusterCriteria> clusterCriteria = new HashSet<ClusterCriteria>();

    /**
     * String representation of the the cluster criteria array list object
     * above. TODO: use pre/post persist to store the above list into the DB
     */
    @Lob
    private String clusterCriteriaString;

    /**
     * Command line arguments (REQUIRED).
     */
    @Lob
    @Basic(optional = false)
    private String cmdArgs;

    /**
     * File dependencies.
     */
    @Lob
    private String fileDependencies;

    /**
     * Set of file dependencies, sent as MIME attachments. This is not persisted
     * in the DB for space reasons.
     */
    @Transient
    //TODO: Why array and not collection?s
    private FileAttachment[] attachments;

    /**
     * Whether to disable archive logs or not - default is false.
     */
    @Basic
    private boolean disableLogArchival;

    /**
     * Email address of the user where he expects an email. This is sent once
     * the aladdin job completes.
     */
    @Basic
    private String userEmail;

    // ------------------------------------------------------------------------
    // ------------------------------------------------------------------------
    // Genie2 command and application combinations to be specified by the user while running jobs.
    // ------------------------------------------------------------------------
    /**
     * Application name - e.g. mapreduce, tez
     */
    @Basic
    private String applicationName;

    /**
     * Application Id to pin to specific application id e.g. mr1
     */
    @Basic
    private String applicationId;

    /**
     * Command name to run - e.g. prodhive, testhive, prodpig, testpig.
     */
    @Basic
    private String commandName;

    /**
     * Command Id to run - Used to pin to a particular command e.g.
     * prodhive11_mr1
     */
    @Basic
    private String commandId;

    // ------------------------------------------------------------------------
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
     * Gets the name for this job.
     *
     * @return jobName
     */
    public String getJobName() {
        return this.jobName;
    }

    /**
     * Sets the name for this job.
     *
     * @param jobName name for the job
     */
    public void setJobName(final String jobName) {
        this.jobName = jobName;
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
     * @return userName
     */
    public String getUserName() {
        return this.userName;
    }

    /**
     * Sets the user who submits the job.
     *
     * @param userName user submitting the job
     */
    public void setUserName(final String userName) {
        this.userName = userName;
    }

    /**
     * Gets the group name of the user who submitted the job.
     *
     * @return groupName
     */
    public String getGroupName() {
        return this.groupName;
    }

    /**
     * Sets the group of the user who submits the job.
     *
     * @param groupName usergroup submitting the job
     */
    public void setGroupName(final String groupName) {
        this.groupName = groupName;
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
    public Set<ClusterCriteria> getClusterCriteria() {
        return this.clusterCriteria;
    }

    /**
     * Sets the list of cluster criteria specified to pick a cluster.
     *
     * @param clusterCriteria The criteria list
     * @throws CloudServiceException
     */
    public void setClusterCriteria(final Set<ClusterCriteria> clusterCriteria) throws CloudServiceException {
        if (clusterCriteria == null) {
            final String msg = "No criteria passed in to set. Unable to continue.";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }
        this.clusterCriteria = clusterCriteria;
    }

    /**
     * Gets the cmdArgs specified to run the job.
     *
     * @return cmdArgs
     */
    public String getCmdArgs() {
        return this.cmdArgs;
    }

    /**
     * Parameters specified to be run and fed as command line arguments to the
     * job run.
     *
     * @param cmdArgs Arguments to be used to run the command with.
     */
    public void setCmdArgs(final String cmdArgs) {
        this.cmdArgs = cmdArgs;
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
    public FileAttachment[] getAttachments() {
        if (this.attachments != null) {
            return Arrays.copyOf(this.attachments, this.attachments.length);
        } else {
            return new FileAttachment[0];
        }
    }

    /**
     * Set the attachments for this job.
     *
     * @param attachments The attachments to set
     * @throws CloudServiceException
     */
    public void setAttachments(final FileAttachment[] attachments) throws CloudServiceException {
        if (attachments != null) {
            this.attachments = Arrays.copyOf(attachments, attachments.length);
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
     * Gets the cmdArgs specified to run the job.
     *
     * @return cmdArgs
     */
    public String getUserEmail() {
        return this.userEmail;
    }

    /**
     * Set user Email address for the job.
     *
     * @param userEmail user email address
     */
    public void setUserEmail(final String userEmail) {
        this.userEmail = userEmail;
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
    //TODO: Why use Long object here?
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
    //TODO: Why Integer and not just int?
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
    public String getClusterCriteriaString() {
        return this.clusterCriteriaString;
    }

    /**
     * Set the cluster criteria string.
     *
     * @param ccList A list of cluster criteria objects
     * @throws CloudServiceException
     */
    //TODO: Can we use pre-persist/post-persist
    public void setClusterCriteriaString(final Set<ClusterCriteria> ccList) throws CloudServiceException {
        if (ccList == null) {
            final String msg = "No cluster criteria passed in to set. Unable to continue.";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }
        final StringBuilder builder = new StringBuilder();
        for (final ClusterCriteria cc : ccList) {
            builder.append(StringUtils.join(cc.getTags(), ","));
        }
        this.clusterCriteriaString = builder.toString();
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

        final List<String> messages = new ArrayList<String>();
        if (StringUtils.isEmpty(job.getUserName())) {
            messages.add("User name is missing.\n");
        }
        if (StringUtils.isEmpty(job.getCommandId()) && StringUtils.isEmpty(job.getCommandName())) {
            messages.add("Need one of command id or command name in order to run a job\n");
        }
        if (StringUtils.isEmpty(job.getCmdArgs())) {
            messages.add("Command arguments are required\n");
        }
        if (job.getClusterCriteria().isEmpty()) {
            messages.add("At least one cluster criteria is required in order to figure out where to run this job.\n");
        }

        if (!messages.isEmpty()) {
            final StringBuilder builder = new StringBuilder();
            builder.append("Job configuration errors:\n");
            for (final String message : messages) {
                builder.append(message);
            }
            final String msg = builder.toString();
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }
    }
}
