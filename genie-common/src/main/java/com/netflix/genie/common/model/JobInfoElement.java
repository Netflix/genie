/*
 *
 *  Copyright 2013 Netflix, Inc.
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

import java.io.Serializable;
import java.net.HttpURLConnection;
import java.util.Arrays;

import javax.persistence.Cacheable;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.genie.common.exceptions.CloudServiceException;

/**
 * Representation of the state of a Genie job.
 *
 * @author skrishnan
 * @author bmundlapudi
 */
@Entity
@Table(schema = "genie")
@Cacheable(false)
public class JobInfoElement implements Serializable {

    private static final long serialVersionUID = 1L;

    private static Logger logger = LoggerFactory
            .getLogger(JobInfoElement.class);

    /**
     * User given or system generated unique job id.
     */
    @Id
    private String jobID;

    /**
     * User given or system generated job name .
     */
    private String jobName;

    /**
     * human readable description.
     */
    @Lob
    private String description;

    /**
     * User who submitted the job.
     */
    private String userName;

    /**
     * The group user belongs.
     */
    private String groupName;

    /**
     * Source System - UC4, Ab Initio, Search.
     */
    private String userAgent;

    /**
     * Job type - hadoop, pig and hive (upper case in DB).
     */
    private String jobType;

    /**
     * Alias - Cluster Name.
     */
    private String clusterName;

    /**
     * ID for the cluster that is running the job.
     */
    private String clusterId;

    /**
     * Schedule type - adhoc or sla (upper case in DB).
     */
    private String schedule;

    /**
     * Configuration - prod, test, unittest (upper case in DB).
     */
    private String configuration;

    /**
     * Command line arguments.
     */
    @Lob
    private String cmdArgs;

    /**
     * File dependencies.
     */
    @Lob
    private String fileDependencies;

    /**
     * Set of file dependencies, sent as MIME attachments.
     * This is not persisted in the DB for space reasons.
     */
    @Transient
    private FileAttachment[] attachments;

    /**
     * Location of logs being archived to s3.
     */
    @Lob
    private String archiveLocation;

    /**
     * An option to override the pig jar from an S3 URL.
     */
    private String pigOverrideUrl;

    /**
     * An option to override the pig version on the client side.
     */
    private String pigVersion;

    /**
     * An option to override the pig config on the client side.
     */
    private String pigConfigId;

    /**
     * An option to override the hive version on the client side (for minor
     * versions maybe).
     */
    private String hiveVersion;

    /**
     * An option to override the hive config on the client side.
     */
    private String hiveConfigId;

    /**
     * PID for job - updated by the server.
     */
    private int processHandle = -1;

    /**
     * Job status - INIT, RUNNING, SUCCEEDED, KILLED, FAILED (upper case in DB).
     */
    private String status;

    /**
     * More verbose status message.
     */
    private String statusMsg;

    /**
     * Start time for job - initialized to null.
     */
    private Long startTime;

    /**
     * Last update time for job - initialized to null.
     */
    private Long updateTime;

    /**
     * Finish time for job - initialized to zero (for historic reasons).
     */
    private Long finishTime = Long.valueOf(0);

    /**
     * The host/ip address of the client submitting job.
     */
    private String clientHost;

    /**
     * The genie host name on which the job is being run.
     */
    private String hostName;

    /**
     * REST URI to do a HTTP DEL on to kill this job - points to running
     * instance.
     */
    private String killURI;

    /**
     * URI to fetch the stdout/err and logs.
     */
    private String outputURI;

    /**
     * Job exit code.
     */
    private Integer exitCode;

    /**
     * Whether this job was forwarded to new instance or not.
     */
    private boolean forwarded;

    /**
     * Whether to disable archive logs or not - default is false.
     */
    private boolean disableLogArchival;

    /**
     * Get the cluster name where this job is run.
     *
     * @return cluster name
     */
    public String getClusterName() {
        return clusterName;
    }

    /**
     * Set the cluster name to run the job.
     *
     * @param clusterName
     *            cluster name for the job
     */
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    /**
     * Get the unique cluster id to run the job.
     *
     * @return cluster id for job
     */
    public String getClusterId() {
        return clusterId;
    }

    /**
     * Set the cluster id for this job.
     *
     * @param clusterId
     *            unique cluster id for the job
     */
    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    /**
     * Get schedule type for this job.
     *
     * @return schedule - possible values Types.Schedule
     */
    public String getSchedule() {
        return schedule;
    }

    /**
     * Set the schedule type for job.
     *
     * @param schedule
     *            possible values Types.Schedule
     */
    public void setSchedule(String schedule) {
        this.schedule = schedule.toUpperCase();
    }

    /**
     * Get the configuration for the job.
     *
     * @return possible values Types.Configuration
     */
    public String getConfiguration() {
        return configuration;
    }

    /**
     * Set the configuration for the job.
     *
     * @param configuration
     *            possible values Types.Configuration
     */
    public void setConfiguration(String configuration) {
        this.configuration = configuration.toUpperCase();
    }

    /**
     * Get human-readable status message for this job.
     *
     * @return human-readable status
     */
    public String getStatusMsg() {
        return statusMsg;
    }

    /**
     * Set human-readable status message for this job.
     *
     * @param statusMsg
     *            human-readable status
     */
    public void setStatusMsg(String statusMsg) {
        this.statusMsg = statusMsg;
    }

    /**
     * Get host name on which this job runs.
     *
     * @return hostName for job
     */
    public String getHostName() {
        return hostName;
    }

    /**
     * Set the host name for this job.
     *
     * @param hostName
     *            hostName for job, set by server
     */
    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    /**
     * Get the host for the client.
     *
     * @return client host
     */
    public String getClientHost() {
        return clientHost;
    }

    /**
     * Set the client host.
     *
     * @param clientHost
     *            hostname for client
     */
    public void setClientHost(String clientHost) {
        this.clientHost = clientHost;
    }

    /**
     * Get the killURI, which one can run HTTP DELETE to kill the job.
     *
     * @return URI for killing the job
     */
    public String getKillURI() {
        return killURI;
    }

    /**
     * Set the killURI, which one can run HTTP DELETE to kill the job.
     *
     * @param killURI
     *            REST URI for killing job
     */
    public void setKillURI(String killURI) {
        this.killURI = killURI;
    }

    /**
     * Get the URL where one can view results from job.
     *
     * @return output URL to view/browse
     */
    public String getOutputURI() {
        return outputURI;
    }

    /**
     * Set the URL where one can view results from job.
     *
     * @param resultURI
     *            output URL to view/browse
     */
    public void setOutputURI(String resultURI) {
        this.outputURI = resultURI;
    }

    /**
     * Get the name for the job.
     *
     * @return job name
     */
    public String getJobName() {
        return jobName;
    }

    /**
     * Set the job name.
     *
     * @param jobName
     *            name for job
     */
    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    /**
     * Get textual description.
     *
     * @return text description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Set textual description.
     *
     * @param description
     *            text description
     */
    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * Get job status.
     *
     * @return possible values can be found in Types.JobStatus
     */
    public String getStatus() {
        return status;
    }

    /**
     * Set job status.
     *
     * @param jobStatus
     *            possible values found in Types.JobStatus
     * @throws CloudServiceException
     *             if it is a bad request
     */
    public void setStatus(String jobStatus) throws CloudServiceException {
        Types.JobStatus statusEnum = Types.JobStatus.parse(jobStatus);

        if (statusEnum != null) {
            this.status = jobStatus;
        } else {
            String msg = "Unkown Job Status string: " + jobStatus;
            logger.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    msg);
        }
    }

    /**
     * Set job status, and update start/update/finish times, if needed.
     *
     * @param jobStatus
     *            status for job
     */
    public void setJobStatus(Types.JobStatus jobStatus) {
        this.status = jobStatus.name();

        if (jobStatus == Types.JobStatus.INIT) {
            setStartTime(System.currentTimeMillis());
        } else if (jobStatus == Types.JobStatus.SUCCEEDED
                || jobStatus == Types.JobStatus.KILLED
                || jobStatus == Types.JobStatus.FAILED) {
            setFinishTime(System.currentTimeMillis());
        }

        setUpdateTime(System.currentTimeMillis());
    }

    /**
     * Sets job status and human-readable message.
     *
     * @param status
     *            predefined status
     * @param msg
     *            human-readable message
     */
    public void setJobStatus(Types.JobStatus status, String msg) {
        setJobStatus(status);
        setStatusMsg(msg);
    }

    /**
     * Get last update time for job.
     *
     * @return update time in ms
     */
    public Long getUpdateTime() {
        return updateTime;
    }

    /**
     * Set last update time for job.
     *
     * @param updateTime
     *            epoch time in ms
     */
    public void setUpdateTime(Long updateTime) {
        this.updateTime = updateTime;
    }

    /**
     * Get pid/process handle for job.
     *
     * @return pid for job
     */
    public int getProcessHandle() {
        return processHandle;
    }

    /**
     * Set pid/process handle for job.
     *
     * @param processHandle
     *            pid/process handle
     */
    public void setProcessHandle(int processHandle) {
        this.processHandle = processHandle;
    }

    /**
     * Get job start time.
     *
     * @return start time in ms
     */
    public Long getStartTime() {
        return startTime;
    }

    /**
     * Set start time for job.
     *
     * @param startTime
     *            epoch time in ms
     */
    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    /**
     * Get finish time for job.
     *
     * @return finish time in ms
     */
    public Long getFinishTime() {
        return finishTime;
    }

    /**
     * Set finish time for job.
     *
     * @param finishTime
     *            epoch time in ms
     */
    public void setFinishTime(Long finishTime) {
        this.finishTime = finishTime;
    }

    /**
     * Get file dependencies (CSV) for job.
     *
     * @return csv of file dependencies
     */
    public String getFileDependencies() {
        return fileDependencies;
    }

    /**
     * Set file dependencies for job.
     *
     * @param fileDependencies
     *            csv of file dependencies
     */
    public void setFileDependencies(String fileDependencies) {
        this.fileDependencies = fileDependencies;
    }


    /**
     * Get the set of attachments for this job.
     *
     * @return the set of attachments for this job
     */
    public FileAttachment[] getAttachments() {
        if (attachments == null) {
            return null;
        } else {
            return Arrays.copyOf(attachments, attachments.length);
        }
    }

    /**
     * Set the attachments for this job.
     *
     * @param attachments the attachments for this job
     */
    public void setAttachments(FileAttachment[] attachments) {
        if (attachments == null) {
            this.attachments = null;
        } else {
            this.attachments = Arrays.copyOf(attachments,
                    attachments.length);
        }
    }

    /**
     * Get location of pig on s3/hdfs to override default.
     *
     * @return pig override on s3/hdfs
     */
    public String getPigOverrideUrl() {
        return pigOverrideUrl;
    }

    /**
     * Set location of pig on s3/hdfs to override default.
     *
     * @param pigOverrideUrl
     *            pig override location on s3/hdfs
     */
    public void setPigOverrideUrl(String pigOverrideUrl) {
        this.pigOverrideUrl = pigOverrideUrl;
    }

    /**
     * Get version of Pig to use/override.
     *
     * @return version of Pig
     */
    public String getPigVersion() {
        return pigVersion;
    }

    /**
     * Override default version of Pig.
     *
     * @param pigVersion
     *            version of Pig to override, if needed
     */
    public void setPigVersion(String pigVersion) {
        this.pigVersion = pigVersion;
    }

    /**
     * Get version of Hive to use/override.
     *
     * @return version of Hive
     */
    public String getHiveVersion() {
        return hiveVersion;
    }

    /**
     * Override default version of Hive.
     *
     * @param hiveVersion
     *            version of Hive to override, if needed
     */
    public void setHiveVersion(String hiveVersion) {
        this.hiveVersion = hiveVersion;
    }

    /**
     * Get the pig config to use/override.
     *
     * @return pig config to use/override
     */
    public String getPigConfigId() {
        return pigConfigId;
    }

    /**
     * Set the the pig config to use/override.
     *
     * @param pigConfigId
     *            the pig config to use/override
     */
    public void setPigConfigId(String pigConfigId) {
        this.pigConfigId = pigConfigId;
    }

    /**
     * Get the hive config to use/override.
     *
     * @return the hive config to use/override
     */
    public String getHiveConfigId() {
        return hiveConfigId;
    }

    /**
     * Set the hive config to use/override.
     *
     * @param hiveConfigId
     *            the hive config to use/override
     */
    public void setHiveConfigId(String hiveConfigId) {
        this.hiveConfigId = hiveConfigId;
    }

    /**
     * Get location where logs are archived.
     *
     * @return s3/hdfs location where logs are archived
     */
    public String getArchiveLocation() {
        return archiveLocation;
    }

    /**
     * Set location where logs are archived.
     *
     * @param archiveLocation
     *            s3/hdfs location where logs are archived
     */
    public void setArchiveLocation(String archiveLocation) {
        this.archiveLocation = archiveLocation;
    }

    /**
     * Get unique ID for job.
     *
     * @return unique job id
     */
    public String getJobID() {
        return jobID;
    }

    /**
     * Set unique ID for job.
     *
     * @param jobID
     *            unique id for job
     */
    public void setJobID(String jobID) {
        this.jobID = jobID;
    }

    /**
     * Get the user name for job submission.
     *
     * @return user name
     */
    public String getUserName() {
        return userName;
    }

    /**
     * Set the user name for job submission.
     *
     * @param userName
     *            user name
     */
    public void setUserName(String userName) {
        this.userName = userName;
    }

    /**
     * Get the group name for user.
     *
     * @return user group
     */
    public String getGroupName() {
        return groupName;
    }

    /**
     * Set group name for user.
     *
     * @param groupName
     *            user group
     */
    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    /**
     * Get user's agent - for e.g. java-client, scheduler, etc.
     *
     * @return user agent
     */
    public String getUserAgent() {
        return userAgent;
    }

    /**
     * Set user's agent - for e.g. java-client, scheduler, etc.
     *
     * @param userAgent
     *            string that conceptually represents what submitted this job
     */
    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    /**
     * Get command-line arguments for job.
     *
     * @return command-line args for job
     */
    public String getCmdArgs() {
        return cmdArgs;
    }

    /**
     * Set the command-line arguments for job.
     *
     * @param cmdArgs
     *            command-line args for job
     */
    public void setCmdArgs(String cmdArgs) {
        this.cmdArgs = cmdArgs;
    }

    /**
     * Get job type.
     *
     * @return possible values Types.JobType
     */
    public String getJobType() {
        return jobType;
    }

    /**
     * Set job type.
     *
     * @param jobType
     *            possible values Types.JobType
     */
    public void setJobType(String jobType) {
        this.jobType = jobType.toUpperCase();
    }

    /**
     * Get exit code for job.
     *
     * @return job exit code
     */
    public Integer getExitCode() {
        return exitCode;
    }

    /**
     * Set exit code for job.
     *
     * @param exitCode
     *            job exit code
     */
    public void setExitCode(Integer exitCode) {
        this.exitCode = exitCode;
    }

    /**
     * Has the job been forwarded to another instance.
     *
     * @return true, if forwarded
     */
    public boolean isForwarded() {
        return forwarded;
    }

    /**
     * Has the job been forwarded to another instance.
     *
     * @param forwarded
     *            true, if forwarded
     */
    public void setForwarded(boolean forwarded) {
        this.forwarded = forwarded;
    }

    /**
     * Whether to disable log archival for this job or not - defaults to false.
     *
     * @return false if logs are to be archived, true otherwise
     */
    public boolean getDisableLogArchival() {
        return disableLogArchival;
    }

    /**
     * Set parameter to disable log archival.
     *
     * @param disableLogArchival if true logs are archived, else not archived
     */
    public void setDisableLogArchival(boolean disableLogArchival) {
        this.disableLogArchival = disableLogArchival;
    }
}
