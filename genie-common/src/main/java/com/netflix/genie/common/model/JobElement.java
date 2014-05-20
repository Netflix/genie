package com.netflix.genie.common.model;

import java.io.Serializable;

import java.util.Iterator;
import java.util.ArrayList;

import javax.persistence.Basic;
import javax.persistence.Cacheable;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Table;
import javax.persistence.Transient;

/**
 * Representation of the state of a Genie 1.0 job.
 * 
 * @author amsharma
 */
@Entity
@Table(schema = "genie")
@Cacheable(false)
public class JobElement implements Serializable {

    private static final long serialVersionUID = 2979506788441089067L;
    
    // ------------------------------------------------------------------------
    // GENERAL COMMON PARAMS FOR ALL JOBS - TO BE SPECIFIED BY CLIENTS
    // ------------------------------------------------------------------------

    /**
     * User-specified or system-generated unique job id.
     */
    @Id
    private String jobID;

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
    @Basic
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
    //private ArrayList<ArrayList<String>> clusterCriteriaList;
    //private ArrayList<String> clusterCriteriaList;
    private ArrayList<ClusterCriteria> clusterCriteriaList;
    
    /**
     * String representation of the the cluster criteria array list object above.
     * TO DO: use pre/post persist to store the above list into the DB
     */
    @Lob
    private String clusterCriteriaString;
    
    /**
     * Command line arguments (REQUIRED).
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
     * Whether to disable archive logs or not - default is false.
     */
    @Basic
    private boolean disableLogArchival;

    /**
     * Email address of the user where he expects an email.
     * This is sent once the aladdin job completes.
     */
    @Basic
    private String userEmail;

    // ------------------------------------------------------------------------


    // ------------------------------------------------------------------------
    // HADOOP 2.0 SPECIFIC PARAMS FOR ALL COMMANDS - TO BE SPECIFIED BY CLIENTS
    // ------------------------------------------------------------------------

    /**
     * Application name - e.g. MR, Tez (as supported by the command).
     */
    @Basic
    private String applicationName;
    
    /**
     * Application Id to pin to specific application id 
     * e.g. TBD.
     */
    @Basic
    private String applicationId;

    /**
     * Command name to run - e.g. prodhive, testhive, prodpig, testpig.
     */
    @Basic
    private String commandName;
    
    /**
     * Command Id to run - Used to pin to a particular command
     * e.g. prodhive11_mr1
     */
    @Basic
    private String commandId;

    // ------------------------------------------------------------------------

    // ------------------------------------------------------------------------
    // GENERAL COMMON STUFF FOR ALL JOBS
    // TO BE GENERATED/USED BY SERVER
    // ------------------------------------------------------------------------

    /**
     * Job type - hadoop, pig and hive (upper case in DB).
     */
    @Basic
    private String jobType;
    
    /**
     * PID for job - updated by the server.
     */
    @Basic
    private int processHandle = -1;

    /**
     * Job status - INIT, RUNNING, SUCCEEDED, KILLED, FAILED (upper case in DB).
     */
    @Basic
    private String status;

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
     * Last update time for job - initialized to null.
     */
    @Basic
    private Long updateTime;

    /**
     * Finish time for job - initialized to zero (for historic reasons).
     */
    @Basic
    private Long finishTime = Long.valueOf(0);

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

    public String getJobID() {
        return jobID;
    }

    public void setJobID(String jobID) {
        this.jobID = jobID;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public String getClient() {
        return client;
    }

    public void setClient(String client) {
        this.client = client;
    }

    public String getJobType() {
        return jobType;
    }

    public void setJobType(String jobType) {
        this.jobType = jobType;
    }

    public String getExecutionClusterName() {
        return executionClusterName;
    }
    
    public void setExecutionClusterName(String executionClusterName) {
        this.executionClusterName = executionClusterName;
    }

    public String getExecutionClusterId() {
        return executionClusterId;
    }
    
    public void setExecutionClusterId(String executionClusterId) {
        this.executionClusterId = executionClusterId;
    }

    public ArrayList<ClusterCriteria> getClusterCriteriaList() {
        return clusterCriteriaList;
    }

    public void setClusterCriteriaList(ArrayList<ClusterCriteria> clusterCriteriaList) {
        this.clusterCriteriaList = clusterCriteriaList;
    }

    public String getCmdArgs() {
        return cmdArgs;
    }

    public void setCmdArgs(String cmdArgs) {
        this.cmdArgs = cmdArgs;
    }

    public String getFileDependencies() {
        return fileDependencies;
    }

    public void setFileDependencies(String fileDependencies) {
        this.fileDependencies = fileDependencies;
    }

    public FileAttachment[] getAttachments() {
        return attachments;
    }

    public void setAttachments(FileAttachment[] attachments) {
        this.attachments = attachments;
    }

    public boolean isDisableLogArchival() {
        return disableLogArchival;
    }

    public void setDisableLogArchival(boolean disableLogArchival) {
        this.disableLogArchival = disableLogArchival;
    }

    public String getUserEmail() {
        return userEmail;
    }

    public void setUserEmail(String userEmail) {
        this.userEmail = userEmail;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }
    
    public String getCommandName() {
        return commandName;
    }

    public void setCommandName(String commandName) {
        this.commandName = commandName;
    }
    
    public String getCommandId() {
        return commandId;
    }

    public void setCommandId(String commandId) {
        this.commandId = commandId;
    }

    public int getProcessHandle() {
        return processHandle;
    }

    public void setProcessHandle(int processHandle) {
        this.processHandle = processHandle;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getStatusMsg() {
        return statusMsg;
    }

    public void setStatusMsg(String statusMsg) {
        this.statusMsg = statusMsg;
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public Long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Long updateTime) {
        this.updateTime = updateTime;
    }

    public Long getFinishTime() {
        return finishTime;
    }

    public void setFinishTime(Long finishTime) {
        this.finishTime = finishTime;
    }

    public String getClientHost() {
        return clientHost;
    }

    public void setClientHost(String clientHost) {
        this.clientHost = clientHost;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public String getKillURI() {
        return killURI;
    }

    public void setKillURI(String killURI) {
        this.killURI = killURI;
    }

    public String getOutputURI() {
        return outputURI;
    }

    public void setOutputURI(String outputURI) {
        this.outputURI = outputURI;
    }

    public Integer getExitCode() {
        return exitCode;
    }

    public void setExitCode(Integer exitCode) {
        this.exitCode = exitCode;
    }

    public boolean isForwarded() {
        return forwarded;
    }

    public void setForwarded(boolean forwarded) {
        this.forwarded = forwarded;
    }

    public String getArchiveLocation() {
        return archiveLocation;
    }

    public void setArchiveLocation(String archiveLocation) {
        this.archiveLocation = archiveLocation;
    }

    public String getClusterCriteriaString() {
        return clusterCriteriaString;
    }

    public void setClusterCriteriaString(String clusterCriteriaString) {
        this.clusterCriteriaString = clusterCriteriaString;
    }
    
    public void setClusterCriteriaString(ArrayList<String> ccl) {
        if(ccl != null) {
            Iterator<String> it = ccl.iterator();
            this.clusterCriteriaString = new String();
            while(it.hasNext()) {
                String tag = (String)it.next();
                this.clusterCriteriaString += tag;
                this.clusterCriteriaString += ",";
            }
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
     * Gets the envPropFile name 
     *
     * @return envPropFile - file name containing environment variables.
     */
    public String getEnvPropFile() {
        return envPropFile;
    }

    /**
     * Sets the env property file name in string form.
     *
     * @param envPropFile
     *           contains the list of env variables to set while running this job.
     */
    public void setEnvPropFile(String envPropFile) {
        this.envPropFile = envPropFile;
    }
}