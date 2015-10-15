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
package com.netflix.genie.core.elasticsearch.documents;

import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.ClusterCriteria;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.elasticsearch.annotations.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Representation of the state of a Genie 2.0 job.
 *
 * @author amsharma
 * @author tgianos
 */
@Document(indexName = "genie", type = "job")
public class JobDocument extends CommonDocument {
    /**
     * Used to split between cluster criteria sets.
     */
    protected static final char CRITERIA_SET_DELIMITER = '|';
    /**
     * Used to split between criteria.
     */
    protected static final char CRITERIA_DELIMITER = ',';

    private String commandArgs;
    private String group;
    private String setupFile;
    private String fileDependencies;
    private String email;
    private Set<String> tags = new HashSet<>();
    private String clusterCriteriasString;
    private String commandCriteriaString;
    private String chosenClusterCriteriaString;
    private String executionClusterId;
    private String commandId;
    private JobStatus status;
    private String statusMsg;
    private Date started = new Date(0);
    private Date finished = new Date(0);
    private String clientHost;
    private String hostName;
    private String killURI;
    private String outputURI;
    private int exitCode = -1;
    private String archiveLocation;

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
        return this.stringToClusterCriterias(this.clusterCriteriasString);
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
        this.started = new Date(started.getTime());
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
        this.finished = new Date(finished.getTime());
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
    public void setHostName(final String hostName) {
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
    }

    /**
     * Gets the command criteria which was specified to pick a command to run
     * the job.
     *
     * @return commandCriteria
     */
    public Set<String> getCommandCriteria() {
        return this.stringToCommandCriteria(this.commandCriteriaString);
    }

    /**
     * Sets the set of command criteria specified to pick a command.
     *
     * @param commandCriteria The criteria list. Not null/empty
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    public void setCommandCriteria(final Set<String> commandCriteria) throws GeniePreconditionException {
        if (commandCriteria == null || commandCriteria.isEmpty()) {
            throw new GeniePreconditionException("No command criteria entered. At least one is required");
        }
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
    public void setCommandCriteriaString(final String commandCriteriaString) throws GeniePreconditionException {
        this.commandCriteriaString = commandCriteriaString;
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
     * @param newStatus predefined status
     * @param msg       human-readable message
     */
    public void setJobStatus(final JobStatus newStatus, final String msg) {
        setJobStatus(newStatus);
        setStatusMsg(msg);
    }

    /**
     * Gets the envPropFile name.
     *
     * @return envPropFile - file name containing environment variables.
     */
    public String getSetupFile() {
        return this.setupFile;
    }

    /**
     * Sets the env property file name in string form.
     *
     * @param envPropFile contains the list of env variables to set while
     *                    running this job.
     */
    public void setSetupFile(final String envPropFile) {
        this.setupFile = envPropFile;
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
        return this.chosenClusterCriteriaString;
    }

    /**
     * Sets the criteria used to select cluster to run this job.
     *
     * @param chosenClusterCriteriaString he criteria used to select cluster to
     *                                    run this job.
     */
    public void setChosenClusterCriteriaString(final String chosenClusterCriteriaString) {
        this.chosenClusterCriteriaString = chosenClusterCriteriaString;
    }

    /**
     * Get a DTO representing this job.
     *
     * @return The read-only DTO.
     */
    public com.netflix.genie.common.dto.Job getDTO() {
        return new com.netflix.genie.common.dto.Job.Builder(
                this.getName(),
                this.getUser(),
                this.getVersion(),
                this.commandArgs,
                this.stringToClusterCriterias(this.clusterCriteriasString),
                this.stringToCommandCriteria(this.commandCriteriaString)
        )
                .withId(this.getId())
                .withCreated(this.getCreated())
                .withDescription(this.getDescription())
                .withEmail(this.email)
                .withFileDependencies(
                        this.fileDependencies != null
                                ? Sets.newHashSet(this.fileDependencies.split(","))
                                : new HashSet<>()
                )
                .withGroup(this.group)
                .withSetupFile(this.setupFile)
                .withTags(this.tags)
                .withUpdated(this.getUpdated())
                .withArchiveLocation(this.archiveLocation)
                .withCommandId(this.commandId)
                .withExecutionClusterId(this.executionClusterId)
                .withExitCode(this.exitCode)
                .withFinished(this.finished)
                .withHostName(this.hostName)
                .withKillURI(this.killURI)
                .withOutputURI(this.outputURI)
                .withStarted(this.started)
                .withStatus(this.status)
                .withStatusMsg(this.statusMsg)
                .build();
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
     * @param commandCriteriaToConvert The criteria to build up from
     * @return The cluster criteria string
     */
    protected String commandCriteriaToString(final Set<String> commandCriteriaToConvert) {
        if (commandCriteriaToConvert == null || commandCriteriaToConvert.isEmpty()) {
            return null;
        } else {
            return StringUtils.join(commandCriteriaToConvert, CRITERIA_DELIMITER);
        }
    }

    /**
     * Convert a string to cluster criteria objects.
     *
     * @param criteriaString The string to convert
     * @return The set of ClusterCriteria
     */
    protected Set<String> stringToCommandCriteria(final String criteriaString) {
        final Set<String> c = new HashSet<>();
        if (criteriaString != null) {
            c.addAll(Arrays.asList(StringUtils.split(criteriaString, CRITERIA_DELIMITER)));
        }
        return c;
    }

    /**
     * Convert a string to cluster criteria objects.
     *
     * @param criteriaString The string to convert
     * @return The set of ClusterCriteria
     */
    protected List<ClusterCriteria> stringToClusterCriterias(final String criteriaString)  {
        //Rebuild the cluster criteria objects
        final List<ClusterCriteria> cc = new ArrayList<>();
        if (criteriaString != null) {
            final String[] criteriaSets = StringUtils.split(criteriaString, CRITERIA_SET_DELIMITER);
            for (final String criteriaSet : criteriaSets) {
                final String[] criterias = StringUtils.split(criteriaSet, CRITERIA_DELIMITER);
                if (criterias == null || criterias.length == 0) {
                    continue;
                }
                final Set<String> c = new HashSet<>();
                c.addAll(Arrays.asList(criterias));
                cc.add(new ClusterCriteria(c));
            }
        }
        return cc;
    }
}
