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
package com.netflix.genie.core.jpa.entities;

import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobStatus;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.Length;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.CollectionTable;
import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.MapsId;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 * Representation of the state of a Genie 3.0 job.
 *
 * @author amsharma
 * @author tgianos
 */
@Entity
@Table(name = "jobs")
public class JobEntity extends CommonFields {
    /**
     * Used as default version when one not entered.
     */
    protected static final String DEFAULT_VERSION = "NA";

    @Basic(optional = false)
    @Column(name = "status", nullable = false, length = 20)
    @Enumerated(EnumType.STRING)
    private JobStatus status;

    @Basic
    @Column(name = "status_msg", length = 255)
    @Length(max = 255, message = "Max length in database is 255 characters")
    private String statusMsg;

    @Basic
    @Column(name = "started")
    @Temporal(TemporalType.TIMESTAMP)
    private Date started = new Date(0);

    @Basic
    @Column(name = "finished")
    @Temporal(TemporalType.TIMESTAMP)
    private Date finished = new Date(0);

    @Basic
    @Column(name = "archive_location", length = 1024)
    @Size(max = 1024, message = "Max length in database is 1024 characters")
    private String archiveLocation;

    @Basic
    @Column(name = "cluster_name", length = 255)
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String clusterName;

    @Basic
    @Column(name = "command_name", length = 255)
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String commandName;

    @ElementCollection(fetch = FetchType.LAZY)
    @CollectionTable(
        name = "job_tags",
        joinColumns = @JoinColumn(name = "job_id", referencedColumnName = "id")
    )
    @Column(name = "tag", nullable = false, length = 255)
    private Set<String> tags = new HashSet<>();

    @OneToOne(optional = false, fetch = FetchType.LAZY)
    @JoinColumn(name = "id")
    @MapsId
    private JobRequestEntity request;

    @OneToOne(
        mappedBy = "job",
        fetch = FetchType.LAZY,
        cascade = CascadeType.ALL,
        orphanRemoval = true
    )
    private JobExecutionEntity execution;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "cluster_id")
    private ClusterEntity cluster;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "command_id")
    private CommandEntity command;

    /**
     * Default Constructor.
     */
    public JobEntity() {
        super();
        // Set version to default if not specified
        if (StringUtils.isBlank(this.getVersion())) {
            this.setVersion(DEFAULT_VERSION);
        }
    }

    /**
     * Construct a new Job.
     *
     * @param user    The name of the user running the job. Not null/empty/blank.
     * @param name    The name specified for the job. Not null/empty/blank.
     * @param version The version of this job. Not null/empty/blank.
     */
    public JobEntity(
        final String user,
        final String name,
        final String version
    ) {
        super(name, user, version);

        // Set version to default if not specified
        if (StringUtils.isBlank(this.getVersion())) {
            this.setVersion(DEFAULT_VERSION);
        }
    }

    /**
     * Gets the name of the cluster on which this job was run.
     *
     * @return the cluster name
     */
    public String getClusterName() {
        return this.clusterName;
    }

    /**
     * Sets the name of the cluster on which this job is run.
     *
     * @param clusterName Name of the cluster on which job was executed.
     */
    public void setClusterName(final String clusterName) {
        this.clusterName = clusterName;
    }

    /**
     * Gets the command name for this job.
     *
     * @return The command name
     */
    public String getCommandName() {
        return this.commandName;
    }

    /**
     * Set command Name with which this job is run.
     *
     * @param commandName Name of the command used to run the job
     */
    public void setCommandName(final String commandName) {
        this.commandName = commandName;
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
     * Get the tags for this job.
     *
     * @return The tags as a set
     */
    public Set<String> getJobTags() {
        return this.getSortedTags() == null
            ? Sets.newHashSet()
            : Sets.newHashSet(this.getSortedTags().split(COMMA));
    }

    /**
     * Set the tags for this job.
     *
     * @param jobTags The job tags
     */
    public void setJobTags(final Set<String> jobTags) {
        this.setSortedTags(jobTags);
        this.tags.clear();
        if (jobTags != null) {
            this.tags.addAll(jobTags);
        }
    }

    /**
     * Gets the tags allocated to this job.
     *
     * @return the tags
     */
    protected Set<String> getTags() {
        return this.tags;
    }

    /**
     * Sets the tags allocated to this job.
     *
     * @param tags the tags to set. Not Null.
     */
    protected void setTags(final Set<String> tags) {
        this.tags.clear();
        if (tags != null) {
            this.tags.addAll(tags);
        }
    }

    /**
     * Get the job request for this job.
     *
     * @return The original job request
     */
    protected JobRequestEntity getRequest() {
        return this.request;
    }

    /**
     * Set the job request for this job.
     *
     * @param request The job request. Not null.
     */
    protected void setRequest(final JobRequestEntity request) {
        this.request = request;
    }

    /**
     * Get the job execution for this job.
     *
     * @return The job execution.
     */
    public JobExecutionEntity getExecution() {
        return this.execution;
    }

    /**
     * Set the job execution for this job.
     *
     * @param execution The execution. Not null.
     */
    public void setExecution(@NotNull(message = "Execution can't be null") final JobExecutionEntity execution) {
        this.execution = execution;
    }

    /**
     * Get the cluster this job ran on.
     *
     * @return The cluster
     */
    public ClusterEntity getCluster() {
        return this.cluster;
    }

    /**
     * Set the cluster this job ran on.
     *
     * @param cluster The cluster this job ran on
     */
    public void setCluster(final ClusterEntity cluster) {
        this.cluster = cluster;

        // Reverse side of the relationship
        if (this.cluster != null) {
            this.cluster.addJob(this);
        }
    }

    /**
     * Get the command this job used to run.
     *
     * @return The command
     */
    public CommandEntity getCommand() {
        return this.command;
    }

    /**
     * Set the command used to run this job.
     *
     * @param command The command
     */
    public void setCommand(final CommandEntity command) {
        this.command = command;

        // Reverse side of the relationship
        if (this.command != null) {
            this.command.addJob(this);
        }
    }

    /**
     * Get a DTO representing this job.
     *
     * @return The read-only DTO.
     */
    public Job getDTO() {
        return new Job.Builder(
            this.getName(),
            this.getUser(),
            this.getVersion()
        )
            .withId(this.getId())
            .withClusterName(this.clusterName)
            .withCommandName(this.commandName)
            .withCreated(this.getCreated())
            .withDescription(this.getDescription())
            .withTags(this.getJobTags())
            .withUpdated(this.getUpdated())
            .withArchiveLocation(this.archiveLocation)
            .withFinished(this.finished)
            .withStarted(this.started)
            .withStatus(this.status)
            .withStatusMsg(this.statusMsg)
            .build();
    }
}
