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

import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobStatus;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.validator.constraints.Length;

import javax.annotation.Nullable;
import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.MapsId;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.OneToOne;
import javax.persistence.OrderColumn;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

/**
 * Representation of the state of a Genie 3.0 job.
 *
 * @author amsharma
 * @author tgianos
 */
@Getter
@Setter
@Entity
@Table(name = "jobs")
@NamedQueries({
    @NamedQuery(
        name = JobEntity.QUERY_GET_STATUS_BY_ID,
        query = "select j.status from JobEntity j where j.id = :id"
    )
})
public class JobEntity extends CommonFieldsEntity {
    /**
     * Query name to get job status.
     */
    public static final String QUERY_GET_STATUS_BY_ID = "getStatusById";
    /**
     * Used as default version when one not entered.
     */
    protected static final String DEFAULT_VERSION = "NA";

    private static final long serialVersionUID = 2849367731657512224L;

    @Basic(optional = false)
    @Column(name = "command_args", nullable = false, length = 10000)
    @Size(min = 1, max = 10000, message = "Must have command line arguments and be no longer than 10000 characters")
    private String commandArgs;

    @Basic(optional = false)
    @Column(name = "status", nullable = false, length = 20)
    @Enumerated(EnumType.STRING)
    private JobStatus status = JobStatus.INIT;

    @Basic
    @Column(name = "status_msg")
    @Length(max = 255, message = "Max length in database is 255 characters")
    private String statusMsg;

    @Basic
    @Column(name = "started")
    @Temporal(TemporalType.TIMESTAMP)
    private Date started;

    @Basic
    @Column(name = "finished")
    @Temporal(TemporalType.TIMESTAMP)
    private Date finished;

    @Basic
    @Column(name = "archive_location", length = 1024)
    @Size(max = 1024, message = "Max length in database is 1024 characters")
    private String archiveLocation;

    @Basic
    @Column(name = "cluster_name")
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String clusterName;

    @Basic
    @Column(name = "command_name")
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String commandName;

    @OneToOne(optional = false, fetch = FetchType.LAZY)
    @JoinColumn(name = "id")
    @MapsId
    private JobRequestEntity request;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "cluster_id")
    private ClusterEntity cluster;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "command_id")
    private CommandEntity command;

    @ManyToMany(fetch = FetchType.LAZY)
    @JoinTable(
        name = "jobs_applications",
        joinColumns = {
            @JoinColumn(name = "job_id", referencedColumnName = "id", nullable = false)
        },
        inverseJoinColumns = {
            @JoinColumn(name = "application_id", referencedColumnName = "id", nullable = false)
        }
    )
    @OrderColumn(name = "application_order", nullable = false)
    private List<ApplicationEntity> applications = new ArrayList<>();

    /**
     * Default Constructor.
     */
    public JobEntity() {
        super();
        this.setVersion(DEFAULT_VERSION);
    }

    /**
     * Gets the name of the cluster on which this job was run.
     *
     * @return the cluster name
     */
    public Optional<String> getClusterName() {
        return Optional.ofNullable(this.clusterName);
    }

    /**
     * Sets the name of the cluster on which this job is run.
     *
     * @param clusterName Name of the cluster on which job was executed.
     */
    protected void setClusterName(final String clusterName) {
        this.clusterName = clusterName;
    }

    /**
     * Gets the command name for this job.
     *
     * @return The command name
     */
    public Optional<String> getCommandName() {
        return Optional.ofNullable(this.commandName);
    }

    /**
     * Set command Name with which this job is run.
     *
     * @param commandName Name of the command used to run the job
     */
    protected void setCommandName(final String commandName) {
        this.commandName = commandName;
    }

    /**
     * Gets the status message or this job.
     *
     * @return statusMsg
     */
    public Optional<String> getStatusMsg() {
        return Optional.ofNullable(this.statusMsg);
    }

    /**
     * Gets the start time for this job.
     *
     * @return startTime as a java.util.Date or null if not yet started
     */
    public Optional<Date> getStarted() {
        return this.started == null ? Optional.empty() : Optional.of(new Date(this.started.getTime()));
    }

    /**
     * Set the startTime for the job.
     *
     * @param started epoch time as java.util.Date or null
     */
    public void setStarted(@Nullable final Date started) {
        this.started = started == null ? null : new Date(started.getTime());
    }

    /**
     * Gets the finish time for this job.
     *
     * @return finished. The job finish timestamp.
     */
    public Optional<Date> getFinished() {
        return this.finished == null ? Optional.empty() : Optional.of(new Date(this.finished.getTime()));
    }

    /**
     * Set the finishTime for the job.
     *
     * @param finished The finished time.
     */
    public void setFinished(@Nullable final Date finished) {
        this.finished = finished == null ? null : new Date(finished.getTime());
    }

    /**
     * Get location where logs are archived.
     *
     * @return s3/hdfs location where logs are archived
     */
    public Optional<String> getArchiveLocation() {
        return Optional.ofNullable(this.archiveLocation);
    }

    /**
     * Set job status, and update start/update/finish times, if needed.
     *
     * @param jobStatus status for job
     */
    public void setJobStatus(@NotNull final JobStatus jobStatus) {
        this.status = jobStatus;

        if (jobStatus == JobStatus.INIT) {
            this.setStarted(new Date());
        } else if (jobStatus != JobStatus.RUNNING) {
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
    public void setRequest(final JobRequestEntity request) {
        this.request = request;
    }

    /**
     * Set the cluster this job ran on.
     *
     * @param cluster The cluster this job ran on
     */
    public void setCluster(final ClusterEntity cluster) {
        if (this.cluster != null) {
            this.clusterName = null;
        }

        this.cluster = cluster;

        // Reverse side of the relationship
        if (this.cluster != null) {
            this.clusterName = cluster.getName();
        }
    }

    /**
     * Set the command used to run this job.
     *
     * @param command The command
     */
    public void setCommand(final CommandEntity command) {
        if (this.command != null) {
            this.commandName = null;
        }

        this.command = command;

        // Reverse side of the relationship
        if (this.command != null) {
            this.commandName = command.getName();
        }
    }

    /**
     * Set the applications used to run this job.
     *
     * @param applications The applications
     */
    public void setApplications(final List<ApplicationEntity> applications) {
        this.applications.clear();
        if (applications != null) {
            this.applications.addAll(applications);
        }
    }

    /**
     * Get a DTO representing this job.
     *
     * @return The read-only DTO.
     */
    public Job getDTO() {
        final Job.Builder builder = new Job.Builder(
            this.getName(),
            this.getUser(),
            this.getVersion(),
            this.commandArgs
        )
            .withId(this.getId())
            .withClusterName(this.clusterName)
            .withCommandName(this.commandName)
            .withCreated(this.getCreated())
            .withTags(this.getTags())
            .withUpdated(this.getUpdated())
            .withArchiveLocation(this.archiveLocation)
            .withFinished(this.finished)
            .withStarted(this.started)
            .withStatus(this.status)
            .withStatusMsg(this.statusMsg);

        this.getDescription().ifPresent(builder::withDescription);

        return builder.build();
    }
}
