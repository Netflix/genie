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
package com.netflix.genie.web.jpa.entities;

import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.web.jpa.entities.projections.JobApplicationsProjection;
import com.netflix.genie.web.jpa.entities.projections.JobClusterProjection;
import com.netflix.genie.web.jpa.entities.projections.JobCommandProjection;
import com.netflix.genie.web.jpa.entities.projections.JobExecutionProjection;
import com.netflix.genie.web.jpa.entities.projections.JobMetadataProjection;
import com.netflix.genie.web.jpa.entities.projections.JobProjection;
import com.netflix.genie.web.jpa.entities.projections.JobRequestProjection;
import com.netflix.genie.web.jpa.entities.projections.JobSearchProjection;
import com.netflix.genie.web.jpa.specifications.JpaSpecificationUtils;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.annotation.Nullable;
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
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OrderColumn;
import javax.persistence.PrePersist;
import javax.persistence.Table;
import javax.validation.constraints.Email;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Representation of the state of a Genie job.
 *
 * @author amsharma
 * @author tgianos
 */
@Getter
@Setter
@ToString(
    callSuper = true,
    of = {
        "genieUserGroup",
        "disableLogArchival",
        "email",
        "cpuRequested",
        "memoryRequested",
        "timeoutRequested",
        "grouping",
        "groupingInstance",
        "clientHost",
        "userAgent",
        "numAttachments",
        "totalSizeOfAttachments",
        "stdOutSize",
        "stdErrSize",
        "clusterName",
        "commandName",
        "status",
        "statusMsg",
        "started",
        "finished",
        "hostName",
        "processId",
        "checkDelay",
        "exitCode",
        "memoryUsed",
        "timeout",
        "archiveLocation"
    }
)
@Entity
@Table(name = "jobs")
public class JobEntity extends BaseEntity implements
    JobProjection,
    JobRequestProjection,
    JobMetadataProjection,
    JobExecutionProjection,
    JobApplicationsProjection,
    JobClusterProjection,
    JobCommandProjection,
    JobSearchProjection {

    private static final long serialVersionUID = 2849367731657512224L;

    @Basic
    @Column(name = "tags", length = 1024, updatable = false)
    @Size(max = 1024, message = "Max length in database is 1024 characters")
    @Getter(AccessLevel.PACKAGE)
    @Setter(AccessLevel.NONE)
    private String tagSearchString;

    @Basic
    @Column(name = "genie_user_group", updatable = false)
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String genieUserGroup;

    @Basic(optional = false)
    @Column(name = "disable_log_archival", nullable = false, updatable = false)
    private boolean disableLogArchival;

    @Basic
    @Column(name = "email", updatable = false)
    @Email
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String email;

    @Basic
    @Column(name = "cpu_requested", updatable = false)
    @Min(value = 1, message = "Can't have less than 1 CPU")
    private Integer cpuRequested;

    @Basic
    @Column(name = "memory_requested", updatable = false)
    @Min(value = 1, message = "Can't have less than 1 MB of memory allocated")
    private Integer memoryRequested;

    @Basic
    @Column(name = "timeout_requested", updatable = false)
    @Min(value = 1)
    private Integer timeoutRequested;

    @Basic
    @Column(name = "grouping", updatable = false)
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String grouping;

    @Basic
    @Column(name = "grouping_instance", updatable = false)
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String groupingInstance;

    @Basic
    @Column(name = "client_host", updatable = false)
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String clientHost;

    @Basic
    @Column(name = "user_agent", length = 1024, updatable = false)
    @Size(max = 1024, message = "Max length in database is 1024 characters")
    private String userAgent;

    @Basic
    @Column(name = "num_attachments", updatable = false)
    @Min(value = 0, message = "Can't have less than zero attachments")
    private Integer numAttachments;

    @Basic
    @Column(name = "total_size_of_attachments", updatable = false)
    @Min(value = 0, message = "Can't have less than zero bytes total attachment size")
    private Long totalSizeOfAttachments;

    @Basic
    @Column(name = "std_out_size")
    @Min(value = 0, message = "Can't have less than zero bytes for std out size")
    private Long stdOutSize;

    @Basic
    @Column(name = "std_err_size")
    @Min(value = 0, message = "Can't have less than zero bytes for std err size")
    private Long stdErrSize;

    @Basic
    @Column(name = "cluster_name")
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String clusterName;

    @Basic
    @Column(name = "command_name")
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String commandName;

    @Basic(optional = false)
    @Column(name = "status", nullable = false, length = 20)
    @Enumerated(EnumType.STRING)
    private JobStatus status = JobStatus.INIT;

    @Basic
    @Column(name = "status_msg")
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String statusMsg;

    @Basic
    @Column(name = "started")
    private Instant started;

    @Basic
    @Column(name = "finished")
    private Instant finished;

    @Basic(optional = false)
    @Column(name = "host_name", nullable = false, updatable = false)
    @Size(min = 1, max = 255, message = "Must have a host name no longer than 255 characters")
    private String hostName;

    @Basic
    @Column(name = "process_id")
    private Integer processId;

    @Basic
    @Column(name = "check_delay")
    @Min(1)
    private Long checkDelay;

    @Basic
    @Column(name = "exit_code")
    private Integer exitCode;

    @Basic
    @Column(name = "memory_used")
    private Integer memoryUsed;

    @Basic
    @Column(name = "timeout")
    private Instant timeout;

    @Basic
    @Column(name = "archive_location", length = 1024)
    @Size(max = 1024, message = "Max length in database is 1024 characters")
    private String archiveLocation;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "cluster_id")
    private ClusterEntity cluster;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "command_id")
    private CommandEntity command;

    @ElementCollection
    @CollectionTable(
        name = "job_command_arguments",
        joinColumns = {
            @JoinColumn(name = "job_id", nullable = false, updatable = false)
        }
    )
    @Column(name = "argument", length = 10_000, nullable = false, updatable = false)
    @OrderColumn(name = "argument_order", nullable = false, updatable = false)
    private List<@NotBlank @Size(max = 10_000) String> commandArgs = new ArrayList<>();

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
    @OrderColumn(name = "application_order", nullable = false, updatable = false)
    private List<ApplicationEntity> applications = new ArrayList<>();

    @ManyToMany(fetch = FetchType.LAZY, cascade = CascadeType.ALL)
    @JoinTable(
        name = "jobs_cluster_criteria",
        joinColumns = {
            @JoinColumn(name = "job_id", referencedColumnName = "id", nullable = false)
        },
        inverseJoinColumns = {
            @JoinColumn(name = "criterion_id", referencedColumnName = "id", nullable = false)
        }
    )
    @OrderColumn(name = "priority_order", nullable = false, updatable = false)
    private List<CriterionEntity> clusterCriteria = new ArrayList<>();

    @ManyToOne(fetch = FetchType.LAZY, cascade = CascadeType.ALL)
    @JoinColumn(name = "command_criterion", updatable = false)
    private CriterionEntity commandCriterion;

    @ElementCollection
    @CollectionTable(
        name = "job_applications_requested",
        joinColumns = {
            @JoinColumn(name = "job_id", nullable = false, updatable = false)
        }
    )
    @Column(name = "application_id", nullable = false, updatable = false)
    @OrderColumn(name = "application_order", nullable = false, updatable = false)
    private List<String> applicationsRequested = new ArrayList<>();

    @ManyToMany(fetch = FetchType.LAZY)
    @JoinTable(
        name = "jobs_configs",
        joinColumns = {
            @JoinColumn(name = "job_id", referencedColumnName = "id", nullable = false, updatable = false)
        },
        inverseJoinColumns = {
            @JoinColumn(name = "file_id", referencedColumnName = "id", nullable = false, updatable = false)
        }
    )
    private Set<FileEntity> configs = new HashSet<>();

    @ManyToMany(fetch = FetchType.LAZY)
    @JoinTable(
        name = "jobs_dependencies",
        joinColumns = {
            @JoinColumn(name = "job_id", referencedColumnName = "id", nullable = false, updatable = false)
        },
        inverseJoinColumns = {
            @JoinColumn(name = "file_id", referencedColumnName = "id", nullable = false, updatable = false)
        }
    )
    private Set<FileEntity> dependencies = new HashSet<>();

    @ManyToMany(fetch = FetchType.LAZY)
    @JoinTable(
        name = "jobs_tags",
        joinColumns = {
            @JoinColumn(name = "job_id", referencedColumnName = "id", nullable = false, updatable = false)
        },
        inverseJoinColumns = {
            @JoinColumn(name = "tag_id", referencedColumnName = "id", nullable = false, updatable = false)
        }
    )
    private Set<TagEntity> tags = new HashSet<>();

    /**
     * Default Constructor.
     */
    public JobEntity() {
        super();
    }

    /**
     * Before a job is created create the job search string.
     */
    @PrePersist
    void onCreateJob() {
        if (!this.tags.isEmpty()) {
            // Tag search string length max is currently 1024 which will be caught by hibernate validator if this
            // exceeds that length
            this.tagSearchString = JpaSpecificationUtils.createTagSearchString(this.tags);
        }
    }

    /**
     * Get the user group for this job.
     *
     * @return the user group
     */
    public Optional<String> getGenieUserGroup() {
        return Optional.ofNullable(this.genieUserGroup);
    }

    /**
     * Get the command criterion if one exists.
     *
     * @return The command criterion if one exists
     */
    public Optional<CriterionEntity> getCommandCriterion() {
        return Optional.ofNullable(this.commandCriterion);
    }

    /**
     * Get the email of the user associated with this job if they desire an email notification at completion
     * of the job.
     *
     * @return The email
     */
    public Optional<String> getEmail() {
        return Optional.ofNullable(this.email);
    }

    /**
     * Get the number of CPU's requested to run this job.
     *
     * @return The number of CPU's as an Optional
     */
    public Optional<Integer> getCpuRequested() {
        return Optional.ofNullable(this.cpuRequested);
    }

    /**
     * Get the memory requested to run this job with.
     *
     * @return The amount of memory the user requested for this job in MB as an Optional
     */
    public Optional<Integer> getMemoryRequested() {
        return Optional.ofNullable(this.memoryRequested);
    }

    /**
     * Get the timeout (in seconds) requested by the user for this job.
     *
     * @return The number of seconds before a timeout as an Optional
     */
    public Optional<Integer> getTimeoutRequested() {
        return Optional.ofNullable(this.timeoutRequested);
    }

    /**
     * Set the command criterion.
     *
     * @param commandCriterion The criterion. Null clears reference.
     */
    public void setCommandCriterion(@Nullable final CriterionEntity commandCriterion) {
        this.commandCriterion = commandCriterion;
    }

    /**
     * Get the grouping this job is a part of. e.g. scheduler job name for job run many times
     *
     * @return The grouping
     */
    public Optional<String> getGrouping() {
        return Optional.ofNullable(this.grouping);
    }

    /**
     * Get the instance identifier of a grouping. e.g. the run id of a given scheduled job
     *
     * @return The grouping instance
     */
    public Optional<String> getGroupingInstance() {
        return Optional.ofNullable(this.groupingInstance);
    }

    /**
     * Get the client host.
     *
     * @return Optional of the client host
     */
    public Optional<String> getClientHost() {
        return Optional.ofNullable(this.clientHost);
    }

    /**
     * Get the user agent.
     *
     * @return Optional of the user agent
     */
    public Optional<String> getUserAgent() {
        return Optional.ofNullable(this.userAgent);
    }

    /**
     * Get the number of attachments.
     *
     * @return The number of attachments as an optional
     */
    public Optional<Integer> getNumAttachments() {
        return Optional.ofNullable(this.numAttachments);
    }

    /**
     * Get the total size of the attachments.
     *
     * @return The total size of attachments as an optional
     */
    public Optional<Long> getTotalSizeOfAttachments() {
        return Optional.ofNullable(this.totalSizeOfAttachments);
    }

    /**
     * Get the size of standard out for this job.
     *
     * @return The size (in bytes) of this jobs standard out file as Optional
     */
    public Optional<Long> getStdOutSize() {
        return Optional.ofNullable(this.stdOutSize);
    }

    /**
     * Get the size of standard error for this job.
     *
     * @return The size (in bytes) of this jobs standard error file as Optional
     */
    public Optional<Long> getStdErrSize() {
        return Optional.ofNullable(this.stdErrSize);
    }

    /**
     * Set the total size in bytes of the std err file for this job.
     *
     * @param stdErrSize The size. Null empties database field
     */
    public void setStdErrSize(@Nullable final Long stdErrSize) {
        this.stdErrSize = stdErrSize;
    }

    /**
     * Set the total size in bytes of the std out file for this job.
     *
     * @param stdOutSize The size. Null empties database field
     */
    public void setStdOutSize(@Nullable final Long stdOutSize) {
        this.stdOutSize = stdOutSize;
    }

    /**
     * Set job status, and update start/update/finish times, if needed.
     *
     * @param jobStatus status for job
     */
    void setJobStatus(@NotNull final JobStatus jobStatus) {
        this.status = jobStatus;

        if (jobStatus == JobStatus.INIT) {
            this.setStarted(Instant.now());
        } else if (jobStatus.isFinished()) {
            this.setFinished(Instant.now());
        }
    }

    /**
     * Sets job status and human-readable message.
     *
     * @param newStatus predefined status
     * @param msg       human-readable message
     */
    void setJobStatus(@NotNull final JobStatus newStatus, final String msg) {
        this.setJobStatus(newStatus);
        this.setStatusMsg(msg);
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
     * @return startTime or empty optional if not yet started
     */
    public Optional<Instant> getStarted() {
        return Optional.ofNullable(this.started);
    }

    /**
     * Set the start time for the job.
     *
     * @param started The started time.
     */
    public void setStarted(@Nullable final Instant started) {
        this.started = started;
    }

    /**
     * Gets the finish time for this job.
     *
     * @return finished. The job finish timestamp.
     */
    public Optional<Instant> getFinished() {
        return Optional.ofNullable(this.finished);
    }

    /**
     * Set the finishTime for the job.
     *
     * @param finished The finished time.
     */
    public void setFinished(@Nullable final Instant finished) {
        this.finished = finished;
    }

    /**
     * Get location where logs are archived.
     *
     * @return Location where logs are archived
     */
    public Optional<String> getArchiveLocation() {
        return Optional.ofNullable(this.archiveLocation);
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
     * Gets the command name for this job.
     *
     * @return The command name
     */
    public Optional<String> getCommandName() {
        return Optional.ofNullable(this.commandName);
    }

    /**
     * Get the process id of the job.
     *
     * @return the process id
     */
    public Optional<Integer> getProcessId() {
        return Optional.ofNullable(this.processId);
    }

    /**
     * Get the amount of time (in milliseconds) to delay the check for the job status.
     *
     * @return Could be null so return optional of the Long
     */
    public Optional<Long> getCheckDelay() {
        return Optional.ofNullable(this.checkDelay);
    }

    /**
     * Get the exit code from the process that ran the job.
     *
     * @return The exit code or -1 if the job hasn't finished yet
     */
    public Optional<Integer> getExitCode() {
        return Optional.ofNullable(this.exitCode);
    }

    /**
     * Get the amount of memory (in MB) that this job is/was run with.
     *
     * @return The memory as an optional as it could be null
     */
    public Optional<Integer> getMemoryUsed() {
        return Optional.ofNullable(this.memoryUsed);
    }

    /**
     * Get the date this job will be killed due to exceeding its set timeout duration.
     *
     * @return The timeout date
     */
    public Optional<Instant> getTimeout() {
        return Optional.ofNullable(this.timeout);
    }

    /**
     * Set the date this job will be killed due to exceeding its set timeout duration.
     *
     * @param timeout The new timeout
     */
    public void setTimeout(@Nullable final Instant timeout) {
        this.timeout = timeout;
    }

    /**
     * Set the command arguments to use with this job.
     *
     * @param commandArgs The command arguments to use
     */
    public void setCommandArgs(@Nullable final List<String> commandArgs) {
        this.commandArgs.clear();
        if (commandArgs != null) {
            this.commandArgs.addAll(commandArgs);
        }
    }

    /**
     * Set all the files associated as configuration files for this job.
     *
     * @param configs The configuration files to set
     */
    public void setConfigs(@Nullable final Set<FileEntity> configs) {
        this.configs.clear();
        if (configs != null) {
            this.configs.addAll(configs);
        }
    }

    /**
     * Set all the files associated as dependency files for this job.
     *
     * @param dependencies The dependency files to set
     */
    public void setDependencies(@Nullable final Set<FileEntity> dependencies) {
        this.dependencies.clear();
        if (dependencies != null) {
            this.dependencies.addAll(dependencies);
        }
    }

    /**
     * Set all the tags associated to this job.
     *
     * @param tags The tags to set
     */
    public void setTags(@Nullable final Set<TagEntity> tags) {
        this.tags.clear();
        if (tags != null) {
            this.tags.addAll(tags);
        }
    }

    /**
     * Get the cluster that is running or did run this job.
     *
     * @return The cluster or empty Optional if it hasn't been set
     */
    public Optional<ClusterEntity> getCluster() {
        return Optional.ofNullable(this.cluster);
    }

    /**
     * Set the cluster this job ran on.
     *
     * @param cluster The cluster this job ran on
     */
    public void setCluster(@Nullable final ClusterEntity cluster) {
        if (this.cluster != null) {
            this.clusterName = null;
        }

        this.cluster = cluster;

        if (this.cluster != null) {
            this.clusterName = cluster.getName();
        }
    }

    /**
     * Get the command that is executing this job.
     *
     * @return The command or empty Optional if one wasn't set yet
     */
    public Optional<CommandEntity> getCommand() {
        return Optional.ofNullable(this.command);
    }

    /**
     * Set the command used to run this job.
     *
     * @param command The command
     */
    public void setCommand(@Nullable final CommandEntity command) {
        if (this.command != null) {
            this.commandName = null;
        }

        this.command = command;

        if (this.command != null) {
            this.commandName = command.getName();
        }
    }

    /**
     * Set the applications used to run this job.
     *
     * @param applications The applications
     */
    public void setApplications(@Nullable final List<ApplicationEntity> applications) {
        this.applications.clear();
        if (applications != null) {
            this.applications.addAll(applications);
        }
    }

    /**
     * Set the cluster criteria set for this job.
     *
     * @param clusterCriteria The cluster criteria in priority order
     */
    public void setClusterCriteria(@Nullable final List<CriterionEntity> clusterCriteria) {
        this.clusterCriteria.clear();
        if (clusterCriteria != null) {
            this.clusterCriteria.addAll(clusterCriteria);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object o) {
        return super.equals(o);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
