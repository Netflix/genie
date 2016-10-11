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

import com.netflix.genie.common.dto.JobExecution;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.MapsId;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.Calendar;
import java.util.Date;
import java.util.Optional;
import java.util.TimeZone;

/**
 * Representation of the original Genie Job request.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Getter
@Setter
@Entity
@Table(name = "job_executions")
@NamedQueries({
    @NamedQuery(
        name = JobExecutionEntity.QUERY_FIND_BY_STATUS_HOST,
        query = "select e.job from JobExecutionEntity e where e.job.status in :statuses and e.hostName = :hostName"
    ),
    @NamedQuery(
        name = JobExecutionEntity.QUERY_FIND_HOSTS_BY_STATUS,
        query = "select distinct e.hostName from JobExecutionEntity e where e.job.status in :statuses"
    )
})
public class JobExecutionEntity extends BaseEntity {
    /**
     * Query name to find jobs by statuses and host.
     */
    public static final String QUERY_FIND_BY_STATUS_HOST = "findByStatusHost";
    /**
     * Query name to find hosts by statuses.
     */
    public static final String QUERY_FIND_HOSTS_BY_STATUS = "findHostsByStatus";
    private static final long serialVersionUID = -5073493356472801960L;
    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    @Basic(optional = false)
    @Column(name = "host_name", nullable = false)
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
    @Column(name = "memory")
    private Integer memory;

    @Basic
    @Column(name = "timeout")
    @Temporal(TemporalType.TIMESTAMP)
    private Date timeout;

    @OneToOne(optional = false, fetch = FetchType.LAZY)
    @JoinColumn(name = "id")
    @MapsId
    private JobEntity job;

    /**
     * Default constructor.
     */
    public JobExecutionEntity() {
        final Calendar calendar = Calendar.getInstance(UTC);
        calendar.add(Calendar.DATE, 7);
        this.timeout = calendar.getTime();
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
    public Optional<Integer> getMemory() {
        return Optional.ofNullable(this.memory);
    }

    /**
     * Get the date this job will be killed due to exceeding its set timeout duration.
     *
     * @return The timeout date
     */
    public Optional<Date> getTimeout() {
        return this.timeout == null ? Optional.empty() : Optional.of(new Date(this.timeout.getTime()));
    }

    /**
     * Set the date this job will be killed due to exceeding its set timeout duration.
     *
     * @param timeout The new timeout
     */
    public void setTimeout(@NotNull final Date timeout) {
        this.timeout = new Date(timeout.getTime());
    }

    /**
     * Set the job for this execution.
     *
     * @param job The job
     */
    public void setJob(final JobEntity job) {
        this.job = job;
    }

    /**
     * Get a DTO representing this job execution.
     *
     * @return The read-only DTO.
     */
    public JobExecution getDTO() {
        return new JobExecution.Builder(this.hostName)
            .withProcessId(this.processId)
            .withCheckDelay(this.checkDelay)
            .withTimeout(this.timeout)
            .withExitCode(this.exitCode)
            .withMemory(this.memory)
            .withId(this.getId())
            .withCreated(this.getCreated())
            .withUpdated(this.getUpdated())
            .build();
    }
}
