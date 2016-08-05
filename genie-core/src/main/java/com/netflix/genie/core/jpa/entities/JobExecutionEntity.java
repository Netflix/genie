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

import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.JobExecution;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.MapsId;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * Representation of the original Genie Job request.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Entity
@Table(name = "job_executions")
@Getter
@Setter
public class JobExecutionEntity extends BaseEntity {

    private static final long serialVersionUID = -5073493356472801960L;
    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    @Basic(optional = false)
    @Column(name = "host_name", nullable = false)
    @Size(min = 1, max = 255, message = "Must have a host name no longer than 255 characters")
    private String hostName;

    @Basic(optional = false)
    @Column(name = "process_id", nullable = false)
    private int processId = JobExecution.DEFAULT_PROCESS_ID;

    @Basic(optional = false)
    @Column(name = "check_delay", nullable = false)
    @Min(1)
    private long checkDelay = Command.DEFAULT_CHECK_DELAY;

    @Basic(optional = false)
    @Column(name = "exit_code", nullable = false)
    private int exitCode = JobExecution.DEFAULT_EXIT_CODE;

    @Basic(optional = false)
    @Column(name = "timeout", nullable = false)
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
     * Get the host name this job is running on.
     *
     * @return The hostname
     */
    public String getHostName() {
        return this.hostName;
    }

    /**
     * Set the hostname this job is running on.
     *
     * @param hostName The hostname
     */
    public void setHostName(final String hostName) {
        this.hostName = hostName;
    }

    /**
     * Get the process id of the job.
     *
     * @return the process id
     */
    public int getProcessId() {
        return this.processId;
    }

    /**
     * Set the process id of the job.
     *
     * @param processId The process id
     */
    public void setProcessId(final int processId) {
        this.processId = processId;
    }

    /**
     * Get the exit code from the process that ran the job.
     *
     * @return The exit code or -1 if the job hasn't finished yet
     */
    public int getExitCode() {
        return this.exitCode;
    }

    /**
     * Set the exit code from the process.
     *
     * @param exitCode The exit code from the process
     */
    public void setExitCode(final int exitCode) {
        this.exitCode = exitCode;
    }

    /**
     * Get the date this job will be killed due to exceeding its set timeout duration.
     *
     * @return The timeout date
     */
    public Date getTimeout() {
        return new Date(this.timeout.getTime());
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
     * Get the job associated with this job execution.
     *
     * @return The job
     */
    public JobEntity getJob() {
        return this.job;
    }

    /**
     * Set the job for this execution.
     *
     * @param job The job
     */
    protected void setJob(final JobEntity job) {
        this.job = job;
    }

    /**
     * Get a DTO representing this job execution.
     *
     * @return The read-only DTO.
     */
    public JobExecution getDTO() {
        return new JobExecution.Builder(
            this.hostName,
            this.processId,
            this.checkDelay,
            this.timeout
        )
            .withExitCode(this.exitCode)
            .withId(this.getId())
            .withCreated(this.getCreated())
            .withUpdated(this.getUpdated())
            .build();
    }
}
