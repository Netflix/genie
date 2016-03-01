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

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.MapsId;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import javax.validation.constraints.Size;

/**
 * Representation of the original Genie Job request.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Entity
@Table(name = "job_executions")
public class JobExecutionEntity extends BaseEntity {
    /**
     * The exit code that will be set to indicate a job is currently executing.
     */
    public static final int DEFAULT_EXIT_CODE = -1;

    private static final long serialVersionUID = -5073493356472801960L;

    @Basic(optional = false)
    @Column(name = "hostname", nullable = false, length = 255)
    @Size(min = 1, max = 255, message = "Must have a hostname no longer than 255 characters")
    private String hostname;

    @Basic(optional = false)
    @Column(name = "process_id", nullable = false)
    private int processId;

    @Basic(optional = false)
    @Column(name = "exit_code", nullable = false)
    private int exitCode = DEFAULT_EXIT_CODE;

    @OneToOne(optional = false, fetch = FetchType.LAZY)
    @JoinColumn(name = "id")
    @MapsId
    private JobEntity job;

    /**
     * Get the hostname this job is running on.
     *
     * @return The hostname
     */
    public String getHostname() {
        return this.hostname;
    }

    /**
     * Set the hostname this job is running on.
     *
     * @param hostname The hostname
     */
    public void setHostname(final String hostname) {
        this.hostname = hostname;
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
    public void setJob(final JobEntity job) {
        this.job = job;
    }

    /**
     * Get a DTO representing this job execution.
     *
     * @return The read-only DTO.
     */
    public JobExecution getDTO() {
        return new JobExecution.Builder(
            this.hostname,
            this.processId
        )
            .withExitCode(this.exitCode)
            .withId(this.getId())
            .withCreated(this.getCreated())
            .withUpdated(this.getUpdated())
            .build();
    }
}
