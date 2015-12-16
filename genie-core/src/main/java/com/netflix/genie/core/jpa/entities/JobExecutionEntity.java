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

import com.fasterxml.jackson.core.type.TypeReference;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.util.JsonUtils;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.MapsId;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import javax.validation.constraints.Size;
import java.util.HashSet;
import java.util.Set;

/**
 * Representation of the original Genie Job request.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Entity
@Table(name = "job_executions")
public class JobExecutionEntity extends BaseEntity {

    @Basic(optional = false)
    @Column(name = "host_name", nullable = false, length = 255)
    @Size(min = 1, max = 255, message = "Must have a hostname no longer than 255 characters")
    private String hostName;

    @Basic(optional = false)
    @Column(name = "process_id", nullable = false)
    private int processId;

    @Basic(optional = false)
    @Column(name = "exit_code", nullable = false)
    private int exitCode = -1;

    @Basic(optional = false)
    @Column(name = "cluster_criteria", nullable = false, length = 1024)
    @Size(min = 1, max = 1024, message = "Must not have cluster criteria longer than 1024 characters")
    private String clusterCriteria = "[]";

    @OneToOne(optional = false, fetch = FetchType.LAZY)
    @JoinColumn(name = "id")
    @MapsId
    private JobEntity job;

    /**
     * Get the hostname this job is running on.
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
     * Get the cluster criteria as a set of strings.
     *
     * @return The cluster criteria used to select the cluster the job ran on
     * @throws GenieException For any serialization error
     */
    public Set<String> getClusterCriteriaAsSet() throws GenieException {
        return JsonUtils.unmarshall(this.clusterCriteria, new TypeReference<Set<String>>() {
        });
    }

    /**
     * Set the cluster criteria from a set of strings.
     *
     * @param clusterCriteriaSet The cluster criteria to set
     * @throws GenieException For any serialization error
     */
    public void setClusterCriteriaFromSet(final Set<String> clusterCriteriaSet) throws GenieException {
        this.clusterCriteria = clusterCriteriaSet == null
            ? JsonUtils.marshall(new HashSet<String>())
            : JsonUtils.marshall(clusterCriteriaSet);
    }

    /**
     * Get the cluster criteria that was used to chose the cluster for the job.
     *
     * @return The cluster criteria as a JSON array string
     */
    protected String getClusterCriteria() {
        return this.clusterCriteria;
    }

    /**
     * Set the cluster criteria that was used to chose the cluster for the job as a JSON array string.
     *
     * @param clusterCriteria The cluster criteria as a JSON array string
     */
    protected void setClusterCriteria(final String clusterCriteria) {
        this.clusterCriteria = clusterCriteria;
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
     * @throws GenieException For any processing error
     */
    public JobExecution getDTO() throws GenieException {
        return new JobExecution.Builder(
            this.hostName,
            this.processId
        )
            .withExitCode(this.exitCode)
            .withClusterCriteria(this.getClusterCriteriaAsSet())
            .build();
    }
}
