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
import com.netflix.genie.common.exceptions.GenieException;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
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

    @Basic(optional = false)
    @Column(name = "host_name", nullable = false, length = 1024)
    @Size(min = 1, max = 1024, message = "Must have a hostname no longer than 1024 characters")
    private String hostName;

    @Basic(optional = false)
    @Column(name = "process_id", nullable = false)
    private int processId;

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
     * Get a DTO representing this job execution.
     *
     * @return The read-only DTO.
     * @throws GenieException For any processing error
     */
    public JobExecution getDTO() throws GenieException {
        return new JobExecution.Builder(
                this.hostName,
                this.processId
        ).build();
    }
}
