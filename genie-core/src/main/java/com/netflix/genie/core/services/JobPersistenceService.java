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
package com.netflix.genie.core.services;

import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import org.hibernate.validator.constraints.NotBlank;

import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * Interfaces for providing persistence functions for jobs other than search.
 *
 * @author tgianos
 * @since 3.0.0
 */
public interface JobPersistenceService {

    /**
     * Save the job object in the data store.
     *
     * @param job the Job object to create
     * @throws GenieException if there is an error
     */
    void createJob(@NotNull final Job job) throws GenieException;

    /**
     * Update the status and status message of the job.
     *
     * @param id The id of the job to update the status for.
     * @param jobStatus The updated status of the job.
     * @param statusMsg The updated status message of the job.
     * @throws GenieException if there is an error
     */
    void updateJobStatus(
        @NotBlank final String id,
        @NotNull final JobStatus jobStatus,
        @NotBlank final String statusMsg
    ) throws GenieException;

    /**
     * Update the job with the various resources used to run the job including the cluster, command and applications.
     *
     * @param jobId The id of the job to update
     * @param clusterId The id of the cluster the job runs on
     * @param commandId The id of the command the job runs with
     * @param applicationIds The ids of the applications used to run the job
     * @throws GenieException For any problems while updating
     */
    void updateJobWithRuntimeEnvironment(
        @NotBlank final String jobId,
        @NotBlank final String clusterId,
        @NotBlank final String commandId,
        @NotNull final List<String> applicationIds
    ) throws GenieException;

    /**
     * Save the jobRequest object in the data store.
     *
     * @param jobRequest the Job object to save
     *
     * @return The job request object that was created
     * @throws GenieException if there is an error
     */
    JobRequest createJobRequest(@NotNull final JobRequest jobRequest) throws GenieException;

    /**
     * Add the information of client host to jobRequest.
     *
     * @param id The id of the job request
     * @param clientHost Host of the client that sent the request.
     * @throws GenieException If there is an error
     */
    void addClientHostToJobRequest(@NotNull final String id, @NotBlank final String clientHost) throws GenieException;

    /**
     * Save the jobExecution object in the data store.
     *
     * @param jobExecution the Job object to save
     * @throws GenieException if there is an error
     */
    void createJobExecution(@NotNull final JobExecution jobExecution) throws GenieException;

    /**
     * Method to set exit code for the job execution.
     *
     * @param id the id of the job to update the exit code
     * @param exitCode The exit code of the process
     * @throws GenieException if there is an error
     */
    void setExitCode(@NotBlank final String id, @NotBlank final int exitCode) throws GenieException;
}
