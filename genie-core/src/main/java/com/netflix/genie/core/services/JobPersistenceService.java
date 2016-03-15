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
    void createJob(
            @NotNull(message = "Job is null so cannot be saved")
            final Job job
    ) throws GenieException;

    /**
     * Update the status and status message of the job.
     *
     * @param id The id of the job to update the status for.
     * @param jobStatus The updated status of the job.
     * @param statusMsg The updated status message of the job.
     * @throws GenieException if there is an error
     */
    void updateJobStatus(
        @NotBlank(message = "No job id entered. Unable to update.")
        final String id,
        @NotNull (message = "Status cannot be null.")
        final JobStatus jobStatus,
        @NotBlank(message = "Status message cannot be empty.")
        final String statusMsg
    ) throws GenieException;

    /**
     * Method that updates the cluster information on which a job is run.
     *
     * @param jobId The id of the job
     * @param clusterId The id of the cluster
     * @throws GenieException Throw exception in case of an error.
     */
    void updateClusterForJob(
        @NotNull(message = "Job id cannot be null while updating cluster information")
        final String jobId,
        @NotNull(message = "Cluster id cannot be null while updating cluster information")
        final String clusterId
    ) throws GenieException;

    /**
     * Method that updates the cluster information on which a job is run.
     *
     * @param jobId The id of the job
     * @param commandId The id of the cluster
     * @throws GenieException Throw exception in case of an error.
     */
    void updateCommandForJob(
        @NotNull(message = "Job id cannot be null while updating command information")
        final String jobId,
        @NotNull(message = "Command id cannot be null while updating command information")
        final String commandId
    ) throws GenieException;

    /**
     * Return the Job Request Entity for the  id provided.
     *
     * @param id The id of the jobRequest to return.
     * @return The job request details or null if not found
     * @throws GenieException if there is an error
     */
    // TODO: Move this to JobSearchService
    JobRequest getJobRequest(final String id) throws GenieException;

    /**
     * Save the jobRequest object in the data store.
     *
     * @param jobRequest the Job object to save
     *
     * @return The job request object that was created
     * @throws GenieException if there is an error
     */
    JobRequest createJobRequest(
            @NotNull(message = "Job Request is null so cannot be saved")
            final JobRequest jobRequest
    ) throws GenieException;

    /**
     * Add the information of client host to jobRequest.
     *
     * @param id The id of the job request
     * @param clientHost Host of the client that sent the request.
     * @throws GenieException If there is an error
     */
    void addClientHostToJobRequest(
        @NotNull(message = "job request id not provided.")
        final String id,
        @NotBlank(message = "client host cannot be null")
        final String clientHost
    ) throws GenieException;
    /**
     * Return the Job Entity for the job id provided.
     *
     * @param id The id of the job to return.
     * @return Job Execution details or null if not found
     * @throws GenieException if there is an error
     */
    // TODO: Move this to JobSearchService
    JobExecution getJobExecution(final String id) throws GenieException;

    /**
     * Save the jobExecution object in the data store.
     *
     * @param jobExecution the Job object to save
     * @throws GenieException if there is an error
     */
    void createJobExecution(
            @NotNull(message = "Job Request is null so cannot be saved")
            final JobExecution jobExecution
    ) throws GenieException;

    /**
     * Method to set exit code for the job execution.
     *
     * @param id the id of the job to update the exit code
     * @param exitCode The exit code of the process
     * @throws GenieException if there is an error
     */
    void setExitCode(
            @NotBlank(message = "No job id entered. Unable to update.")
            final String id,
            @NotBlank(message = "Exit code cannot be blank")
            final int exitCode
    ) throws GenieException;
}
