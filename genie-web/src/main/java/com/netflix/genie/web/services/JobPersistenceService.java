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
package com.netflix.genie.web.services;

import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobMetadata;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import org.hibernate.validator.constraints.NotBlank;
import org.springframework.validation.annotation.Validated;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.Date;
import java.util.List;

/**
 * Interfaces for providing persistence functions for jobs other than search.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Validated
public interface JobPersistenceService {

    /**
     * Save all the initial job fields in the data store.
     *
     * @param jobRequest   the Job request object to save. Not null
     * @param jobMetadata  metadata about the job request. Not null
     * @param job          The Job object to create
     * @param jobExecution The job execution object to create
     * @throws GenieException if there is an error
     */
    void createJob(
        @NotNull final JobRequest jobRequest,
        @NotNull final JobMetadata jobMetadata,
        @NotNull final Job job,
        @NotNull final JobExecution jobExecution
    ) throws GenieException;

    /**
     * Update the job with the various resources used to run the job including the cluster, command and applications.
     *
     * @param jobId          The id of the job to update
     * @param clusterId      The id of the cluster the job runs on
     * @param commandId      The id of the command the job runs with
     * @param applicationIds The ids of the applications used to run the job
     * @param memory         The amount of memory (in MB) to run the job with
     * @throws GenieException For any problems while updating
     */
    void updateJobWithRuntimeEnvironment(
        @NotBlank final String jobId,
        @NotBlank final String clusterId,
        @NotBlank final String commandId,
        @NotNull final List<String> applicationIds,
        @Min(1) final int memory
    ) throws GenieException;

    /**
     * Update the status and status message of the job.
     *
     * @param id        The id of the job to update the status for.
     * @param jobStatus The updated status of the job.
     * @param statusMsg The updated status message of the job.
     * @throws GenieException if there is an error
     */
    void updateJobStatus(
        @NotBlank(message = "No job id entered. Unable to update.") final String id,
        @NotNull(message = "Status cannot be null.") final JobStatus jobStatus,
        @NotBlank(message = "Status message cannot be empty.") final String statusMsg
    ) throws GenieException;

    /**
     * Update the job with information for the running job process.
     *
     * @param id         the id of the job to update the process id for
     * @param processId  The id of the process on the box for this job
     * @param checkDelay The delay to check the process with
     * @param timeout    The date at which this job should timeout
     * @throws GenieException if there is an error
     */
    void setJobRunningInformation(
        @NotBlank final String id,
        @Min(value = 0, message = "Must be no lower than zero") final int processId,
        @Min(value = 1, message = "Must be at least 1 millisecond, preferably much more") final long checkDelay,
        @NotNull final Date timeout
    ) throws GenieException;

    /**
     * Method to set all job completion information for a job execution.
     *
     * @param id            the id of the job to update the exit code
     * @param exitCode      The exit code of the process
     * @param status        The final job status for the job
     * @param statusMessage The final job status message
     * @param stdOutSize    The size (in bytes) of the standard out file or null if there isn't one
     * @param stdErrSize    The size (in bytes) of the standard error file or null if there isn't one
     * @throws GenieException if there is an error
     */
    void setJobCompletionInformation(
        @NotBlank(message = "No job id entered. Unable to update.") final String id,
        final int exitCode,
        @NotNull(message = "No job status entered. Unable to update") final JobStatus status,
        @NotBlank(message = "Status message can't be blank. Unable to update") final String statusMessage,
        @Nullable final Long stdOutSize,
        @Nullable final Long stdErrSize
    ) throws GenieException;

    /**
     * This method will delete a chunk of jobs whose creation time is earlier than the given date.
     *
     * @param date       The date before which all jobs should be deleted
     * @param maxDeleted The maximum number of jobs that should be deleted
     *                   (soft limit, can be rounded up to multiple of page size)
     * @param pageSize   Page size used to iterate through jobs
     * @return the number of deleted jobs
     */
    long deleteBatchOfJobsCreatedBeforeDate(@NotNull final Date date,
                                            @Min(1) final int maxDeleted,
                                            @Min(1) final int pageSize);
}
