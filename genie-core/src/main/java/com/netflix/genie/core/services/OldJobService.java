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
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import org.hibernate.validator.constraints.NotBlank;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Set;

/**
 * Helper APIs for working with Jobs.
 *
 * @author tgianos
 * @since 2.0.0
 */
@Validated
public interface OldJobService {

    /**
     * Validate the job and persist it if it passes validation.
     *
     * @param jobRequest The job request
     * @return The validated/saved job object
     * @throws GenieException if there is an error
     */
    Job createJob(
            @NotNull(message = "No job entered. Unable to create.")
            @Valid
            final JobRequest jobRequest
    ) throws GenieException;

    /**
     * Get job information for given job id.
     *
     * @param id id of job to look up
     * @return the job
     * @throws GenieException if there is an error
     */
    Job getJob(
            @NotBlank(message = "No id entered. Unable to get job.")
            final String id
    ) throws GenieException;

    /**
     * Get job info for given filter criteria.
     *
     * @param id          id for job
     * @param jobName     name of job (can be a SQL-style pattern such as HIVE%)
     * @param userName    user who submitted job
     * @param statuses    statuses of job
     * @param tags        tags for the job
     * @param clusterName name of cluster for job
     * @param clusterId   id of cluster for job
     * @param commandName name of the command run in the job
     * @param commandId   id of the command run in the job
     * @param page        Page information of jobs to get
     * @return All jobs which match the criteria
     */
    Page<Job> getJobs(
            final String id,
            final String jobName,
            final String userName,
            final Set<JobStatus> statuses,
            final Set<String> tags,
            final String clusterName,
            final String clusterId,
            final String commandName,
            final String commandId,
            final Pageable page
    );

    /**
     * Get job status for give job id.
     *
     * @param id id for job to look up
     * @return successful response, or one with HTTP error code
     * @throws GenieException if there is an error
     */
    JobStatus getJobStatus(
            @NotBlank(message = "No id entered. Unable to get status.")
            final String id
    ) throws GenieException;

    /**
     * Set the status for a given job.
     *
     * @param id     The id of the job to fail.
     * @param status The status to set.
     * @param msg    The message of the failure.
     * @throws GenieException if there is an error
     */
    void setJobStatus(
            @NotBlank(message = "No id entered for the job. Unable to update the status.")
            final String id,
            @NotNull(message = "No status entered unable to update.")
            final JobStatus status,
            final String msg
    ) throws GenieException;

    /**
     * Update a job with the last updated time.
     *
     * @param id The id of the job to update.
     * @return The time in milliseconds when the job was updated.
     * @throws GenieException if there is an error
     */
    long setUpdateTime(
            @NotBlank(message = "No job id entered. Unable to set update time.")
            final String id
    ) throws GenieException;

    /**
     * Set the java process id that will run the given job.
     *
     * @param id  The id of the job to attach the process to.
     * @param pid The id of the process that will run the job.
     * @throws GenieException if there is an error
     */
    void setProcessIdForJob(
            @NotBlank(message = "No job id entered. Unable to set process id")
            final String id,
            final int pid
    ) throws GenieException;

    /**
     * Set the command information for a given job.
     *
     * @param id          The id of the job.
     * @param commandId   The id of the command used to run the job.
     * @param commandName The name of the command used to run the job.
     * @throws GenieException if there is an error
     */
    void setCommandInfoForJob(
            @NotBlank(message = "No job id entered. Unable to set command info for job.")
            final String id,
            @NotBlank(message = "No command id entered. Unable to set command info for job.")
            final String commandId,
            @NotBlank(message = "No command name entered. Unable to set command info for job.")
            final String commandName
    ) throws GenieException;

    /**
     * Set the application information for a given job.
     *
     * @param id      The id of the job.
     * @param appId   The id of the application used to run the job.
     * @param appName The name of the application used to run the job.
     * @throws GenieException if there is an error
     */
    void setApplicationInfoForJob(
            @NotBlank(message = "No job id entered. Unable to update app info for job.")
            final String id,
            @NotBlank(message = "No app id entered. Unable to update app info for job.")
            final String appId,
            @NotBlank(message = "No app name entered. unable to update app info for job.")
            final String appName
    ) throws GenieException;

    /**
     * Set the cluster information for a given job.
     *
     * @param id          The id of the job.
     * @param clusterId   The id of the cluster used to run the job.
     * @param clusterName The name of the cluster used to run the job.
     * @throws GenieException if there is an error
     */
    void setClusterInfoForJob(
            @NotBlank(message = "No job id entered. Unable to update cluster info for job.")
            final String id,
            @NotBlank(message = "No cluster id entered. Unable to update cluster info for job.")
            final String clusterId,
            @NotBlank(message = "No cluster name entered. Unable to update cluster info for job.")
            final String clusterName
    ) throws GenieException;

    /**
     * Run the job using a JobLauncher.
     *
     * @param job The job to run.
     * @return The job object that's returned after launch
     * @throws GenieException if there is an error
     */
    Job runJob(
            @NotNull(message = "No job entered unable to run")
            @Valid
            final Job job
    ) throws GenieException;
}
