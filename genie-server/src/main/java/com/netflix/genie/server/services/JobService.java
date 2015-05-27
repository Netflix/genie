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
package com.netflix.genie.server.services;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.model.Job;
import com.netflix.genie.common.model.JobStatus;
import org.hibernate.validator.constraints.NotBlank;
import org.hibernate.validator.constraints.NotEmpty;
import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Set;

/**
 * Helper API's for working with Jobs.
 *
 * @author tgianos
 */
@Validated
public interface JobService {

    /**
     * Validate the job and persist it if it passes validation.
     *
     * @param job The job to validate and maybe save
     * @return The validated/saved job object
     * @throws GenieException if there is an error
     */
    Job createJob(
            @NotNull(message = "No job entered. Unable to create.")
            @Valid
            final Job job
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
     * @param page        page number for job
     * @param limit       max number of jobs to return
     * @param descending  Whether the results should be returned in descending or ascending order
     * @param orderBys    The fields to order the results by
     * @return All jobs which match the criteria
     */
    List<Job> getJobs(
            final String id,
            final String jobName,
            final String userName,
            final Set<JobStatus> statuses,
            final Set<String> tags,
            final String clusterName,
            final String clusterId,
            final String commandName,
            final String commandId,
            final int page,
            final int limit,
            final boolean descending,
            final Set<String> orderBys
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
     * Add tags to the job.
     *
     * @param id   The id of the job to add the tags to. Not null/empty/blank.
     * @param tags The tags to add. Not null/empty.
     * @return The active set of tags
     * @throws GenieException if there is an error
     */
    Set<String> addTagsForJob(
            @NotBlank(message = "No id entered. Unable to add tags.")
            final String id,
            @NotEmpty(message = "No tags entered to add.")
            final Set<String> tags
    ) throws GenieException;

    /**
     * Get the set of tags associated with the job with given id.
     *
     * @param id The id of the job to get the tags for. Not null/empty/blank.
     * @return The set of tags as paths
     * @throws GenieException if there is an error
     */
    Set<String> getTagsForJob(
            @NotBlank(message = "No job id entered. Unable to get tags.")
            final String id
    ) throws GenieException;

    /**
     * Update the set of tags associated with the job with given id.
     *
     * @param id   The id of the job to update the tags for. Not null/empty/blank.
     * @param tags The tags to replace existing tags with. Not null/empty.
     * @return The active set of tags
     * @throws GenieException if there is an error
     */
    Set<String> updateTagsForJob(
            @NotBlank(message = "No job id entered. Unable to update tags.")
            final String id,
            @NotEmpty(message = "No tags entered. Unable to update tags.")
            final Set<String> tags
    ) throws GenieException;

    /**
     * Remove all tags from the job.
     *
     * @param id The id of the job to remove the tags from. Not
     *           null/empty/blank.
     * @return The active set of tags
     * @throws GenieException if there is an error
     */
    Set<String> removeAllTagsForJob(
            @NotBlank(message = "No job id entered. Unable to remove tags.")
            final String id
    ) throws GenieException;

    /**
     * Remove a tag from the job.
     *
     * @param id  The id of the job to remove the tag from. Not null/empty/blank.
     * @param tag The tag to remove. Not null/empty/blank.
     * @return The active set of tags
     * @throws GenieException if there is an error
     */
    Set<String> removeTagForJob(
            @NotBlank(message = "No id entered for job. Unable to remove tag.")
            final String id,
            @NotBlank(message = "No tag entered. Unable to remove")
            final String tag
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
