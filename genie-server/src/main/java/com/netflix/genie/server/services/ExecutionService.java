/*
 *
 *  Copyright 2014 Netflix, Inc.
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
import java.util.List;
import java.util.Set;

/**
 * Interface for the Execution Service.<br>
 * Implementations must be thread-safe.
 *
 * @author skrishnan
 * @author amsharma
 * @author tgianos
 */
public interface ExecutionService {

    /**
     * Submit a new job.
     *
     * @param job the job to submit
     * @return The job that was submitted
     * @throws GenieException
     */
    Job submitJob(final Job job) throws GenieException;

    /**
     * Get job information for given job id.
     *
     * @param id id of job to look up
     * @return the job
     * @throws GenieException
     */
    Job getJobInfo(final String id) throws GenieException;

    /**
     * Get job status for give job id.
     *
     * @param id id for job to look up
     * @return successful response, or one with HTTP error code
     * @throws GenieException
     */
    JobStatus getJobStatus(final String id) throws GenieException;

    /**
     * Kill job based on given job iD.
     *
     * @param id id for job to kill
     * @return The killed job
     * @throws GenieException
     */
    Job killJob(final String id) throws GenieException;

    /**
     * Get job info for given filter criteria.
     *
     * @param id id for job
     * @param jobName name of job (can be a SQL-style pattern such as HIVE%)
     * @param userName user who submitted job
     * @param status status of job - possible types Type.JobStatus
     * @param clusterName name of cluster for job
     * @param clusterId id of cluster for job
     * @param limit max number of jobs to return
     * @param page page number for job
     * @return All jobs which match the criteria
     * @throws GenieException
     */
    List<Job> getJobs(
            final String id,
            final String jobName,
            final String userName,
            final JobStatus status,
            final String clusterName,
            final String clusterId,
            final int limit,
            final int page) throws GenieException;

    /**
     * Mark jobs as zombies if status hasn't been updated for
     * netflix.genie.server.janitor.zombie.delta.ms.
     *
     * @return Number of jobs marked as zombies
     */
    int markZombies();

    /**
     * Add tags to the job.
     *
     * @param id The id of the job to add the tags to. Not
     * null/empty/blank.
     * @param tags The tags to add. Not null/empty.
     * @return The active set of tags
     * @throws GenieException
     */
    Set<String> addTagsForJob(
            final String id,
            final Set<String> tags) throws GenieException;

    /**
     * Get the set of tags associated with the job with given
     * id.
     *
     * @param id The id of the job to get the tags for. Not
     * null/empty/blank.
     * @return The set of tags as paths
     * @throws GenieException
     */
    Set<String> getTagsForJob(
            final String id) throws GenieException;

    /**
     * Update the set of tags associated with the job with
     * given id.
     *
     * @param id The id of the job to update the tags for.
     * Not null/empty/blank.
     * @param tags The tags to replace existing tags
     * with. Not null/empty.
     * @return The active set of tags
     * @throws GenieException
     */
    Set<String> updateTagsForJob(
            final String id,
            final Set<String> tags) throws GenieException;

    /**
     * Remove all tags from the job.
     *
     * @param id The id of the job to remove the tags from.
     * Not null/empty/blank.
     * @return The active set of tags
     * @throws GenieException
     */
    Set<String> removeAllTagsForJob(
            final String id) throws GenieException;

    /**
     * Remove a tag from the job.
     *
     * @param id The id of the job to remove the tag from. Not
     * null/empty/blank.
     * @param tag The tag to remove. Not null/empty/blank.
     * @return The active set of tags
     * @throws GenieException
     */
    Set<String> removeTagForJob(final String id, final String tag) throws GenieException;
}
