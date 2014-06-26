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

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.model.Job;
import com.netflix.genie.common.model.Types.JobStatus;
import java.util.List;

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
     * @throws CloudServiceException
     */
    Job submitJob(final Job job) throws CloudServiceException;

    /**
     * Get job information for given job id.
     *
     * @param id id of job to look up
     * @return the job
     * @throws CloudServiceException
     */
    Job getJobInfo(final String id) throws CloudServiceException;

    /**
     * Get job status for give job id.
     *
     * @param id id for job to look up
     * @return successful response, or one with HTTP error code
     * @throws CloudServiceException
     */
    JobStatus getJobStatus(final String id) throws CloudServiceException;

    /**
     * Kill job based on given job iD.
     *
     * @param id id for job to kill
     * @return The killed job
     * @throws CloudServiceException
     */
    Job killJob(final String id) throws CloudServiceException;

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
     * @throws CloudServiceException
     */
    List<Job> getJobs(
            final String id,
            final String jobName,
            final String userName,
            final JobStatus status,
            final String clusterName,
            final String clusterId,
            final int limit,
            final int page) throws CloudServiceException;
    
    /**
     * Mark jobs as zombies if status hasn't been updated for
     * netflix.genie.server.janitor.zombie.delta.ms.
     *
     * @return Number of jobs marked as zombies
     * @throws CloudServiceException if there is any error during the process
     */
    int markZombies() throws Exception;
}
