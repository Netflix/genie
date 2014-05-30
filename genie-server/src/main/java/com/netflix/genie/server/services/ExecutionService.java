/*
 *
 *  Copyright 2013 Netflix, Inc.
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

import com.netflix.genie.common.messages.JobRequest;
import com.netflix.genie.common.messages.JobResponse;
import com.netflix.genie.common.messages.JobStatusResponse;

/**
 * Interface for the Execution Service.<br>
 * Implementations must be thread-safe.
 *
 * @author skrishnan
 * @author amsharma
 */
public interface ExecutionService {

    /**
     * Submit a new job.
     *
     * @param request request object containing job info element for new job
     * @return successful response, or one with HTTP error code
     */
    JobResponse submitJob(JobRequest request);

    /**
     * Get job information for given job id.
     *
     * @param jobId id for job to look up
     * @return successful response, or one with HTTP error code
     */
    JobResponse getJobInfo(String jobId);

    /**
     * Get job status for give job id.
     *
     * @param jobId id for job to look up
     * @return successful response, or one with HTTP error code
     */
    JobStatusResponse getJobStatus(String jobId);

    /**
     * Kill job based on given job iD.
     *
     * @param jobId id for job to kill
     * @return successful response, or one with HTTP error code
     */
    JobStatusResponse killJob(String jobId);

    /**
     * Get job info for given filter criteria.
     *
     * @param jobId id for job
     * @param jobName name of job (can be a SQL-style pattern such as HIVE%)
     * @param userName user who submitted job
     * @param status status of job - possible types Type.JobStatus
     * @param clusterName name of cluster for job
     * @param clusterId id of cluster for job
     * @param limit max number of jobs to return
     * @param page page number for job
     * @return successful response, or one with HTTP error code
     */
    JobResponse getJobs(String jobId, String jobName, String userName,
            String status, String clusterName, String clusterId, Integer limit, Integer page);
}
