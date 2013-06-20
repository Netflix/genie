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

import com.netflix.genie.common.messages.JobInfoRequest;
import com.netflix.genie.common.messages.JobInfoResponse;
import com.netflix.genie.common.messages.JobStatusResponse;

/**
 * Interface for the Execution Service.<br>
 * Implementations must be thread-safe.
 *
 * @author skrishnan
 */
public interface ExecutionService {

    /**
     * Submit a new job.
     *
     * @param rRequest
     *            request object containing job info element for new job
     * @return successful response, or one with HTTP error code
     */
    JobInfoResponse submitJob(JobInfoRequest rRequest);

    /**
     * Get job information for given job id.
     *
     * @param jobID
     *            id for job to look up
     * @return successful response, or one with HTTP error code
     */
    JobInfoResponse getJobInfo(String jobID);

    /**
     * Get job status for give job id.
     *
     * @param jobID
     *            id for job to look up
     * @return successful response, or one with HTTP error code
     */
    JobStatusResponse getJobStatus(String jobID);

    /**
     * Kill job based on given job iD.
     *
     * @param jobID
     *            id for job to kill
     * @return successful response, or one with HTTP error code
     */
    JobStatusResponse killJob(String jobID);

    /**
     * Get job info for given filter criteria.
     *
     * @param jobID
     *            id for job
     * @param jobName
     *            name of job (can be a SQL-style pattern such as HIVE%)
     * @param userName
     *            user who submitted job
     * @param jobType
     *            type of job - possible types Type.JobType
     * @param status
     *            status of job - possible types Type.JobStatus
     * @param limit
     *            max number of jobs to return
     * @param page
     *            page number for job
     * @return successful response, or one with HTTP error code
     */
    JobInfoResponse getJobs(String jobID, String jobName, String userName,
            String jobType, String status, Integer limit, Integer page);
}
