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
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.dto.search.JobSearchResult;
import com.netflix.genie.common.exceptions.GenieException;
import org.hibernate.validator.constraints.NotBlank;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import javax.validation.constraints.NotNull;
import java.util.Date;
import java.util.Set;

/**
 * Interface for searching jobs.
 *
 * @author amsharma
 * @since 3.0.0
 */
public interface JobSearchService {

    /**
     * Search for jobs which match the given filter criteria.
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
     * @param minStarted  The time which the job had to start after in order to be return (inclusive)
     * @param maxStarted  The time which the job had to start before in order to be returned (exclusive)
     * @param minFinished The time which the job had to finish after in order to be return (inclusive)
     * @param maxFinished The time which the job had to finish before in order to be returned (exclusive)
     * @param page        Page information of job to get
     * @return Metadata information on jobs which match the criteria
     */
    Page<JobSearchResult> findJobs(
            final String id,
            final String jobName,
            final String userName,
            final Set<JobStatus> statuses,
            final Set<String> tags,
            final String clusterName,
            final String clusterId,
            final String commandName,
            final String commandId,
            final Date minStarted,
            final Date maxStarted,
            final Date minFinished,
            final Date maxFinished,
            @NotNull final Pageable page
    );

    /**
     * Given a hostname return a set of all the job executions currently running on that host.
     *
     * @param hostname The host name to search for. Not null or empty.
     * @return All the jobs running on the host as a set of JobExecution objects
     * @throws GenieException on error
     */
    Set<JobExecution> getAllRunningJobExecutionsOnHost(@NotBlank final String hostname) throws GenieException;

    /**
     * Get job information for given job id.
     *
     * @param id id of job to look up
     * @return the job
     * @throws GenieException if there is an error
     */
    Job getJob(@NotBlank final String id) throws GenieException;

    /**
     * Get the status of the job with the given id.
     *
     * @param id The id of the job to get status for
     * @return The job status
     * @throws GenieException When any error, including not found, is encountered
     */
    JobStatus getJobStatus(@NotBlank final String id) throws GenieException;
}
