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
package com.netflix.genie.core.services.impl;

import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.dto.search.JobSearchResult;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.core.services.JobPersistenceService;
import com.netflix.genie.core.services.JobSearchService;
import com.netflix.genie.core.services.JobService;
import com.netflix.genie.core.services.JobSubmitterService;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.validator.constraints.NotBlank;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Set;


/**
 * Implementation of the JobService APIs.
 *
 * @author amsharma
 */
@Service
@Slf4j
public class JobServiceImpl implements JobService {

    private final JobPersistenceService jobPersistenceService;
    private final JobSearchService jobSearchService;
    private final JobSubmitterService jobSubmitterService;

    /**
     * Constructor.
     *
     * @param jobPersistenceService implementation of job persistence service interface
     * @param jobSearchService      implementation of job search service interface
     * @param jobSubmitterService   implementation of the job submitter service
     */
    @Autowired
    public JobServiceImpl(
        final JobPersistenceService jobPersistenceService,
        final JobSearchService jobSearchService,
        final JobSubmitterService jobSubmitterService
    ) {
        this.jobPersistenceService = jobPersistenceService;
        this.jobSearchService = jobSearchService;
        this.jobSubmitterService = jobSubmitterService;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String runJob(
        @NotNull(message = "No jobRequest provided. Unable to submit job for execution.")
        @Valid
        final JobRequest jobRequest
    ) throws GenieException {
        log.debug("Called with job request {}", jobRequest);

        // TODO get client host at this point?
        // Log the request as soon as it comes in. This method returns a job request DTO with an id in it as the
        // orginal request may or may not have it.
        //    final JobRequest jobRequestWithId =
        //          this.jobPersistenceService.createJobRequest(jobRequest);

        final JobRequest jobRequestWithId = jobRequest;
        this.jobSubmitterService.submitJob(jobRequestWithId);

        return jobRequestWithId.getId();

        // do basic validation of the request
        // persist in various storage layers
        // submit the job request to Job submitter interface
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Job getJob(@NotBlank final String jobId) throws GenieException {
        log.debug("Called with id {}", jobId);
        return this.jobSearchService.getJob(jobId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobStatus getJobStatus(@NotBlank final String id) throws GenieException {
        log.debug("Called to get status for job {}", id);
        return this.jobSearchService.getJobStatus(id);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Page<JobSearchResult> findJobs(
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
    ) {
        log.debug("called");

        return this.jobSearchService.findJobs(
            id,
            jobName,
            userName,
            statuses,
            tags,
            clusterName,
            clusterId,
            commandName,
            commandId,
            page
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void killJob(
        @NotBlank(message = "No job id provided. Unable to retrieve job.")
        final String jobId
    ) throws GenieException {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getJobHost(@NotBlank final String jobId) throws GenieException {
        final JobExecution jobExecution = this.jobPersistenceService.getJobExecution(jobId);
        if (jobExecution != null) {
            return jobExecution.getHostname();
        } else {
            throw new GenieNotFoundException("No job execution found for id " + jobId);
        }
    }
}
