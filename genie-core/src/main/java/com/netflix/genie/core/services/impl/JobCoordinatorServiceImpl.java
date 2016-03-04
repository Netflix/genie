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
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.core.services.JobCoordinatorService;
import com.netflix.genie.core.services.JobKillService;
import com.netflix.genie.core.services.JobPersistenceService;
import com.netflix.genie.core.services.JobSearchService;
import com.netflix.genie.core.services.JobSubmitterService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.NotBlank;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
public class JobCoordinatorServiceImpl implements JobCoordinatorService {

    private final JobPersistenceService jobPersistenceService;
    private final JobSearchService jobSearchService;
    private final JobSubmitterService jobSubmitterService;
    private final JobKillService jobKillService;
    private String baseArchiveLocation;

    /**
     * Constructor.
     *
     * @param jobPersistenceService implementation of job persistence service interface
     * @param jobSearchService      implementation of job search service interface
     * @param jobSubmitterService   implementation of the job submitter service
     * @param jobKillService        The job kill service to use
     * @param baseArchiveLocation   The base directory location of where the job dir should be archived
     */
    @Autowired
    public JobCoordinatorServiceImpl(
        final JobPersistenceService jobPersistenceService,
        final JobSearchService jobSearchService,
        final JobSubmitterService jobSubmitterService,
        final JobKillService jobKillService,
        @Value("${com.netflix.genie.server.s3.archive.location:#{null}}")
        final String baseArchiveLocation
    ) {
        this.jobPersistenceService = jobPersistenceService;
        this.jobSearchService = jobSearchService;
        this.jobSubmitterService = jobSubmitterService;
        this.jobKillService = jobKillService;
        this.baseArchiveLocation = baseArchiveLocation;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String coordinateJob(
        @NotNull(message = "No jobRequest provided. Unable to submit job for execution.")
        @Valid
        final JobRequest jobRequest,
        final String clientHost
    ) throws GenieException {
        log.debug("Called with job request {}", jobRequest);

        // Log the request as soon as it comes in. This method returns a job request DTO with an id in it as the
        // orginal request may or may not have an id.
        final JobRequest jobRequestWithId =
            this.jobPersistenceService.createJobRequest(jobRequest);

        if (StringUtils.isNotBlank(clientHost)) {
            this.jobPersistenceService.addClientHostToJobRequest(jobRequestWithId.getId(), clientHost);
        }

        String jobArchivalLocation = null;
        if (!jobRequestWithId.isDisableLogArchival()) {
            if (baseArchiveLocation == null) {
                throw new
                    GeniePreconditionException("Job archival is enabled but base location for archival is null.");
            }
            jobArchivalLocation = baseArchiveLocation + "/" + jobRequestWithId.getId();
        }
        // create the job object in the database with status INIT
        final Job job = new Job.Builder(
            jobRequest.getName(),
            jobRequest.getUser(),
            jobRequest.getVersion()
        )
            .withArchiveLocation(jobArchivalLocation)
            .withDescription(jobRequest.getDescription())
            .withId(jobRequestWithId.getId())
            .withStatus(JobStatus.INIT)
            .withStatusMsg("Job Accepted and in initialization phase.")
            .build();

        this.jobPersistenceService.createJob(job);
        this.jobSubmitterService.submitJob(jobRequestWithId);
        return jobRequestWithId.getId();
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
    public void killJob(@NotBlank final String jobId) throws GenieException {
        this.jobKillService.killJob(jobId);
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
