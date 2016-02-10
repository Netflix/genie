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
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.core.services.JobCoordinatorService;
import com.netflix.genie.core.services.JobPersistenceService;
import com.netflix.genie.core.services.JobSearchService;
import com.netflix.genie.core.services.JobSubmitterService;
import org.apache.commons.lang3.StringUtils;
import lombok.extern.slf4j.Slf4j;
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
 * Implementation of the JobService apis.
 *
 * @author amsharma
 */
@Service
@Slf4j
public class JobCoordinatorServiceImpl implements JobCoordinatorService {

    private final JobPersistenceService jobPersistenceService;
    private final JobSearchService jobSearchService;
    private final JobSubmitterService jobSubmitterService;
    @Value("${com.netflix.genie.server.s3.archive.location:#{null}}")
    private String baseArchiveLocation;

    /**
     * Constructor.
     *
     * @param jobPersistenceService implementation of job persistence service interface
     * @param jobSearchService      implementation of job search service interface
     * @param jobSubmitterService   implementation of the job submitter service
     */
    @Autowired
    public JobCoordinatorServiceImpl(
        final JobPersistenceService jobPersistenceService,
        final JobSearchService jobSearchService,
        final JobSubmitterService jobSubmitterService
    ) {
        this.jobPersistenceService = jobPersistenceService;
        this.jobSearchService = jobSearchService;
        this.jobSubmitterService = jobSubmitterService;
    }

    /**
     * Takes in a Job Request persists it in db and then hands of the job to submitter interface to submit.
     *
     * @param jobRequest of job to run
     * @throws GenieException if there is an error
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
        // orginal request may or may not have it.
        final JobRequest jobRequestWithId =
            this.jobPersistenceService.createJobRequest(jobRequest);

        if (StringUtils.isNotBlank(clientHost)) {
            this.jobPersistenceService.addClientHostToJobRequest(jobRequestWithId.getId(), clientHost);
        }

        String jobArchivalLocation = null;
        if (!jobRequestWithId.isDisableLogArchival() && baseArchiveLocation != null) {
           jobArchivalLocation = baseArchiveLocation + "/" + jobRequestWithId.getId();
        }
        // create the job object in the database with status INIT
        final Job job  = new Job.Builder(
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
     * Gets the Job object to return to user given the id.
     *
     * @param jobId of job to retrieve
     * @return job object
     * @throws GenieException if there is an error
     */
    @Override
    public Job getJob(
        @NotBlank(message = "No job id provided. Unable to retrieve job.")
        final String jobId
    ) throws GenieException {
        log.debug("Called with id {}", jobId);
        return this.jobPersistenceService.getJob(jobId);
    }

    /**
     * Get list of jobs for given filter criteria.
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
    @Override
    public Page<Job> getJobs(
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

        return this.jobSearchService.getJobs(
            id,
            jobName,
            userName,
            statuses,
            tags,
            clusterName,
            clusterId,
            commandName,
            commandId,
            page);
    }

    /**
     * Takes in a id of the job to kill.
     *
     * @param jobId id of the job to kill
     * @throws GenieException if there is an error
     */
    @Override
    public void killJob(
        @NotBlank(message = "No job id provided. Unable to retrieve job.")
        final String jobId
    ) throws GenieException {

    }
}
