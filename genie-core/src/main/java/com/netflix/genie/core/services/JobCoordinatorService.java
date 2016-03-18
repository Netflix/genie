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
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.dto.search.JobSearchResult;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.core.jobs.JobConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.NotBlank;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Date;
import java.util.Set;


/**
 * Implementation of the JobService APIs.
 *
 * @author amsharma
 * @author tgianos
 * @since 3.0.0
 */
@Slf4j
public class JobCoordinatorService {

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
    public JobCoordinatorService(
        final JobPersistenceService jobPersistenceService,
        final JobSearchService jobSearchService,
        final JobSubmitterService jobSubmitterService,
        final JobKillService jobKillService,
        final String baseArchiveLocation
    ) {
        this.jobPersistenceService = jobPersistenceService;
        this.jobSearchService = jobSearchService;
        this.jobSubmitterService = jobSubmitterService;
        this.jobKillService = jobKillService;
        this.baseArchiveLocation = baseArchiveLocation;
    }

    /**
     * Takes in a Job Request object and does necessary preparation for execution.
     *
     * @param jobRequest of job to kill
     * @param clientHost Host which is sending the job request
     * @return the id of the job run
     * @throws GenieException if there is an error
     */
    public String coordinateJob(
        @NotNull(message = "No jobRequest provided. Unable to submit job for execution.")
        @Valid
        final JobRequest jobRequest,
        final String clientHost
    ) throws GenieException {
        log.debug("Called with job request {}", jobRequest);

        // Log the job request
        this.jobPersistenceService.createJobRequest(jobRequest);

        if (StringUtils.isNotBlank(clientHost)) {
            this.jobPersistenceService.addClientHostToJobRequest(jobRequest.getId(), clientHost);
        }

        String jobArchivalLocation = null;
        if (!jobRequest.isDisableLogArchival()) {
            if (baseArchiveLocation == null) {
                throw new
                    GeniePreconditionException("Job archival is enabled but base location for archival is null.");
            }
            jobArchivalLocation = baseArchiveLocation
                + JobConstants.FILE_PATH_DELIMITER
                + jobRequest.getId()
                + ".tar.gz";
        }
        // create the job object in the database with status INIT
        final Job job = new Job.Builder(
            jobRequest.getName(),
            jobRequest.getUser(),
            jobRequest.getVersion(),
            jobRequest.getCommandArgs()
        )
            .withArchiveLocation(jobArchivalLocation)
            .withDescription(jobRequest.getDescription())
            .withId(jobRequest.getId())
            .withStatus(JobStatus.INIT)
            .withTags(jobRequest.getTags())
            .withStatusMsg("Job Accepted and in initialization phase.")
            .build();

        this.jobPersistenceService.createJob(job);
        this.jobSubmitterService.submitJob(jobRequest);
        return jobRequest.getId();
    }

    /**
     * Gets the Job object to return to user given the id.
     *
     * @param jobId of job to retrieve
     * @return job object
     * @throws GenieException if there is an error
     */
    public Job getJob(@NotBlank final String jobId) throws GenieException {
        log.debug("Called with id {}", jobId);
        return this.jobSearchService.getJob(jobId);
    }

    /**
     * Gets the status of the job with the given id.
     *
     * @param id The id job to get status for
     * @return The job status
     * @throws GenieException for any problem particularly not found exceptions
     */
    public JobStatus getJobStatus(@NotBlank final String id) throws GenieException {
        log.debug("Called to get status for job {}", id);
        return this.jobSearchService.getJobStatus(id);
    }

    /**
     * Find subset of metadata about jobs for given filter criteria.
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
     * @param page        Page information of jobs to get
     * @return All jobs which match the criteria
     */
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
        final Date minStarted,
        final Date maxStarted,
        final Date minFinished,
        final Date maxFinished,
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
            minStarted,
            maxStarted,
            minFinished,
            maxFinished,
            page
        );
    }

    /**
     * Kill the job identified by the given id.
     *
     * @param jobId id of the job to kill
     * @throws GenieException if there is an error
     */
    public void killJob(@NotBlank final String jobId) throws GenieException {
        this.jobKillService.killJob(jobId);
    }

    /**
     * Get the hostname a job is running on.
     *
     * @param jobId The id of the job to get the hostname for
     * @return The hostname
     * @throws GenieException If the job isn't found or any other error
     */
    public String getJobHost(@NotBlank final String jobId) throws GenieException {
        final JobExecution jobExecution = this.jobSearchService.getJobExecution(jobId);
        if (jobExecution != null) {
            return jobExecution.getHostname();
        } else {
            throw new GenieNotFoundException("No job execution found for id " + jobId);
        }
    }

    /**
     * Get the original job request for the job with the given id.
     *
     * @param id The id of the job to get the request for. Not blank
     * @return The job request for the given id
     * @throws GenieException If no job request with the given id exists
     */
    public JobRequest getJobRequest(@NotBlank final String id) throws GenieException {
        return this.jobSearchService.getJobRequest(id);
    }

    /**
     * Get the job execution for the job with the given id.
     *
     * @param id The id of the job to get the execution for. Not blank
     * @return The job execution for the given id
     * @throws GenieException If no job execution with the given id exists
     */
    public JobExecution getJobExecution(@NotBlank final String id) throws GenieException {
        return this.jobSearchService.getJobExecution(id);
    }
}
