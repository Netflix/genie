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
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.exceptions.GenieServerUnavailableException;
import com.netflix.genie.core.jobs.JobConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.NotBlank;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;


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
    private final JobSubmitterService jobSubmitterService;
    private final JobKillService jobKillService;
    private final JobCountService jobCountService;
    private final int maxRunningJobs;
    private String baseArchiveLocation;

    /**
     * Constructor.
     *
     * @param jobPersistenceService implementation of job persistence service interface
     * @param jobSubmitterService   implementation of the job submitter service
     * @param jobKillService        The job kill service to use
     * @param jobCountService       The service which will return the number of running jobs on this host
     * @param baseArchiveLocation   The base directory location of where the job dir should be archived
     * @param maxRunningJobs        The maximum number of jobs that can run on this host at any time
     */
    public JobCoordinatorService(
        @NotNull final JobPersistenceService jobPersistenceService,
        @NotNull final JobSubmitterService jobSubmitterService,
        @NotNull final JobKillService jobKillService,
        @NotNull final JobCountService jobCountService,
        @NotNull final String baseArchiveLocation,
        final int maxRunningJobs
    ) {
        this.jobPersistenceService = jobPersistenceService;
        this.jobSubmitterService = jobSubmitterService;
        this.jobKillService = jobKillService;
        this.jobCountService = jobCountService;
        this.baseArchiveLocation = baseArchiveLocation;
        this.maxRunningJobs = maxRunningJobs;
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
        if (StringUtils.isBlank(jobRequest.getId())) {
            throw new GenieServerException("Id of the jobRequest cannot be null");
        }

        // Log the job request and optionally the client host
        this.jobPersistenceService.createJobRequest(jobRequest, clientHost);

        String archiveLocation = null;
        if (!jobRequest.isDisableLogArchival()) {
            archiveLocation = this.baseArchiveLocation
                + JobConstants.FILE_PATH_DELIMITER
                + jobRequest.getId()
                + ".tar.gz";
        }

        // create the job object in the database with status INIT
        final Job.Builder jobBuilder = new Job.Builder(
            jobRequest.getName(),
            jobRequest.getUser(),
            jobRequest.getVersion(),
            jobRequest.getCommandArgs()
        )
            .withArchiveLocation(archiveLocation)
            .withDescription(jobRequest.getDescription())
            .withId(jobRequest.getId())
            .withTags(jobRequest.getTags());

        if (this.canRunJob()) {
            jobBuilder
                .withStatus(JobStatus.INIT)
                .withStatusMsg("Job Accepted and in initialization phase.");
            this.jobPersistenceService.createJob(jobBuilder.build());
            this.jobSubmitterService.submitJob(jobRequest);
            return jobRequest.getId();
        } else {
            jobBuilder
                .withStatus(JobStatus.FAILED)
                .withStatusMsg("Unable to run job due to host being too busy during request.");
            this.jobPersistenceService.createJob(jobBuilder.build());
            throw new GenieServerUnavailableException("Reached max running jobs on this host. Unable to run job.");
        }
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
     * Synchronized to make sure only one request thread at a time is checking whether they can run.
     *
     * @return true if the job can run on this node or not
     */
    private synchronized boolean canRunJob() {
        return this.jobCountService.getNumRunningJobs() < this.maxRunningJobs;
    }
}
