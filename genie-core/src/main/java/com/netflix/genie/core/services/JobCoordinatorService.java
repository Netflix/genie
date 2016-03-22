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
import com.netflix.genie.common.exceptions.GeniePreconditionException;
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
        // TODO: Should likely validate it has an ID at this point since we return it without ever setting it
        log.debug("Called with job request {}", jobRequest);

        // Log the job request
        this.jobPersistenceService.createJobRequest(jobRequest);

        if (StringUtils.isNotBlank(clientHost)) {
            this.jobPersistenceService.addClientHostToJobRequest(jobRequest.getId(), clientHost);
        }

        String archiveLocation = null;
        if (!jobRequest.isDisableLogArchival()) {
            if (this.baseArchiveLocation == null) {
                throw new GeniePreconditionException("Job archival is enabled but base location for archival is null.");
            }
            archiveLocation = this.baseArchiveLocation
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
            .withArchiveLocation(archiveLocation)
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
     * Kill the job identified by the given id.
     *
     * @param jobId id of the job to kill
     * @throws GenieException if there is an error
     */
    public void killJob(@NotBlank final String jobId) throws GenieException {
        this.jobKillService.killJob(jobId);
    }
}
