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
import com.netflix.genie.core.events.JobScheduledEvent;
import com.netflix.genie.core.jobs.JobConstants;
import com.netflix.genie.core.jobs.JobLauncher;
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.NotBlank;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.TaskRejectedException;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.concurrent.Future;


/**
 * Implementation of the JobService APIs.
 *
 * @author amsharma
 * @author tgianos
 * @since 3.0.0
 */
@Slf4j
public class JobCoordinatorService {

    private final AsyncTaskExecutor taskExecutor;
    private final JobPersistenceService jobPersistenceService;
    private final JobSubmitterService jobSubmitterService;
    private final JobKillService jobKillService;
    private final JobCountService jobCountService;
    private final int maxRunningJobs;
    private final String baseArchiveLocation;
    private final Registry registry;
    private final ApplicationEventPublisher eventPublisher;

    /**
     * Constructor.
     *
     * @param taskExecutor          The executor to use to launch jobs
     * @param jobPersistenceService implementation of job persistence service interface
     * @param jobSubmitterService   implementation of the job submitter service
     * @param jobKillService        The job kill service to use
     * @param jobCountService       The service which will return the number of running jobs on this host
     * @param baseArchiveLocation   The base directory location of where the job dir should be archived
     * @param maxRunningJobs        The maximum number of jobs that can run on this host at any time
     * @param registry              The registry to use for metrics
     * @param eventPublisher        The application event publisher to use
     */
    public JobCoordinatorService(
        @NotNull final AsyncTaskExecutor taskExecutor,
        @NotNull final JobPersistenceService jobPersistenceService,
        @NotNull final JobSubmitterService jobSubmitterService,
        @NotNull final JobKillService jobKillService,
        @NotNull final JobCountService jobCountService,
        @NotNull final String baseArchiveLocation,
        final int maxRunningJobs,
        @NotNull final Registry registry,
        @NotNull final ApplicationEventPublisher eventPublisher
    ) {
        this.taskExecutor = taskExecutor;
        this.jobPersistenceService = jobPersistenceService;
        this.jobSubmitterService = jobSubmitterService;
        this.jobKillService = jobKillService;
        this.jobCountService = jobCountService;
        this.baseArchiveLocation = baseArchiveLocation;
        this.maxRunningJobs = maxRunningJobs;
        this.registry = registry;
        this.eventPublisher = eventPublisher;
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

        synchronized (this) {
            final int numActiveJobs = this.jobCountService.getNumJobs();
            if (numActiveJobs < this.maxRunningJobs) {
                jobBuilder
                    .withStatus(JobStatus.INIT)
                    .withStatusMsg("Job Accepted and in initialization phase.");
                // TODO: if this throws exception the job will never be marked failed
                this.jobPersistenceService.createJob(jobBuilder.build());
                try {
                    final Future<?> task = this.taskExecutor.submit(
                        new JobLauncher(this.jobSubmitterService, jobRequest, this.registry)
                    );

                    // Tell the system a new job has been scheduled so any actions can be taken
                    this.eventPublisher.publishEvent(new JobScheduledEvent(jobRequest.getId(), task, this));
                } catch (final TaskRejectedException e) {
                    final String errorMsg = "Unable to launch job due to exception: " + e.getMessage();
                    this.jobPersistenceService.updateJobStatus(jobRequest.getId(), JobStatus.FAILED, errorMsg);
                    throw new GenieServerException(errorMsg, e);
                }
                return jobRequest.getId();
            } else {
                jobBuilder
                    .withStatus(JobStatus.FAILED)
                    .withStatusMsg("Unable to run job due to host being too busy during request.");
                this.jobPersistenceService.createJob(jobBuilder.build());
                throw new GenieServerUnavailableException(
                    "Running ("
                        + numActiveJobs
                        + ") when max running jobs is ("
                        + this.maxRunningJobs
                        + ") on this host. Unable to run job."
                );
            }
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
}
