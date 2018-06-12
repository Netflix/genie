/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.genie.web.services.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobMetadata;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.exceptions.GenieServerUnavailableException;
import com.netflix.genie.common.exceptions.GenieUserLimitExceededException;
import com.netflix.genie.common.internal.dto.v4.Application;
import com.netflix.genie.common.internal.dto.v4.Cluster;
import com.netflix.genie.common.internal.dto.v4.Command;
import com.netflix.genie.common.internal.dto.v4.JobSpecification;
import com.netflix.genie.common.internal.jobs.JobConstants;
import com.netflix.genie.web.controllers.DtoConverters;
import com.netflix.genie.web.properties.JobsProperties;
import com.netflix.genie.web.properties.JobsUsersActiveLimitProperties;
import com.netflix.genie.web.services.ApplicationPersistenceService;
import com.netflix.genie.web.services.ClusterPersistenceService;
import com.netflix.genie.web.services.CommandPersistenceService;
import com.netflix.genie.web.services.JobCoordinatorService;
import com.netflix.genie.web.services.JobKillService;
import com.netflix.genie.web.services.JobPersistenceService;
import com.netflix.genie.web.services.JobSearchService;
import com.netflix.genie.web.services.JobSpecificationService;
import com.netflix.genie.web.services.JobStateService;
import com.netflix.genie.web.util.MetricsUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Implementation of the JobCoordinatorService APIs.
 *
 * @author amsharma
 * @author tgianos
 * @since 3.0.0
 */
@Slf4j
public class JobCoordinatorServiceImpl implements JobCoordinatorService {

    static final String OVERALL_COORDINATION_TIMER_NAME = "genie.jobs.coordination.timer";
    static final String SET_JOB_ENVIRONMENT_TIMER_NAME = "genie.jobs.submit.localRunner.setJobEnvironment.timer";

    private static final String NO_ID_FOUND = "No id found";

    private final JobPersistenceService jobPersistenceService;
    private final JobKillService jobKillService;
    private final JobStateService jobStateService;
    private final ApplicationPersistenceService applicationPersistenceService;
    private final JobSearchService jobSearchService;
    private final ClusterPersistenceService clusterPersistenceService;
    private final CommandPersistenceService commandPersistenceService;
    private final JobSpecificationService specificationService;
    private final JobsProperties jobsProperties;
    private final String hostname;

    // Metrics
    private final MeterRegistry registry;

    /**
     * Constructor.
     *
     * @param jobPersistenceService         implementation of job persistence service interface
     * @param jobKillService                The job kill service to use
     * @param jobStateService               The service where we report the job state and keep track of
     *                                      various metrics about jobs currently running
     * @param jobsProperties                The jobs properties to use
     * @param applicationPersistenceService Implementation of application service interface
     * @param jobSearchService              Implementation of job search service
     * @param clusterPersistenceService     Implementation of cluster service interface
     * @param commandPersistenceService     Implementation of command service interface
     * @param specificationService          The job specification service to use
     * @param registry                      The registry
     * @param hostname                      The name of the host this Genie instance is running on
     */
    public JobCoordinatorServiceImpl(
        @NotNull final JobPersistenceService jobPersistenceService,
        @NotNull final JobKillService jobKillService,
        @NotNull final JobStateService jobStateService,
        @NotNull final JobsProperties jobsProperties,
        @NotNull final ApplicationPersistenceService applicationPersistenceService,
        @NotNull final JobSearchService jobSearchService,
        @NotNull final ClusterPersistenceService clusterPersistenceService,
        @NotNull final CommandPersistenceService commandPersistenceService,
        @NotNull final JobSpecificationService specificationService,
        @NotNull final MeterRegistry registry,
        @NotBlank final String hostname
    ) {
        this.jobPersistenceService = jobPersistenceService;
        this.jobKillService = jobKillService;
        this.jobStateService = jobStateService;
        this.applicationPersistenceService = applicationPersistenceService;
        this.jobSearchService = jobSearchService;
        this.clusterPersistenceService = clusterPersistenceService;
        this.commandPersistenceService = commandPersistenceService;
        this.specificationService = specificationService;
        this.jobsProperties = jobsProperties;
        this.hostname = hostname;

        // Metrics
        this.registry = registry;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String coordinateJob(
        @Valid
        @NotNull(message = "No job request provided. Unable to execute.") final JobRequest jobRequest,
        @Valid
        @NotNull(message = "No job metadata provided. Unable to execute.") final JobMetadata jobMetadata
    ) throws GenieException {
        final long coordinationStart = System.nanoTime();
        final Set<Tag> tags = Sets.newHashSet();
        final String jobId = jobRequest
            .getId()
            .orElseThrow(() -> new GenieServerException("Id of the jobRequest cannot be null"));
        JobStatus jobStatus = JobStatus.FAILED;
        try {
            log.info("Called to schedule job launch for job {}", jobId);
            // create the job object in the database with status INIT
            final Job.Builder jobBuilder = new Job.Builder(
                jobRequest.getName(),
                jobRequest.getUser(),
                jobRequest.getVersion()
            )
                .withId(jobId)
                .withTags(jobRequest.getTags())
                .withStatus(JobStatus.INIT)
                .withStatusMsg("Job Accepted and in initialization phase.");

            jobRequest.getCommandArgs().ifPresent(
                commandArgs ->
                    jobBuilder
                        .withCommandArgs(
                            Lists.newArrayList(StringUtils.splitByWholeSeparator(commandArgs, StringUtils.SPACE))
                        )
            );
            jobRequest.getDescription().ifPresent(jobBuilder::withDescription);
            if (!jobRequest.isDisableLogArchival()) {
                jobBuilder.withArchiveLocation(
                    this.jobsProperties.getLocations().getArchives()
                        + JobConstants.FILE_PATH_DELIMITER + jobId + ".tar.gz"
                );
            }

            final JobExecution jobExecution = new JobExecution
                .Builder(this.hostname)
                .withId(jobId)
                .build();

            // Log all the job initial job information
            this.jobPersistenceService.createJob(jobRequest, jobMetadata, jobBuilder.build(), jobExecution);
            this.jobStateService.init(jobId);
            log.info("Finding possible clusters and commands for job {}", jobRequest.getId().orElse(NO_ID_FOUND));
            final JobSpecification jobSpecification;
            try {
                jobSpecification = this.specificationService.resolveJobSpecification(
                    jobId,
                    DtoConverters.toV4JobRequest(jobRequest)
                );
            } catch (final RuntimeException re) {
                //TODO: Here for now as we figure out what to do with exceptions for JobSpecificationServiceImpl
                throw new GeniePreconditionException(re.getMessage(), re);
            }
            final Cluster cluster = this.clusterPersistenceService.getCluster(jobSpecification.getCluster().getId());
            final Command command = this.commandPersistenceService.getCommand(jobSpecification.getCommand().getId());

            // Now that we have command how much memory should the job use?
            final int memory = jobRequest.getMemory()
                .orElse(command.getMemory().orElse(this.jobsProperties.getMemory().getDefaultJobMemory()));

            final ImmutableList.Builder<Application> applicationsBuilder = ImmutableList.builder();
            for (final JobSpecification.ExecutionResource applicationResource : jobSpecification.getApplications()) {
                applicationsBuilder.add(this.applicationPersistenceService.getApplication(applicationResource.getId()));
            }
            final ImmutableList<Application> applications = applicationsBuilder.build();

            // Save all the runtime information
            this.setRuntimeEnvironment(jobId, cluster, command, applications, memory);

            final int maxJobMemory = this.jobsProperties.getMemory().getMaxJobMemory();
            if (memory > maxJobMemory) {
                jobStatus = JobStatus.INVALID;
                throw new GeniePreconditionException(
                    "Requested "
                        + memory
                        + " MB to run job which is more than the "
                        + maxJobMemory
                        + " MB allowed"
                );
            }

            log.info("Checking if can run job {} from user {}", jobRequest.getId(), jobRequest.getUser());
            final JobsUsersActiveLimitProperties activeLimit = this.jobsProperties.getUsers().getActiveLimit();
            if (activeLimit.isEnabled()) {
                final long activeJobsLimit = activeLimit.getCount();
                final long activeJobsCount = this.jobSearchService.getActiveJobCountForUser(jobRequest.getUser());
                if (activeJobsCount >= activeJobsLimit) {
                    throw GenieUserLimitExceededException.createForActiveJobsLimit(
                        jobRequest.getUser(),
                        activeJobsCount,
                        activeJobsLimit);
                }
            }

            synchronized (this) {
                log.info("Checking if can run job {} on this node", jobRequest.getId());
                final int maxSystemMemory = this.jobsProperties.getMemory().getMaxSystemMemory();
                final int usedMemory = this.jobStateService.getUsedMemory();
                if (usedMemory + memory <= maxSystemMemory) {
                    log.info(
                        "Job {} can run on this node as only {}/{} MB are used and requested {} MB",
                        jobId,
                        usedMemory,
                        maxSystemMemory,
                        memory
                    );
                    // Tell the system a new job has been scheduled so any actions can be taken
                    log.info("Publishing job scheduled event for job {}", jobId);
                    this.jobStateService.schedule(
                        jobId,
                        jobRequest,
                        cluster,
                        command,
                        applications,
                        memory
                    );
                    MetricsUtils.addSuccessTags(tags);
                    return jobId;
                } else {
                    throw new GenieServerUnavailableException(
                        "Job "
                            + jobId
                            + " can't run on this node "
                            + usedMemory
                            + "/"
                            + maxSystemMemory
                            + " MB are used and requested "
                            + memory
                            + " MB"
                    );
                }
            }
        } catch (final GenieConflictException e) {
            MetricsUtils.addFailureTagsWithException(tags, e);
            // Job has not been initiated so we don't have to call JobStateService.done()
            throw e;
        } catch (final GenieException e) {
            MetricsUtils.addFailureTagsWithException(tags, e);
            //
            // Need to check if the job exists in the JobStateService
            // because this error can happen before the job is initiated.
            //
            if (this.jobStateService.jobExists(jobId)) {
                this.jobStateService.done(jobId);
                this.jobPersistenceService.updateJobStatus(jobId, jobStatus, e.getMessage());
            }
            throw e;
        } catch (final Exception e) {
            MetricsUtils.addFailureTagsWithException(tags, e);
            //
            // Need to check if the job exists in the JobStateService
            // because this error can happen before the job is initiated.
            //
            if (this.jobStateService.jobExists(jobId)) {
                this.jobStateService.done(jobId);
                this.jobPersistenceService.updateJobStatus(jobId, jobStatus, e.getMessage());
            }
            throw new GenieServerException("Failed to coordinate job launch", e);
        } catch (final Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw t;
        } finally {
            this.registry
                .timer(OVERALL_COORDINATION_TIMER_NAME, tags)
                .record(System.nanoTime() - coordinationStart, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void killJob(@NotBlank final String jobId, @NotBlank final String reason) throws GenieException {
        this.jobKillService.killJob(jobId, reason);
    }

    private void setRuntimeEnvironment(
        final String jobId,
        final Cluster cluster,
        final Command command,
        final List<Application> applications,
        final int memory
    ) throws GenieException {
        final long jobEnvironmentStart = System.nanoTime();
        final Set<Tag> tags = Sets.newHashSet();
        try {
            final String clusterId = cluster.getId();
            final String commandId = command.getId();
            this.jobPersistenceService.updateJobWithRuntimeEnvironment(
                jobId,
                clusterId,
                commandId,
                applications
                    .stream()
                    .map(Application::getId)
                    .collect(Collectors.toList()),
                memory
            );
            MetricsUtils.addSuccessTags(tags);
        } catch (final Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw t;
        } finally {
            this.registry
                .timer(SET_JOB_ENVIRONMENT_TIMER_NAME, tags)
                .record(System.nanoTime() - jobEnvironmentStart, TimeUnit.NANOSECONDS);
        }
    }
}
