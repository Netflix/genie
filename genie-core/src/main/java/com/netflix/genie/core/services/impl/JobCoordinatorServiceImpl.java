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
package com.netflix.genie.core.services.impl;

import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
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
import com.netflix.genie.core.jobs.JobConstants;
import com.netflix.genie.core.properties.JobsProperties;
import com.netflix.genie.core.services.ApplicationService;
import com.netflix.genie.core.services.ClusterLoadBalancer;
import com.netflix.genie.core.services.ClusterService;
import com.netflix.genie.core.services.CommandService;
import com.netflix.genie.core.services.JobCoordinatorService;
import com.netflix.genie.core.services.JobKillService;
import com.netflix.genie.core.services.JobPersistenceService;
import com.netflix.genie.core.services.JobStateService;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.validator.constraints.NotBlank;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
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

    private final JobPersistenceService jobPersistenceService;
    private final JobKillService jobKillService;
    private final JobStateService jobStateService;
    private final ApplicationService applicationService;
    private final ClusterService clusterService;
    private final CommandService commandService;
    private final ClusterLoadBalancer clusterLoadBalancer;
    private final JobsProperties jobsProperties;
    private final String hostName;

    // For reuse in queries
    private final Set<CommandStatus> commandStatuses;

    // Metrics
    private final Timer coordinationTimer;
    private final Timer selectClusterTimer;
    private final Timer selectCommandTimer;
    private final Timer selectApplicationsTimer;
    private final Timer setJobEnvironmentTimer;

    /**
     * Constructor.
     *
     * @param jobPersistenceService implementation of job persistence service interface
     * @param jobKillService        The job kill service to use
     * @param jobStateService       The service where we report the job state and keep track of various metrics about
     *                              jobs currently running
     * @param jobsProperties        The jobs properties to use
     * @param applicationService    Implementation of application service interface
     * @param clusterService        Implementation of cluster service interface
     * @param commandService        Implementation of command service interface
     * @param clusterLoadBalancer   Implementation of the cluster load balancer interface
     * @param registry              The registry
     * @param hostName              The name of the host this Genie instance is running on
     */
    public JobCoordinatorServiceImpl(
        @NotNull final JobPersistenceService jobPersistenceService,
        @NotNull final JobKillService jobKillService,
        @NotNull final JobStateService jobStateService,
        @NotNull final JobsProperties jobsProperties,
        @NotNull final ApplicationService applicationService,
        @NotNull final ClusterService clusterService,
        @NotNull final CommandService commandService,
        @NotNull final ClusterLoadBalancer clusterLoadBalancer,
        @NotNull final Registry registry,
        @NotBlank final String hostName
    ) {
        this.jobPersistenceService = jobPersistenceService;
        this.jobKillService = jobKillService;
        this.jobStateService = jobStateService;
        this.applicationService = applicationService;
        this.clusterService = clusterService;
        this.commandService = commandService;
        this.clusterLoadBalancer = clusterLoadBalancer;
        this.jobsProperties = jobsProperties;
        this.hostName = hostName;

        // We'll only care about active statuses
        this.commandStatuses = EnumSet.noneOf(CommandStatus.class);
        this.commandStatuses.add(CommandStatus.ACTIVE);

        // Metrics
        this.coordinationTimer = registry.timer("genie.jobs.coordination.timer");
        this.selectClusterTimer = registry.timer("genie.jobs.submit.localRunner.selectCluster.timer");
        this.selectCommandTimer = registry.timer("genie.jobs.submit.localRunner.selectCommand.timer");
        this.selectApplicationsTimer = registry.timer("genie.jobs.submit.localRunner.selectApplications.timer");
        this.setJobEnvironmentTimer = registry.timer("genie.jobs.submit.localRunner.setJobEnvironment.timer");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String coordinateJob(
        @Valid
        @NotNull(message = "No job request provided. Unable to submit job for execution.")
        final JobRequest jobRequest,
        @Valid
        @NotNull(message = "No job metadata provided. Unable to submit job for execution.")
        final JobMetadata jobMetadata
    ) throws GenieException {
        final long coordinationStart = System.nanoTime();
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
                jobRequest.getVersion(),
                jobRequest.getCommandArgs()
            )
                .withId(jobId)
                .withTags(jobRequest.getTags())
                .withStatus(JobStatus.INIT)
                .withStatusMsg("Job Accepted and in initialization phase.");

            jobRequest.getDescription().ifPresent(jobBuilder::withDescription);
            if (!jobRequest.isDisableLogArchival()) {
                jobBuilder.withArchiveLocation(
                    this.jobsProperties.getLocations().getArchives()
                        + JobConstants.FILE_PATH_DELIMITER + jobId + ".tar.gz"
                );
            }

            final JobExecution jobExecution = new JobExecution.Builder(
                this.hostName
            )
                .withId(jobId)
                .build();

            // Log all the job initial job information
            this.jobPersistenceService.createJob(jobRequest, jobMetadata, jobBuilder.build(), jobExecution);
            jobStateService.init(jobId);
            //TODO: Combine the cluster and command selection into a single method/database query for efficiency
            // Resolve the cluster for the job request based on the tags specified
            final Cluster cluster = this.getCluster(jobRequest);
            // Resolve the command for the job request based on command tags and cluster chosen
            final Command command = this.getCommand(jobRequest, cluster);
            // Resolve the applications to use based on the command that was selected
            final List<Application> applications = this.getApplications(jobRequest, command);
            // Now that we have command how much memory should the job use?
            final int memory = jobRequest.getMemory()
                .orElse(command.getMemory().orElse(this.jobsProperties.getMemory().getDefaultJobMemory()));

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
                    jobStateService.schedule(jobId, jobRequest, cluster, command, applications, memory);
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
        } catch (GenieConflictException e) {
            throw e;
        } catch (GenieException e) {
            if (jobStateService.jobExists(jobId)) {
                jobStateService.done(jobId);
                jobPersistenceService.updateJobStatus(jobId, jobStatus, e.getMessage());
            }
            throw e;
        } catch (Exception e) {
            if (jobStateService.jobExists(jobId)) {
                jobStateService.done(jobId);
                jobPersistenceService.updateJobStatus(jobId, jobStatus, e.getMessage());
            }
            throw new GenieServerException(e);
        } finally {
            this.coordinationTimer.record(System.nanoTime() - coordinationStart, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void killJob(@NotBlank final String jobId) throws GenieException {
        this.jobKillService.killJob(jobId);
    }

    private void setRuntimeEnvironment(
        final String jobId,
        final Cluster cluster,
        final Command command,
        final List<Application> applications,
        final int memory
    ) throws GenieException {
        final long jobEnvironmentStart = System.nanoTime();
        final String clusterId = cluster
            .getId()
            .orElseThrow(() -> new GenieServerException("Cluster has no id"));
        final String commandId = command
            .getId()
            .orElseThrow(() -> new GenieServerException("Command has no id"));
        try {
            this.jobPersistenceService.updateJobWithRuntimeEnvironment(
                jobId,
                clusterId,
                commandId,
                applications
                    .stream()
                    .map(Application::getId)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toList()),
                memory
            );
        } finally {
            this.setJobEnvironmentTimer.record(System.nanoTime() - jobEnvironmentStart, TimeUnit.NANOSECONDS);
        }
    }

    private Cluster getCluster(final JobRequest jobRequest) throws GenieException {
        final long start = System.nanoTime();
        try {
            log.info("Selecting cluster for job {}", jobRequest.getId());
            final Cluster cluster
                = this.clusterLoadBalancer.selectCluster(this.clusterService.chooseClusterForJobRequest(jobRequest));
            log.info("Selected cluster {} for job {}", cluster.getId(), jobRequest.getId());
            return cluster;
        } finally {
            this.selectClusterTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    private Command getCommand(final JobRequest jobRequest, final Cluster cluster) throws GenieException {
        final long start = System.nanoTime();
        try {
            final String clusterId = cluster.getId().orElseThrow(() -> new GenieServerException("No cluster id."));
            final String jobId = jobRequest.getId().orElseThrow(() -> new GenieServerException("No job id"));
            log.info("Selecting command attached to cluster {} for job {} ", clusterId, jobId);
            final Set<String> commandCriteria = jobRequest.getCommandCriteria();
            // TODO: what happens if the get method throws an error we don't mark the job failed here
            for (
                final Command command : this.clusterService.getCommandsForCluster(clusterId, this.commandStatuses)
                ) {
                if (command.getTags().containsAll(jobRequest.getCommandCriteria())) {
                    log.info("Selected command {} for job {} ", command.getId(), jobRequest.getId());
                    return command;
                }
            }

            throw new GeniePreconditionException(
                "No command found matching all command criteria ["
                    + commandCriteria
                    + "] attached to cluster with id: "
                    + cluster.getId()
            );
        } finally {
            this.selectCommandTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    private List<Application> getApplications(
        final JobRequest jobRequest,
        final Command command
    ) throws GenieException {
        final long start = System.nanoTime();
        try {
            final String jobId = jobRequest.getId().orElseThrow(() -> new GenieServerException("No job Id"));
            final String commandId = command.getId().orElseThrow(() -> new GenieServerException("No command Id"));
            log.info("Selecting applications for job {} and command {}", jobId, commandId);
            // TODO: What do we do about application status? Should probably check here
            final List<Application> applications = new ArrayList<>();
            if (jobRequest.getApplications().isEmpty()) {
                applications.addAll(this.commandService.getApplicationsForCommand(commandId));
            } else {
                for (final String applicationId : jobRequest.getApplications()) {
                    applications.add(this.applicationService.getApplication(applicationId));
                }
            }
            log.info(
                "Selected applications {} for job {}",
                applications
                    .stream()
                    .map(Application::getId)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .reduce((one, two) -> one + "," + two),
                jobRequest.getId()
            );
            return applications;
        } finally {
            this.selectApplicationsTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }
}
