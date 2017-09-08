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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
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
import com.netflix.genie.common.exceptions.GenieUserLimitExceededException;
import com.netflix.genie.core.jobs.JobConstants;
import com.netflix.genie.core.properties.JobsProperties;
import com.netflix.genie.core.properties.JobsUsersActiveLimitProperties;
import com.netflix.genie.core.services.ApplicationService;
import com.netflix.genie.core.services.ClusterLoadBalancer;
import com.netflix.genie.core.services.ClusterService;
import com.netflix.genie.core.services.CommandService;
import com.netflix.genie.core.services.JobCoordinatorService;
import com.netflix.genie.core.services.JobKillService;
import com.netflix.genie.core.services.JobPersistenceService;
import com.netflix.genie.core.services.JobSearchService;
import com.netflix.genie.core.services.JobStateService;
import com.netflix.genie.core.util.MetricsConstants;
import com.netflix.genie.core.util.MetricsUtils;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.validator.constraints.NotBlank;
import org.hibernate.validator.constraints.NotEmpty;
import org.springframework.aop.TargetClassAware;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
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

    private static final String NO_ID_FOUND = "No id found";
    private static final String LOAD_BALANCER_STATUS_SUCCESS = "success";
    private static final String LOAD_BALANCER_STATUS_NO_PREFERENCE = "no preference";
    private static final String LOAD_BALANCER_STATUS_EXCEPTION = "exception";
    private static final String LOAD_BALANCER_STATUS_INVALID = "invalid";

    private final JobPersistenceService jobPersistenceService;
    private final JobKillService jobKillService;
    private final JobStateService jobStateService;
    private final ApplicationService applicationService;
    private final JobSearchService jobSearchService;
    private final ClusterService clusterService;
    private final CommandService commandService;
    private final List<ClusterLoadBalancer> clusterLoadBalancers;
    private final JobsProperties jobsProperties;
    private final String hostName;

    // For reuse in queries
    private final Set<CommandStatus> commandStatuses;

    // Metrics
    private final Registry registry;
    private final Id coordinationTimerId;
    private final Id selectClusterTimerId;
    private final Id selectCommandTimerId;
    private final Id selectApplicationsTimerId;
    private final Id setJobEnvironmentTimerId;
    private final Counter noClusterSelectedCounter;
    private final Id loadBalancerCounterId;
    private final Counter noClusterFoundCounter;

    /**
     * Constructor.
     *
     * @param jobPersistenceService implementation of job persistence service interface
     * @param jobKillService        The job kill service to use
     * @param jobStateService       The service where we report the job state and keep track of various metrics about
     *                              jobs currently running
     * @param jobsProperties        The jobs properties to use
     * @param applicationService    Implementation of application service interface
     * @param jobSearchService      Implementation of job search service
     * @param clusterService        Implementation of cluster service interface
     * @param commandService        Implementation of command service interface
     * @param clusterLoadBalancers  Implementations of the cluster load balancer interface in invocation order
     * @param registry              The registry
     * @param hostName              The name of the host this Genie instance is running on
     */
    public JobCoordinatorServiceImpl(
        @NotNull final JobPersistenceService jobPersistenceService,
        @NotNull final JobKillService jobKillService,
        @NotNull final JobStateService jobStateService,
        @NotNull final JobsProperties jobsProperties,
        @NotNull final ApplicationService applicationService,
        @NotNull final JobSearchService jobSearchService,
        @NotNull final ClusterService clusterService,
        @NotNull final CommandService commandService,
        @NotNull @NotEmpty final List<ClusterLoadBalancer> clusterLoadBalancers,
        @NotNull final Registry registry,
        @NotBlank final String hostName
    ) {
        this.jobPersistenceService = jobPersistenceService;
        this.jobKillService = jobKillService;
        this.jobStateService = jobStateService;
        this.applicationService = applicationService;
        this.jobSearchService = jobSearchService;
        this.clusterService = clusterService;
        this.commandService = commandService;
        this.clusterLoadBalancers = clusterLoadBalancers;
        this.jobsProperties = jobsProperties;
        this.hostName = hostName;

        // We'll only care about active statuses
        this.commandStatuses = EnumSet.noneOf(CommandStatus.class);
        this.commandStatuses.add(CommandStatus.ACTIVE);

        // Metrics
        this.registry = registry;
        this.coordinationTimerId = registry.createId("genie.jobs.coordination.timer");
        this.selectClusterTimerId = registry.createId("genie.jobs.submit.localRunner.selectCluster.timer");
        this.selectCommandTimerId = registry.createId("genie.jobs.submit.localRunner.selectCommand.timer");
        this.selectApplicationsTimerId = registry.createId("genie.jobs.submit.localRunner.selectApplications.timer");
        this.setJobEnvironmentTimerId = registry.createId("genie.jobs.submit.localRunner.setJobEnvironment.timer");
        this.loadBalancerCounterId = registry.createId("genie.jobs.submit.selectCluster.loadBalancer.counter");
        this.noClusterSelectedCounter = registry.counter("genie.jobs.submit.selectCluster.noneSelected.counter");
        this.noClusterFoundCounter = registry.counter("genie.jobs.submit.selectCluster.noneFound.counter");
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
        final Map<String, String> tags = MetricsUtils.newSuccessTagsMap();
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
            this.jobStateService.init(jobId);
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
                    this.jobStateService.schedule(jobId, jobRequest, cluster, command, applications, memory);
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
                .timer(this.coordinationTimerId.withTags(tags))
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
        final Map<String, String> tags = MetricsUtils.newSuccessTagsMap();
        try {
            final String clusterId = cluster
                .getId()
                .orElseThrow(() -> new GenieServerException("Cluster has no id"));
            final String commandId = command
                .getId()
                .orElseThrow(() -> new GenieServerException("Command has no id"));
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
        } catch (Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw t;
        } finally {
            this.registry.timer(
                setJobEnvironmentTimerId.withTags(tags)
            ).record(System.nanoTime() - jobEnvironmentStart, TimeUnit.NANOSECONDS);
        }
    }

    private Cluster getCluster(final JobRequest jobRequest) throws GenieException {
        final long start = System.nanoTime();
        final Map<String, String> timerTags = MetricsUtils.newSuccessTagsMap();
        final Map<String, String> counterTags = Maps.newHashMap();
        try {
            log.info("Selecting cluster for job {}", jobRequest.getId().orElse(NO_ID_FOUND));
            final List<Cluster> clusters = ImmutableList.copyOf(
                this.clusterService.chooseClusterForJobRequest(jobRequest)
            );
            Cluster cluster = null;
            if (clusters.isEmpty()) {
                this.noClusterFoundCounter.increment();
                throw new GeniePreconditionException(
                    "No cluster/command combination found for the given criteria. Unable to continue"
                );
            } else if (clusters.size() == 1) {
                cluster = clusters.get(0);
            } else {
                for (final ClusterLoadBalancer loadBalancer : this.clusterLoadBalancers) {
                    final String loadBalancerClass =
                        (
                            loadBalancer instanceof TargetClassAware
                                ? ((TargetClassAware) loadBalancer).getTargetClass()
                                : loadBalancer.getClass()
                        ).getCanonicalName();
                    counterTags.put(MetricsConstants.TagKeys.CLASS_NAME, loadBalancerClass);
                    try {
                        final Cluster selectedCluster = loadBalancer.selectCluster(clusters, jobRequest);
                        if (selectedCluster != null) {
                            // Make sure the cluster existed in the original list of clusters
                            if (clusters.contains(selectedCluster)) {
                                log.debug(
                                    "Successfully selected cluster {} using load balancer {}",
                                    selectedCluster.getId().orElse(NO_ID_FOUND),
                                    loadBalancerClass
                                );
                                counterTags.put(MetricsConstants.TagKeys.STATUS, LOAD_BALANCER_STATUS_SUCCESS);
                                this.registry.counter(
                                    this.loadBalancerCounterId.withTags(counterTags)
                                ).increment();
                                cluster = selectedCluster;
                                break;
                            } else {
                                log.error(
                                    "Successfully selected cluster {} using load balancer {} but "
                                        + "it wasn't in original cluster list {}",
                                    selectedCluster.getId().orElse(NO_ID_FOUND),
                                    loadBalancerClass,
                                    clusters
                                );
                                counterTags.put(MetricsConstants.TagKeys.STATUS, LOAD_BALANCER_STATUS_INVALID);

                                this.registry.counter(
                                    this.loadBalancerCounterId.withTags(counterTags)
                                ).increment();
                            }
                        } else {
                            counterTags.put(MetricsConstants.TagKeys.STATUS, LOAD_BALANCER_STATUS_NO_PREFERENCE);
                            this.registry.counter(
                                this.loadBalancerCounterId.withTags(counterTags)
                            ).increment();
                        }
                    } catch (final Exception e) {
                        log.error("Cluster load balancer {} threw exception:", loadBalancer, e);
                        counterTags.put(MetricsConstants.TagKeys.STATUS, LOAD_BALANCER_STATUS_EXCEPTION);
                        this.registry.counter(
                            this.loadBalancerCounterId.withTags(counterTags)
                        ).increment();
                    }
                }

                // Make sure we selected a cluster
                if (cluster == null) {
                    this.noClusterSelectedCounter.increment();
                    throw new GeniePreconditionException(
                        "Unable to select a cluster from using any of the available load balancers."
                    );
                }
            }

            log.info(
                "Selected cluster {} for job {}",
                cluster.getId().orElse(NO_ID_FOUND),
                jobRequest.getId().orElse(NO_ID_FOUND)
            );
            return cluster;
        } catch (Throwable t) {
            MetricsUtils.addFailureTagsWithException(timerTags, t);
            throw t;
        } finally {
            this.registry.timer(
                selectClusterTimerId.withTags(timerTags)
            ).record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    private Command getCommand(final JobRequest jobRequest, final Cluster cluster) throws GenieException {
        final long start = System.nanoTime();
        final Map<String, String> tags = MetricsUtils.newSuccessTagsMap();
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
                    log.info(
                        "Selected command {} for job {} ",
                        command.getId().orElse(NO_ID_FOUND),
                        jobRequest.getId().orElse(NO_ID_FOUND)
                    );
                    return command;
                }
            }

            throw new GeniePreconditionException(
                "No command found matching all command criteria ["
                    + commandCriteria
                    + "] attached to cluster with id: "
                    + cluster.getId().orElse(NO_ID_FOUND)
            );

        } catch (Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw t;
        } finally {
            this.registry.timer(
                selectCommandTimerId.withTags(tags)
            ).record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    private List<Application> getApplications(
        final JobRequest jobRequest,
        final Command command
    ) throws GenieException {
        final long start = System.nanoTime();
        final Map<String, String> tags = MetricsUtils.newSuccessTagsMap();
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
                    .reduce((one, two) -> one + "," + two)
                    .orElse(NO_ID_FOUND),
                jobRequest.getId().orElse(NO_ID_FOUND)
            );
            return applications;

        } catch (Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw t;
        } finally {
            this.registry.timer(
                selectApplicationsTimerId.withTags(tags)
            ).record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }
}
