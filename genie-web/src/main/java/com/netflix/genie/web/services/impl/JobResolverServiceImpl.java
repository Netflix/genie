/*
 *
 *  Copyright 2018 Netflix, Inc.
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.ClusterCriteria;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.external.dtos.v4.Application;
import com.netflix.genie.common.external.dtos.v4.Cluster;
import com.netflix.genie.common.external.dtos.v4.Command;
import com.netflix.genie.common.external.dtos.v4.Criterion;
import com.netflix.genie.common.external.dtos.v4.ExecutionEnvironment;
import com.netflix.genie.common.external.dtos.v4.JobEnvironment;
import com.netflix.genie.common.external.dtos.v4.JobMetadata;
import com.netflix.genie.common.external.dtos.v4.JobRequest;
import com.netflix.genie.common.external.dtos.v4.JobSpecification;
import com.netflix.genie.common.external.dtos.v4.JobStatus;
import com.netflix.genie.common.internal.exceptions.checked.GenieJobResolutionException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobNotFoundException;
import com.netflix.genie.common.internal.jobs.JobConstants;
import com.netflix.genie.web.data.services.ApplicationPersistenceService;
import com.netflix.genie.web.data.services.ClusterPersistenceService;
import com.netflix.genie.web.data.services.CommandPersistenceService;
import com.netflix.genie.web.data.services.JobPersistenceService;
import com.netflix.genie.web.dtos.ResolvedJob;
import com.netflix.genie.web.dtos.ResourceSelectionResult;
import com.netflix.genie.web.exceptions.checked.ResourceSelectionException;
import com.netflix.genie.web.properties.JobsProperties;
import com.netflix.genie.web.selectors.ClusterSelector;
import com.netflix.genie.web.selectors.CommandSelector;
import com.netflix.genie.web.services.JobResolverService;
import com.netflix.genie.web.util.MetricsConstants;
import com.netflix.genie.web.util.MetricsUtils;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.aop.TargetClassAware;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.validation.annotation.Validated;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import java.io.File;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Implementation of the {@link JobResolverService} APIs.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Slf4j
@Validated
public class JobResolverServiceImpl implements JobResolverService {

    /**
     * How long it takes to completely resolve a job given inputs.
     */
    private static final String RESOLVE_JOB_TIMER = "genie.services.jobResolver.resolve.timer";

    /**
     * How long it takes to query the database for cluster command combinations matching supplied criteria.
     */
    private static final String CLUSTER_COMMAND_QUERY_TIMER = "genie.services.jobResolver.clusterCommandQuery.timer";

    /**
     * How long it takes to resolve a cluster for a job given the resolved command and the request criteria.
     */
    private static final String RESOLVE_CLUSTER_TIMER = "genie.services.jobResolver.resolveCluster.timer";

    /**
     * The number of criteria combinations attempted while resolving a cluster.
     */
    private static final String RESOLVE_CLUSTER_CRITERIA_COMBINATION_COUNTER
        = "genie.services.jobResolver.resolveCluster.criteriaCombination.count";

    /**
     * The number of criteria combinations attempted while resolving a cluster.
     */
    private static final String RESOLVE_CLUSTER_QUERY_COUNTER = "genie.services.jobResolver.resolveCluster.query.count";

    /**
     * How long it takes to resolve a command for a job given the supplied command criterion.
     */
    private static final String RESOLVE_COMMAND_TIMER = "genie.services.jobResolver.resolveCommand.timer";

    /**
     * How long it takes to select a cluster from the set of clusters returned by database query.
     */
    private static final String SELECT_CLUSTER_TIMER = "genie.services.jobResolver.selectCluster.timer";

    /**
     * How long it takes to select a command for a given cluster.
     */
    private static final String SELECT_COMMAND_TIMER = "genie.services.jobResolver.selectCommand.timer";

    /**
     * How long it takes to select the applications for a given command.
     */
    private static final String SELECT_APPLICATIONS_TIMER = "genie.services.jobResolver.selectApplications.timer";

    /**
     * How many times a cluster selector is invoked.
     */
    private static final String CLUSTER_SELECTOR_COUNTER = "genie.services.jobResolver.clusterSelector.counter";

    private static final String NO_RATIONALE = "No rationale provided";
    private static final String NO_ID_FOUND = "No id found";
    private static final String VERSION_4 = "4";
    private static final Tag SAVED_TAG = Tag.of("saved", "true");
    private static final Tag NOT_SAVED_TAG = Tag.of("saved", "false");

    private static final String ID_FIELD = "id";
    private static final String NAME_FIELD = "name";
    private static final String STATUS_FIELD = "status";
    private static final String VERSION_FIELD = "version";

    private static final String RESOLVE_CLUSTER_CRITERIA_COMBINATION_TAG = "criteriaCombinationCount";
    private static final String RESOLVE_CLUSTER_QUERY_TAG = "queryCount";

    private static final String CLUSTER_SELECTOR_STATUS_SUCCESS = "success";
    private static final String CLUSTER_SELECTOR_STATUS_NO_PREFERENCE = "no preference";
    private static final String CLUSTER_SELECTOR_STATUS_EXCEPTION = "exception";
    private static final String CLUSTER_SELECTOR_STATUS_INVALID = "invalid";

    private final ApplicationPersistenceService applicationPersistenceService;
    private final ClusterPersistenceService clusterPersistenceService;
    private final CommandPersistenceService commandPersistenceService;
    private final JobPersistenceService jobPersistenceService;
    private final List<ClusterSelector> clusterSelectors;
    private final CommandSelector commandSelector;
    private final MeterRegistry registry;
    private final int defaultMemory;
    // TODO: Switch to path
    private final File defaultJobDirectory;
    private final String defaultArchiveLocation;

    private final Counter noClusterSelectedCounter;
    private final Counter noClusterFoundCounter;

    /**
     * Constructor.
     *
     * @param applicationPersistenceService The {@link ApplicationPersistenceService} to use to manipulate applications
     * @param clusterPersistenceService     The {@link ClusterPersistenceService} to use to manipulate clusters
     * @param commandPersistenceService     The {@link CommandPersistenceService} to use to manipulate commands
     * @param jobPersistenceService         The {@link JobPersistenceService} instance to use
     * @param clusterSelectors              The {@link ClusterSelector} implementations to use
     * @param commandSelector               The {@link CommandSelector} implementation to use
     * @param registry                      The {@link MeterRegistry }metrics repository to use
     * @param jobsProperties                The properties for running a job set by the user
     */
    public JobResolverServiceImpl(
        final ApplicationPersistenceService applicationPersistenceService,
        final ClusterPersistenceService clusterPersistenceService,
        final CommandPersistenceService commandPersistenceService,
        final JobPersistenceService jobPersistenceService,
        @NotEmpty final List<ClusterSelector> clusterSelectors,
        final CommandSelector commandSelector, // TODO: For now this is a single value but maybe support List
        final MeterRegistry registry,
        final JobsProperties jobsProperties
    ) {
        this.applicationPersistenceService = applicationPersistenceService;
        this.clusterPersistenceService = clusterPersistenceService;
        this.commandPersistenceService = commandPersistenceService;
        this.jobPersistenceService = jobPersistenceService;
        this.clusterSelectors = clusterSelectors;
        this.commandSelector = commandSelector;
        this.defaultMemory = jobsProperties.getMemory().getDefaultJobMemory();

        final URI jobDirProperty = jobsProperties.getLocations().getJobs();
        this.defaultJobDirectory = Paths.get(jobDirProperty).toFile();
        this.defaultArchiveLocation = jobsProperties.getLocations().getArchives().toString();

        // Metrics
        this.registry = registry;
        this.noClusterSelectedCounter
            = this.registry.counter("genie.services.jobResolver.selectCluster.noneSelected.counter");
        this.noClusterFoundCounter
            = this.registry.counter("genie.services.jobResolver.selectCluster.noneFound.counter");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    @Transactional
    public ResolvedJob resolveJob(final String id) throws GenieJobResolutionException {
        final long start = System.nanoTime();
        final Set<Tag> tags = Sets.newHashSet(SAVED_TAG);
        try {
            log.info("Received request to resolve a job with id {}", id);
            final JobStatus jobStatus = this.jobPersistenceService.getJobStatus(id);
            if (!jobStatus.isResolvable()) {
                throw new IllegalArgumentException("Job " + id + " is already resolved: " + jobStatus);
            }

            final JobRequest jobRequest = this.jobPersistenceService
                .getJobRequest(id)
                .orElseThrow(() -> new GenieJobNotFoundException("No job with id " + id + " exists."));

            // Possible improvement to combine this query with a few others to save DB trips but for now...
            final boolean apiJob = this.jobPersistenceService.isApiJob(id);

            final ResolvedJob resolvedJob = this.resolve(id, jobRequest, apiJob);
            this.jobPersistenceService.saveResolvedJob(id, resolvedJob);
            MetricsUtils.addSuccessTags(tags);
            return resolvedJob;
        } catch (final GenieJobResolutionException e) {
            MetricsUtils.addFailureTagsWithException(tags, e);
            throw e;
        } catch (final Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw new GenieJobResolutionException(t);
        } finally {
            this.registry
                .timer(RESOLVE_JOB_TIMER, tags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public ResolvedJob resolveJob(
        final String id,
        @Valid final JobRequest jobRequest,
        final boolean apiJob
    ) throws GenieJobResolutionException {
        final long start = System.nanoTime();
        final Set<Tag> tags = Sets.newHashSet(NOT_SAVED_TAG);
        try {
            log.info(
                "Received request to resolve a job for id {} and request {}",
                id,
                jobRequest
            );

            final ResolvedJob resolvedJob = this.resolve(id, jobRequest, apiJob);
            MetricsUtils.addSuccessTags(tags);
            return resolvedJob;
        } catch (final GenieJobResolutionException e) {
            MetricsUtils.addFailureTagsWithException(tags, e);
            throw e;
        } catch (final Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw new GenieJobResolutionException(t);
        } finally {
            this.registry
                .timer(RESOLVE_JOB_TIMER, tags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    private ResolvedJob resolve(
        final String id,
        final JobRequest jobRequest,
        final boolean apiJob
    ) throws GenieJobResolutionException {
        final Cluster cluster;
        final Command command;

        if (this.useV4ResourceSelection()) {
            command = this.resolveCommand(jobRequest);
            cluster = this.resolveCluster(command, jobRequest, id);
        } else {
            final Map<Cluster, String> clustersAndCommandsForJob = this.queryForClustersAndCommands(
                jobRequest.getCriteria().getClusterCriteria(),
                jobRequest.getCriteria().getCommandCriterion()
            );
            // Resolve the cluster for the job request based on the tags specified
            cluster = this.selectCluster(id, jobRequest, clustersAndCommandsForJob.keySet());
            // Resolve the command for the job request based on command tags and cluster chosen
            command = this.getCommand(clustersAndCommandsForJob.get(cluster), id);
        }

        // Resolve the applications to use based on the command that was selected
        final List<JobSpecification.ExecutionResource> applicationResources = Lists.newArrayList();
        for (final Application application : this.getApplications(id, jobRequest, command)) {
            applicationResources.add(
                new JobSpecification.ExecutionResource(application.getId(), application.getResources())
            );
        }

        final int jobMemory = this.resolveJobMemory(jobRequest, command);

        final Map<String, String> environmentVariables = this.generateEnvironmentVariables(
            id,
            jobRequest,
            cluster,
            command,
            jobMemory
        );

        final Integer timeout;
        if (jobRequest.getRequestedAgentConfig().getTimeoutRequested().isPresent()) {
            timeout = jobRequest.getRequestedAgentConfig().getTimeoutRequested().get();
        } else if (apiJob) {
            // For backwards V3 compatibility
            timeout = com.netflix.genie.common.dto.JobRequest.DEFAULT_TIMEOUT_DURATION;
        } else {
            timeout = null;
        }

        final JobSpecification jobSpecification = new JobSpecification(
            command.getExecutable(),
            jobRequest.getCommandArgs(),
            new JobSpecification.ExecutionResource(id, jobRequest.getResources()),
            new JobSpecification.ExecutionResource(cluster.getId(), cluster.getResources()),
            new JobSpecification.ExecutionResource(command.getId(), command.getResources()),
            applicationResources,
            environmentVariables,
            jobRequest.getRequestedAgentConfig().isInteractive(),
            jobRequest
                .getRequestedAgentConfig()
                .getRequestedJobDirectoryLocation()
                .orElse(this.defaultJobDirectory),
            // TODO: Disable ability to disable archival for all jobs during internal V4 migration.
            //       Will allow us to reach out to clients who may set this variable but still expect output after
            //       job completion due to it being served off the node after completion in V3 but now it won't.
            //       Put this back in once all use cases have been hunted down and users are sure of their expected
            //       behavior
            this.toArchiveLocation(
                jobRequest
                    .getRequestedJobArchivalData()
                    .getRequestedArchiveLocationPrefix()
                    .orElse(this.defaultArchiveLocation),
                id
            ),
            timeout
        );

        final JobEnvironment jobEnvironment = new JobEnvironment
            .Builder(jobMemory)
            .withEnvironmentVariables(environmentVariables)
            .build();

        return new ResolvedJob(jobSpecification, jobEnvironment, jobRequest.getMetadata());
    }

    private Map<Cluster, String> queryForClustersAndCommands(
        final List<Criterion> clusterCriteria,
        final Criterion commandCriterion
    ) throws GenieJobResolutionException {
        final long start = System.nanoTime();
        final Set<Tag> tags = Sets.newHashSet();
        try {
            final Map<Cluster, String> clustersAndCommands
                = this.clusterPersistenceService.findClustersAndCommandsForCriteria(clusterCriteria, commandCriterion);
            MetricsUtils.addSuccessTags(tags);
            return clustersAndCommands;
        } catch (final Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw new GenieJobResolutionException(t);
        } finally {
            this.registry
                .timer(CLUSTER_COMMAND_QUERY_TIMER, tags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    private Cluster selectCluster(
        final String id,
        final JobRequest jobRequest,
        final Set<Cluster> clusters
    ) throws GenieJobResolutionException {
        final long start = System.nanoTime();
        final Set<Tag> timerTags = Sets.newHashSet();
        final Set<Tag> counterTags = Sets.newHashSet();
        try {
            final Cluster cluster;
            if (clusters.isEmpty()) {
                this.noClusterFoundCounter.increment();
                throw new GeniePreconditionException(
                    "No cluster/command combination found for the given criteria. Unable to continue"
                );
            } else if (clusters.size() == 1) {
                cluster = clusters
                    .stream()
                    .findFirst()
                    .orElseThrow(() -> new GenieServerException("Couldn't get cluster when size was one"));
            } else {
                cluster = this.selectClusterUsingClusterSelectors(counterTags, clusters, id, jobRequest);
            }

            if (cluster == null) {
                this.noClusterSelectedCounter.increment();
                throw new GenieJobResolutionException("No cluster found matching given criteria");
            }

            log.debug("Selected cluster {} for job {}", cluster.getId(), id);
            MetricsUtils.addSuccessTags(timerTags);
            return cluster;
        } catch (final Throwable t) {
            MetricsUtils.addFailureTagsWithException(timerTags, t);
            if (t instanceof GenieJobResolutionException) {
                throw (GenieJobResolutionException) t;
            } else {
                throw new GenieJobResolutionException(t);
            }
        } finally {
            this.registry
                .timer(SELECT_CLUSTER_TIMER, timerTags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }

    }

    private Command getCommand(
        final String commandId,
        final String jobId
    ) throws GenieJobResolutionException {
        final long start = System.nanoTime();
        final Set<Tag> tags = Sets.newHashSet();
        try {
            log.info("Selecting command for job {} ", jobId);
            final Command command = this.commandPersistenceService.getCommand(commandId);
            log.info("Selected command {} for job {} ", commandId, jobId);
            MetricsUtils.addSuccessTags(tags);
            return command;
        } catch (final Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw new GenieJobResolutionException(t);
        } finally {
            this.registry
                .timer(SELECT_COMMAND_TIMER, tags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    private List<Application> getApplications(
        final String id,
        final JobRequest jobRequest,
        final Command command
    ) throws GenieJobResolutionException {
        final long start = System.nanoTime();
        final Set<Tag> tags = Sets.newHashSet();
        try {
            final String commandId = command.getId();
            log.info("Selecting applications for job {} and command {}", id, commandId);
            // TODO: What do we do about application status? Should probably check here
            final List<Application> applications = Lists.newArrayList();
            if (jobRequest.getCriteria().getApplicationIds().isEmpty()) {
                applications.addAll(this.commandPersistenceService.getApplicationsForCommand(commandId));
            } else {
                for (final String applicationId : jobRequest.getCriteria().getApplicationIds()) {
                    applications.add(this.applicationPersistenceService.getApplication(applicationId));
                }
            }
            log.info(
                "Selected applications {} for job {}",
                applications
                    .stream()
                    .map(Application::getId)
                    .reduce((one, two) -> one + "," + two)
                    .orElse(NO_ID_FOUND),
                id
            );
            MetricsUtils.addSuccessTags(tags);
            return applications;
        } catch (final Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw new GenieJobResolutionException(t);
        } finally {
            this.registry
                .timer(SELECT_APPLICATIONS_TIMER, tags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    @Nullable
    private Cluster selectClusterUsingClusterSelectors(
        final Set<Tag> counterTags,
        final Set<Cluster> clusters,
        final String id,
        final JobRequest jobRequest
    ) {
        Cluster cluster = null;
        for (final ClusterSelector clusterSelector : this.clusterSelectors) {
            // TODO: We might want to get rid of this and use the result returned from the selector
            //       Keeping for now in interest of dev time
            final String clusterSelectorClass;
            if (clusterSelector instanceof TargetClassAware) {
                final Class<?> targetClass = ((TargetClassAware) clusterSelector).getTargetClass();
                if (targetClass != null) {
                    clusterSelectorClass = targetClass.getCanonicalName();
                } else {
                    clusterSelectorClass = clusterSelector.getClass().getCanonicalName();
                }
            } else {
                clusterSelectorClass = clusterSelector.getClass().getCanonicalName();
            }
            counterTags.add(Tag.of(MetricsConstants.TagKeys.CLASS_NAME, clusterSelectorClass));
            try {
                final ResourceSelectionResult<Cluster> result = clusterSelector.selectCluster(
                    clusters,
                    this.toV3JobRequest(id, jobRequest)
                );
                final Optional<Cluster> selectedClusterOptional = result.getSelectedResource();
                if (selectedClusterOptional.isPresent()) {
                    final Cluster selectedCluster = selectedClusterOptional.get();
                    // Make sure the cluster existed in the original list of clusters
                    if (clusters.contains(selectedCluster)) {
                        log.debug(
                            "Successfully selected cluster {} using selector {}",
                            selectedCluster.getId(),
                            clusterSelectorClass
                        );
                        counterTags.addAll(
                            Lists.newArrayList(
                                Tag.of(MetricsConstants.TagKeys.STATUS, CLUSTER_SELECTOR_STATUS_SUCCESS),
                                Tag.of(MetricsConstants.TagKeys.CLUSTER_ID, selectedCluster.getId()),
                                Tag.of(MetricsConstants.TagKeys.CLUSTER_NAME, selectedCluster.getMetadata().getName()),
                                Tag.of(MetricsConstants.TagKeys.CLUSTER_SELECTOR_CLASS, clusterSelectorClass)
                            )
                        );
                        cluster = selectedCluster;
                        break;
                    } else {
                        log.error(
                            "Successfully selected cluster {} using selector {} but it wasn't in original cluster "
                                + "list {}",
                            selectedCluster.getId(),
                            clusterSelectorClass,
                            clusters
                        );
                        counterTags.add(Tag.of(MetricsConstants.TagKeys.STATUS, CLUSTER_SELECTOR_STATUS_INVALID));
                    }
                } else {
                    counterTags.add(Tag.of(MetricsConstants.TagKeys.STATUS, CLUSTER_SELECTOR_STATUS_NO_PREFERENCE));
                }
            } catch (final Exception e) {
                log.error("Cluster selector {} evaluation threw exception:", clusterSelector, e);
                counterTags.add(Tag.of(MetricsConstants.TagKeys.STATUS, CLUSTER_SELECTOR_STATUS_EXCEPTION));
            } finally {
                this.registry.counter(CLUSTER_SELECTOR_COUNTER, counterTags).increment();
            }
        }

        return cluster;
    }

    private ImmutableMap<String, String> generateEnvironmentVariables(
        final String id,
        final JobRequest jobRequest,
        final Cluster cluster,
        final Command command,
        final int memory
    ) {
        // N.B. variables may be evaluated in a different order than they are added to this map (due to serialization).
        // Hence variables in this set should not depend on each-other.
        final ImmutableMap.Builder<String, String> envVariables = ImmutableMap.builder();
        envVariables.put(JobConstants.GENIE_VERSION_ENV_VAR, VERSION_4);
        envVariables.put(JobConstants.GENIE_CLUSTER_ID_ENV_VAR, cluster.getId());
        envVariables.put(JobConstants.GENIE_CLUSTER_NAME_ENV_VAR, cluster.getMetadata().getName());
        envVariables.put(JobConstants.GENIE_CLUSTER_TAGS_ENV_VAR, this.tagsToString(cluster.getMetadata().getTags()));
        envVariables.put(JobConstants.GENIE_COMMAND_ID_ENV_VAR, command.getId());
        envVariables.put(JobConstants.GENIE_COMMAND_NAME_ENV_VAR, command.getMetadata().getName());
        envVariables.put(JobConstants.GENIE_COMMAND_TAGS_ENV_VAR, this.tagsToString(command.getMetadata().getTags()));
        envVariables.put(JobConstants.GENIE_JOB_ID_ENV_VAR, id);
        envVariables.put(JobConstants.GENIE_JOB_NAME_ENV_VAR, jobRequest.getMetadata().getName());
        envVariables.put(JobConstants.GENIE_JOB_MEMORY_ENV_VAR, String.valueOf(memory));
        envVariables.put(JobConstants.GENIE_JOB_TAGS_ENV_VAR, this.tagsToString(jobRequest.getMetadata().getTags()));
        envVariables.put(
            JobConstants.GENIE_JOB_GROUPING_ENV_VAR,
            jobRequest.getMetadata().getGrouping().orElse("")
        );
        envVariables.put(
            JobConstants.GENIE_JOB_GROUPING_INSTANCE_ENV_VAR,
            jobRequest.getMetadata().getGroupingInstance().orElse("")
        );
        envVariables.put(
            JobConstants.GENIE_REQUESTED_COMMAND_TAGS_ENV_VAR,
            this.tagsToString(jobRequest.getCriteria().getCommandCriterion().getTags())
        );
        final List<Criterion> clusterCriteria = jobRequest.getCriteria().getClusterCriteria();
        final List<String> clusterCriteriaTags = Lists.newArrayListWithExpectedSize(clusterCriteria.size());
        for (int i = 0; i < clusterCriteria.size(); i++) {
            final Criterion criterion = clusterCriteria.get(i);
            final String criteriaTagsString = this.tagsToString(criterion.getTags());
            envVariables.put(JobConstants.GENIE_REQUESTED_CLUSTER_TAGS_ENV_VAR + "_" + i, criteriaTagsString);
            clusterCriteriaTags.add("[" + criteriaTagsString + "]");
        }
        envVariables.put(
            JobConstants.GENIE_REQUESTED_CLUSTER_TAGS_ENV_VAR,
            "[" + StringUtils.join(clusterCriteriaTags, ',') + "]"
        );
        envVariables.put(JobConstants.GENIE_USER_ENV_VAR, jobRequest.getMetadata().getUser());
        envVariables.put(JobConstants.GENIE_USER_GROUP_ENV_VAR, jobRequest.getMetadata().getGroup().orElse(""));
        return envVariables.build();
    }

    /**
     * Helper method to convert a v4 JobRequest to a v3 job request.
     *
     * @param jobRequest The v4 job request instance
     * @return The v3 job request instance
     */
    // TODO: This should be removed once we fully port rest of application to v4 and only have v3 interface with
    //       Adapters at API level
    private com.netflix.genie.common.dto.JobRequest toV3JobRequest(final String id, final JobRequest jobRequest) {
        final com.netflix.genie.common.dto.JobRequest.Builder v3Builder
            = new com.netflix.genie.common.dto.JobRequest.Builder(
            jobRequest.getMetadata().getName(),
            jobRequest.getMetadata().getUser(),
            jobRequest.getMetadata().getVersion(),
            jobRequest
                .getCriteria()
                .getClusterCriteria()
                .stream()
                .map(this::toClusterCriteria)
                .collect(Collectors.toList()),
            this.toV3Tags(jobRequest.getCriteria().getCommandCriterion())
        )
            .withId(id)
            .withApplications(jobRequest.getCriteria().getApplicationIds())
            .withCommandArgs(jobRequest.getCommandArgs())
            .withDisableLogArchival(jobRequest.getRequestedAgentConfig().isArchivingDisabled())
            .withTags(jobRequest.getMetadata().getTags());

        final JobMetadata metadata = jobRequest.getMetadata();
        metadata.getEmail().ifPresent(v3Builder::withEmail);
        metadata.getGroup().ifPresent(v3Builder::withGroup);
        metadata.getGrouping().ifPresent(v3Builder::withGrouping);
        metadata.getGroupingInstance().ifPresent(v3Builder::withGroupingInstance);
        metadata.getDescription().ifPresent(v3Builder::withDescription);
        metadata.getMetadata().ifPresent(v3Builder::withMetadata);

        final ExecutionEnvironment jobResources = jobRequest.getResources();
        v3Builder.withConfigs(jobResources.getConfigs());
        v3Builder.withDependencies(jobResources.getDependencies());
        jobResources.getSetupFile().ifPresent(v3Builder::withSetupFile);

        jobRequest.getRequestedAgentConfig().getTimeoutRequested().ifPresent(v3Builder::withTimeout);

        return v3Builder.build();
    }

    private ClusterCriteria toClusterCriteria(final Criterion criterion) {
        return new ClusterCriteria(this.toV3Tags(criterion));
    }

    private Set<String> toV3Tags(final Criterion criterion) {
        final Set<String> tags = Sets.newHashSet();
        criterion.getId().ifPresent(id -> tags.add("genie.id:" + id));
        criterion.getName().ifPresent(name -> tags.add("genie.name:" + name));
        tags.addAll(criterion.getTags());
        return tags;
    }

    /**
     * Helper to convert a set of tags into a string that is a suitable value for a shell environment variable.
     * Adds double quotes as necessary (i.e. in case of spaces, newlines), performs escaping of in-tag quotes.
     * Input tags are sorted to produce a deterministic output value.
     *
     * @param tags a set of tags or null
     * @return a CSV string
     */
    private String tagsToString(final Set<String> tags) {
        final List<String> sortedTags = Lists.newArrayList(tags);
        // Sort tags for the sake of determinism (e.g., tests)
        sortedTags.sort(Comparator.naturalOrder());
        final String joinedString = StringUtils.join(sortedTags, ',');
        // Escape quotes
        return RegExUtils.replaceAll(RegExUtils.replaceAll(joinedString, "\'", "\\\'"), "\"", "\\\"");
    }

    /**
     * Helper to convert archive location prefix to an archive location.
     *
     * @param requestedArchiveLocationPrefix archive location prefix uri
     * @param jobId                          job id
     * @return archive location for the job
     */
    private String toArchiveLocation(
        final String requestedArchiveLocationPrefix,
        final String jobId
    ) {
        final String archivePrefix = StringUtils.isBlank(requestedArchiveLocationPrefix)
            ? this.defaultArchiveLocation
            : requestedArchiveLocationPrefix;

        if (archivePrefix.endsWith(File.separator)) {
            return archivePrefix + jobId;
        } else {
            return archivePrefix + File.separator + jobId;
        }
    }

    private int resolveJobMemory(final JobRequest jobRequest, final Command command) {
        return jobRequest
            .getRequestedJobEnvironment()
            .getRequestedJobMemory()
            .orElse(command.getMemory().orElse(this.defaultMemory));
    }

    private boolean useV4ResourceSelection() {
        // TODO: Flesh this out with dynamic behavior to aid migration between V3 and V4
        return false;
    }

    private Command resolveCommand(final JobRequest jobRequest) throws GenieJobResolutionException {
        final long start = System.nanoTime();
        final Set<Tag> tags = Sets.newHashSet();
        try {
            final Criterion criterion = jobRequest.getCriteria().getCommandCriterion();
            final Set<Command> commands = this.commandPersistenceService.findCommandsMatchingCriterion(criterion, true);
            final Command command;
            if (commands.isEmpty()) {
                throw new GenieJobResolutionException("No command matching command criterion found");
            } else if (commands.size() == 1) {
                command = commands
                    .stream()
                    .findFirst()
                    .orElseThrow(() -> new GenieJobResolutionException("No command matching criterion found."));
                log.debug("Found single command {} matching criterion {}", command.getId(), criterion);
            } else {
                try {
                    final ResourceSelectionResult<Command> result
                        = this.commandSelector.selectCommand(commands, jobRequest);
                    command = result
                        .getSelectedResource()
                        .orElseThrow(
                            () -> new GenieJobResolutionException(
                                "Expected a command but "
                                    + result.getSelectorClass().getSimpleName()
                                    + " didn't select anything. Rationale: "
                                    + result.getSelectionRationale().orElse(NO_RATIONALE)
                            )
                        );
                    log.debug(
                        "Selected command {} for criterion {} using {} due to {}",
                        command.getId(),
                        criterion,
                        result.getSelectorClass().getName(),
                        result.getSelectionRationale().orElse(NO_RATIONALE)
                    );
                } catch (final ResourceSelectionException selectionException) {
                    // TODO: Improve error handling?
                    throw new GenieJobResolutionException(selectionException);
                }
            }

            MetricsUtils.addSuccessTags(tags);
            tags.add(Tag.of(MetricsConstants.TagKeys.COMMAND_ID, command.getId()));
            tags.add(Tag.of(MetricsConstants.TagKeys.COMMAND_NAME, command.getMetadata().getName()));
            return command;
        } catch (final Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            if (t instanceof GenieJobResolutionException) {
                throw t;
            } else {
                throw new GenieJobResolutionException(t);
            }
        } finally {
            this.registry
                .timer(RESOLVE_COMMAND_TIMER, tags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    private Cluster resolveCluster(
        final Command command,
        final JobRequest jobRequest,
        final String jobId
    ) throws GenieJobResolutionException {
        final long start = System.nanoTime();
        final Set<Tag> tags = Sets.newHashSet();
        int queryCount = 0;
        int criteriaCombinationCount = 0;
        try {
            Cluster cluster = null;

            // This makes it so that if the command has no Cluster Criteria no cluster will possibly
            // be found. We'd have to change if we'd rather have it so that if there are no command cluster criteria
            // We just bypass this loop and only consider the job cluster criteria
            for (final Criterion commandClusterCriterion : command.getClusterCriteria()) {
                for (final Criterion jobClusterCriterion : jobRequest.getCriteria().getClusterCriteria()) {
                    criteriaCombinationCount++;
                    final Criterion mergedCriterion;
                    try {
                        // Failing to merge the criteria is equivalent to a round-trip DB query that returns
                        // zero results. This is an in memory optimization which also solves the need to implement
                        // the db query as a join with a subquery.
                        mergedCriterion = this.mergeCriteria(commandClusterCriterion, jobClusterCriterion);
                    } catch (final IllegalArgumentException e) {
                        log.debug(
                            "Unable to merge command cluster criterion {} and job cluster criterion {}. Skipping.",
                            commandClusterCriterion,
                            jobClusterCriterion,
                            e
                        );
                        // Skip and move to the next combination
                        continue;
                    }

                    queryCount++;
                    final Set<Cluster> clusters
                        = this.clusterPersistenceService.findClustersMatchingCriterion(mergedCriterion, true);
                    if (clusters.isEmpty()) {
                        log.debug("No clusters found for {}", mergedCriterion);
                        this.noClusterFoundCounter.increment();
                    } else if (clusters.size() == 1) {
                        log.debug("Found single cluster for {}", mergedCriterion);
                        cluster = clusters.stream().findFirst().orElse(null);
                    } else {
                        log.debug("Found {} clusters for {}", clusters.size(), mergedCriterion);
                        cluster = this.selectClusterUsingClusterSelectors(
                            Sets.newHashSet(),
                            clusters,
                            jobId,
                            jobRequest
                        );
                    }
                }

                if (cluster != null) {
                    break;
                }
            }

            if (cluster == null) {
                this.noClusterSelectedCounter.increment();
                throw new GenieJobResolutionException("No cluster selected given criteria for job " + jobId);
            }

            log.debug("Selected cluster {} for job {}", cluster.getId(), jobId);

            MetricsUtils.addSuccessTags(tags);
            tags.add(Tag.of(MetricsConstants.TagKeys.CLUSTER_ID, cluster.getId()));
            tags.add(Tag.of(MetricsConstants.TagKeys.CLUSTER_NAME, cluster.getMetadata().getName()));
            return cluster;
        } catch (final Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            if (t instanceof GenieJobResolutionException) {
                throw (GenieJobResolutionException) t;
            } else {
                throw new GenieJobResolutionException(t);
            }
        } finally {
            this.registry
                .counter(RESOLVE_CLUSTER_CRITERIA_COMBINATION_COUNTER)
                .increment(criteriaCombinationCount);
            this.registry
                .counter(RESOLVE_CLUSTER_QUERY_COUNTER)
                .increment(queryCount);
            tags.add(Tag.of(RESOLVE_CLUSTER_CRITERIA_COMBINATION_TAG, String.valueOf(criteriaCombinationCount)));
            tags.add(Tag.of(RESOLVE_CLUSTER_QUERY_TAG, String.valueOf(queryCount)));
            this.registry
                .timer(RESOLVE_CLUSTER_TIMER, tags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * Helper method for merging two criterion.
     * <p>
     * This method makes several assumptions:
     * - If any of these fields: {@literal id, name, version, status} are in both criterion their values must match
     * or this criterion combination of criteria can't possibly be matched so an {@link IllegalArgumentException}
     * is thrown
     * - If only one criterion has any of these fields {@literal id, name, version, status} then that value is present
     * in the resulting criterion
     * - Any {@literal tags} present in either criterion are merged into the super set of both sets of tags
     *
     * @param one The first {@link Criterion}
     * @param two The second {@link Criterion}
     * @return A merged {@link Criterion} that can be used to search the database
     * @throws IllegalArgumentException If the criteria can't be merged due to the described assumptions
     */
    private Criterion mergeCriteria(final Criterion one, final Criterion two) throws IllegalArgumentException {
        final Criterion.Builder builder = new Criterion.Builder();
        builder.withId(
            this.mergeCriteriaStrings(one.getId().orElse(null), two.getId().orElse(null), ID_FIELD)
        );
        builder.withName(
            this.mergeCriteriaStrings(one.getName().orElse(null), two.getName().orElse(null), NAME_FIELD)
        );
        builder.withStatus(
            this.mergeCriteriaStrings(one.getStatus().orElse(null), two.getStatus().orElse(null), STATUS_FIELD)
        );
        builder.withVersion(
            this.mergeCriteriaStrings(one.getVersion().orElse(null), two.getVersion().orElse(null), VERSION_FIELD)
        );
        final Set<String> tags = Sets.newHashSet(one.getTags());
        tags.addAll(two.getTags());
        builder.withTags(tags);
        return builder.build();
    }

    private String mergeCriteriaStrings(
        @Nullable final String one,
        @Nullable final String two,
        final String fieldName
    ) throws IllegalArgumentException {
        if (StringUtils.equals(one, two)) {
            // This handles null == null for us
            return one;
        } else if (one == null) {
            return two;
        } else if (two == null) {
            return one;
        } else {
            // Both have values but aren't equal
            throw new IllegalArgumentException(fieldName + "'s were both present but not equal");
        }
    }
}
