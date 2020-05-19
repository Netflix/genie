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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.external.dtos.v4.Application;
import com.netflix.genie.common.external.dtos.v4.Cluster;
import com.netflix.genie.common.external.dtos.v4.ClusterMetadata;
import com.netflix.genie.common.external.dtos.v4.Command;
import com.netflix.genie.common.external.dtos.v4.Criterion;
import com.netflix.genie.common.external.dtos.v4.JobEnvironment;
import com.netflix.genie.common.external.dtos.v4.JobRequest;
import com.netflix.genie.common.external.dtos.v4.JobSpecification;
import com.netflix.genie.common.external.dtos.v4.JobStatus;
import com.netflix.genie.common.internal.exceptions.checked.GenieJobResolutionException;
import com.netflix.genie.common.internal.jobs.JobConstants;
import com.netflix.genie.web.data.services.DataServices;
import com.netflix.genie.web.data.services.PersistenceService;
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
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.aop.TargetClassAware;
import org.springframework.core.env.Environment;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.validation.annotation.Validated;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import java.io.File;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
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
    //region Metric Constants
    /**
     * How long it takes to completely resolve a job given inputs.
     */
    private static final String RESOLVE_JOB_TIMER = "genie.services.jobResolver.resolve.timer";

    /**
     * How long it takes to resolve a command for a job given the supplied command criterion.
     */
    private static final String RESOLVE_COMMAND_TIMER = "genie.services.jobResolver.resolveCommand.timer";

    /**
     * How long it takes to resolve a cluster for a job given the resolved command and the request criteria.
     */
    private static final String RESOLVE_CLUSTER_TIMER = "genie.services.jobResolver.resolveCluster.timer";

    /**
     * How long it takes to resolve the applications for a given command.
     */
    private static final String RESOLVE_APPLICATIONS_TIMER = "genie.services.jobResolver.resolveApplications.timer";

    /**
     * How long it takes to resolve a cluster for a job given the resolved command and the request criteria.
     */
    private static final String GENERATE_CRITERIA_PERMUTATIONS_TIMER
        = "genie.services.jobResolver.generateClusterCriteriaPermutations.timer";

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
     * How long it takes to select a cluster from the set of clusters returned by database query.
     */
    private static final String SELECT_CLUSTER_TIMER = "genie.services.jobResolver.selectCluster.timer";

    /**
     * How long it takes to select a command for a given cluster.
     */
    private static final String SELECT_COMMAND_TIMER = "genie.services.jobResolver.selectCommand.timer";

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
    //endregion

    //region Temporary or Deprecated Constants
    // TODO: Remove all these after migration to V4 algorithm is complete
    /**
     * How long it takes to query the database for cluster command combinations matching supplied criteria.
     */
    private static final String CLUSTER_COMMAND_QUERY_TIMER = "genie.services.jobResolver.clusterCommandQuery.timer";
    private static final String V4_PROBABILITY_PROPERTY_KEY = "genie.services.job-resolver.v4-probability";
    private static final double DEFAULT_V4_PROBABILITY = 0.0;
    private static final double MIN_V4_PROBABILITY = 0.0;
    private static final double MAX_V4_PROBABILITY = 1.0;
    private static final String ALGORITHM_COUNTER = "genie.services.jobResolver.resolutionAlgorithm.counter";
    private static final String ALGORITHM_TAG = "algorithm";
    private static final Set<Tag> V3_ALGORITHM_TAGS = ImmutableSet.of(Tag.of(ALGORITHM_TAG, "v3"));
    private static final Set<Tag> V4_ALGORITHM_TAGS = ImmutableSet.of(Tag.of(ALGORITHM_TAG, "v4"));
    private static final String V3_COMMAND_TAG = "v3Command";
    private static final String V4_COMMAND_TAG = "v4Command";
    private static final String MATCHED_TAG = "matched";
    private static final Tag MATCHED_TAG_FALSE = Tag.of(MATCHED_TAG, "false");
    private static final Tag MATCHED_TAG_TRUE = Tag.of(MATCHED_TAG, "true");
    private static final String DUAL_RESOLVE_PROPERTY_KEY = "genie.services.job-resolver.dual-mode.enabled";
    private static final String DUAL_RESOLVE_TIMER = "genie.services.jobResolver.v4DualResolve.timer";
    //endregion

    //region Members
    private final PersistenceService persistenceService;
    private final List<ClusterSelector> clusterSelectors;
    private final CommandSelector commandSelector;
    private final MeterRegistry registry;
    private final int defaultMemory;
    // TODO: Switch to path
    private final File defaultJobDirectory;
    private final String defaultArchiveLocation;

    private final Counter noClusterSelectedCounter;
    private final Counter noClusterFoundCounter;

    private final Environment environment;
    private final Random random;
    //endregion

    //region Public APIs
    /**
     * Constructor.
     *
     * @param dataServices     The {@link DataServices} encapsulation instance to use
     * @param clusterSelectors The {@link ClusterSelector} implementations to use
     * @param commandSelector  The {@link CommandSelector} implementation to use
     * @param registry         The {@link MeterRegistry }metrics repository to use
     * @param jobsProperties   The properties for running a job set by the user
     * @param environment      The Spring application {@link Environment} for dynamic property resolution
     */
    public JobResolverServiceImpl(
        final DataServices dataServices,
        @NotEmpty final List<ClusterSelector> clusterSelectors,
        final CommandSelector commandSelector, // TODO: For now this is a single value but maybe support List
        final MeterRegistry registry,
        final JobsProperties jobsProperties,
        final Environment environment
    ) {
        this.persistenceService = dataServices.getPersistenceService();
        this.clusterSelectors = clusterSelectors;
        this.commandSelector = commandSelector;
        this.defaultMemory = jobsProperties.getMemory().getDefaultJobMemory();
        this.environment = environment;
        this.random = new Random();

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
            final JobStatus jobStatus = this.persistenceService.getJobStatus(id);
            if (!jobStatus.isResolvable()) {
                throw new IllegalArgumentException("Job " + id + " is already resolved: " + jobStatus);
            }

            final JobRequest jobRequest = this.persistenceService.getJobRequest(id);

            // TODO: Possible improvement to combine this query with a few others to save DB trips but for now...
            final boolean apiJob = this.persistenceService.isApiJob(id);

            final JobResolutionContext context = new JobResolutionContext(id, jobRequest, apiJob);

            final ResolvedJob resolvedJob = this.resolve(context);
            this.persistenceService.saveResolvedJob(id, resolvedJob);
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

            final JobResolutionContext context = new JobResolutionContext(id, jobRequest, apiJob);

            final ResolvedJob resolvedJob = this.resolve(context);
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
    //endregion

    //region Resolution Helpers
    private ResolvedJob resolve(final JobResolutionContext context) throws GenieJobResolutionException {
        final String id = context.getJobId();
        final JobRequest jobRequest = context.getJobRequest();

        if (this.useV4ResourceSelection()) {
            this.resolveCommand(context);
            this.resolveCluster(context);
        } else {
            final Map<Cluster, String> clustersAndCommandsForJob = this.queryForClustersAndCommands(
                jobRequest.getCriteria().getClusterCriteria(),
                jobRequest.getCriteria().getCommandCriterion()
            );
            // Resolve the cluster for the job request based on the tags specified
            final Cluster v3Cluster = this.selectCluster(id, jobRequest, clustersAndCommandsForJob.keySet());
            // Resolve the command for the job request based on command tags and cluster chosen
            final Command v3Command = this.getCommand(clustersAndCommandsForJob.get(v3Cluster), id);

            // For help during migration. Requested by compute team.
            if (this.environment.getProperty(DUAL_RESOLVE_PROPERTY_KEY, Boolean.class, false)) {
                final long dualStart = System.nanoTime();
                final String v3CommandId = v3Command.getId();
                final Set<Tag> dualModeTags = Sets.newHashSet(Tag.of(V3_COMMAND_TAG, v3CommandId));
                try {
                    this.resolveCommand(context);
                    final Command v4Command = context
                        .getCommand()
                        .orElseThrow(() -> new IllegalStateException("Expected command to have been resolved"));
                    final String v4CommandId = v4Command.getId();
                    dualModeTags.add(Tag.of(V4_COMMAND_TAG, v4CommandId));

                    if (v4CommandId.equals(v3CommandId)) {
                        dualModeTags.add(MATCHED_TAG_TRUE);
                        log.info("V4 resource resolution match for job {} command {}", id, v3CommandId);
                    } else {
                        dualModeTags.add(MATCHED_TAG_FALSE);
                        log.info(
                            "V4 resource resolution mismatch for job {} V3 command {} V4 command {}",
                            id,
                            v3CommandId,
                            v4CommandId
                        );
                    }
                    MetricsUtils.addSuccessTags(dualModeTags);
                } catch (final Exception e) {
                    // Swallow but capture it as a failure and a mismatch
                    MetricsUtils.addFailureTagsWithException(dualModeTags, e);
                    dualModeTags.add(MATCHED_TAG_FALSE);
                    log.info("V4 resource resolution mismatch for job {} due to exception {}", id, e.getMessage(), e);
                } finally {
                    this.registry
                        .timer(DUAL_RESOLVE_TIMER, dualModeTags)
                        .record(System.nanoTime() - dualStart, TimeUnit.NANOSECONDS);
                }
            }
            // For backwards compatibility
            context.setCommand(v3Command);
            context.setCluster(v3Cluster);
        }

        this.resolveApplications(context);
        this.resolveJobMemory(context);
        this.resolveEnvironmentVariables(context);
        this.resolveTimeout(context);
        this.resolveArchiveLocation(context);
        this.resolveJobDirectory(context);

        return context.build();
    }

    private void resolveCommand(final JobResolutionContext context) throws GenieJobResolutionException {
        final long start = System.nanoTime();
        final Set<Tag> tags = Sets.newHashSet();
        try {
            final JobRequest jobRequest = context.getJobRequest();
            final Criterion criterion = jobRequest.getCriteria().getCommandCriterion();
            final Set<Command> commands = this.persistenceService.findCommandsMatchingCriterion(criterion, true);
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
                    final ResourceSelectionResult<Command> result = this.commandSelector.select(
                        commands,
                        jobRequest,
                        context.getJobId()
                    );
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
            context.setCommand(command);
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

    private void resolveCluster(final JobResolutionContext context) throws GenieJobResolutionException {
        final long start = System.nanoTime();
        final Set<Tag> tags = Sets.newHashSet();
        int queryCount = 0;
        int criteriaCombinationCount = 0;

        final String jobId = context.getJobId();
        final JobRequest jobRequest = context.getJobRequest();
        try {
            final Command command = context
                .getCommand()
                .orElseThrow(
                    () -> new IllegalStateException("Command not resolved before attempting to resolve a cluster")
                );
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
                        = this.persistenceService.findClustersMatchingCriterion(mergedCriterion, true);
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
                            jobRequest,
                            jobId
                        );
                    }

                    if (cluster != null) {
                        break;
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
            context.setCluster(cluster);
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

    private void resolveApplications(final JobResolutionContext context) throws GenieJobResolutionException {
        final long start = System.nanoTime();
        final Set<Tag> tags = Sets.newHashSet();
        final String id = context.getJobId();
        final JobRequest jobRequest = context.getJobRequest();
        try {
            final String commandId = context
                .getCommand()
                .orElseThrow(() -> new IllegalStateException("Command hasn't been resolved before applications"))
                .getId();
            log.info("Selecting applications for job {} and command {}", id, commandId);
            // TODO: What do we do about application status? Should probably check here
            final List<Application> applications = Lists.newArrayList();
            if (jobRequest.getCriteria().getApplicationIds().isEmpty()) {
                applications.addAll(this.persistenceService.getApplicationsForCommand(commandId));
            } else {
                for (final String applicationId : jobRequest.getCriteria().getApplicationIds()) {
                    applications.add(this.persistenceService.getApplication(applicationId));
                }
            }
            log.info(
                "Resolved applications {} for job {}",
                applications
                    .stream()
                    .map(Application::getId)
                    .reduce((one, two) -> one + "," + two)
                    .orElse(NO_ID_FOUND),
                id
            );
            MetricsUtils.addSuccessTags(tags);
            context.setApplications(applications);
        } catch (final Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw new GenieJobResolutionException(t);
        } finally {
            this.registry
                .timer(RESOLVE_APPLICATIONS_TIMER, tags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    private void resolveJobMemory(final JobResolutionContext context) {
        context.setJobMemory(
            context.getJobRequest()
                .getRequestedJobEnvironment()
                .getRequestedJobMemory()
                .orElse(
                    context
                        .getCommand()
                        .orElseThrow(
                            () -> new IllegalStateException(
                                "Command not resolved before attempting to resolve job memory"
                            )
                        )
                        .getMemory()
                        .orElse(this.defaultMemory)
                )
        );
    }

    private void resolveEnvironmentVariables(final JobResolutionContext context) {
        final Command command = context
            .getCommand()
            .orElseThrow(
                () -> new IllegalStateException("Command not resolved before attempting to resolve env variables")
            );
        final Cluster cluster = context
            .getCluster()
            .orElseThrow(
                () -> new IllegalStateException("Cluster not resolved before attempting to resolve env variables")
            );
        final String id = context.getJobId();
        final JobRequest jobRequest = context.getJobRequest();
        final int jobMemory = context
            .getJobMemory()
            .orElseThrow(
                () -> new IllegalStateException("Job memory not resolved before attempting to resolve env variables")
            );
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
        envVariables.put(JobConstants.GENIE_JOB_MEMORY_ENV_VAR, String.valueOf(jobMemory));
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

        context.setEnvironmentVariables(envVariables.build());
    }

    private void resolveTimeout(final JobResolutionContext context) {
        final JobRequest jobRequest = context.getJobRequest();
        if (jobRequest.getRequestedAgentConfig().getTimeoutRequested().isPresent()) {
            context.setTimeout(jobRequest.getRequestedAgentConfig().getTimeoutRequested().get());
        } else if (context.isApiJob()) {
            // For backwards V3 compatibility
            context.setTimeout(com.netflix.genie.common.dto.JobRequest.DEFAULT_TIMEOUT_DURATION);
        }
    }

    private void resolveArchiveLocation(final JobResolutionContext context) {
        // TODO: Disable ability to disable archival for all jobs during internal V4 migration.
        //       Will allow us to reach out to clients who may set this variable but still expect output after
        //       job completion due to it being served off the node after completion in V3 but now it won't.
        //       Put this back in once all use cases have been hunted down and users are sure of their expected
        //       behavior
        final String requestedArchiveLocationPrefix =
            context.getJobRequest()
                .getRequestedJobArchivalData()
                .getRequestedArchiveLocationPrefix()
                .orElse(this.defaultArchiveLocation);
        final String jobId = context.getJobId();
        final String archivePrefix = StringUtils.isBlank(requestedArchiveLocationPrefix)
            ? this.defaultArchiveLocation
            : requestedArchiveLocationPrefix;

        context.setArchiveLocation(
            archivePrefix.endsWith(File.separator)
                ? archivePrefix + jobId
                : archivePrefix + File.separator + jobId
        );
    }

    private void resolveJobDirectory(final JobResolutionContext context) {
        context.setJobDirectory(
            context.getJobRequest()
                .getRequestedAgentConfig()
                .getRequestedJobDirectoryLocation()
                .orElse(this.defaultJobDirectory)
        );
    }
    //endregion

    //region Additional Helpers
    /**
     * Helper method to generate all the possible viable cluster criterion permutations for the given set of commands
     * and the given job request. The resulting map will be each command to its associated priority ordered list of
     * merged cluster criteria. The priority order is generated as follows:
     * <pre>
     * for (commandClusterCriterion : command.getClusterCriteria()) {
     *     for (jobClusterCriterion : jobRequest.getClusterCriteria()) {
     *         // merge
     *     }
     * }
     * </pre>
     *
     * @param commands   The set of {@link Command}s whose cluster criteria should be evaluated
     * @param jobRequest The {@link JobRequest} whose cluster criteria should be combined with the commands
     * @return The resulting map of each command to their associated merged criterion list in priority order
     */
    private Map<Command, List<Criterion>> generateClusterCriteriaPermutations(
        final Set<Command> commands,
        final JobRequest jobRequest
    ) {
        final long start = System.nanoTime();
        try {
            final ImmutableMap.Builder<Command, List<Criterion>> mapBuilder = ImmutableMap.builder();
            for (final Command command : commands) {
                final ImmutableList.Builder<Criterion> listBuilder = ImmutableList.builder();
                for (final Criterion commandClusterCriterion : command.getClusterCriteria()) {
                    for (final Criterion jobClusterCriterion : jobRequest.getCriteria().getClusterCriteria()) {
                        try {
                            // Failing to merge the criteria is equivalent to a round-trip DB query that returns
                            // zero results. This is an in memory optimization which also solves the need to implement
                            // the db query as a join with a subquery.
                            listBuilder.add(this.mergeCriteria(commandClusterCriterion, jobClusterCriterion));
                        } catch (final IllegalArgumentException e) {
                            log.debug(
                                "Unable to merge command cluster criterion {} and job cluster criterion {}. Skipping.",
                                commandClusterCriterion,
                                jobClusterCriterion,
                                e
                            );
                        }
                    }
                }
                mapBuilder.put(command, listBuilder.build());
            }
            return mapBuilder.build();
        } finally {
            this.registry
                .timer(GENERATE_CRITERIA_PERMUTATIONS_TIMER)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    private Set<Criterion> flattenClusterCriteriaPermutations(final Map<Command, List<Criterion>> commandCriteriaMap) {
        return commandCriteriaMap.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
    }

    /**
     * This is an in memory evaluation of the matching done against persistence.
     *
     * @param cluster   The cluster to evaluate the criterion against
     * @param criterion The criterion the cluster is being tested against
     * @return {@literal true} if the {@link Cluster} matches the {@link Criterion}
     */
    private boolean clusterMatchesCriterion(final Cluster cluster, final Criterion criterion) {
        // TODO: This runs the risk of diverging from DB query mechanism. Perhaps way to unite somewhat?
        final ClusterMetadata metadata = cluster.getMetadata();

        return criterion.getId().map(id -> cluster.getId().equals(id)).orElse(true)
            && criterion.getName().map(name -> metadata.getName().equals(name)).orElse(true)
            && criterion.getVersion().map(version -> metadata.getVersion().equals(version)).orElse(true)
            && criterion.getStatus().map(status -> metadata.getStatus().name().equals(status)).orElse(true)
            && metadata.getTags().containsAll(criterion.getTags());
    }

    private Map<Command, Set<Cluster>> generateCommandClustersMap(
        final Map<Command, List<Criterion>> commandClusterCriteria,
        final Set<Cluster> candidateClusters
    ) {
        final ImmutableMap.Builder<Command, Set<Cluster>> matrixBuilder = ImmutableMap.builder();
        for (final Map.Entry<Command, List<Criterion>> entry : commandClusterCriteria.entrySet()) {
            final Command command = entry.getKey();
            final ImmutableSet.Builder<Cluster> matchedClustersBuilder = ImmutableSet.builder();

            // Loop through the criterion in the priority order first
            for (final Criterion criterion : entry.getValue()) {
                for (final Cluster candidateCluster : candidateClusters) {
                    if (this.clusterMatchesCriterion(candidateCluster, criterion)) {
                        log.debug(
                            "Cluster {} matched criterion {} for command {}",
                            candidateCluster.getId(),
                            criterion,
                            command.getId()
                        );
                        matchedClustersBuilder.add(candidateCluster);
                    }
                }

                final ImmutableSet<Cluster> matchedClusters = matchedClustersBuilder.build();
                if (!matchedClusters.isEmpty()) {
                    // If we found some clusters the evaluation for this command is done
                    matrixBuilder.put(command, matchedClusters);
                    log.debug("For command {} matched clusters {}", command, matchedClusters);
                    // short circuit further criteria evaluation for this command
                    break;
                }
            }
            // If the command never matched any clusters it should be filtered out
            // of resulting map as no value would be added to the result builder
        }

        final ImmutableMap<Command, Set<Cluster>> matrix = matrixBuilder.build();
        log.debug("Complete command -> clusters matrix: {}", matrix);
        return matrix;
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

    @Nullable
    private Cluster selectClusterUsingClusterSelectors(
        final Set<Tag> counterTags,
        final Set<Cluster> clusters,
        final JobRequest jobRequest,
        final String jobId
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
                final ResourceSelectionResult<Cluster> result = clusterSelector.select(
                    clusters,
                    jobRequest,
                    jobId
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
    //endregion

    //region Deprecated V3 Resolution or Migration Helpers
    /*
     * This API will only exist during migration between V3 and V4 resource resolution logic which hopefully will be
     * short.
     */
    @Deprecated
    private boolean useV4ResourceSelection() {
        double v4Probability;
        try {
            v4Probability = this.environment.getProperty(
                V4_PROBABILITY_PROPERTY_KEY,
                Double.class,
                DEFAULT_V4_PROBABILITY
            );
        } catch (final IllegalStateException e) {
            log.error("Invalid V4 probability. Expected a number between 0.0 and 1.0 inclusive.", e);
            v4Probability = MIN_V4_PROBABILITY;
        }
        // Check for validity
        if (v4Probability < MIN_V4_PROBABILITY) {
            log.warn(
                "Invalid V4 resolution probability {}. Must be >= 0.0. Resetting to {}",
                v4Probability,
                MIN_V4_PROBABILITY
            );
            v4Probability = MIN_V4_PROBABILITY;
        }
        if (v4Probability > MAX_V4_PROBABILITY) {
            log.warn(
                "Invalid V4 resolution probability {}. Must be <= 1.0. Resetting to {}",
                v4Probability,
                MAX_V4_PROBABILITY
            );
            v4Probability = MAX_V4_PROBABILITY;
        }

        if (this.random.nextDouble() < v4Probability) {
            this.registry.counter(ALGORITHM_COUNTER, V4_ALGORITHM_TAGS).increment();
            return true;
        } else {
            this.registry.counter(ALGORITHM_COUNTER, V3_ALGORITHM_TAGS).increment();
            return false;
        }
    }

    @Deprecated
    private Map<Cluster, String> queryForClustersAndCommands(
        final List<Criterion> clusterCriteria,
        final Criterion commandCriterion
    ) throws GenieJobResolutionException {
        final long start = System.nanoTime();
        final Set<Tag> tags = Sets.newHashSet();
        try {
            final Map<Cluster, String> clustersAndCommands
                = this.persistenceService.findClustersAndCommandsForCriteria(clusterCriteria, commandCriterion);
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

    @Deprecated
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
                cluster = this.selectClusterUsingClusterSelectors(counterTags, clusters, jobRequest, id);
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

    @Deprecated
    private Command getCommand(
        final String commandId,
        final String jobId
    ) throws GenieJobResolutionException {
        final long start = System.nanoTime();
        final Set<Tag> tags = Sets.newHashSet();
        try {
            log.info("Selecting command for job {} ", jobId);
            final Command command = this.persistenceService.getCommand(commandId);
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
    //endregion

    //region Helper Classes
    /**
     * A helper data class for passing information around / along the resolution pipeline.
     *
     * @author tgianos
     * @since 4.0.0
     */
    @RequiredArgsConstructor
    @Getter
    @Setter
    @ToString(doNotUseGetters = true)
    static class JobResolutionContext {
        private final String jobId;
        private final JobRequest jobRequest;
        private final boolean apiJob;

        private Command command;
        private Cluster cluster;
        private List<Application> applications;

        private Integer jobMemory;
        private Map<String, String> environmentVariables;
        private Integer timeout;
        private String archiveLocation;
        private File jobDirectory;

        private Optional<Command> getCommand() {
            return Optional.ofNullable(this.command);
        }

        Optional<Cluster> getCluster() {
            return Optional.ofNullable(this.cluster);
        }

        Optional<List<Application>> getApplications() {
            return Optional.ofNullable(this.applications);
        }

        Optional<Integer> getJobMemory() {
            return Optional.ofNullable(this.jobMemory);
        }

        Optional<Map<String, String>> getEnvironmentVariables() {
            return Optional.ofNullable(this.environmentVariables);
        }

        Optional<Integer> getTimeout() {
            return Optional.ofNullable(this.timeout);
        }

        Optional<String> getArchiveLocation() {
            return Optional.ofNullable(this.archiveLocation);
        }

        Optional<File> getJobDirectory() {
            return Optional.ofNullable(this.jobDirectory);
        }

        ResolvedJob build() {
            // Error checking
            if (this.command == null) {
                throw new IllegalStateException("Command was never resolved for job " + this.jobId);
            }
            if (this.cluster == null) {
                throw new IllegalStateException("Cluster was never resolved for job " + this.jobId);
            }
            if (this.applications == null) {
                throw new IllegalStateException("Applications were never resolved for job " + this.jobId);
            }
            if (this.jobMemory == null) {
                throw new IllegalStateException("Job memory was never resolved for job " + this.jobId);
            }
            if (this.environmentVariables == null) {
                throw new IllegalStateException("Environment variables were never resolved for job " + this.jobId);
            }
            if (this.archiveLocation == null) {
                throw new IllegalStateException("Archive location was never resolved for job " + this.jobId);
            }
            if (this.jobDirectory == null) {
                throw new IllegalStateException("Job directory was never resolved for job " + this.jobId);
            }

            // Note: Currently no check for timeout due to it being ok for it to be null at the moment

            final JobSpecification jobSpecification = new JobSpecification(
                this.command.getExecutable(),
                this.jobRequest.getCommandArgs(),
                new JobSpecification.ExecutionResource(this.jobId, this.jobRequest.getResources()),
                new JobSpecification.ExecutionResource(this.cluster.getId(), this.cluster.getResources()),
                new JobSpecification.ExecutionResource(this.command.getId(), this.command.getResources()),
                this.applications
                    .stream()
                    .map(
                        application -> new JobSpecification.ExecutionResource(
                            application.getId(),
                            application.getResources()
                        )
                    )
                    .collect(Collectors.toList()),
                this.environmentVariables,
                this.jobRequest.getRequestedAgentConfig().isInteractive(),
                this.jobDirectory,
                this.archiveLocation,
                this.timeout
            );

            final JobEnvironment jobEnvironment = new JobEnvironment
                .Builder(this.jobMemory)
                .withEnvironmentVariables(this.environmentVariables)
                .build();

            return new ResolvedJob(jobSpecification, jobEnvironment, this.jobRequest.getMetadata());
        }
    }
    //endregion
}
