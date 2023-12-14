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

import brave.SpanCustomizer;
import brave.Tracer;
import com.netflix.genie.common.internal.dtos.Application;
import com.netflix.genie.common.internal.dtos.Cluster;
import com.netflix.genie.common.internal.dtos.ClusterMetadata;
import com.netflix.genie.common.internal.dtos.Command;
import com.netflix.genie.common.internal.dtos.ComputeResources;
import com.netflix.genie.common.internal.dtos.Criterion;
import com.netflix.genie.common.internal.dtos.Image;
import com.netflix.genie.common.internal.dtos.JobEnvironment;
import com.netflix.genie.common.internal.dtos.JobMetadata;
import com.netflix.genie.common.internal.dtos.JobRequest;
import com.netflix.genie.common.internal.dtos.JobSpecification;
import com.netflix.genie.common.internal.dtos.JobStatus;
import com.netflix.genie.common.internal.exceptions.checked.GenieJobResolutionException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobResolutionRuntimeException;
import com.netflix.genie.common.internal.jobs.JobConstants;
import com.netflix.genie.common.internal.tracing.TracingConstants;
import com.netflix.genie.common.internal.tracing.brave.BraveTagAdapter;
import com.netflix.genie.common.internal.tracing.brave.BraveTracingComponents;
import com.netflix.genie.web.data.services.DataServices;
import com.netflix.genie.web.data.services.PersistenceService;
import com.netflix.genie.web.dtos.ResolvedJob;
import com.netflix.genie.web.dtos.ResourceSelectionResult;
import com.netflix.genie.web.exceptions.checked.ResourceSelectionException;
import com.netflix.genie.web.properties.JobResolutionProperties;
import com.netflix.genie.web.properties.JobsProperties;
import com.netflix.genie.web.selectors.ClusterSelectionContext;
import com.netflix.genie.web.selectors.ClusterSelector;
import com.netflix.genie.web.selectors.CommandSelectionContext;
import com.netflix.genie.web.selectors.CommandSelector;
import com.netflix.genie.web.services.JobResolverService;
import com.netflix.genie.web.util.MetricsConstants;
import com.netflix.genie.web.util.MetricsUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.TargetClassAware;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.validation.annotation.Validated;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import java.io.File;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Implementation of the {@link JobResolverService} APIs.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Validated
public class JobResolverServiceImpl implements JobResolverService {
    private static final Logger LOG = LoggerFactory.getLogger(JobResolverServiceImpl.class);

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
     * How many times a cluster selector is invoked.
     */
    private static final String CLUSTER_SELECTOR_COUNTER
        = "genie.services.jobResolver.resolveCluster.clusterSelector.counter";

    private static final int DEFAULT_CPU = 1;
    private static final int DEFAULT_GPU = 0;
    private static final long DEFAULT_MEMORY = 1_500L;
    private static final long DEFAULT_DISK = 10_000L;
    private static final long DEFAULT_NETWORK = 256L;
    private static final String NO_RATIONALE = "No rationale provided";
    private static final String NO_ID_FOUND = "No id found";
    private static final String VERSION_4 = "4";
    private static final Tag SAVED_TAG = Tag.of("saved", "true");
    private static final Tag NOT_SAVED_TAG = Tag.of("saved", "false");
    private static final Tag NO_CLUSTER_RESOLVED_ID = Tag.of(MetricsConstants.TagKeys.CLUSTER_ID, "None Resolved");
    private static final Tag NO_CLUSTER_RESOLVED_NAME = Tag.of(MetricsConstants.TagKeys.CLUSTER_NAME, "None Resolved");
    private static final Tag NO_COMMAND_RESOLVED_ID = Tag.of(MetricsConstants.TagKeys.COMMAND_ID, "None Resolved");
    private static final Tag NO_COMMAND_RESOLVED_NAME = Tag.of(MetricsConstants.TagKeys.COMMAND_NAME, "None Resolved");

    private static final String ID_FIELD = "id";
    private static final String NAME_FIELD = "name";
    private static final String STATUS_FIELD = "status";
    private static final String VERSION_FIELD = "version";

    private static final String CLUSTER_SELECTOR_STATUS_SUCCESS = "success";
    private static final String CLUSTER_SELECTOR_STATUS_NO_PREFERENCE = "no preference";
    //endregion

    //region Members
    private final PersistenceService persistenceService;
    private final List<ClusterSelector> clusterSelectors;
    private final CommandSelector commandSelector;
    private final MeterRegistry registry;
    // TODO: Switch to path
    private final File defaultJobDirectory;
    private final String defaultArchiveLocation;
    private final Tracer tracer;
    private final BraveTagAdapter tagAdapter;
    private final JobResolutionProperties jobResolutionProperties;
    //endregion

    //region Public APIs

    /**
     * Constructor.
     *
     * @param dataServices            The {@link DataServices} encapsulation instance to use
     * @param clusterSelectors        The {@link ClusterSelector} implementations to use
     * @param commandSelector         The {@link CommandSelector} implementation to use
     * @param registry                The {@link MeterRegistry }metrics repository to use
     * @param jobsProperties          The properties for running a job set by the user
     * @param jobResolutionProperties The {@link JobResolutionProperties} instance
     * @param tracingComponents       The {@link BraveTracingComponents} instance to use
     */
    public JobResolverServiceImpl(
        final DataServices dataServices,
        @NotEmpty final List<ClusterSelector> clusterSelectors,
        final CommandSelector commandSelector, // TODO: For now this is a single value but maybe support List
        final MeterRegistry registry,
        final JobsProperties jobsProperties,
        final JobResolutionProperties jobResolutionProperties,
        final BraveTracingComponents tracingComponents
    ) {
        this.persistenceService = dataServices.getPersistenceService();
        this.clusterSelectors = clusterSelectors;
        this.commandSelector = commandSelector;
        this.jobResolutionProperties = jobResolutionProperties;

        final URI jobDirProperty = jobsProperties.getLocations().getJobs();
        this.defaultJobDirectory = Paths.get(jobDirProperty).toFile();
        final String archiveLocation = jobsProperties.getLocations().getArchives().toString();
        this.defaultArchiveLocation = archiveLocation.endsWith(File.separator)
            ? archiveLocation
            : archiveLocation + File.separator;

        // Metrics
        this.registry = registry;

        // tracing
        this.tracer = tracingComponents.getTracer();
        this.tagAdapter = tracingComponents.getTagAdapter();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    @Transactional
    public ResolvedJob resolveJob(
        final String id
    ) throws GenieJobResolutionException, GenieJobResolutionRuntimeException {
        final long start = System.nanoTime();
        final Set<Tag> tags = new HashSet<>();
        tags.add(SAVED_TAG);
        try {
            LOG.info("Received request to resolve a job with id {}", id);
            final JobStatus jobStatus = this.persistenceService.getJobStatus(id);
            if (!jobStatus.isResolvable()) {
                throw new IllegalArgumentException("Job " + id + " is already resolved: " + jobStatus);
            }

            final JobRequest jobRequest = this.persistenceService.getJobRequest(id);

            // TODO: Possible improvement to combine this query with a few others to save DB trips but for now...
            final boolean apiJob = this.persistenceService.isApiJob(id);

            final JobResolutionContext context = new JobResolutionContext(
                id,
                jobRequest,
                apiJob,
                this.tracer.currentSpanCustomizer()
            );

            final ResolvedJob resolvedJob = this.resolve(context);

            /*
             * TODO: There is currently a gap in database schema where the resolved CPU value is not persisted. This
             *       means that it requires that the returned resolvedJob object here be used within the same call. If
             *       we for some reason eventually put the job id on a queue or something and pull data back from DB
             *       it WILL NOT be accurate. I'm purposely not doing this right now as it's not critical and modifying
             *       the schema will require a prod downtime and there are likely other fields (requestedNetwork,
             *       usedNetwork, usedDisk, resolvedDisk, requestedImage, usedImage) we want to add at the
             *       same time to minimize downtimes. - TJG 2/2/21
             */
            this.persistenceService.saveResolvedJob(id, resolvedJob);
            MetricsUtils.addSuccessTags(tags);
            return resolvedJob;
        } catch (final GenieJobResolutionException e) {
            MetricsUtils.addFailureTagsWithException(tags, e);
            throw e;
        } catch (final Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw new GenieJobResolutionRuntimeException(t);
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
    ) throws GenieJobResolutionException, GenieJobResolutionRuntimeException {
        final long start = System.nanoTime();
        final Set<Tag> tags = new HashSet<>();
        tags.add(NOT_SAVED_TAG);
        try {
            LOG.info(
                "Received request to resolve a job for id {} and request {}",
                id,
                jobRequest
            );

            final JobResolutionContext context = new JobResolutionContext(
                id,
                jobRequest,
                apiJob,
                this.tracer.currentSpanCustomizer()
            );

            final ResolvedJob resolvedJob = this.resolve(context);
            MetricsUtils.addSuccessTags(tags);
            return resolvedJob;
        } catch (final GenieJobResolutionException e) {
            MetricsUtils.addFailureTagsWithException(tags, e);
            throw e;
        } catch (final Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw new GenieJobResolutionRuntimeException(t);
        } finally {
            this.registry
                .timer(RESOLVE_JOB_TIMER, tags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }
    //endregion

    //region Resolution Helpers
    private ResolvedJob resolve(
        final JobResolutionContext context
    ) throws GenieJobResolutionException, GenieJobResolutionRuntimeException {
        this.tagSpanWithJobMetadata(context);
        this.resolveCommand(context);
        this.resolveCluster(context);
        this.resolveApplications(context);
        this.resolveComputeResources(context);
        this.resolveImages(context);
        this.resolveEnvironmentVariables(context);
        this.resolveTimeout(context);
        this.resolveArchiveLocation(context);
        this.resolveJobDirectory(context);

        return context.build();
    }

    /*
     * Overall Algorithm:
     *
     * 1. Take command criterion from user job request and query database for all possible matching commands
     * 2. Take clusterCriteria from jobRequest and clusterCriteria from each command and create uber query which finds
     *    ALL clusters that match at least one of the resulting merged criterion (merged meaning combining a job and
     *    command cluster criterion)
     * 3. Iterate through commands from step 1 and evaluate job/command cluster criterion against resulting set of
     *    clusters from step 2. Filter out any commands that don't match any clusters. Save resulting cluster set for
     *    each command in map command -> Set<Cluster>
     * 4. Pass set<command>, jobRequest, jobId, map<command<set<Cluster>>  to command selector which will return single
     *    command
     * 5. Using command result pass previously computed Set<Cluster> to cluster selector
     * 6. Save results and run job
     */
    private void resolveCommand(final JobResolutionContext context) throws GenieJobResolutionException {
        final long start = System.nanoTime();
        final Set<Tag> tags = new HashSet<>();
        try {
            final JobRequest jobRequest = context.getJobRequest();
            final Criterion criterion = jobRequest.getCriteria().getCommandCriterion();

            //region Algorithm Step 1
            final Set<Command> commands = this.persistenceService.findCommandsMatchingCriterion(criterion, true);

            // Short circuit if there are no commands
            if (commands.isEmpty()) {
                throw new GenieJobResolutionException("No command matching command criterion found");
            }
            //endregion

            //region Algorithm Step 2
            final Map<Command, List<Criterion>> commandClusterCriterions = this.generateClusterCriteriaPermutations(
                commands,
                jobRequest
            );

            final Set<Criterion> uniqueCriteria = this.flattenClusterCriteriaPermutations(commandClusterCriterions);

            final Set<Cluster> allCandidateClusters = this.persistenceService.findClustersMatchingAnyCriterion(
                uniqueCriteria,
                true
            );
            if (allCandidateClusters.isEmpty()) {
                throw new GenieJobResolutionException("No clusters available to run any candidate command on");
            }
            //endregion

            //region Algorithm Step 3
            final Map<Command, Set<Cluster>> commandClusters = this.generateCommandClustersMap(
                commandClusterCriterions,
                allCandidateClusters
            );
            // this should never really happen based on above check but just in case
            if (commandClusters.isEmpty()) {
                throw new GenieJobResolutionException("No clusters available to run any candidate command on");
            }
            // save the map for use later by cluster resolution
            context.setCommandClusters(commandClusters);
            //endregion

            //region Algorithm Step 4
            final ResourceSelectionResult<Command> result = this.commandSelector.select(
                new CommandSelectionContext(
                    context.getJobId(),
                    jobRequest,
                    context.isApiJob(),
                    commandClusters
                )
            );
            //endregion

            final Command command = result
                .getSelectedResource()
                .orElseThrow(
                    () -> new GenieJobResolutionException(
                        "Expected a command but "
                            + result.getSelectorClass().getSimpleName()
                            + " didn't select anything. Rationale: "
                            + result.getSelectionRationale().orElse(NO_RATIONALE)
                    )
                );
            LOG.debug(
                "Selected command {} for criterion {} using {} due to {}",
                command.getId(),
                criterion,
                result.getSelectorClass().getName(),
                result.getSelectionRationale().orElse(NO_RATIONALE)
            );

            MetricsUtils.addSuccessTags(tags);
            final String commandId = command.getId();
            final String commandName = command.getMetadata().getName();
            tags.add(Tag.of(MetricsConstants.TagKeys.COMMAND_ID, commandId));
            tags.add(Tag.of(MetricsConstants.TagKeys.COMMAND_NAME, commandName));
            final SpanCustomizer spanCustomizer = context.getSpanCustomizer();
            this.tagAdapter.tag(spanCustomizer, TracingConstants.JOB_COMMAND_ID_TAG, commandId);
            this.tagAdapter.tag(spanCustomizer, TracingConstants.JOB_COMMAND_NAME_TAG, commandName);
            context.setCommand(command);
        } catch (final GenieJobResolutionException e) {
            // No candidates or selector choose none
            tags.add(NO_COMMAND_RESOLVED_ID);
            tags.add(NO_COMMAND_RESOLVED_NAME);
            MetricsUtils.addFailureTagsWithException(tags, e);
            throw e;
        } catch (final ResourceSelectionException t) {
            // Selector runtime error
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw new GenieJobResolutionRuntimeException(t);
        } finally {
            this.registry
                .timer(RESOLVE_COMMAND_TIMER, tags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    /*
     * At this point we should have resolved a command and now we can use the map command -> clusters that was
     * previously computed to invoke the cluster selectors to narrow down the candidate clusters to a single cluster
     * for use.
     */
    private void resolveCluster(final JobResolutionContext context) throws GenieJobResolutionException {
        final long start = System.nanoTime();
        final Set<Tag> tags = new HashSet<>();

        final String jobId = context.getJobId();
        try {
            final Command command = context
                .getCommand()
                .orElseThrow(
                    () -> new IllegalStateException(
                        "Command not resolved before attempting to resolve a cluster for job " + jobId
                    )
                );
            final Set<Cluster> candidateClusters = context
                .getCommandClusters()
                .orElseThrow(
                    () -> new IllegalStateException("Command to candidate cluster map not available for job " + jobId)
                )
                .get(command);
            if (candidateClusters == null || candidateClusters.isEmpty()) {
                throw new IllegalStateException(
                    "Command " + command.getId() + " had no candidate clusters for job " + jobId
                );
            }

            Cluster cluster = null;
            for (final ClusterSelector clusterSelector : this.clusterSelectors) {
                // Create subset of tags just for this selector. Copy existing tags if any.
                final Set<Tag> selectorTags = new HashSet<>(tags);
                // Note: This is done before the selection because if we do it after and the selector throws
                //       exception then we don't have this tag in the metrics. Which is unfortunate since the result
                //       does return the selector
                final String clusterSelectorClass = this.getProxyObjectClassName(clusterSelector);
                selectorTags.add(Tag.of(MetricsConstants.TagKeys.CLASS_NAME, clusterSelectorClass));

                try {
                    final ResourceSelectionResult<Cluster> result = clusterSelector.select(
                        new ClusterSelectionContext(
                            jobId,
                            context.getJobRequest(),
                            context.isApiJob(),
                            command,
                            candidateClusters
                        )
                    );

                    final Optional<Cluster> selectedClusterOptional = result.getSelectedResource();
                    if (selectedClusterOptional.isPresent()) {
                        cluster = selectedClusterOptional.get();
                        LOG.debug(
                            "Successfully selected cluster {} using selector {} for job {} with rationale: {}",
                            cluster.getId(),
                            clusterSelectorClass,
                            jobId,
                            result.getSelectionRationale().orElse(NO_RATIONALE)
                        );
                        selectorTags.add(Tag.of(MetricsConstants.TagKeys.STATUS, CLUSTER_SELECTOR_STATUS_SUCCESS));
                        selectorTags.add(Tag.of(MetricsConstants.TagKeys.CLUSTER_ID, cluster.getId()));
                        selectorTags.add(
                            Tag.of(MetricsConstants.TagKeys.CLUSTER_NAME, cluster.getMetadata().getName())
                        );
                        break;
                    } else {
                        selectorTags.add(
                            Tag.of(MetricsConstants.TagKeys.STATUS, CLUSTER_SELECTOR_STATUS_NO_PREFERENCE)
                        );
                        selectorTags.add(NO_CLUSTER_RESOLVED_ID);
                        selectorTags.add(NO_CLUSTER_RESOLVED_NAME);
                        LOG.debug(
                            "Selector {} returned no preference with rationale: {}",
                            clusterSelectorClass,
                            result.getSelectionRationale().orElse(NO_RATIONALE)
                        );
                    }
                } catch (final Exception e) {
                    // Swallow exception and proceed to next selector.
                    // This is a choice to provides "best-service": select a cluster as long as it matches criteria,
                    // even if one of the selectors encountered an error and cannot choose the best candidate.
                    MetricsUtils.addFailureTagsWithException(selectorTags, e);
                    LOG.warn(
                        "Cluster selector {} evaluation threw exception for job {}",
                        clusterSelectorClass,
                        jobId,
                        e
                    );
                } finally {
                    this.registry.counter(CLUSTER_SELECTOR_COUNTER, selectorTags).increment();
                }
            }

            if (cluster == null) {
                throw new GenieJobResolutionException("No cluster resolved for job " + jobId);
            }

            LOG.debug("Resolved cluster {} for job {}", cluster.getId(), jobId);

            context.setCluster(cluster);
            MetricsUtils.addSuccessTags(tags);
            final String clusterId = cluster.getId();
            final String clusterName = cluster.getMetadata().getName();
            tags.add(Tag.of(MetricsConstants.TagKeys.CLUSTER_ID, clusterId));
            tags.add(Tag.of(MetricsConstants.TagKeys.CLUSTER_NAME, clusterName));
            final SpanCustomizer spanCustomizer = context.getSpanCustomizer();
            this.tagAdapter.tag(spanCustomizer, TracingConstants.JOB_CLUSTER_ID_TAG, clusterId);
            this.tagAdapter.tag(spanCustomizer, TracingConstants.JOB_CLUSTER_NAME_TAG, clusterName);
        } catch (final GenieJobResolutionException e) {
            tags.add(NO_CLUSTER_RESOLVED_ID);
            tags.add(NO_CLUSTER_RESOLVED_NAME);
            MetricsUtils.addFailureTagsWithException(tags, e);
            throw e;
        } catch (final Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw new GenieJobResolutionRuntimeException(t);
        } finally {
            this.registry
                .timer(RESOLVE_CLUSTER_TIMER, tags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    private void resolveApplications(final JobResolutionContext context) throws GenieJobResolutionException {
        final long start = System.nanoTime();
        final Set<Tag> tags = new HashSet<>();
        final String id = context.getJobId();
        final JobRequest jobRequest = context.getJobRequest();
        try {
            final String commandId = context
                .getCommand()
                .orElseThrow(() -> new IllegalStateException("Command hasn't been resolved before applications"))
                .getId();
            LOG.debug("Selecting applications for job {} and command {}", id, commandId);
            // TODO: What do we do about application status? Should probably check here
            final List<Application> applications = new ArrayList<>();
            if (jobRequest.getCriteria().getApplicationIds().isEmpty()) {
                applications.addAll(this.persistenceService.getApplicationsForCommand(commandId));
            } else {
                for (final String applicationId : jobRequest.getCriteria().getApplicationIds()) {
                    applications.add(this.persistenceService.getApplication(applicationId));
                }
            }
            LOG.debug(
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
            throw new GenieJobResolutionRuntimeException(t);
        } finally {
            this.registry
                .timer(RESOLVE_APPLICATIONS_TIMER, tags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
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
        final long jobMemory = context
            .getComputeResources()
            .orElseThrow(
                () -> new IllegalStateException("Job memory not resolved before attempting to resolve env variables")
            )
            .getMemoryMb()
            .orElseThrow(() -> new IllegalStateException("No memory has been resolved before attempting to resolve"));
        // N.B. variables may be evaluated in a different order than they are added to this map (due to serialization).
        // Hence variables in this set should not depend on each-other.
        final Map<String, String> envVariables = new HashMap<>();
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
        final List<String> clusterCriteriaTags = new ArrayList<>(clusterCriteria.size());
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

        context.setEnvironmentVariables(Collections.unmodifiableMap(envVariables));
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

    private void resolveComputeResources(final JobResolutionContext context) {
        final ComputeResources req = context
            .getJobRequest()
            .getRequestedJobEnvironment()
            .getRequestedComputeResources();
        final ComputeResources command = context
            .getCommand()
            .orElseThrow(() -> new IllegalStateException("Command hasn't been resolved before compute resources"))
            .getComputeResources();
        final ComputeResources defaults = this.jobResolutionProperties.getDefaultComputeResources();
        context.setComputeResources(
            new ComputeResources.Builder()
                .withCpu(this.resolveComputeResource(req::getCpu, command::getCpu, defaults::getCpu, DEFAULT_CPU))
                .withGpu(this.resolveComputeResource(req::getGpu, command::getGpu, defaults::getGpu, DEFAULT_GPU))
                .withMemoryMb(
                    this.resolveComputeResource(
                        req::getMemoryMb,
                        command::getMemoryMb,
                        defaults::getMemoryMb,
                        DEFAULT_MEMORY
                    )
                )
                .withDiskMb(
                    this.resolveComputeResource(req::getDiskMb, command::getDiskMb, defaults::getDiskMb, DEFAULT_DISK)
                )
                .withNetworkMbps(
                    this.resolveComputeResource(
                        req::getNetworkMbps,
                        command::getNetworkMbps,
                        defaults::getNetworkMbps,
                        DEFAULT_NETWORK
                    )
                )
                .build()
        );
    }

    private <T> T resolveComputeResource(
        final Supplier<Optional<T>> requestedResource,
        final Supplier<Optional<T>> commandResource,
        final Supplier<Optional<T>> configuredDefault,
        final T hardCodedDefault
    ) {
        return requestedResource
            .get()
            .orElse(
                commandResource
                    .get()
                    .orElse(
                        configuredDefault
                            .get()
                            .orElse(hardCodedDefault)
                    )
            );
    }

    private void resolveImages(final JobResolutionContext context) {
        final Map<String, Image> requestImages = context
            .getJobRequest()
            .getRequestedJobEnvironment()
            .getRequestedImages();
        final Map<String, Image> commandImages = context
            .getCommand()
            .orElseThrow(() -> new IllegalStateException("No command resolved before trying to resolve images"))
            .getImages();
        final Map<String, Image> defaultImages = this.jobResolutionProperties.getDefaultImages();

        // Find all the image keys
        final Map<String, Image> resolvedImages = new HashMap<>(defaultImages);
        for (final Map.Entry<String, Image> entry : commandImages.entrySet()) {
            resolvedImages.merge(entry.getKey(), entry.getValue(), this::mergeImages);
        }
        for (final Map.Entry<String, Image> entry : requestImages.entrySet()) {
            resolvedImages.merge(entry.getKey(), entry.getValue(), this::mergeImages);
        }
        context.setImages(resolvedImages);
    }

    private void resolveArchiveLocation(final JobResolutionContext context) {
        // TODO: Disable ability to disable archival for all jobs during internal V4 migration.
        //       Will allow us to reach out to clients who may set this variable but still expect output after
        //       job completion due to it being served off the node after completion in V3 but now it won't.
        //       Put this back in once all use cases have been hunted down and users are sure of their expected
        //       behavior
        context.setArchiveLocation(this.defaultArchiveLocation + context.getJobId());
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
            final Map<Command, List<Criterion>> mapBuilder = new HashMap<>();
            for (final Command command : commands) {
                final List<Criterion> listBuilder = new ArrayList<>();
                for (final Criterion commandClusterCriterion : command.getClusterCriteria()) {
                    for (final Criterion jobClusterCriterion : jobRequest.getCriteria().getClusterCriteria()) {
                        try {
                            // Failing to merge the criteria is equivalent to a round-trip DB query that returns
                            // zero results. This is an in memory optimization which also solves the need to implement
                            // the db query as a join with a subquery.
                            listBuilder.add(this.mergeCriteria(commandClusterCriterion, jobClusterCriterion));
                        } catch (final IllegalArgumentException e) {
                            LOG.debug(
                                "Unable to merge command cluster criterion {} and job cluster criterion {}. Skipping.",
                                commandClusterCriterion,
                                jobClusterCriterion,
                                e
                            );
                        }
                    }
                }
                mapBuilder.put(command, Collections.unmodifiableList(listBuilder));
            }
            return Collections.unmodifiableMap(mapBuilder);
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
        final Map<Command, Set<Cluster>> matrixBuilder = new HashMap<>();
        for (final Map.Entry<Command, List<Criterion>> entry : commandClusterCriteria.entrySet()) {
            final Command command = entry.getKey();
            final Set<Cluster> matchedClustersBuilder = new HashSet<>();

            // Loop through the criterion in the priority order first
            for (final Criterion criterion : entry.getValue()) {
                for (final Cluster candidateCluster : candidateClusters) {
                    if (this.clusterMatchesCriterion(candidateCluster, criterion)) {
                        LOG.debug(
                            "Cluster {} matched criterion {} for command {}",
                            candidateCluster.getId(),
                            criterion,
                            command.getId()
                        );
                        matchedClustersBuilder.add(candidateCluster);
                    }
                }

                final Set<Cluster> matchedClusters = Collections.unmodifiableSet(matchedClustersBuilder);
                if (!matchedClusters.isEmpty()) {
                    // If we found some clusters the evaluation for this command is done
                    matrixBuilder.put(command, matchedClusters);
                    LOG.debug("For command {} matched clusters {}", command, matchedClusters);
                    // short circuit further criteria evaluation for this command
                    break;
                }
            }
            // If the command never matched any clusters it should be filtered out
            // of resulting map as no value would be added to the result builder
        }

        final Map<Command, Set<Cluster>> matrix = Collections.unmodifiableMap(matrixBuilder);
        LOG.debug("Complete command -> clusters matrix: {}", matrix);
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
        final Set<String> tags = new HashSet<>(one.getTags());
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

    private Image mergeImages(final Image secondary, final Image primary) {
        return new Image.Builder()
            .withName(primary.getName().orElse(secondary.getName().orElse(null)))
            .withTag(primary.getTag().orElse(secondary.getTag().orElse(null)))
            .withArguments(primary.getArguments().isEmpty() ? secondary.getArguments() : primary.getArguments())
            .build();
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
        final List<String> sortedTags = new ArrayList<>(tags);
        // Sort tags for the sake of determinism (e.g., tests)
        sortedTags.sort(Comparator.naturalOrder());
        final String joinedString = StringUtils.join(sortedTags, ',');
        // Escape quotes
        return RegExUtils.replaceAll(RegExUtils.replaceAll(joinedString, "'", "\\'"), "\"", "\\\"");
    }

    private String getProxyObjectClassName(final Object possibleProxyObject) {
        final String className;
        if (possibleProxyObject instanceof TargetClassAware aware) {
            final Class<?> targetClass = aware.getTargetClass();
            if (targetClass != null) {
                className = targetClass.getCanonicalName();
            } else {
                className = possibleProxyObject.getClass().getCanonicalName();
            }
        } else {
            className = possibleProxyObject.getClass().getCanonicalName();
        }
        return className;
    }

    private void tagSpanWithJobMetadata(final JobResolutionContext context) {
        final SpanCustomizer spanCustomizer = this.tracer.currentSpanCustomizer();
        this.tagAdapter.tag(spanCustomizer, TracingConstants.JOB_ID_TAG, context.getJobId());
        final JobMetadata jobMetadata = context.getJobRequest().getMetadata();
        this.tagAdapter.tag(spanCustomizer, TracingConstants.JOB_NAME_TAG, jobMetadata.getName());
        this.tagAdapter.tag(spanCustomizer, TracingConstants.JOB_USER_TAG, jobMetadata.getUser());
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
        private final SpanCustomizer spanCustomizer;

        private Command command;
        private Cluster cluster;
        private List<Application> applications;

        private ComputeResources computeResources;
        private Map<String, String> environmentVariables;
        private Integer timeout;
        private String archiveLocation;
        private File jobDirectory;
        private Map<Command, Set<Cluster>> commandClusters;
        private Map<String, Image> images;

        Optional<Command> getCommand() {
            return Optional.ofNullable(this.command);
        }

        Optional<Cluster> getCluster() {
            return Optional.ofNullable(this.cluster);
        }

        Optional<List<Application>> getApplications() {
            return Optional.ofNullable(this.applications);
        }

        Optional<ComputeResources> getComputeResources() {
            return Optional.ofNullable(this.computeResources);
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

        Optional<Map<Command, Set<Cluster>>> getCommandClusters() {
            return Optional.ofNullable(this.commandClusters);
        }

        Optional<Map<String, Image>> getImages() {
            return Optional.ofNullable(this.images);
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
            if (this.computeResources == null) {
                throw new IllegalStateException("Compute resources were never resolved for job " + this.jobId);
            }
            if (this.images == null) {
                throw new IllegalStateException("Images were never resolved for job " + this.jobId);
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
                .Builder()
                .withComputeResources(this.computeResources)
                .withEnvironmentVariables(this.environmentVariables)
                .withImages(this.images)
                .build();

            return new ResolvedJob(jobSpecification, jobEnvironment, this.jobRequest.getMetadata());
        }
    }
    //endregion
}
