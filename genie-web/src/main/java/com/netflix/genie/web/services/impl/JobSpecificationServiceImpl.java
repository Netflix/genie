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
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.internal.dto.v4.Application;
import com.netflix.genie.common.internal.dto.v4.Cluster;
import com.netflix.genie.common.internal.dto.v4.Command;
import com.netflix.genie.common.internal.dto.v4.Criterion;
import com.netflix.genie.common.internal.dto.v4.ExecutionEnvironment;
import com.netflix.genie.common.internal.dto.v4.JobMetadata;
import com.netflix.genie.common.internal.dto.v4.JobRequest;
import com.netflix.genie.common.internal.dto.v4.JobSpecification;
import com.netflix.genie.common.internal.jobs.JobConstants;
import com.netflix.genie.web.properties.JobsProperties;
import com.netflix.genie.web.services.ApplicationPersistenceService;
import com.netflix.genie.web.services.ClusterLoadBalancer;
import com.netflix.genie.web.services.ClusterPersistenceService;
import com.netflix.genie.web.services.CommandPersistenceService;
import com.netflix.genie.web.services.JobSpecificationService;
import com.netflix.genie.web.util.MetricsConstants;
import com.netflix.genie.web.util.MetricsUtils;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.aop.TargetClassAware;
import org.springframework.validation.annotation.Validated;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import java.io.File;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Implementation of the JobSpecificationService APIs.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Slf4j
@Validated
@ParametersAreNonnullByDefault
public class JobSpecificationServiceImpl implements JobSpecificationService {

    /**
     * How long it takes to completely resolve a job specification given inputs.
     */
    private static final String RESOLVE_JOB_SPECIFICATION_TIMER = "genie.services.specification.resolve.timer";

    /**
     * How long it takes to query the database for cluster command combinations matching supplied criteria.
     */
    private static final String CLUSTER_COMMAND_QUERY_TIMER_NAME
        = "genie.services.specification.clusterCommandQuery.timer";

    /**
     * How long it takes to select a cluster from the set of clusters returned by database query.
     */
    private static final String SELECT_CLUSTER_TIMER_NAME
        = "genie.services.specification.selectCluster.timer";

    /**
     * How long it takes to select a command for a given cluster.
     */
    private static final String SELECT_COMMAND_TIMER_NAME
        = "genie.services.specification.selectCommand.timer";

    /**
     * How long it takes to select the applications for a given command.
     */
    private static final String SELECT_APPLICATIONS_TIMER_NAME
        = "genie.services.specification.selectApplications.timer";

    /**
     * How many times a cluster load balancer is invoked.
     */
    private static final String SELECT_LOAD_BALANCER_COUNTER_NAME
        = "genie.services.specification.loadBalancer.counter";

    private static final File DEFAULT_JOB_DIRECTORY = new File("/tmp/genie/jobs");

    private static final String NO_ID_FOUND = "No id found";

    private static final String LOAD_BALANCER_STATUS_SUCCESS = "success";
    private static final String LOAD_BALANCER_STATUS_NO_PREFERENCE = "no preference";
    private static final String LOAD_BALANCER_STATUS_EXCEPTION = "exception";
    private static final String LOAD_BALANCER_STATUS_INVALID = "invalid";

    private final ApplicationPersistenceService applicationPersistenceService;
    private final ClusterPersistenceService clusterPersistenceService;
    private final CommandPersistenceService commandPersistenceService;
    private final List<ClusterLoadBalancer> clusterLoadBalancers;
    private final MeterRegistry registry;
    private final int defaultMemory;

    private final Counter noClusterSelectedCounter;
    private final Counter noClusterFoundCounter;

    /**
     * Constructor.
     *
     * @param applicationPersistenceService The service to use to manipulate applications
     * @param clusterPersistenceService     The service to use to manipulate clusters
     * @param commandPersistenceService     The service to use to manipulate commands
     * @param clusterLoadBalancers          The load balancer implementations to use
     * @param registry                      The metrics repository to use
     * @param jobsProperties                The properties for running a job set by the user
     */
    public JobSpecificationServiceImpl(
        final ApplicationPersistenceService applicationPersistenceService,
        final ClusterPersistenceService clusterPersistenceService,
        final CommandPersistenceService commandPersistenceService,
        @NotEmpty final List<ClusterLoadBalancer> clusterLoadBalancers,
        final MeterRegistry registry,
        final JobsProperties jobsProperties
    ) {
        this.applicationPersistenceService = applicationPersistenceService;
        this.clusterPersistenceService = clusterPersistenceService;
        this.commandPersistenceService = commandPersistenceService;
        this.clusterLoadBalancers = clusterLoadBalancers;
        this.defaultMemory = jobsProperties.getMemory().getDefaultJobMemory();

        // Metrics
        this.registry = registry;
        this.noClusterSelectedCounter
            = this.registry.counter("genie.services.specification.selectCluster.noneSelected.counter");
        this.noClusterFoundCounter
            = this.registry.counter("genie.services.specification.selectCluster.noneFound.counter");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobSpecification resolveJobSpecification(final String id, @Valid final JobRequest jobRequest) {
        final long start = System.nanoTime();
        final Set<Tag> tags = Sets.newHashSet();
        try {
            log.info(
                "Received request to resolve a job specification for job id {} and parameters {}",
                id,
                jobRequest
            );
            final Map<Cluster, String> clustersAndCommandsForJob = this.queryForClustersAndCommands(
                jobRequest.getCriteria().getClusterCriteria(),
                jobRequest.getCriteria().getCommandCriterion()
            );
            // Resolve the cluster for the job request based on the tags specified
            final Cluster cluster = this.selectCluster(id, jobRequest, clustersAndCommandsForJob.keySet());
            // Resolve the command for the job request based on command tags and cluster chosen
            final Command command = this.getCommand(clustersAndCommandsForJob.get(cluster), id);
            // Resolve the applications to use based on the command that was selected
            final List<JobSpecification.ExecutionResource> applicationResources = Lists.newArrayList();
            for (final Application application : this.getApplications(id, jobRequest, command)) {
                applicationResources.add(
                    new JobSpecification.ExecutionResource(application.getId(), application.getResources())
                );
            }

            final List<String> commandArgs = Lists.newArrayList(command.getExecutable());
            commandArgs.addAll(jobRequest.getCommandArgs());

            //TODO: Set the default job location as a server property?
            final JobSpecification jobSpecification = new JobSpecification(
                commandArgs,
                new JobSpecification.ExecutionResource(id, jobRequest.getResources()),
                new JobSpecification.ExecutionResource(cluster.getId(), cluster.getResources()),
                new JobSpecification.ExecutionResource(command.getId(), command.getResources()),
                applicationResources,
                this.generateEnvironmentVariables(id, jobRequest, cluster, command),
                jobRequest.getRequestedAgentConfig().isInteractive(),
                jobRequest.getRequestedAgentConfig().getRequestedJobDirectoryLocation().orElse(DEFAULT_JOB_DIRECTORY)
            );

            MetricsUtils.addSuccessTags(tags);
            return jobSpecification;
        } catch (final Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw new RuntimeException(t);
        } finally {
            this.registry
                .timer(RESOLVE_JOB_SPECIFICATION_TIMER, tags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    private Map<Cluster, String> queryForClustersAndCommands(
        final List<Criterion> clusterCriteria,
        final Criterion commandCriterion
    ) throws GenieException {
        final long start = System.nanoTime();
        final Set<Tag> tags = Sets.newHashSet();
        try {
            final Map<Cluster, String> clustersAndCommands
                = this.clusterPersistenceService.findClustersAndCommandsForCriteria(clusterCriteria, commandCriterion);
            MetricsUtils.addSuccessTags(tags);
            return clustersAndCommands;
        } catch (final Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw t;
        } finally {
            this.registry
                .timer(CLUSTER_COMMAND_QUERY_TIMER_NAME, tags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    private Cluster selectCluster(
        final String id,
        final JobRequest jobRequest,
        final Set<Cluster> clusters
    ) throws GenieException {
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
                cluster = this.selectClusterWithLoadBalancer(counterTags, clusters, id, jobRequest);
            }

            log.info("Selected cluster {} for job {}", cluster.getId(), id);
            MetricsUtils.addSuccessTags(timerTags);
            return cluster;
        } catch (final Throwable t) {
            MetricsUtils.addFailureTagsWithException(timerTags, t);
            throw t;
        } finally {
            this.registry
                .timer(SELECT_CLUSTER_TIMER_NAME, timerTags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }

    }

    private Command getCommand(
        final String commandId,
        final String jobId
    ) throws GenieException {
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
            throw t;
        } finally {
            this.registry
                .timer(SELECT_COMMAND_TIMER_NAME, tags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    private List<Application> getApplications(
        final String id,
        final JobRequest jobRequest,
        final Command command
    ) throws GenieException {
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
            throw t;
        } finally {
            this.registry
                .timer(SELECT_APPLICATIONS_TIMER_NAME, tags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    private Cluster selectClusterWithLoadBalancer(
        final Set<Tag> counterTags,
        final Set<Cluster> clusters,
        final String id,
        final JobRequest jobRequest
    ) throws GeniePreconditionException {
        Cluster cluster = null;
        for (final ClusterLoadBalancer loadBalancer : this.clusterLoadBalancers) {
            final String loadBalancerClass;
            if (loadBalancer instanceof TargetClassAware) {
                final Class<?> targetClass = ((TargetClassAware) loadBalancer).getTargetClass();
                if (targetClass != null) {
                    loadBalancerClass = targetClass.getCanonicalName();
                } else {
                    loadBalancerClass = loadBalancer.getClass().getCanonicalName();
                }
            } else {
                loadBalancerClass = loadBalancer.getClass().getCanonicalName();
            }
            counterTags.add(Tag.of(MetricsConstants.TagKeys.CLASS_NAME, loadBalancerClass));
            try {
                final Cluster selectedCluster = loadBalancer.selectCluster(
                    clusters,
                    this.toV3JobRequest(id, jobRequest)
                );
                if (selectedCluster != null) {
                    // Make sure the cluster existed in the original list of clusters
                    if (clusters.contains(selectedCluster)) {
                        log.debug(
                            "Successfully selected cluster {} using load balancer {}",
                            selectedCluster.getId(),
                            loadBalancerClass
                        );
                        counterTags.add(Tag.of(MetricsConstants.TagKeys.STATUS, LOAD_BALANCER_STATUS_SUCCESS));
                        this.registry.counter(SELECT_LOAD_BALANCER_COUNTER_NAME, counterTags).increment();
                        cluster = selectedCluster;
                        break;
                    } else {
                        log.error(
                            "Successfully selected cluster {} using load balancer {} but it wasn't in original cluster "
                                + "list {}",
                            selectedCluster.getId(),
                            loadBalancerClass,
                            clusters
                        );
                        counterTags.add(Tag.of(MetricsConstants.TagKeys.STATUS, LOAD_BALANCER_STATUS_INVALID));
                        this.registry.counter(SELECT_LOAD_BALANCER_COUNTER_NAME, counterTags).increment();
                    }
                } else {
                    counterTags.add(
                        Tag.of(MetricsConstants.TagKeys.STATUS, LOAD_BALANCER_STATUS_NO_PREFERENCE)
                    );
                    this.registry.counter(SELECT_LOAD_BALANCER_COUNTER_NAME, counterTags).increment();
                }
            } catch (final Exception e) {
                log.error("Cluster load balancer {} threw exception:", loadBalancer, e);
                counterTags.add(Tag.of(MetricsConstants.TagKeys.STATUS, LOAD_BALANCER_STATUS_EXCEPTION));
                this.registry.counter(SELECT_LOAD_BALANCER_COUNTER_NAME, counterTags).increment();
            }
        }

        // Make sure we selected a cluster
        if (cluster == null) {
            this.noClusterSelectedCounter.increment();
            throw new GeniePreconditionException(
                "Unable to select a cluster from using any of the available load balancer's."
            );
        }

        return cluster;
    }

    private ImmutableMap<String, String> generateEnvironmentVariables(
        final String id,
        final JobRequest jobRequest,
        final Cluster cluster,
        final Command command
    ) {
        final ImmutableMap.Builder<String, String> envVariables = ImmutableMap.builder();
        envVariables.put("GENIE_VERSION", "4");
        envVariables.put(JobConstants.GENIE_CLUSTER_ID_ENV_VAR, cluster.getId());
        envVariables.put(JobConstants.GENIE_CLUSTER_NAME_ENV_VAR, cluster.getMetadata().getName());
        envVariables.put(JobConstants.GENIE_CLUSTER_TAGS_ENV_VAR, this.tagsToString(cluster.getMetadata().getTags()));
        envVariables.put(JobConstants.GENIE_COMMAND_ID_ENV_VAR, command.getId());
        envVariables.put(JobConstants.GENIE_COMMAND_NAME_ENV_VAR, command.getMetadata().getName());
        envVariables.put(JobConstants.GENIE_COMMAND_TAGS_ENV_VAR, this.tagsToString(command.getMetadata().getTags()));
        envVariables.put(JobConstants.GENIE_JOB_ID_ENV_VAR, id);
        envVariables.put(JobConstants.GENIE_JOB_NAME_ENV_VAR, jobRequest.getMetadata().getName());
        envVariables.put(
            JobConstants.GENIE_JOB_MEMORY_ENV_VAR,
            String.valueOf(command.getMemory().orElse(this.defaultMemory))
        );
        envVariables.put(JobConstants.GENIE_JOB_TAGS_ENV_VAR, this.tagsToString(jobRequest.getMetadata().getTags()));
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
        return StringUtils.replaceAll(StringUtils.replaceAll(joinedString, "\'", "\\\'"), "\"", "\\\"");
    }
}
