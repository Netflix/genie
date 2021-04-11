/*
 *
 *  Copyright 2019 Netflix, Inc.
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
package com.netflix.genie.web.agent.launchers.impl;

import brave.Span;
import brave.Tracer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.genie.common.external.dtos.v4.JobMetadata;
import com.netflix.genie.common.internal.tracing.brave.BraveTracePropagator;
import com.netflix.genie.common.internal.tracing.brave.BraveTracingComponents;
import com.netflix.genie.web.agent.launchers.AgentLauncher;
import com.netflix.genie.web.data.services.DataServices;
import com.netflix.genie.web.data.services.PersistenceService;
import com.netflix.genie.web.data.services.impl.jpa.queries.aggregates.JobInfoAggregate;
import com.netflix.genie.web.dtos.ResolvedJob;
import com.netflix.genie.web.exceptions.checked.AgentLaunchException;
import com.netflix.genie.web.introspection.GenieWebHostInfo;
import com.netflix.genie.web.introspection.GenieWebRpcInfo;
import com.netflix.genie.web.properties.LocalAgentLauncherProperties;
import com.netflix.genie.web.util.ExecutorFactory;
import com.netflix.genie.web.util.MetricsUtils;
import com.netflix.genie.web.util.UNIXUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.lang3.SystemUtils;
import org.springframework.boot.actuate.health.Health;

import javax.annotation.Nullable;
import javax.validation.Valid;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implementation of {@link AgentLauncher} which launched Agent instances on the local Genie hardware.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Slf4j
public class LocalAgentLauncherImpl implements AgentLauncher {

    private static final String NUMBER_ACTIVE_JOBS_KEY = "numActiveJobs";
    private static final String ALLOCATED_MEMORY_KEY = "allocatedMemory";
    private static final String USED_MEMORY_KEY = "usedMemory";
    private static final String AVAILABLE_MEMORY_KEY = "availableMemory";
    private static final String AVAILABLE_MAX_JOB_CAPACITY_KEY = "availableMaxJobCapacity";
    private static final Map<String, String> INFO_UNAVAILABLE_DETAILS = ImmutableMap.of(
        "jobInfoUnavailable",
        "Unable to retrieve host job information. State unknown."
    );

    private static final String RUN_USER_PLACEHOLDER = "<GENIE_USER>";
    private static final String SETS_ID = "setsid";
    private static final Object MEMORY_CHECK_LOCK = new Object();
    private static final String THIS_CLASS = LocalAgentLauncherImpl.class.getCanonicalName();
    private static final Tag CLASS_TAG = Tag.of(LAUNCHER_CLASS_KEY, THIS_CLASS);

    private final String hostname;
    private final PersistenceService persistenceService;
    private final LocalAgentLauncherProperties launcherProperties;
    private final ExecutorFactory executorFactory;
    private final MeterRegistry registry;
    private final Executor sharedExecutor;
    private final int rpcPort;
    private final LoadingCache<String, JobInfoAggregate> jobInfoCache;
    private final JsonNode launcherExt;

    private final AtomicLong numActiveJobs;
    private final AtomicLong usedMemory;

    private final Tracer tracer;
    private final BraveTracePropagator tracePropagator;

    /**
     * Constructor.
     *
     * @param hostInfo           The {@link GenieWebHostInfo} instance
     * @param rpcInfo            The {@link GenieWebRpcInfo} instance
     * @param dataServices       The {@link DataServices} encapsulation instance to use
     * @param launcherProperties The properties from the configuration that control agent behavior
     * @param executorFactory    A {@link ExecutorFactory} to create {@link org.apache.commons.exec.Executor}
     *                           instances
     * @param tracingComponents  The {@link BraveTracingComponents} instance to use
     * @param registry           Metrics repository
     */
    public LocalAgentLauncherImpl(
        final GenieWebHostInfo hostInfo,
        final GenieWebRpcInfo rpcInfo,
        final DataServices dataServices,
        final LocalAgentLauncherProperties launcherProperties,
        final ExecutorFactory executorFactory,
        final BraveTracingComponents tracingComponents,
        final MeterRegistry registry
    ) {
        this.hostname = hostInfo.getHostname();
        this.rpcPort = rpcInfo.getRpcPort();
        this.persistenceService = dataServices.getPersistenceService();
        this.launcherProperties = launcherProperties;
        this.executorFactory = executorFactory;
        this.registry = registry;
        this.sharedExecutor = this.executorFactory.newInstance(false);

        this.numActiveJobs = new AtomicLong(0L);
        this.usedMemory = new AtomicLong(0L);

        this.tracer = tracingComponents.getTracer();
        this.tracePropagator = tracingComponents.getTracePropagator();

        final Set<Tag> tags = Sets.newHashSet(
            Tag.of("launcherClass", this.getClass().getSimpleName())
        );
        // TODO: These metrics should either be renamed or tagged so that it's easier to slice and dice them
        //       Currently we have a single launcher but as more come this won't represent necessarily what the name
        //       implies. Even now there are agent jobs not launched through the API which are not captured in this
        //       metric and thus it doesn't accurately give a number of the active jobs in the system instead it gives
        //       only active jobs running locally on a given node. I'm not renaming it now (5/28/2020) since we don't
        //       yet have a firm plan in place for a) handling multiple launcher and b) if a leadership task should
        //       publish the number of running jobs and other aggregate metrics from the system. Leaving these named
        //       this way also makes it so we don't have to modify as many dashboards or auto scaling policies ATM
        this.registry.gauge("genie.jobs.active.gauge", tags, this.numActiveJobs);
        this.registry.gauge("genie.jobs.memory.used.gauge", tags, this.usedMemory);

        // Leverage a loading cache to handle the timed async fetching for us rather than creating a thread
        // on a scheduler etc. This also provides atomicity.
        // Note that this is not intended to be used for exact calculations and more for metrics and health checks
        // as the data could be somewhat stale
        this.jobInfoCache = Caffeine
            .newBuilder()
            // The refresh fails silently this will protect from stale data
            .expireAfterWrite(this.launcherProperties.getHostInfoExpireAfter())
            .refreshAfterWrite(this.launcherProperties.getHostInfoRefreshAfter())
            .initialCapacity(1)
            .build(
                host -> {
                    final JobInfoAggregate info = this.persistenceService.getHostJobInformation(host);

                    // this should always be the case but just in case
                    if (info != null) {
                        // Proactively update the metric reporting
                        this.numActiveJobs.set(info.getNumberOfActiveJobs());
                        this.usedMemory.set(info.getTotalMemoryAllocated());
                    }

                    return info;
                }
            );

        this.launcherExt = JsonNodeFactory.instance.objectNode()
            .put(LAUNCHER_CLASS_EXT_FIELD, THIS_CLASS)
            .put(SOURCE_HOST_EXT_FIELD, this.hostname);

        // Force the initial fetch so that all subsequent fetches will be non-blocking
        try {
            this.jobInfoCache.get(this.hostname);
        } catch (final Exception e) {
            log.error("Unable to fetch initial job information", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<JsonNode> launchAgent(
        @Valid final ResolvedJob resolvedJob,
        @Nullable final JsonNode requestedLauncherExt
    ) throws AgentLaunchException {
        final long start = System.nanoTime();
        log.info("Received request to launch local agent to run job: {}", resolvedJob);
        final Set<Tag> tags = new HashSet<>();
        tags.add(CLASS_TAG);

        try {
            final JobMetadata jobMetadata = resolvedJob.getJobMetadata();
            final String user = jobMetadata.getUser();

            if (this.launcherProperties.isRunAsUserEnabled()) {
                final String group = jobMetadata.getGroup().orElse(null);
                try {
                    UNIXUtils.createUser(user, group, this.sharedExecutor);
                } catch (IOException e) {
                    log.error("Failed to create user {}: {}", jobMetadata.getUser(), e.getMessage(), e);
                    throw new AgentLaunchException(e);
                }
            }

            // Check error conditions
            final int jobMemory = resolvedJob.getJobEnvironment().getMemory();
            final String jobId = resolvedJob.getJobSpecification().getJob().getId();

            // Job was resolved with more memory allocated than the system was configured to allow
            if (jobMemory > this.launcherProperties.getMaxJobMemory()) {
                throw new AgentLaunchException(
                    "Unable to launch job as the requested job memory ("
                        + jobMemory
                        + "MB) exceeds the maximum allowed by the configuration of the system ("
                        + this.launcherProperties.getMaxJobMemory()
                        + "MB)"
                );
            }

            final CommandLine commandLine = this.createCommandLine(
                ImmutableMap.of(
                    LocalAgentLauncherProperties.SERVER_HOST_PLACEHOLDER, this.launcherProperties.getServerHostname(),
                    LocalAgentLauncherProperties.SERVER_PORT_PLACEHOLDER, Integer.toString(this.rpcPort),
                    LocalAgentLauncherProperties.JOB_ID_PLACEHOLDER, jobId,
                    RUN_USER_PLACEHOLDER, user,
                    LocalAgentLauncherProperties.AGENT_JAR_PLACEHOLDER, this.launcherProperties.getAgentJarPath()
                )
            );

            // One at a time to ensure we don't overflow configured max
            synchronized (MEMORY_CHECK_LOCK) {
                final long usedMemoryOnHost = this.persistenceService.getUsedMemoryOnHost(this.hostname);
                final long expectedUsedMemoryOnHost = usedMemoryOnHost + jobMemory;
                if (expectedUsedMemoryOnHost > this.launcherProperties.getMaxTotalJobMemory()) {
                    throw new AgentLaunchException(
                        "Running job "
                            + jobId
                            + " with "
                            + jobMemory
                            + "MB of memory would cause there to be more memory used than the configured amount of "
                            + this.launcherProperties.getMaxTotalJobMemory()
                            + "MB. "
                            + usedMemoryOnHost
                            + "MB worth of jobs are currently running on this node."
                    );
                }
            }

            // Inherit server environment
            final Map<String, String> environment = Maps.newHashMap(System.getenv());
            // Add extra environment from configuration, if any
            environment.putAll(this.launcherProperties.getAdditionalEnvironment());
            // Add tracing context so agent continues trace
            final Span currentSpan = this.tracer.currentSpan();
            if (currentSpan != null) {
                environment.putAll(this.tracePropagator.injectForAgent(currentSpan.context()));
            }
            log.debug("Launching agent: {}, env: {}", commandLine, environment);

            // TODO: What happens if the server crashes? Does the process live on? Make sure this is totally detached
            final Executor executor = this.executorFactory.newInstance(true);

            if (this.launcherProperties.isProcessOutputCaptureEnabled()) {
                final String debugOutputPath =
                    System.getProperty(SystemUtils.JAVA_IO_TMPDIR, "/tmp") + "/agent-job-" + jobId + ".txt";
                try {
                    final FileOutputStream fileOutput = new FileOutputStream(debugOutputPath, false);
                    executor.setStreamHandler(new PumpStreamHandler(fileOutput));
                } catch (final FileNotFoundException e) {
                    log.error("Failed to create agent process output file", e);
                    throw new AgentLaunchException(e);
                }
            }

            log.info("Launching agent for job {}", jobId);

            final AgentResultHandler resultHandler = new AgentResultHandler(jobId);

            try {
                executor.execute(commandLine, environment, resultHandler);
            } catch (final IOException ioe) {
                throw new AgentLaunchException(
                    "Unable to launch agent using command: " + commandLine.toString(),
                    ioe
                );
            }

            MetricsUtils.addSuccessTags(tags);
            return Optional.of(this.launcherExt);
        } catch (final AgentLaunchException e) {
            MetricsUtils.addFailureTagsWithException(tags, e);
            throw e;
        } catch (final Exception e) {
            log.error("Unable to launch local agent due to {}", e.getMessage(), e);
            MetricsUtils.addFailureTagsWithException(tags, e);
            throw new AgentLaunchException("Unable to launch local agent due to unhandled error", e);
        } finally {
            this.registry.timer(LAUNCH_TIMER, tags).record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Health health() {
        final JobInfoAggregate jobInfo;
        try {
            jobInfo = this.jobInfoCache.get(this.hostname);
        } catch (final Exception e) {
            log.error("Computing host info threw exception", e);
            // Could do unknown but if the persistence tier threw an exception we're likely down anyway
            return Health.down(e).build();
        }
        // This should never happen but if it does it's likely a problem deeper in system (persistence tier)
        if (jobInfo == null) {
            log.error("Unable to retrieve host info from cache");
            return Health.unknown().withDetails(INFO_UNAVAILABLE_DETAILS).build();
        }

        // Use allocated memory to make the host go OOS early enough that we don't throw as many exceptions on
        // accepted jobs during launch
        final long memoryAllocated = jobInfo.getTotalMemoryAllocated();
        final long availableMemory = this.launcherProperties.getMaxTotalJobMemory() - memoryAllocated;
        final int maxJobMemory = this.launcherProperties.getMaxJobMemory();

        final Health.Builder builder;

        // If we can fit one more max job in we're still healthy
        if (availableMemory >= maxJobMemory) {
            builder = Health.up();
        } else {
            builder = Health.down();
        }

        return builder
            .withDetail(NUMBER_ACTIVE_JOBS_KEY, jobInfo.getNumberOfActiveJobs())
            .withDetail(ALLOCATED_MEMORY_KEY, memoryAllocated)
            .withDetail(AVAILABLE_MEMORY_KEY, availableMemory)
            .withDetail(USED_MEMORY_KEY, jobInfo.getTotalMemoryUsed())
            .withDetail(
                AVAILABLE_MAX_JOB_CAPACITY_KEY,
                (availableMemory >= 0 && maxJobMemory > 0) ? (availableMemory / maxJobMemory) : 0)
            .build();
    }

    private CommandLine createCommandLine(
        final Map<String, String> argumentValueReplacements
    ) {
        final List<String> commandLineTemplate = Lists.newArrayList();

        // Run detached with setsid on Linux
        if (SystemUtils.IS_OS_LINUX) {
            commandLineTemplate.add(SETS_ID);
        }

        // Run as different user with sudo
        if (this.launcherProperties.isRunAsUserEnabled()) {
            commandLineTemplate.addAll(Lists.newArrayList("sudo", "-E", "-u", RUN_USER_PLACEHOLDER));
        }

        // Agent  command line to launch agent (i.e. JVM and its options)
        commandLineTemplate.addAll(this.launcherProperties.getLaunchCommandTemplate());

        final CommandLine commandLine = new CommandLine(commandLineTemplate.get(0));

        for (int i = 1; i < commandLineTemplate.size(); i++) {
            final String argument = commandLineTemplate.get(i);
            // If the argument placeholder is a key in the map, replace it with the corresponding value.
            // Otherwise it's not a placeholder, add it as-is to the command-line.
            commandLine.addArgument(argumentValueReplacements.getOrDefault(argument, argument));
        }

        return commandLine;
    }

    /**
     * Simple {@link org.apache.commons.exec.ExecuteResultHandler} implementation that logs completion.
     *
     * @author tgianos
     * @since 4.0.0
     */
    @Slf4j
    @VisibleForTesting
    static class AgentResultHandler extends DefaultExecuteResultHandler {

        private final String jobId;

        /**
         * Constructor.
         *
         * @param jobId The id of the job the agent this handler is attached to is running
         */
        AgentResultHandler(final String jobId) {
            this.jobId = jobId;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onProcessComplete(final int exitValue) {
            super.onProcessComplete(exitValue);
            log.info("Agent process for job {} completed with exit value {}", this.jobId, exitValue);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onProcessFailed(final ExecuteException e) {
            super.onProcessFailed(e);
            log.error("Agent process failed for job {} due to {}", this.jobId, e.getMessage(), e);
        }
    }
}
