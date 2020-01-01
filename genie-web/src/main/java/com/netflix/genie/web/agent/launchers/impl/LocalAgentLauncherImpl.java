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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.genie.common.external.dtos.v4.JobMetadata;
import com.netflix.genie.web.agent.launchers.AgentLauncher;
import com.netflix.genie.web.data.services.JobSearchService;
import com.netflix.genie.web.dtos.ResolvedJob;
import com.netflix.genie.web.exceptions.checked.AgentLaunchException;
import com.netflix.genie.web.introspection.GenieWebHostInfo;
import com.netflix.genie.web.introspection.GenieWebRpcInfo;
import com.netflix.genie.web.properties.LocalAgentLauncherProperties;
import com.netflix.genie.web.util.ExecutorFactory;
import com.netflix.genie.web.util.UNIXUtils;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.lang3.SystemUtils;

import javax.validation.Valid;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link AgentLauncher} which launched Agent instances on the local Genie hardware.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Slf4j
public class LocalAgentLauncherImpl implements AgentLauncher {

    private static final String RUN_USER_PLACEHOLDER = "<GENIE_USER>";
    private static final String SETS_ID = "setsid";
    private static final Object MEMORY_CHECK_LOCK = new Object();


    private final String hostname;
    private final JobSearchService jobSearchService;
    private final LocalAgentLauncherProperties launcherProperties;
    private final ExecutorFactory executorFactory;
    private final MeterRegistry registry;
    private final Executor sharedExecutor;
    private int rpcPort;

    /**
     * Constructor.
     *
     * @param hostInfo           The {@link GenieWebHostInfo} instance
     * @param rpcInfo            The {@link GenieWebRpcInfo} instance
     * @param jobSearchService   The {@link JobSearchService} used to get metrics about the jobs on this node
     * @param launcherProperties The properties from the configuration that control agent behavior
     * @param executorFactory    A {@link ExecutorFactory} to create {@link org.apache.commons.exec.Executor}
     *                           instances
     * @param registry           Metrics repository
     */
    public LocalAgentLauncherImpl(
        final GenieWebHostInfo hostInfo,
        final GenieWebRpcInfo rpcInfo,
        final JobSearchService jobSearchService,
        final LocalAgentLauncherProperties launcherProperties,
        final ExecutorFactory executorFactory,
        final MeterRegistry registry
    ) {
        this.hostname = hostInfo.getHostname();
        this.rpcPort = rpcInfo.getRpcPort();
        this.jobSearchService = jobSearchService;
        this.launcherProperties = launcherProperties;
        this.executorFactory = executorFactory;
        this.registry = registry;
        this.sharedExecutor = this.executorFactory.newInstance(false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void launchAgent(@Valid final ResolvedJob resolvedJob) throws AgentLaunchException {
        log.debug("Received request to launch local agent to run job: {}", resolvedJob);

        final JobMetadata jobMetadata = resolvedJob.getJobMetadata();
        final String user = jobMetadata.getUser();

        if (launcherProperties.isRunAsUserEnabled()) {
            final String group = jobMetadata.getGroup().orElse(null);
            try {
                UNIXUtils.createUser(user, group, sharedExecutor);
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
                LocalAgentLauncherProperties.SERVER_PORT_PLACEHOLDER, Integer.toString(this.rpcPort),
                LocalAgentLauncherProperties.JOB_ID_PLACEHOLDER, jobId,
                RUN_USER_PLACEHOLDER, user,
                LocalAgentLauncherProperties.AGENT_JAR_PLACEHOLDER, this.launcherProperties.getAgentJarPath()
            )
        );

        // One at a time to ensure we don't overflow configured max
        synchronized (MEMORY_CHECK_LOCK) {
            final long usedMemoryOnHost = this.jobSearchService.getUsedMemoryOnHost(this.hostname);
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
        log.debug("Launching agent: {}, env: {}", commandLine, environment);

        // TODO: What happens if the server crashes? Does the process live on? Make sure this is totally detached
        final Executor executor = this.executorFactory.newInstance(true);

        if (this.launcherProperties.isProcessOutputCaptureEnabled()) {
            final String debugOutputPath =
                System.getProperty(SystemUtils.JAVA_IO_TMPDIR, "/tmp") + "/agent-job-" + jobId + ".txt";
            try {
                final FileOutputStream fileOutput = new FileOutputStream(debugOutputPath, false);
                executor.setStreamHandler(new PumpStreamHandler(fileOutput));
            } catch (FileNotFoundException e) {
                log.error("Failed to create agent process output file", e);
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
