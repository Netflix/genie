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
import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.web.agent.launchers.AgentLauncher;
import com.netflix.genie.web.data.services.JobSearchService;
import com.netflix.genie.web.dtos.ResolvedJob;
import com.netflix.genie.web.exceptions.checked.AgentLaunchException;
import com.netflix.genie.web.properties.LocalAgentLauncherProperties;
import com.netflix.genie.web.util.ExecutorFactory;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.Executor;
import org.apache.commons.lang3.SystemUtils;

import javax.validation.Valid;
import java.io.IOException;

/**
 * Implementation of {@link AgentLauncher} which launched Agent instances on the local Genie hardware.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Slf4j
public class LocalAgentLauncherImpl implements AgentLauncher {

    private static final String SETS_ID = "setsid";
    private static final String EXEC_COMMAND = "exec";
    private static final String SERVER_HOST_OPTION = "--serverHost";
    private static final String SERVER_HOST_VALUE = "localhost";
    private static final String SERVER_PORT_OPTION = "--serverPort";
    private static final String API_JOB_OPTION = "--api-job";
    private static final String JOB_ID_OPTION = "--jobId";
    private static final String FULL_CLEANUP_OPTION = "--full-cleanup";

    private static final Object MEMORY_CHECK_LOCK = new Object();

    private final JobSearchService jobSearchService;
    private final LocalAgentLauncherProperties launcherProperties;
    private final String hostname;
    private final ExecutorFactory executorFactory;
    private final MeterRegistry registry;
    private final CommandLine commandTemplate;

    /**
     * Constructor.
     *
     * @param hostInfo           The {@link GenieHostInfo} instance
     * @param jobSearchService   The {@link JobSearchService} used to get metrics about the jobs on this node
     * @param launcherProperties The properties from the configuration that control agent behavior
     * @param agentRpcPort       The port the RPC service is listening on for the agent to connect to
     * @param executorFactory    A {@link ExecutorFactory} to create {@link org.apache.commons.exec.Executor}
     *                           instances
     * @param registry           Metrics repository
     */
    public LocalAgentLauncherImpl(
        final GenieHostInfo hostInfo,
        final JobSearchService jobSearchService,
        final LocalAgentLauncherProperties launcherProperties,
        // TODO: Roll this into GenieHostInfo
        final int agentRpcPort,
        final ExecutorFactory executorFactory,
        final MeterRegistry registry
    ) {
        this.hostname = hostInfo.getHostname();
        this.jobSearchService = jobSearchService;
        this.launcherProperties = launcherProperties;
        this.executorFactory = executorFactory;
        this.registry = registry;

        final String[] agentExecutable = this.launcherProperties.getExecutable().toArray(new String[0]);

        if (SystemUtils.IS_OS_LINUX) {
            this.commandTemplate = new CommandLine(SETS_ID);
            this.commandTemplate.addArguments(agentExecutable);
        } else {
            this.commandTemplate = new CommandLine(agentExecutable[0]);
            for (int i = 1; i < agentExecutable.length; i++) {
                // Add the remaining parts of the default executable
                this.commandTemplate.addArgument(agentExecutable[i]);
            }
        }

        // Add the default parameters
        this.commandTemplate.addArgument(EXEC_COMMAND);
        this.commandTemplate.addArgument(SERVER_HOST_OPTION);
        this.commandTemplate.addArgument(SERVER_HOST_VALUE);
        this.commandTemplate.addArgument(SERVER_PORT_OPTION);
        this.commandTemplate.addArgument(Integer.toString(agentRpcPort));
        this.commandTemplate.addArgument(FULL_CLEANUP_OPTION);
        this.commandTemplate.addArgument(API_JOB_OPTION);
        this.commandTemplate.addArgument(JOB_ID_OPTION);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void launchAgent(@Valid final ResolvedJob resolvedJob) throws AgentLaunchException {
        log.debug("Received request to launch local agent to run job: {}", resolvedJob);

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

        // TODO: What happens if the server crashes? Does the process live on? Make sure this is totally detached
        final Executor executor = this.executorFactory.newInstance(true);
        final CommandLine commandLine = new CommandLine(this.commandTemplate);

        // Should now come after `--jobId`
        commandLine.addArgument(jobId);

        // TODO: May not exist when agent is started so probably can't set this...?
        // executor.setWorkingDirectory(jobSpecification.getJobDirectoryLocation());

        final AgentResultHandler resultHandler = new AgentResultHandler(jobId);

        try {
            executor.execute(commandLine, resultHandler);
        } catch (final IOException ioe) {
            throw new AgentLaunchException(
                "Unable to launch agent using command: " + commandLine.toString(),
                ioe
            );
        }
    }

    // TODO: Likely need to handle user creation locally to match V3 behavior. Getting basics done first.

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
