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
package com.netflix.genie.agent.execution.statemachine.actions;

import com.netflix.genie.agent.AgentMetadata;
import com.netflix.genie.agent.cli.ArgumentDelegates;
import com.netflix.genie.agent.cli.JobRequestConverter;
import com.netflix.genie.agent.execution.ExecutionContext;
import com.netflix.genie.agent.execution.process.JobProcessManager;
import com.netflix.genie.agent.execution.services.AgentFileStreamService;
import com.netflix.genie.agent.execution.services.AgentHeartBeatService;
import com.netflix.genie.agent.execution.services.AgentJobKillService;
import com.netflix.genie.agent.execution.services.AgentJobService;
import com.netflix.genie.agent.execution.services.JobSetupService;
import com.netflix.genie.common.internal.services.JobArchiveService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

/**
 * Spring auto configuration to provide beans for all available actions within the Agent job execution state machine.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Configuration
public class StateMachineActionsAutoConfiguration {

    /**
     * Provide a lazy {@link CleanupJobAction} bean.
     *
     * @param executionContext The job execution context to use
     * @param agentJobService  The agent job service to use
     * @return A {@link CleanupJobAction} instance
     */
    @Bean
    @Lazy
    public CleanupJobAction cleanupJobAction(
        final ExecutionContext executionContext,
        final AgentJobService agentJobService
    ) {
        return new CleanupJobAction(executionContext, agentJobService);
    }

    /**
     * Provide a lazy {@link ConfigureAgentAction} bean.
     *
     * @param executionContext The job execution context to use
     * @return A {@link ConfigureAgentAction} instance
     */
    @Bean
    @Lazy
    public ConfigureAgentAction configureAgentAction(final ExecutionContext executionContext) {
        return new ConfigureAgentAction(executionContext);
    }

    /**
     * Provide a lazy {@link HandleErrorAction} bean.
     *
     * @param executionContext The job execution context to use
     * @param agentJobService  The agent job service to use
     * @return A {@link HandleErrorAction} instance
     */
    @Bean
    @Lazy
    public HandleErrorAction handleErrorAction(
        final ExecutionContext executionContext,
        final AgentJobService agentJobService
    ) {
        return new HandleErrorAction(executionContext, agentJobService);
    }

    /**
     * Provide a lazy {@link InitializeAction} bean.
     *
     * @param executionContext The job execution context to use
     * @param agentJobService  The agent job service to use
     * @param agentMetadata    The agent metadata to use
     * @return An {@link InitializeAction} instance
     */
    @Bean
    @Lazy
    public InitializeAction initializeAction(
        final ExecutionContext executionContext,
        final AgentJobService agentJobService,
        final AgentMetadata agentMetadata
    ) {
        return new InitializeAction(executionContext, agentJobService, agentMetadata);
    }

    /**
     * Provide a lazy {@link LaunchJobAction} bean.
     *
     * @param executionContext              The job execution context to use
     * @param jobProcessManager             The launch job service to use
     * @param agentJobService               The agent job service to use
     * @param agentFileStreamService        The agent file stream service to use
     * @param runtimeConfigurationArguments the runtime configuration arguments
     * @return A {@link LaunchJobAction} instance
     */
    @Bean
    @Lazy
    public LaunchJobAction launchJobAction(
        final ExecutionContext executionContext,
        final JobProcessManager jobProcessManager,
        final AgentJobService agentJobService,
        final AgentFileStreamService agentFileStreamService,
        final ArgumentDelegates.RuntimeConfigurationArguments runtimeConfigurationArguments
    ) {
        return new LaunchJobAction(
            executionContext,
            jobProcessManager,
            agentJobService,
            agentFileStreamService,
            runtimeConfigurationArguments
        );
    }

    /**
     * Provide a lazy {@link MonitorJobAction} bean.
     *
     * @param executionContext  The job execution context to use
     * @param jobProcessManager The launch job service to use
     * @param agentJobService   The agent job service to use
     * @param jobSetupService   The job setup service
     * @param jobArchiveService The job archive service
     * @param cleanupArguments  The cleanup arguments group
     * @return A {@link MonitorJobAction} instance
     */
    @Bean
    @Lazy
    public MonitorJobAction monitorJobAction(
        final ExecutionContext executionContext,
        final JobProcessManager jobProcessManager,
        final AgentJobService agentJobService,
        final JobSetupService jobSetupService,
        final JobArchiveService jobArchiveService,
        final ArgumentDelegates.CleanupArguments cleanupArguments
    ) {
        return new MonitorJobAction(
            executionContext,
            agentJobService,
            jobProcessManager,
            jobSetupService,
            jobArchiveService,
            cleanupArguments
        );
    }

    /**
     * Provide a lazy {@link ResolveJobSpecificationAction} bean.
     *
     * @param executionContext    The job execution context to use
     * @param jobRequestArguments The job request arguments to use
     * @param agentJobService     The agent job service to use
     * @param agentMetadata       The agent metadata to use
     * @param jobRequestConverter The job request converter to use
     * @return A {@link ResolveJobSpecificationAction} instance
     */
    @Bean
    @Lazy
    public ResolveJobSpecificationAction resolveJobSpecificationAction(
        final ExecutionContext executionContext,
        final ArgumentDelegates.JobRequestArguments jobRequestArguments,
        final AgentJobService agentJobService,
        final AgentMetadata agentMetadata,
        final JobRequestConverter jobRequestConverter
    ) {
        return new ResolveJobSpecificationAction(
            executionContext,
            jobRequestArguments,
            agentJobService,
            agentMetadata,
            jobRequestConverter
        );
    }

    /**
     * Provide a lazy {@link SetUpJobAction} bean.
     *
     * @param executionContext      The job execution context to use
     * @param jobSetupService       The job setup service to use
     * @param agentJobService       The agent job service to use
     * @param agentHeartBeatService The agent heart beat service to use
     * @param agentJobKillService   The agent job kill service to use
     * @param fileStreamService     The agent file stream service to use
     * @return A {@link SetUpJobAction} instance
     */
    @Bean
    @Lazy
    public SetUpJobAction setUpJobAction(
        final ExecutionContext executionContext,
        final JobSetupService jobSetupService,
        final AgentJobService agentJobService,
        final AgentHeartBeatService agentHeartBeatService,
        final AgentJobKillService agentJobKillService,
        final AgentFileStreamService fileStreamService
    ) {
        return new SetUpJobAction(
            executionContext,
            jobSetupService,
            agentJobService,
            agentHeartBeatService,
            agentJobKillService,
            fileStreamService
        );
    }

    /**
     * Provide a {@link ShutdownAction} bean.
     *
     * @param executionContext The job execution context to use
     * @return A {@link ShutdownAction} instance
     */
    @Bean
    @Lazy
    public ShutdownAction shutdownAction(
        final ExecutionContext executionContext
    ) {
        return new ShutdownAction(executionContext);
    }
}
