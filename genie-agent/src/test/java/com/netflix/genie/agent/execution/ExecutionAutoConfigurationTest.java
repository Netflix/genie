/*
 *
 *  Copyright 2020 Netflix, Inc.
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
package com.netflix.genie.agent.execution;

import brave.Tracer;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.netflix.genie.agent.AgentMetadata;
import com.netflix.genie.agent.cli.ArgumentDelegates;
import com.netflix.genie.agent.cli.JobRequestConverter;
import com.netflix.genie.agent.cli.logging.AgentLogManager;
import com.netflix.genie.agent.execution.process.JobProcessManager;
import com.netflix.genie.agent.execution.services.AgentFileStreamService;
import com.netflix.genie.agent.execution.services.AgentHeartBeatService;
import com.netflix.genie.agent.execution.services.AgentJobKillService;
import com.netflix.genie.agent.execution.services.AgentJobService;
import com.netflix.genie.agent.execution.services.JobMonitorService;
import com.netflix.genie.agent.execution.services.JobSetupService;
import com.netflix.genie.agent.execution.statemachine.ExecutionContext;
import com.netflix.genie.agent.execution.statemachine.ExecutionStage;
import com.netflix.genie.agent.execution.statemachine.JobExecutionStateMachine;
import com.netflix.genie.agent.execution.statemachine.listeners.ConsoleLogListener;
import com.netflix.genie.agent.execution.statemachine.listeners.LoggingListener;
import com.netflix.genie.agent.execution.statemachine.stages.ArchiveJobOutputsStage;
import com.netflix.genie.agent.execution.statemachine.stages.ClaimJobStage;
import com.netflix.genie.agent.execution.statemachine.stages.CleanupJobDirectoryStage;
import com.netflix.genie.agent.execution.statemachine.stages.ConfigureAgentStage;
import com.netflix.genie.agent.execution.statemachine.stages.ConfigureExecutionStage;
import com.netflix.genie.agent.execution.statemachine.stages.CreateJobDirectoryStage;
import com.netflix.genie.agent.execution.statemachine.stages.CreateJobScriptStage;
import com.netflix.genie.agent.execution.statemachine.stages.DetermineJobFinalStatusStage;
import com.netflix.genie.agent.execution.statemachine.stages.DownloadDependenciesStage;
import com.netflix.genie.agent.execution.statemachine.stages.HandshakeStage;
import com.netflix.genie.agent.execution.statemachine.stages.InitializeAgentStage;
import com.netflix.genie.agent.execution.statemachine.stages.LaunchJobStage;
import com.netflix.genie.agent.execution.statemachine.stages.LogExecutionErrorsStage;
import com.netflix.genie.agent.execution.statemachine.stages.ObtainJobSpecificationStage;
import com.netflix.genie.agent.execution.statemachine.stages.RefreshManifestStage;
import com.netflix.genie.agent.execution.statemachine.stages.RelocateLogFileStage;
import com.netflix.genie.agent.execution.statemachine.stages.ReserveJobIdStage;
import com.netflix.genie.agent.execution.statemachine.stages.SetJobStatusFinal;
import com.netflix.genie.agent.execution.statemachine.stages.SetJobStatusInit;
import com.netflix.genie.agent.execution.statemachine.stages.SetJobStatusRunning;
import com.netflix.genie.agent.execution.statemachine.stages.ShutdownStage;
import com.netflix.genie.agent.execution.statemachine.stages.StartFileServiceStage;
import com.netflix.genie.agent.execution.statemachine.stages.StartHeartbeatServiceStage;
import com.netflix.genie.agent.execution.statemachine.stages.StartKillServiceStage;
import com.netflix.genie.agent.execution.statemachine.stages.StopFileServiceStage;
import com.netflix.genie.agent.execution.statemachine.stages.StopHeartbeatServiceStage;
import com.netflix.genie.agent.execution.statemachine.stages.StopKillServiceStage;
import com.netflix.genie.agent.execution.statemachine.stages.WaitJobCompletionStage;
import com.netflix.genie.agent.properties.AgentProperties;
import com.netflix.genie.common.internal.services.JobArchiveService;
import com.netflix.genie.common.internal.tracing.brave.BraveTagAdapter;
import com.netflix.genie.common.internal.tracing.brave.BraveTracePropagator;
import com.netflix.genie.common.internal.tracing.brave.BraveTracingCleanup;
import com.netflix.genie.common.internal.tracing.brave.BraveTracingComponents;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;
import java.util.Set;

/**
 * Tests ExecutionAutoConfiguration.
 */
class ExecutionAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner =
        new ApplicationContextRunner()
            .withConfiguration(
                AutoConfigurations.of(
                    ExecutionAutoConfiguration.class
                )
            )
            .withUserConfiguration(MocksConfiguration.class);

    private final Set<Class<? extends ExecutionStage>> uniqueExecutionStages = ImmutableSet.of(
        ArchiveJobOutputsStage.class,
        ClaimJobStage.class,
        CleanupJobDirectoryStage.class,
        ConfigureAgentStage.class,
        ConfigureExecutionStage.class,
        CreateJobDirectoryStage.class,
        CreateJobScriptStage.class,
        DetermineJobFinalStatusStage.class,
        DownloadDependenciesStage.class,
        HandshakeStage.class,
        InitializeAgentStage.class,
        LaunchJobStage.class,
        LogExecutionErrorsStage.class,
        ObtainJobSpecificationStage.class,
        RelocateLogFileStage.class,
        ReserveJobIdStage.class,
        SetJobStatusFinal.class,
        SetJobStatusInit.class,
        SetJobStatusRunning.class,
        ShutdownStage.class,
        StartFileServiceStage.class,
        StartHeartbeatServiceStage.class,
        StartKillServiceStage.class,
        StopFileServiceStage.class,
        StopHeartbeatServiceStage.class,
        StopKillServiceStage.class,
        WaitJobCompletionStage.class
    );

    private final Map<Class<? extends ExecutionStage>, Integer> repeatedExecutionStages = ImmutableMap.of(
        RefreshManifestStage.class, 3
    );

    /**
     * Test beans are created successfully.
     */
    @Test
    void executionContext() {
        contextRunner.run(
            context -> {
                Assertions.assertThat(context).hasSingleBean(LoggingListener.class);
                Assertions.assertThat(context).hasSingleBean(ConsoleLogListener.class);
                Assertions.assertThat(context).hasSingleBean(ExecutionContext.class);
                Assertions.assertThat(context).hasSingleBean(JobExecutionStateMachine.class);
                Assertions.assertThat(context).hasSingleBean(AgentProperties.class);
                uniqueExecutionStages.forEach(
                    stageClass ->
                        Assertions.assertThat(context).hasSingleBean(stageClass)
                );
                repeatedExecutionStages.forEach(
                    (clazz, count) ->
                        Assertions.assertThat(context).getBeans(clazz).hasSize(count)
                );
            }
        );
    }

    @Configuration
    static class MocksConfiguration {
        @Bean
        AgentJobService agentJobService() {
            return Mockito.mock(AgentJobService.class);
        }

        @Bean
        AgentMetadata agentMetadata() {
            return Mockito.mock(AgentMetadata.class);
        }

        @Bean
        AgentHeartBeatService heartbeatService() {
            return Mockito.mock(AgentHeartBeatService.class);
        }

        @Bean
        AgentJobKillService killService() {
            return Mockito.mock(AgentJobKillService.class);
        }

        @Bean
        JobSetupService jobSetupService() {
            return Mockito.mock(JobSetupService.class);
        }

        @Bean
        JobRequestConverter jobRequestConverter() {
            return Mockito.mock(JobRequestConverter.class);
        }

        @Bean
        ArgumentDelegates.JobRequestArguments jobRequestArguments() {
            return Mockito.mock(ArgumentDelegates.JobRequestArguments.class);
        }

        @Bean
        AgentFileStreamService agentFileStreamService() {
            return Mockito.mock(AgentFileStreamService.class);
        }

        @Bean
        ArgumentDelegates.RuntimeConfigurationArguments runtimeConfigArguments() {
            return Mockito.mock(ArgumentDelegates.RuntimeConfigurationArguments.class);
        }

        @Bean
        JobProcessManager jobProcessManager() {
            return Mockito.mock(JobProcessManager.class);
        }

        @Bean
        JobArchiveService jobArchiveService() {
            return Mockito.mock(JobArchiveService.class);
        }

        @Bean
        ArgumentDelegates.CleanupArguments cleanupArguments() {
            return Mockito.mock(ArgumentDelegates.CleanupArguments.class);
        }

        @Bean
        AgentLogManager agentLogManager() {
            return Mockito.mock(AgentLogManager.class);
        }

        @Bean
        JobMonitorService jobMonitorService() {
            return Mockito.mock(JobMonitorService.class);
        }

        @Bean
        BraveTracingComponents genieTracingComponents() {
            return new BraveTracingComponents(
                Mockito.mock(Tracer.class),
                Mockito.mock(BraveTracePropagator.class),
                Mockito.mock(BraveTracingCleanup.class),
                Mockito.mock(BraveTagAdapter.class)
            );
        }
    }
}
