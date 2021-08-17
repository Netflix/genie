/*
 *
 *  Copyright 2021 Netflix, Inc.
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
package com.netflix.genie.agent.cli;

import brave.Tracer;
import com.netflix.genie.agent.AgentMetadata;
import com.netflix.genie.agent.cli.logging.AgentLogManager;
import com.netflix.genie.agent.execution.services.AgentHeartBeatService;
import com.netflix.genie.agent.execution.services.AgentJobService;
import com.netflix.genie.agent.execution.services.DownloadService;
import com.netflix.genie.agent.execution.services.KillService;
import com.netflix.genie.agent.execution.statemachine.JobExecutionStateMachineImpl;
import com.netflix.genie.agent.properties.AgentProperties;
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

import javax.xml.validation.Validator;

/**
 * Tests for {@link CliAutoConfiguration}.
 *
 * @author tgianos
 * @since 4.0.0
 */
class CliAutoConfigurationTest {
    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                CliAutoConfiguration.class
            )
        )
        .withUserConfiguration(ExternalBeans.class);

    @Test
    void expectedBeansExist() {
        this.contextRunner.run(
            context -> {
                Assertions.assertThat(context).hasSingleBean(AgentProperties.class);
                Assertions.assertThat(context).hasSingleBean(ArgumentDelegates.CacheArguments.class);
                Assertions.assertThat(context).hasSingleBean(DownloadCommand.DownloadCommandArguments.class);
                Assertions.assertThat(context).hasSingleBean(ExecCommand.ExecCommandArguments.class);
                Assertions.assertThat(context).hasSingleBean(ExecCommand.class);
                Assertions.assertThat(context).hasSingleBean(GenieAgentRunner.class);
                Assertions.assertThat(context).hasSingleBean(HeartBeatCommand.HeartBeatCommandArguments.class);
                Assertions.assertThat(context).hasSingleBean(HeartBeatCommand.class);
                Assertions.assertThat(context).hasSingleBean(HelpCommand.HelpCommandArguments.class);
                Assertions.assertThat(context).hasSingleBean(HelpCommand.class);
                Assertions.assertThat(context).hasSingleBean(InfoCommand.InfoCommandArguments.class);
                Assertions.assertThat(context).hasSingleBean(InfoCommand.class);
                Assertions.assertThat(context).hasSingleBean(MainCommandArguments.class);
                Assertions.assertThat(context).hasSingleBean(ArgumentDelegates.JobRequestArguments.class);
                Assertions.assertThat(context).hasSingleBean(JobRequestConverter.class);
                Assertions.assertThat(context).hasSingleBean(PingCommand.PingCommandArguments.class);
                Assertions.assertThat(context).hasSingleBean(PingCommand.class);
                Assertions
                    .assertThat(context)
                    .hasSingleBean(ResolveJobSpecCommand.ResolveJobSpecCommandArguments.class);
                Assertions.assertThat(context).hasSingleBean(ResolveJobSpecCommand.class);
                Assertions.assertThat(context).hasSingleBean(ArgumentDelegates.ServerArguments.class);
                Assertions.assertThat(context).hasSingleBean(ArgumentDelegates.CleanupArguments.class);
                Assertions.assertThat(context).hasSingleBean(ArgumentDelegates.RuntimeConfigurationArguments.class);
                Assertions.assertThat(context).hasSingleBean(AgentLogManager.class);
            }
        );
    }

    private static final class ExternalBeans {

        @Bean
        DownloadService downloadService() {
            return Mockito.mock(DownloadService.class);
        }

        @Bean
        KillService killService() {
            return Mockito.mock(KillService.class);
        }

        @Bean
        JobExecutionStateMachineImpl jobExecutionStateMachine() {
            return Mockito.mock(JobExecutionStateMachineImpl.class);
        }

        @Bean
        ArgumentParser argumentParser() {
            return Mockito.mock(ArgumentParser.class);
        }

        @Bean
        CommandFactory commandFactory() {
            return Mockito.mock(CommandFactory.class);
        }

        @Bean
        AgentHeartBeatService agentHeartBeatService() {
            return Mockito.mock(AgentHeartBeatService.class);
        }

        @Bean
        AgentMetadata agentMetadata() {
            return Mockito.mock(AgentMetadata.class);
        }

        @Bean
        Validator validator() {
            return Mockito.mock(Validator.class);
        }

        @Bean
        AgentJobService agentJobService() {
            return Mockito.mock(AgentJobService.class);
        }

        @Bean
        AgentLogManager agentLogManager() {
            return Mockito.mock(AgentLogManager.class);
        }

        @Bean
        BraveTracingComponents braveTracingComponents() {
            return new BraveTracingComponents(
                Mockito.mock(Tracer.class),
                Mockito.mock(BraveTracePropagator.class),
                Mockito.mock(BraveTracingCleanup.class),
                Mockito.mock(BraveTagAdapter.class)
            );
        }
    }
}
