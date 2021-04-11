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
package com.netflix.genie.agent.execution.services.impl.grpc;

import com.netflix.genie.agent.cli.ArgumentDelegates;
import com.netflix.genie.agent.execution.services.AgentFileStreamService;
import com.netflix.genie.agent.execution.services.AgentHeartBeatService;
import com.netflix.genie.agent.execution.services.AgentJobKillService;
import com.netflix.genie.agent.execution.services.AgentJobService;
import com.netflix.genie.agent.execution.services.KillService;
import com.netflix.genie.agent.properties.AgentProperties;
import com.netflix.genie.agent.rpc.GRpcAutoConfiguration;
import com.netflix.genie.common.internal.dtos.v4.converters.JobDirectoryManifestProtoConverter;
import com.netflix.genie.common.internal.dtos.v4.converters.JobServiceProtoConverter;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.TaskScheduler;

/**
 * Tests for {@link GRpcAutoConfiguration}.
 */
class GRpcServicesAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                GRpcServicesAutoConfiguration.class,
                GRpcAutoConfiguration.class
            )
        )
        .withUserConfiguration(ExternalBeans.class);

    @Test
    void expectedBeansExist() {
        this.contextRunner.run(
            context -> Assertions
                .assertThat(context)
                .hasSingleBean(AgentProperties.class)
                .hasSingleBean(AgentHeartBeatService.class)
                .hasSingleBean(AgentJobKillService.class)
                .hasSingleBean(AgentJobService.class)
                .hasSingleBean(AgentFileStreamService.class)
        );
    }

    private static class ExternalBeans {
        @Bean
        ArgumentDelegates.ServerArguments serverArguments() {
            final ArgumentDelegates.ServerArguments mock = Mockito.mock(ArgumentDelegates.ServerArguments.class);
            Mockito.when(mock.getServerHost()).thenReturn("server.com");
            Mockito.when(mock.getServerPort()).thenReturn(1234);
            Mockito.when(mock.getRpcTimeout()).thenReturn(3L);
            return mock;
        }

        @Bean
        JobServiceProtoConverter jobServiceProtoConverter() {
            return Mockito.mock(JobServiceProtoConverter.class);
        }

        @Bean
        KillService killService() {
            return Mockito.mock(KillService.class);
        }

        @Bean(name = "sharedAgentTaskExecutor")
        TaskExecutor taskExecutor() {
            return Mockito.mock(TaskExecutor.class);
        }

        @Bean(name = "sharedAgentTaskScheduler")
        TaskScheduler sharedAgentTaskScheduler() {
            return Mockito.mock(TaskScheduler.class);
        }

        @Bean(name = "heartBeatServiceTaskScheduler")
        TaskScheduler heartBeatServiceTaskScheduler() {
            return Mockito.mock(TaskScheduler.class);
        }

        @Bean
        JobDirectoryManifestProtoConverter jobDirectoryManifestProtoConverter() {
            return Mockito.mock(JobDirectoryManifestProtoConverter.class);
        }
    }
}
