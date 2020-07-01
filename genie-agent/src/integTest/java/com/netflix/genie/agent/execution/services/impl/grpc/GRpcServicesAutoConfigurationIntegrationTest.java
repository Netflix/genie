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
import com.netflix.genie.agent.rpc.GRpcAutoConfiguration;
import com.netflix.genie.common.internal.configs.CommonServicesAutoConfiguration;
import com.netflix.genie.common.internal.dtos.v4.converters.JobDirectoryManifestProtoConverter;
import com.netflix.genie.common.internal.dtos.v4.converters.JobServiceProtoConverter;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Test for {@link GRpcAutoConfiguration}.
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration(
    classes = {
        CommonServicesAutoConfiguration.class,
        GRpcAutoConfiguration.class,
        GRpcServicesAutoConfiguration.class,
        GRpcServicesAutoConfigurationIntegrationTest.MockConfig.class
    }
)
// TODO: Perhaps this should be using the context runner?
// TODO: Use mockbean instead of the whole configuration?
class GRpcServicesAutoConfigurationIntegrationTest {

    @Autowired
    private ApplicationContext applicationContext;

    @Test
    void getServicesBeans() {
        final Class<?>[] serviceClasses = {
            AgentHeartBeatService.class,
            AgentJobKillService.class,
            AgentJobService.class,
            AgentFileStreamService.class,
        };

        for (final Class<?> serviceClass : serviceClasses) {
            Assertions.assertThat(this.applicationContext.getBean(serviceClass)).isNotNull();
        }
    }

    static class MockConfig {
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
