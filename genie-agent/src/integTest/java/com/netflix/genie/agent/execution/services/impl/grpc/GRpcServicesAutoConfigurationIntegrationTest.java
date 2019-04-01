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
import com.netflix.genie.agent.execution.services.AgentHeartBeatService;
import com.netflix.genie.agent.execution.services.AgentJobKillService;
import com.netflix.genie.agent.execution.services.AgentJobService;
import com.netflix.genie.agent.execution.services.KillService;
import com.netflix.genie.agent.rpc.GRpcAutoConfiguration;
import com.netflix.genie.common.internal.dto.v4.converters.JobServiceProtoConverter;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Test for {@link GRpcAutoConfiguration}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
    classes = {
        GRpcAutoConfiguration.class,
        GRpcServicesAutoConfiguration.class,
        GRpcServicesAutoConfigurationIntegrationTest.MockConfig.class
    }
)
// TODO: Perhaps this should be using the context runner?
// TODO: Use mockbean instead of the whole configuration?
public class GRpcServicesAutoConfigurationIntegrationTest {

    @Autowired
    private ApplicationContext applicationContext;

    /**
     * Create services.
     */
    @Test
    public void getServicesBeans() {

        final Class<?>[] serviceClasses = {
            AgentHeartBeatService.class,
            AgentJobKillService.class,
            AgentJobService.class,
        };

        for (final Class<?> serviceClass : serviceClasses) {
            Assert.assertNotNull(applicationContext.getBean(serviceClass));
        }
    }

    @Configuration
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

        @Bean
        @Qualifier("sharedAgentTaskExecutor")
        TaskExecutor taskExecutor() {
            return Mockito.mock(TaskExecutor.class);
        }

        @Bean
        @Qualifier("heartBeatServiceTaskExecutor")
        TaskScheduler taskScheduler() {
            return Mockito.mock(TaskScheduler.class);
        }
    }
}
