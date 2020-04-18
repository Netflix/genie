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
package com.netflix.genie.web.spring.autoconfigure.agent.services;

import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.web.agent.inspectors.AgentMetadataInspector;
import com.netflix.genie.web.agent.services.AgentConnectionTrackingService;
import com.netflix.genie.web.agent.services.AgentFilterService;
import com.netflix.genie.web.agent.services.AgentJobService;
import com.netflix.genie.web.agent.services.AgentMetricsService;
import com.netflix.genie.web.agent.services.AgentRoutingService;
import com.netflix.genie.web.data.services.DataServices;
import com.netflix.genie.web.data.services.PersistenceService;
import com.netflix.genie.web.services.JobResolverService;
import com.netflix.genie.web.spring.autoconfigure.agent.apis.rpc.v4.endpoints.AgentRpcEndpointsAutoConfiguration;
import io.micrometer.core.instrument.MeterRegistry;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.TaskScheduler;

/**
 * Tests for {@link AgentRpcEndpointsAutoConfiguration}.
 *
 * @author mprimi
 * @since 4.0.0
 */
class AgentServicesAutoConfigurationTest {

    private ApplicationContextRunner contextRunner =
        new ApplicationContextRunner()
            .withConfiguration(
                AutoConfigurations.of(
                    AgentServicesAutoConfiguration.class
                )
            )
            .withUserConfiguration(RequiredBeans.class);

    /**
     * Default beans created.
     */
    @Test
    void expectedBeansExist() {
        this.contextRunner
            .run(
                context -> {
                    Assertions.assertThat(context).hasSingleBean(AgentJobService.class);
                    Assertions.assertThat(context).hasSingleBean(AgentConnectionTrackingService.class);
                    Assertions.assertThat(context).hasSingleBean(AgentRoutingService.class);
                    Assertions.assertThat(context).hasSingleBean(AgentFilterService.class);
                    Assertions.assertThat(context).hasSingleBean(AgentMetricsService.class);
                }
            );
    }

    static class RequiredBeans {
        @Bean
        DataServices dataServices() {
            return Mockito.mock(DataServices.class);
        }

        @Bean
        JobResolverService jobResolverService() {
            return Mockito.mock(JobResolverService.class);
        }

        @Bean
        MeterRegistry meterRegistry() {
            return Mockito.mock(MeterRegistry.class);
        }

        @Bean(name = "genieTaskScheduler")
        TaskScheduler taskScheduler() {
            return Mockito.mock(TaskScheduler.class);
        }

        @Bean
        GenieHostInfo genieHostInfo() {
            return Mockito.mock(GenieHostInfo.class);
        }

        @Bean
        AgentMetadataInspector agentMetadataInspector() {
            return Mockito.mock(AgentMetadataInspector.class);
        }

        @Bean
        PersistenceService geniePersistenceService() {
            return Mockito.mock(PersistenceService.class);
        }
    }
}
