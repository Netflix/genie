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
import com.netflix.genie.common.internal.util.PropertiesMapCache;
import com.netflix.genie.web.agent.inspectors.AgentMetadataInspector;
import com.netflix.genie.web.agent.services.AgentConfigurationService;
import com.netflix.genie.web.agent.services.AgentConnectionTrackingService;
import com.netflix.genie.web.agent.services.AgentFilterService;
import com.netflix.genie.web.agent.services.AgentJobService;
import com.netflix.genie.web.agent.services.AgentRoutingService;
import com.netflix.genie.web.agent.services.impl.AgentRoutingServiceCuratorDiscoveryImpl;
import com.netflix.genie.web.data.services.DataServices;
import com.netflix.genie.web.data.services.PersistenceService;
import com.netflix.genie.web.properties.AgentConfigurationProperties;
import com.netflix.genie.web.properties.AgentConnectionTrackingServiceProperties;
import com.netflix.genie.web.properties.AgentRoutingServiceProperties;
import com.netflix.genie.web.services.JobResolverService;
import com.netflix.genie.web.spring.autoconfigure.agent.apis.rpc.v4.endpoints.AgentRpcEndpointsAutoConfiguration;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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
     * Verify beans in case Zookeeper is not enabled.
     */
    @Test
    void expectedBeansExistWithZookeeperDisabled() {
        this.contextRunner
            .run(
                context -> {
                    Assertions.assertThat(context).doesNotHaveBean(ServiceDiscovery.class);
                    Assertions.assertThat(context).hasSingleBean(AgentRoutingService.class);
                    Assertions.assertThat(context).hasSingleBean(AgentJobService.class);
                    Assertions.assertThat(context).hasSingleBean(AgentConnectionTrackingService.class);
                    Assertions.assertThat(context).hasSingleBean(AgentFilterService.class);
                    Assertions.assertThat(context).hasSingleBean(AgentConfigurationProperties.class);
                    Assertions.assertThat(context).hasSingleBean(AgentConfigurationService.class);
                    Assertions.assertThat(context).hasSingleBean(AgentRoutingServiceProperties.class);
                    Assertions.assertThat(context).hasSingleBean(AgentConnectionTrackingServiceProperties.class);
                }
            );
    }

    /**
     * Verify beans in case Zookeeper is enabled.
     */
    @Test
    void expectedBeansExistWithZookeeperEnabled() {
        this.contextRunner
            .withUserConfiguration(ZookeeperMockConfig.class)
            .run(
                context -> {
                    Assertions.assertThat(context).hasSingleBean(ServiceDiscovery.class);
                    Assertions.assertThat(context).hasSingleBean(AgentJobService.class);
                    Assertions.assertThat(context).hasSingleBean(AgentConnectionTrackingService.class);
                    Assertions.assertThat(context).hasSingleBean(AgentRoutingService.class);
                    Assertions.assertThat(context).hasSingleBean(AgentFilterService.class);
                    Assertions.assertThat(context).hasSingleBean(AgentConfigurationProperties.class);
                    Assertions.assertThat(context).hasSingleBean(AgentConfigurationService.class);
                    Assertions.assertThat(context).hasSingleBean(AgentRoutingServiceProperties.class);
                    Assertions.assertThat(context).hasSingleBean(AgentConnectionTrackingServiceProperties.class);
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

        @Bean
        PropertiesMapCache.Factory propertiesMapCacheFactory() {
            return Mockito.mock(PropertiesMapCache.Factory.class);
        }
    }

    /**
     * Mock configuration for pretending zookeeper is enabled.
     */
    @Configuration
    static class ZookeeperMockConfig {

        @Bean
        @SuppressWarnings("unchecked")
        ServiceDiscovery<AgentRoutingServiceCuratorDiscoveryImpl.Agent> serviceDiscovery() {
            return Mockito.mock(ServiceDiscovery.class);
        }

        @Bean
        @SuppressWarnings("unchecked")
        Listenable<ConnectionStateListener> listenableCuratorConnectionState() {
            return Mockito.mock(Listenable.class);
        }
    }
}
