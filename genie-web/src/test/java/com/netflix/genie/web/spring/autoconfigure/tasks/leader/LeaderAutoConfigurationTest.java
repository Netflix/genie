/*
 *
 *  Copyright 2017 Netflix, Inc.
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
package com.netflix.genie.web.spring.autoconfigure.tasks.leader;

import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.web.agent.services.AgentRoutingService;
import com.netflix.genie.web.data.services.DataServices;
import com.netflix.genie.web.data.services.PersistenceService;
import com.netflix.genie.web.events.GenieEventBus;
import com.netflix.genie.web.properties.AgentCleanupProperties;
import com.netflix.genie.web.properties.ArchiveStatusCleanupProperties;
import com.netflix.genie.web.properties.DatabaseCleanupProperties;
import com.netflix.genie.web.properties.JobsProperties;
import com.netflix.genie.web.properties.LeadershipProperties;
import com.netflix.genie.web.properties.UserMetricsProperties;
import com.netflix.genie.web.services.ClusterLeaderService;
import com.netflix.genie.web.spring.actuators.LeaderElectionActuator;
import com.netflix.genie.web.spring.autoconfigure.tasks.TasksAutoConfiguration;
import com.netflix.genie.web.tasks.leader.AgentJobCleanupTask;
import com.netflix.genie.web.tasks.leader.ArchiveStatusCleanupTask;
import com.netflix.genie.web.tasks.leader.DatabaseCleanupTask;
import com.netflix.genie.web.tasks.leader.LeaderTasksCoordinator;
import com.netflix.genie.web.tasks.leader.LocalLeader;
import com.netflix.genie.web.tasks.leader.UserMetricsTask;
import io.micrometer.core.instrument.MeterRegistry;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.actuate.autoconfigure.endpoint.web.WebEndpointProperties;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.zookeeper.leader.LeaderInitiator;
import org.springframework.web.client.RestTemplate;

/**
 * Unit tests for the {@link LeaderAutoConfiguration} class.
 *
 * @author tgianos
 * @since 3.1.0
 */
class LeaderAutoConfigurationTest {

    private ApplicationContextRunner contextRunner =
        new ApplicationContextRunner()
            .withConfiguration(
                AutoConfigurations.of(
                    TasksAutoConfiguration.class,
                    LeaderAutoConfiguration.class
                )
            )
            .withUserConfiguration(MockBeanConfig.class);

    /**
     * All the expected default beans exist.
     */
    @Test
    void expectedBeansExist() {
        this.contextRunner.run(
            context -> {
                Assertions.assertThat(context).hasSingleBean(AgentCleanupProperties.class);
                Assertions.assertThat(context).hasSingleBean(ArchiveStatusCleanupProperties.class);
                Assertions.assertThat(context).hasSingleBean(DatabaseCleanupProperties.class);
                Assertions.assertThat(context).hasSingleBean(LeadershipProperties.class);
                Assertions.assertThat(context).hasSingleBean(UserMetricsProperties.class);

                Assertions.assertThat(context).hasSingleBean(LeaderTasksCoordinator.class);
                Assertions.assertThat(context).hasSingleBean(LocalLeader.class);
                Assertions.assertThat(context).hasSingleBean(ClusterLeaderService.class);
                Assertions.assertThat(context).hasSingleBean(LeaderElectionActuator.class);

                // Optional beans
                Assertions.assertThat(context).doesNotHaveBean(DatabaseCleanupTask.class);
                Assertions.assertThat(context).doesNotHaveBean(UserMetricsTask.class);
                Assertions.assertThat(context).doesNotHaveBean(AgentJobCleanupTask.class);
                Assertions.assertThat(context).doesNotHaveBean(ArchiveStatusCleanupTask.class);
            }
        );
    }

    /**
     * All the expected optional beans exist.
     */
    @Test
    void optionalBeansCreated() {
        this.contextRunner
            .withPropertyValues(
                "genie.tasks.database-cleanup.enabled=true",
                "genie.tasks.user-metrics.enabled=true",
                "genie.tasks.agent-cleanup.enabled=true",
                "genie.tasks.archive-status-cleanup.enabled=true"
            )
            .run(
                context -> {
                    Assertions.assertThat(context).hasSingleBean(AgentCleanupProperties.class);
                    Assertions.assertThat(context).hasSingleBean(ArchiveStatusCleanupProperties.class);
                    Assertions.assertThat(context).hasSingleBean(DatabaseCleanupProperties.class);
                    Assertions.assertThat(context).hasSingleBean(LeadershipProperties.class);
                    Assertions.assertThat(context).hasSingleBean(UserMetricsProperties.class);

                    Assertions.assertThat(context).hasSingleBean(LeaderTasksCoordinator.class);
                    Assertions.assertThat(context).hasSingleBean(LocalLeader.class);

                    Assertions.assertThat(context).hasSingleBean(ClusterLeaderService.class);
                    Assertions.assertThat(context).hasSingleBean(LeaderElectionActuator.class);

                    // Optional beans
                    Assertions.assertThat(context).hasSingleBean(DatabaseCleanupTask.class);
                    Assertions.assertThat(context).hasSingleBean(UserMetricsTask.class);
                    Assertions.assertThat(context).hasSingleBean(AgentJobCleanupTask.class);
                    Assertions.assertThat(context).hasSingleBean(ArchiveStatusCleanupTask.class);
                }
            );
    }

    /**
     * All the expected beans exist when zookeeper is enabled.
     */
    @Test
    void expectedZookeeperBeansExist() {
        this.contextRunner
            .withUserConfiguration(ZookeeperMockConfig.class)
            .run(
                context -> {
                    Assertions.assertThat(context).hasSingleBean(AgentCleanupProperties.class);
                    Assertions.assertThat(context).hasSingleBean(DatabaseCleanupProperties.class);
                    Assertions.assertThat(context).hasSingleBean(LeadershipProperties.class);
                    Assertions.assertThat(context).hasSingleBean(UserMetricsProperties.class);

                    Assertions.assertThat(context).hasSingleBean(LeaderTasksCoordinator.class);
                    Assertions.assertThat(context).doesNotHaveBean(LocalLeader.class);

                    Assertions.assertThat(context).hasSingleBean(ClusterLeaderService.class);
                    Assertions.assertThat(context).hasSingleBean(LeaderElectionActuator.class);

                    // Optional beans
                    Assertions.assertThat(context).doesNotHaveBean(DatabaseCleanupTask.class);
                    Assertions.assertThat(context).doesNotHaveBean(UserMetricsTask.class);
                    Assertions.assertThat(context).doesNotHaveBean(AgentJobCleanupTask.class);
                }
            );
    }

    static class MockBeanConfig {

        @Bean
        GenieHostInfo genieHostInfo() {
            return Mockito.mock(GenieHostInfo.class);
        }

        @Bean
        JobsProperties jobsProperties() {
            return JobsProperties.getJobsPropertiesDefaults();
        }

        @Bean
        RestTemplate genieRestTemplate() {
            return Mockito.mock(RestTemplate.class);
        }

        @Bean
        WebEndpointProperties webEndpointProperties() {
            return Mockito.mock(WebEndpointProperties.class);
        }

        @Bean
        MeterRegistry meterRegistry() {
            return Mockito.mock(MeterRegistry.class);
        }

        @Bean
        GenieEventBus genieEventBus() {
            return Mockito.mock(GenieEventBus.class);
        }

        @Bean
        DataServices genieDataServices(final PersistenceService persistenceService) {
            return new DataServices(persistenceService);
        }

        @Bean
        PersistenceService geniePersistenceService() {
            return Mockito.mock(PersistenceService.class);
        }

        @Bean
        AgentRoutingService agentRoutingService() {
            return Mockito.mock(AgentRoutingService.class);
        }
    }

    /**
     * Mock configuration for pretending zookeeper is enabled.
     */
    @Configuration
    static class ZookeeperMockConfig {

        @Bean
        LeaderInitiator leaderInitiatorFactoryBean() {
            return Mockito.mock(LeaderInitiator.class);
        }
    }
}
