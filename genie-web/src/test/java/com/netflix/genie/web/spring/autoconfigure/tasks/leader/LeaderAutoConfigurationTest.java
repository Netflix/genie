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
import com.netflix.genie.web.data.services.ClusterPersistenceService;
import com.netflix.genie.web.data.services.FilePersistenceService;
import com.netflix.genie.web.data.services.JobPersistenceService;
import com.netflix.genie.web.data.services.JobSearchService;
import com.netflix.genie.web.data.services.TagPersistenceService;
import com.netflix.genie.web.events.GenieEventBus;
import com.netflix.genie.web.properties.AgentCleanupProperties;
import com.netflix.genie.web.properties.ClusterCheckerProperties;
import com.netflix.genie.web.properties.DatabaseCleanupProperties;
import com.netflix.genie.web.properties.JobsProperties;
import com.netflix.genie.web.properties.LeadershipProperties;
import com.netflix.genie.web.properties.UserMetricsProperties;
import com.netflix.genie.web.properties.ZookeeperLeadershipProperties;
import com.netflix.genie.web.spring.autoconfigure.tasks.TasksAutoConfiguration;
import com.netflix.genie.web.tasks.leader.AgentJobCleanupTask;
import com.netflix.genie.web.tasks.leader.ClusterCheckerTask;
import com.netflix.genie.web.tasks.leader.DatabaseCleanupTask;
import com.netflix.genie.web.tasks.leader.LeadershipTasksCoordinator;
import com.netflix.genie.web.tasks.leader.LocalLeader;
import com.netflix.genie.web.tasks.leader.UserMetricsTask;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.Listenable;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.boot.actuate.autoconfigure.endpoint.web.WebEndpointProperties;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.zookeeper.config.LeaderInitiatorFactoryBean;
import org.springframework.web.client.RestTemplate;

/**
 * Unit tests for the {@link LeaderAutoConfiguration} class.
 *
 * @author tgianos
 * @since 3.1.0
 */
public class LeaderAutoConfigurationTest {

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
    public void expectedBeansExist() {
        this.contextRunner.run(
            context -> {
                Assertions.assertThat(context).hasSingleBean(AgentCleanupProperties.class);
                Assertions.assertThat(context).hasSingleBean(ClusterCheckerProperties.class);
                Assertions.assertThat(context).hasSingleBean(DatabaseCleanupProperties.class);
                Assertions.assertThat(context).hasSingleBean(LeadershipProperties.class);
                Assertions.assertThat(context).hasSingleBean(UserMetricsProperties.class);
                Assertions.assertThat(context).hasSingleBean(ZookeeperLeadershipProperties.class);

                Assertions.assertThat(context).hasSingleBean(LeadershipTasksCoordinator.class);
                Assertions.assertThat(context).doesNotHaveBean(LeaderInitiatorFactoryBean.class);
                Assertions.assertThat(context).hasSingleBean(LocalLeader.class);
                Assertions.assertThat(context).hasSingleBean(ClusterCheckerTask.class);

                // Optional beans
                Assertions.assertThat(context).doesNotHaveBean(DatabaseCleanupTask.class);
                Assertions.assertThat(context).doesNotHaveBean(UserMetricsTask.class);
                Assertions.assertThat(context).doesNotHaveBean(AgentJobCleanupTask.class);
            }
        );
    }

    /**
     * All the expected optional beans exist.
     */
    @Test
    public void optionalBeansCreated() {
        this.contextRunner
            .withPropertyValues(
                "genie.tasks.database-cleanup.enabled=true",
                "genie.tasks.user-metrics.enabled=true",
                "genie.tasks.agent-cleanup.enabled=true"
            )
            .run(
                context -> {
                    Assertions.assertThat(context).hasSingleBean(AgentCleanupProperties.class);
                    Assertions.assertThat(context).hasSingleBean(ClusterCheckerProperties.class);
                    Assertions.assertThat(context).hasSingleBean(DatabaseCleanupProperties.class);
                    Assertions.assertThat(context).hasSingleBean(LeadershipProperties.class);
                    Assertions.assertThat(context).hasSingleBean(UserMetricsProperties.class);
                    Assertions.assertThat(context).hasSingleBean(ZookeeperLeadershipProperties.class);

                    Assertions.assertThat(context).hasSingleBean(LeadershipTasksCoordinator.class);
                    Assertions.assertThat(context).doesNotHaveBean(LeaderInitiatorFactoryBean.class);
                    Assertions.assertThat(context).hasSingleBean(LocalLeader.class);
                    Assertions.assertThat(context).hasSingleBean(ClusterCheckerTask.class);

                    // Optional beans
                    Assertions.assertThat(context).hasSingleBean(DatabaseCleanupTask.class);
                    Assertions.assertThat(context).hasSingleBean(UserMetricsTask.class);
                    Assertions.assertThat(context).hasSingleBean(AgentJobCleanupTask.class);
                }
            );
    }

    /**
     * All the expected beans exist when zookeeper is enabled.
     */
    @Test
    public void expectedZookeeperBeansExist() {
        this.contextRunner
            .withUserConfiguration(ZookeeperMockConfig.class)
            .run(
                context -> {
                    Assertions.assertThat(context).hasSingleBean(AgentCleanupProperties.class);
                    Assertions.assertThat(context).hasSingleBean(ClusterCheckerProperties.class);
                    Assertions.assertThat(context).hasSingleBean(DatabaseCleanupProperties.class);
                    Assertions.assertThat(context).hasSingleBean(LeadershipProperties.class);
                    Assertions.assertThat(context).hasSingleBean(UserMetricsProperties.class);
                    Assertions.assertThat(context).hasSingleBean(ZookeeperLeadershipProperties.class);

                    Assertions.assertThat(context).hasSingleBean(LeadershipTasksCoordinator.class);
                    Assertions.assertThat(context).hasSingleBean(LeaderInitiatorFactoryBean.class);
                    Assertions.assertThat(context).doesNotHaveBean(LocalLeader.class);
                    Assertions.assertThat(context).hasSingleBean(ClusterCheckerTask.class);

                    // Optional beans
                    Assertions.assertThat(context).doesNotHaveBean(DatabaseCleanupTask.class);
                    Assertions.assertThat(context).doesNotHaveBean(UserMetricsTask.class);
                    Assertions.assertThat(context).doesNotHaveBean(AgentJobCleanupTask.class);
                }
            );
    }

    /**
     * Configuration for beans that are dependencies of the auto configured beans in {@link TasksAutoConfiguration}.
     *
     * @author tgianos
     * @since 4.0.0
     */
    static class MockBeanConfig {

        /**
         * Mocked bean.
         *
         * @return Mocked bean instance
         */
        @Bean
        public GenieHostInfo genieHostInfo() {
            return Mockito.mock(GenieHostInfo.class);
        }

        /**
         * Mocked bean.
         *
         * @return Mocked bean instance
         */
        @Bean
        public JobSearchService jobSearchService() {
            return Mockito.mock(JobSearchService.class);
        }

        /**
         * Mocked bean.
         *
         * @return Mocked bean instance
         */
        @Bean
        public JobPersistenceService jobPersistenceService() {
            return Mockito.mock(JobPersistenceService.class);
        }

        /**
         * Mocked bean.
         *
         * @return Mocked bean instance
         */
        @Bean
        public ClusterPersistenceService clusterPersistenceService() {
            return Mockito.mock(ClusterPersistenceService.class);
        }

        /**
         * Mocked bean.
         *
         * @return Mocked bean instance
         */
        @Bean
        public FilePersistenceService filePersistenceService() {
            return Mockito.mock(FilePersistenceService.class);
        }

        /**
         * Mocked bean.
         *
         * @return Mocked bean instance
         */
        @Bean
        public JobsProperties jobsProperties() {
            return JobsProperties.getJobsPropertiesDefaults();
        }

        /**
         * Mocked bean.
         *
         * @return Mocked bean instance
         */
        @Bean
        public TagPersistenceService tagPersistenceService() {
            return Mockito.mock(TagPersistenceService.class);
        }

        /**
         * Mocked bean.
         *
         * @return Mocked bean instance
         */
        @Bean
        public RestTemplate genieRestTemplate() {
            return Mockito.mock(RestTemplate.class);
        }

        /**
         * Mocked bean.
         *
         * @return Mocked bean instance
         */
        @Bean
        public WebEndpointProperties webEndpointProperties() {
            return Mockito.mock(WebEndpointProperties.class);
        }

        /**
         * Mocked bean.
         *
         * @return Mocked bean instance
         */
        @Bean
        public MeterRegistry meterRegistry() {
            return Mockito.mock(MeterRegistry.class);
        }

        /**
         * Mocked bean.
         *
         * @return Mocked bean instance
         */
        @Bean
        public GenieEventBus genieEventBus() {
            return Mockito.mock(GenieEventBus.class);
        }
    }

    /**
     * Mock configuration for pretending zookeeper is enabled.
     */
    @Configuration
    static class ZookeeperMockConfig {

        /**
         * Mocked bean.
         *
         * @return Mocked bean instance.
         */
        @Bean
        @SuppressWarnings("unchecked")
        public CuratorFramework curatorFramework() {
            final CuratorFramework curatorFramework = Mockito.mock(CuratorFramework.class);
            Mockito
                .when(curatorFramework.getConnectionStateListenable())
                .thenReturn(Mockito.mock(Listenable.class));
            return curatorFramework;
        }
    }
}
