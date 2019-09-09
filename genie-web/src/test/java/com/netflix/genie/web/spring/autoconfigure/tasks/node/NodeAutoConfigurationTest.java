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
package com.netflix.genie.web.spring.autoconfigure.tasks.node;

import com.netflix.genie.web.data.services.JobSearchService;
import com.netflix.genie.web.properties.DiskCleanupProperties;
import com.netflix.genie.web.properties.JobsProperties;
import com.netflix.genie.web.spring.autoconfigure.tasks.TasksAutoConfiguration;
import com.netflix.genie.web.tasks.node.DiskCleanupTask;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.commons.exec.Executor;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * Tests for {@link NodeAutoConfiguration}.
 *
 * @author tgianos
 * @since 4.0.0
 */
public class NodeAutoConfigurationTest {

    private ApplicationContextRunner contextRunner =
        new ApplicationContextRunner()
            .withConfiguration(
                AutoConfigurations.of(
                    NodeAutoConfiguration.class
                )
            )
            .withUserConfiguration(MockBeanConfig.class);

    /**
     * All the expected beans exist.
     */
    @Test
    public void expectedBeansExist() {
        this.contextRunner.run(
            context -> {
                Assertions.assertThat(context).hasSingleBean(DiskCleanupProperties.class);

                // Optional beans
                Assertions.assertThat(context).doesNotHaveBean(DiskCleanupTask.class);
            }
        );
    }

    /**
     * All the expected beans exist.
     */
    @Test
    public void optionalBeansCreated() {
        this.contextRunner
            .withPropertyValues(
                "genie.tasks.disk-cleanup.enabled=true"
            )
            .run(
                context -> {
                    Assertions.assertThat(context).hasSingleBean(DiskCleanupProperties.class);

                    // Optional beans
                    Assertions.assertThat(context).hasSingleBean(DiskCleanupTask.class);
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
        public ThreadPoolTaskScheduler genieTaskScheduler() {
            return Mockito.mock(ThreadPoolTaskScheduler.class);
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
        public Resource jobsDir() {
            final Resource resource = Mockito.mock(Resource.class);
            Mockito.when(resource.exists()).thenReturn(true);
            return resource;
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
        public MeterRegistry meterRegistry() {
            return Mockito.mock(MeterRegistry.class);
        }

        /**
         * Mocked bean.
         *
         * @return Mocked bean instance
         */
        @Bean
        public Executor executor() {
            return Mockito.mock(Executor.class);
        }
    }
}
