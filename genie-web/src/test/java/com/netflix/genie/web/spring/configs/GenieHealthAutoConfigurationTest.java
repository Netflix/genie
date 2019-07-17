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
package com.netflix.genie.web.spring.configs;

import com.netflix.genie.web.health.GenieAgentHealthIndicator;
import com.netflix.genie.web.health.GenieMemoryHealthIndicator;
import com.netflix.genie.web.properties.JobsProperties;
import com.netflix.genie.web.agent.services.AgentMetricsService;
import com.netflix.genie.web.services.JobMetricsService;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Unit tests for {@link GenieHealthAutoConfiguration}.
 *
 * @author tgianos
 * @since 4.0.0
 */
public class GenieHealthAutoConfigurationTest {

    private ApplicationContextRunner contextRunner =
        new ApplicationContextRunner()
            .withConfiguration(
                AutoConfigurations.of(
                    GenieHealthAutoConfiguration.class
                )
            )
            .withUserConfiguration(UserConfiguration.class);

    /**
     * Make sure expected beans are provided for health indicators.
     */
    @Test
    public void bothHealthBeansCreatedIfNoOthersExist() {
        this.contextRunner.run(
            context -> {
                Assertions.assertThat(context).hasSingleBean(GenieMemoryHealthIndicator.class);
                Assertions.assertThat(context).hasSingleBean(GenieAgentHealthIndicator.class);
            }
        );
    }

    /**
     * Provide some junk beans if needed.
     */
    @Configuration
    static class UserConfiguration {
        @Bean
        public JobMetricsService jobMetricsService() {
            return new JobMetricsService() {
                @Override
                public int getNumActiveJobs() {
                    return 0;
                }

                @Override
                public int getUsedMemory() {
                    return 0;
                }
            };
        }

        @Bean
        public JobsProperties jobsProperties() {
            return Mockito.mock(JobsProperties.class);
        }

        @Bean
        public AgentMetricsService agentMetricsService() {
            return new AgentMetricsService() {
            };
        }
    }
}
