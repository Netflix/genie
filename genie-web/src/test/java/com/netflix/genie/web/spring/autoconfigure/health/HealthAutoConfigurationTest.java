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
package com.netflix.genie.web.spring.autoconfigure.health;

import com.netflix.genie.web.agent.services.AgentConnectionTrackingService;
import com.netflix.genie.web.health.GenieAgentHealthIndicator;
import com.netflix.genie.web.properties.JobsProperties;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;

/**
 * Unit tests for {@link HealthAutoConfiguration}.
 *
 * @author tgianos
 * @since 4.0.0
 */
class HealthAutoConfigurationTest {

    private ApplicationContextRunner contextRunner =
        new ApplicationContextRunner()
            .withConfiguration(
                AutoConfigurations.of(
                    HealthAutoConfiguration.class
                )
            )
            .withUserConfiguration(UserConfiguration.class);

    /**
     * Make sure expected beans are provided for health indicators.
     */
    @Test
    void healthBeansCreatedIfNoOthersExist() {
        this.contextRunner.run(
            context -> {
                Assertions.assertThat(context).hasSingleBean(GenieAgentHealthIndicator.class);
            }
        );
    }

    /**
     * Provide some junk beans if needed.
     */
    static class UserConfiguration {

        @Bean
        JobsProperties jobsProperties() {
            return Mockito.mock(JobsProperties.class);
        }

        @Bean
        AgentConnectionTrackingService agentConnectionTrackingService() {
            return Mockito.mock(AgentConnectionTrackingService.class);
        }
    }
}
