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
import com.netflix.genie.web.properties.HealthProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Auto configuration for Health indicators related to Genie.
 *
 * @author tgianos
 * @see com.netflix.genie.web.health
 * @since 4.0.0
 */
@Configuration
@EnableConfigurationProperties(
    {
        HealthProperties.class
    }
)
public class HealthAutoConfiguration {

    /**
     * Provide a health indicator tied to agent related information if one hasn't already been provided elsewhere.
     *
     * @param agentConnectionTrackingService the agent connection tracking service
     * @return An instance of {@link GenieAgentHealthIndicator}
     */
    @Bean
    @ConditionalOnMissingBean(GenieAgentHealthIndicator.class)
    public GenieAgentHealthIndicator genieAgentHealthIndicator(
        final AgentConnectionTrackingService agentConnectionTrackingService
    ) {
        return new GenieAgentHealthIndicator(agentConnectionTrackingService);
    }
}
