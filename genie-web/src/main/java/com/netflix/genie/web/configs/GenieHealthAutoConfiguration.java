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
package com.netflix.genie.web.configs;

import com.netflix.genie.web.health.GenieAgentHealthIndicator;
import com.netflix.genie.web.health.GenieMemoryHealthIndicator;
import com.netflix.genie.web.properties.JobsProperties;
import com.netflix.genie.web.services.AgentMetricsService;
import com.netflix.genie.web.services.JobMetricsService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
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
public class GenieHealthAutoConfiguration {

    /**
     * Provide a health indicator related to job memory usage if one hasn't already been provided elsewhere.
     *
     * @param jobMetricsService Implementation of {@link JobMetricsService} to use
     * @param jobsProperties    The {@link JobsProperties} to use
     * @return An instance of {@link GenieMemoryHealthIndicator}
     */
    @Bean
    @ConditionalOnMissingBean(GenieMemoryHealthIndicator.class)
    public GenieMemoryHealthIndicator genieMemoryHealthIndicator(
        final JobMetricsService jobMetricsService,
        final JobsProperties jobsProperties
    ) {
        return new GenieMemoryHealthIndicator(jobMetricsService, jobsProperties);
    }

    /**
     * Provide a health indicator tied to agent related information if one hasn't already been provided elsewhere.
     *
     * @param agentMetricsService {@link AgentMetricsService} implementation to use
     * @return An instance of {@link GenieAgentHealthIndicator}
     */
    @Bean
    @ConditionalOnMissingBean(GenieAgentHealthIndicator.class)
    public GenieAgentHealthIndicator genieAgentHealthIndicator(final AgentMetricsService agentMetricsService) {
        return new GenieAgentHealthIndicator(agentMetricsService);
    }
}
