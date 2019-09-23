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
import com.netflix.genie.web.agent.services.AgentFilterService;
import com.netflix.genie.web.agent.services.AgentJobService;
import com.netflix.genie.web.agent.services.AgentMetricsService;
import com.netflix.genie.web.agent.services.AgentRoutingService;
import com.netflix.genie.web.agent.services.impl.AgentFilterServiceImpl;
import com.netflix.genie.web.agent.services.impl.AgentJobServiceImpl;
import com.netflix.genie.web.agent.services.impl.AgentMetricsServiceImpl;
import com.netflix.genie.web.agent.services.impl.AgentRoutingServiceImpl;
import com.netflix.genie.web.data.services.AgentConnectionPersistenceService;
import com.netflix.genie.web.data.services.JobPersistenceService;
import com.netflix.genie.web.services.JobResolverService;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * Auto configuration for services needed in the {@literal agent} module.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Configuration
public class AgentServicesAutoConfiguration {
    /**
     * Get a {@link AgentJobService} instance if there isn't already one.
     *
     * @param jobPersistenceService The persistence service to use
     * @param jobResolverService    The specification service to use
     * @param agentFilterService    The agent filter service to use
     * @param meterRegistry         The metrics registry to use
     * @return An {@link AgentJobServiceImpl} instance.
     */
    @Bean
    @ConditionalOnMissingBean(AgentJobService.class)
    public AgentJobServiceImpl agentJobService(
        final JobPersistenceService jobPersistenceService,
        final JobResolverService jobResolverService,
        final AgentFilterService agentFilterService,
        final MeterRegistry meterRegistry
    ) {
        return new AgentJobServiceImpl(
            jobPersistenceService,
            jobResolverService,
            agentFilterService,
            meterRegistry
        );
    }

    /**
     * Get an implementation of {@link AgentRoutingService} if one hasn't already been defined.
     *
     * @param agentConnectionPersistenceService The persistence service to use for agent connections
     * @param genieHostInfo                     The local genie host information
     * @return A {@link AgentRoutingServiceImpl} instance
     */
    @Bean
    @ConditionalOnMissingBean(AgentRoutingService.class)
    public AgentRoutingServiceImpl agentRoutingService(
        final AgentConnectionPersistenceService agentConnectionPersistenceService,
        final GenieHostInfo genieHostInfo
    ) {
        return new AgentRoutingServiceImpl(
            agentConnectionPersistenceService,
            genieHostInfo
        );
    }

    /**
     * A {@link AgentFilterService} implementation that federates the decision to a set of
     * {@link AgentMetadataInspector}s.
     *
     * @param agentMetadataInspectorsList the list of inspectors.
     * @return An {@link AgentFilterService} instance.
     */
    @Bean
    @ConditionalOnMissingBean(AgentFilterService.class)
    public AgentFilterServiceImpl agentFilterService(
        final List<AgentMetadataInspector> agentMetadataInspectorsList
    ) {
        return new AgentFilterServiceImpl(agentMetadataInspectorsList);
    }

    /**
     * Provide an implementation of {@link AgentMetricsService} if one hasn't been provided.
     *
     * @param genieHostInfo                     The Genie host information
     * @param agentConnectionPersistenceService Implementation of {@link AgentConnectionPersistenceService} to get
     *                                          information about running agents in the ecosystem
     * @param registry                          The metrics repository
     * @return An instance of {@link AgentMetricsServiceImpl}
     */
    @Bean
    @ConditionalOnMissingBean(AgentMetricsService.class)
    public AgentMetricsServiceImpl agentMetricsService(
        final GenieHostInfo genieHostInfo,
        final AgentConnectionPersistenceService agentConnectionPersistenceService,
        final MeterRegistry registry
    ) {
        return new AgentMetricsServiceImpl(genieHostInfo, agentConnectionPersistenceService, registry);
    }
}
