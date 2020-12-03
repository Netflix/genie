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
import com.netflix.genie.web.agent.services.impl.AgentConfigurationServiceImpl;
import com.netflix.genie.web.agent.services.impl.AgentConnectionTrackingServiceImpl;
import com.netflix.genie.web.agent.services.impl.AgentFilterServiceImpl;
import com.netflix.genie.web.agent.services.impl.AgentJobServiceImpl;
import com.netflix.genie.web.agent.services.impl.AgentRoutingServiceCuratorDiscoveryImpl;
import com.netflix.genie.web.agent.services.impl.AgentRoutingServiceSingleNodeImpl;
import com.netflix.genie.web.data.services.DataServices;
import com.netflix.genie.web.properties.AgentConfigurationProperties;
import com.netflix.genie.web.properties.AgentConnectionTrackingServiceProperties;
import com.netflix.genie.web.properties.AgentRoutingServiceProperties;
import com.netflix.genie.web.services.JobResolverService;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;

import java.util.List;

/**
 * Auto configuration for services needed in the {@literal agent} module.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Configuration
@EnableConfigurationProperties(
    {
        AgentConfigurationProperties.class,
        AgentRoutingServiceProperties.class,
        AgentConnectionTrackingServiceProperties.class
    }
)
public class AgentServicesAutoConfiguration {
    /**
     * Get a {@link AgentJobService} instance if there isn't already one.
     *
     * @param dataServices              The {@link DataServices} instance to use
     * @param jobResolverService        The specification service to use
     * @param agentFilterService        The agent filter service to use
     * @param agentConfigurationService The agent configuration service
     * @param meterRegistry             The metrics registry to use
     * @return An {@link AgentJobServiceImpl} instance.
     */
    @Bean
    @ConditionalOnMissingBean(AgentJobService.class)
    public AgentJobServiceImpl agentJobService(
        final DataServices dataServices,
        final JobResolverService jobResolverService,
        final AgentFilterService agentFilterService,
        final AgentConfigurationService agentConfigurationService,
        final MeterRegistry meterRegistry
    ) {
        return new AgentJobServiceImpl(
            dataServices,
            jobResolverService,
            agentFilterService,
            agentConfigurationService,
            meterRegistry
        );
    }

    /**
     * Get an implementation of {@link AgentConnectionTrackingService} if one hasn't already been defined.
     *
     * @param agentRoutingService the agent routing service
     * @param taskScheduler       the task scheduler
     * @param serviceProperties   the service properties
     * @return A {@link AgentConnectionTrackingServiceImpl} instance
     */
    @Bean
    @ConditionalOnMissingBean(AgentConnectionTrackingService.class)
    public AgentConnectionTrackingService agentConnectionTrackingService(
        final AgentRoutingService agentRoutingService,
        @Qualifier("genieTaskScheduler") final TaskScheduler taskScheduler,
        final AgentConnectionTrackingServiceProperties serviceProperties
    ) {
        return new AgentConnectionTrackingServiceImpl(
            agentRoutingService,
            taskScheduler,
            serviceProperties
        );
    }

    /**
     * Get an implementation of {@link AgentRoutingService} if one hasn't already been defined.
     * This bean is created if Zookeeper is disabled (single-node Genie deployments and tests).
     *
     * @param genieHostInfo The local genie host information
     * @return A {@link AgentRoutingServiceSingleNodeImpl} instance
     */
    @Bean
    @ConditionalOnMissingBean(
        {
            AgentRoutingService.class,
            ServiceDiscovery.class
        }
    )
    public AgentRoutingService agentRoutingServiceSingleNodeImpl(
        final GenieHostInfo genieHostInfo
    ) {
        return new AgentRoutingServiceSingleNodeImpl(genieHostInfo);
    }

    /**
     * Get an implementation of {@link AgentRoutingService} if one hasn't already been defined.
     * This bean is created if Zookeeper is enabled, it uses Curator's {@link ServiceDiscovery}.
     *
     * @param genieHostInfo                    The local genie host information
     * @param serviceDiscovery                 The Zookeeper Curator service discovery
     * @param taskScheduler                    The task scheduler
     * @param listenableCuratorConnectionState the connection state listenable
     * @param registry                         The metrics registry
     * @param properties                       The service properties
     * @return A {@link AgentRoutingServiceCuratorDiscoveryImpl} instance
     */
    @Bean
    @ConditionalOnBean(ServiceDiscovery.class)
    @ConditionalOnMissingBean(AgentRoutingService.class)
    public AgentRoutingService agentRoutingServiceCurator(
        final GenieHostInfo genieHostInfo,
        final ServiceDiscovery<AgentRoutingServiceCuratorDiscoveryImpl.Agent> serviceDiscovery,
        @Qualifier("genieTaskScheduler") final TaskScheduler taskScheduler,
        final Listenable<ConnectionStateListener> listenableCuratorConnectionState,
        final MeterRegistry registry,
        final AgentRoutingServiceProperties properties
    ) {
        return new AgentRoutingServiceCuratorDiscoveryImpl(
            genieHostInfo,
            serviceDiscovery,
            taskScheduler,
            listenableCuratorConnectionState,
            registry,
            properties
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
     * Provide a {@link AgentConfigurationService} if one is not defined.
     *
     * @param agentConfigurationProperties the service properties
     * @param propertiesMapCacheFactory    the properties map cache factory
     * @param registry                     the metrics registry
     * @return a {@link AgentConfigurationService} instance
     */
    @Bean
    @ConditionalOnMissingBean(AgentConfigurationService.class)
    public AgentConfigurationServiceImpl agentConfigurationService(
        final AgentConfigurationProperties agentConfigurationProperties,
        final PropertiesMapCache.Factory propertiesMapCacheFactory,
        final MeterRegistry registry
    ) {
        return new AgentConfigurationServiceImpl(
            agentConfigurationProperties,
            propertiesMapCacheFactory.get(
                agentConfigurationProperties.getCacheRefreshInterval(),
                AgentConfigurationProperties.DYNAMIC_PROPERTIES_PREFIX
            ),
            registry
        );
    }
}
