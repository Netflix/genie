/*
 *
 *  Copyright 2016 Netflix, Inc.
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

import com.netflix.genie.web.properties.AgentFilterProperties;
import com.netflix.genie.web.services.AgentFilterService;
import com.netflix.genie.web.services.impl.AgentFilterServiceImpl;
import com.netflix.genie.web.util.BlacklistedVersionAgentMetadataInspector;
import com.netflix.genie.web.util.MinimumVersionAgentMetadataInspector;
import com.netflix.genie.web.util.WhitelistedVersionAgentMetadataInspector;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

import java.util.List;

/**
 * Auto-configuration for the {@link AgentFilterService} default implementation.
 * This component is activated unless another bean is present AND if the corresponding property is set to true.
 * <p>
 * This configuration also creates a set of pre-configured {@link AgentFilterService.AgentMetadataInspector} that the
 * service will load. In order:
 * - Version whitelist: immediately accept an agent whose version matches a given pattern
 * - Version blacklist: immediately reject an agent whose version matches a given pattern
 * - Minimum version: immediately reject an agent whose version is lower than a given minimum
 *
 * @author mprimi
 * @since 4.0.0
 */
@Configuration
@ConditionalOnProperty(value = AgentFilterProperties.VERSION_FILTER_ENABLED_PROPERTY, havingValue = "true")
@EnableConfigurationProperties(
    {
        AgentFilterProperties.class,
    }
)
public class GenieAgentFilterAutoConfiguration {

    /**
     * Get a {@link AgentFilterService} instance if there isn't already one.
     * Annotate inspector beans with {@link Order} if precedence is important.
     *
     * @param agentMetadataInspectorsList the list of inspectors.
     * @return An {@link AgentFilterService} instance.
     */
    @Bean
    public AgentFilterService agentFilterService(
        final List<AgentFilterService.AgentMetadataInspector> agentMetadataInspectorsList
    ) {
        return new AgentFilterServiceImpl(agentMetadataInspectorsList);
    }

    /**
     * Fallback NOOP {@link AgentFilterService.AgentMetadataInspector} created only if no other
     * bean of this type is present, to satisfy the dependency expressed by {@link AgentFilterService}.
     *
     * @return an noop inspector that always returns CONTINUE
     */
    @Bean
    @ConditionalOnMissingBean(AgentFilterService.AgentMetadataInspector.class)
    public AgentFilterService.AgentMetadataInspector defaultAgentMetadataInspector() {
        return agentClientMetadata -> new AgentFilterService.InspectionReport(
            AgentFilterService.InspectionReport.InspectionDecision.CONTINUE,
            "No preference."
        );
    }

    /**
     * A {@link AgentFilterService.AgentMetadataInspector} that accepts agents whose version matches a given regex.
     *
     * @param agentFilterProperties the agent filter properties
     * @return a {@link AgentFilterService.AgentMetadataInspector}
     */
    @Bean
    @Order(1000)
    public AgentFilterService.AgentMetadataInspector whitelistedVersionAgentMetadataInspector(
        final AgentFilterProperties agentFilterProperties
    ) {
        return new WhitelistedVersionAgentMetadataInspector(
            agentFilterProperties
        );
    }

    /**
     * A {@link AgentFilterService.AgentMetadataInspector} that rejects agents whose version matches a given regex.
     *
     * @param agentFilterProperties the agent filter properties
     * @return a {@link AgentFilterService.AgentMetadataInspector}
     */
    @Bean
    @Order(1100)
    public AgentFilterService.AgentMetadataInspector blacklistedVersionAgentMetadataInspector(
        final AgentFilterProperties agentFilterProperties
    ) {
        return new BlacklistedVersionAgentMetadataInspector(
            agentFilterProperties
        );
    }

    /**
     * A {@link AgentFilterService.AgentMetadataInspector} that rejects agents whose version is lower than a given
     * version.
     *
     * @param agentFilterProperties the agent filter properties
     * @return a {@link AgentFilterService.AgentMetadataInspector}
     */
    @Bean
    @Order(1200)
    public AgentFilterService.AgentMetadataInspector minimumVersionAgentMetadataInspector(
        final AgentFilterProperties agentFilterProperties
    ) {
        return new MinimumVersionAgentMetadataInspector(
            agentFilterProperties
        );
    }

}
