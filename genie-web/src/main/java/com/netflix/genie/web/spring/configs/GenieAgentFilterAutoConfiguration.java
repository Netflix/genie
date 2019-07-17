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

import com.netflix.genie.web.agent.services.AgentFilterService;
import com.netflix.genie.web.agent.services.impl.AgentFilterServiceImpl;
import com.netflix.genie.web.agent.utils.AgentMetadataInspector;
import com.netflix.genie.web.agent.utils.BlacklistedVersionAgentMetadataInspector;
import com.netflix.genie.web.agent.utils.MinimumVersionAgentMetadataInspector;
import com.netflix.genie.web.agent.utils.WhitelistedVersionAgentMetadataInspector;
import com.netflix.genie.web.properties.AgentFilterProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * Auto-configuration for the {@link AgentFilterService} default implementation.
 * This component is activated unless another bean is present AND if the corresponding property is set to true.
 * <p>
 * This configuration also creates a set of pre-configured {@link AgentMetadataInspector} that the
 * service loads and uses:
 * - Version whitelist: accept agent only if version matches a given pattern
 * - Version blacklist: reject agent if version matches a given pattern
 * - Minimum version: reject agent whose version is lower than a given version
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
     * A {@link AgentFilterService} implementation that federates the decision to a set of
     * {@link AgentMetadataInspector}s.
     *
     * @param agentMetadataInspectorsList the list of inspectors.
     * @return An {@link AgentFilterService} instance.
     */
    @Bean
    @ConditionalOnMissingBean(AgentFilterService.class)
    public AgentFilterServiceImpl agentFilterService(final List<AgentMetadataInspector> agentMetadataInspectorsList) {
        return new AgentFilterServiceImpl(agentMetadataInspectorsList);
    }

    /**
     * A {@link AgentMetadataInspector} that only accepts agents whose version matches a given regex.
     *
     * @param agentFilterProperties the agent filter properties
     * @return a {@link WhitelistedVersionAgentMetadataInspector} instance
     */
    @Bean
    @ConditionalOnMissingBean(WhitelistedVersionAgentMetadataInspector.class)
    public WhitelistedVersionAgentMetadataInspector whitelistedVersionAgentMetadataInspector(
        final AgentFilterProperties agentFilterProperties
    ) {
        return new WhitelistedVersionAgentMetadataInspector(
            agentFilterProperties
        );
    }

    /**
     * A {@link AgentMetadataInspector} that rejects agents whose version matches a given regex.
     *
     * @param agentFilterProperties the agent filter properties
     * @return a {@link BlacklistedVersionAgentMetadataInspector}
     */
    @Bean
    @ConditionalOnMissingBean(BlacklistedVersionAgentMetadataInspector.class)
    public BlacklistedVersionAgentMetadataInspector blacklistedVersionAgentMetadataInspector(
        final AgentFilterProperties agentFilterProperties
    ) {
        return new BlacklistedVersionAgentMetadataInspector(agentFilterProperties);
    }

    /**
     * A {@link AgentMetadataInspector} that rejects agents whose version is lower than a given version.
     *
     * @param agentFilterProperties the agent filter properties
     * @return a {@link AgentMetadataInspector}
     */
    @Bean
    @ConditionalOnMissingBean(MinimumVersionAgentMetadataInspector.class)
    public MinimumVersionAgentMetadataInspector minimumVersionAgentMetadataInspector(
        final AgentFilterProperties agentFilterProperties
    ) {
        return new MinimumVersionAgentMetadataInspector(agentFilterProperties);
    }

}
