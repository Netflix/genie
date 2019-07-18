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
package com.netflix.genie.web.spring.autoconfigure.agent.resources;

import com.netflix.genie.web.agent.resources.AgentFileProtocolResolver;
import com.netflix.genie.web.agent.resources.AgentFileProtocolResolverRegistrar;
import com.netflix.genie.web.agent.services.AgentFileStreamService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Auto Configuration for {@link org.springframework.core.io.Resource} classes exposed in the Agent module.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Configuration
public class AgentResourcesAutoConfiguration {

    /**
     * Protocol resolver agent file resources.
     *
     * @param agentFileStreamService the agent file stream service
     * @return a {@link AgentFileProtocolResolver}
     */
    @Bean
    @ConditionalOnMissingBean(AgentFileProtocolResolver.class)
    public AgentFileProtocolResolver agentFileProtocolResolver(
        final AgentFileStreamService agentFileStreamService
    ) {
        return new AgentFileProtocolResolver(agentFileStreamService);
    }

    /**
     * Registrar for the {@link AgentFileProtocolResolver}.
     *
     * @param agentFileProtocolResolver a protocol {@link AgentFileProtocolResolver} to register
     * @return a {@link AgentFileProtocolResolverRegistrar}
     */
    @Bean
    @ConditionalOnMissingBean(AgentFileProtocolResolverRegistrar.class)
    public AgentFileProtocolResolverRegistrar agentFileProtocolResolverRegistrar(
        final AgentFileProtocolResolver agentFileProtocolResolver
    ) {
        return new AgentFileProtocolResolverRegistrar(agentFileProtocolResolver);
    }
}
