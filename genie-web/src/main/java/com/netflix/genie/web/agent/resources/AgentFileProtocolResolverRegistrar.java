/*
 *
 *  Copyright 2018 Netflix, Inc.
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
package com.netflix.genie.web.agent.resources;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanNotOfRequiredTypeException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Registers AgentFileProtocolResolver in the application context.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class AgentFileProtocolResolverRegistrar implements ApplicationContextAware {

    private final AgentFileProtocolResolver agentFileProtocolResolver;

    /**
     * Constructor.
     *
     * @param agentFileProtocolResolver the resolver to register
     */
    public AgentFileProtocolResolverRegistrar(final AgentFileProtocolResolver agentFileProtocolResolver) {
        this.agentFileProtocolResolver = agentFileProtocolResolver;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setApplicationContext(final ApplicationContext applicationContext) throws BeansException {
        if (applicationContext instanceof ConfigurableApplicationContext) {

            final ConfigurableApplicationContext configurableApplicationContext
                = (ConfigurableApplicationContext) applicationContext;

            log.info(
                "Adding instance of {} to the set of protocol resolvers",
                this.agentFileProtocolResolver.getClass().getCanonicalName()
            );
            configurableApplicationContext.addProtocolResolver(this.agentFileProtocolResolver);
        } else {
            throw new BeanNotOfRequiredTypeException(
                "applicationContext",
                ConfigurableApplicationContext.class,
                ApplicationContext.class
            );
        }
    }
}
