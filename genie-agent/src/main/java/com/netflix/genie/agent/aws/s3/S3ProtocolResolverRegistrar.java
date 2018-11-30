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
package com.netflix.genie.agent.aws.s3;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.cloud.aws.core.io.s3.SimpleStorageProtocolResolver;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.core.io.ProtocolResolver;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A class which takes an instance of {@link S3ProtocolResolver} and adds it to the Spring {@link ApplicationContext}
 * set of {@link ProtocolResolver}. This class will also search for any existing instances of
 * {@link SimpleStorageProtocolResolver} within the current protocol resolver set. Since the protocol resolvers are
 * iterated in the order they're added, due to being backed by {@link java.util.LinkedHashMap}, any call to
 * {@link ApplicationContext#getResource(String)} would always use {@link SimpleStorageProtocolResolver} for S3
 * resources if it was already in the set before this class is invoked.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Slf4j
public class S3ProtocolResolverRegistrar implements ApplicationContextAware {

    private final S3ProtocolResolver s3ProtocolResolver;

    /**
     * Constructor.
     *
     * @param s3ProtocolResolver the resolver that this class will register with the application context
     */
    public S3ProtocolResolverRegistrar(final S3ProtocolResolver s3ProtocolResolver) {
        this.s3ProtocolResolver = s3ProtocolResolver;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Add the {@link S3ProtocolResolver} to the set of protocol resolvers in the application context. Remove any
     * instances of {@link SimpleStorageProtocolResolver}.
     */
    @Override
    public void setApplicationContext(final ApplicationContext applicationContext) throws BeansException {
        if (applicationContext instanceof ConfigurableApplicationContext) {
            final ConfigurableApplicationContext configurableApplicationContext
                = (ConfigurableApplicationContext) applicationContext;

            if (configurableApplicationContext instanceof AbstractApplicationContext) {
                final AbstractApplicationContext abstractApplicationContext
                    = (AbstractApplicationContext) configurableApplicationContext;

                final Collection<ProtocolResolver> protocolResolvers
                    = abstractApplicationContext.getProtocolResolvers();

                final Set<ProtocolResolver> simpleStorageProtocolResolvers = protocolResolvers
                    .stream()
                    .filter(SimpleStorageProtocolResolver.class::isInstance)
                    .collect(Collectors.toSet());

                protocolResolvers.removeAll(simpleStorageProtocolResolvers);
            }

            log.info(
                "Adding instance of {} to the set of protocol resolvers",
                this.s3ProtocolResolver.getClass().getCanonicalName()
            );
            configurableApplicationContext.addProtocolResolver(this.s3ProtocolResolver);
        }
    }
}
