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
package com.netflix.genie.web.spring.autoconfigure.agent.launchers;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.web.agent.launchers.AgentLauncher;
import com.netflix.genie.web.agent.launchers.impl.LocalAgentLauncherImpl;
import com.netflix.genie.web.agent.launchers.impl.TitusAgentLauncherImpl;
import com.netflix.genie.web.data.services.DataServices;
import com.netflix.genie.web.introspection.GenieWebHostInfo;
import com.netflix.genie.web.introspection.GenieWebRpcInfo;
import com.netflix.genie.web.properties.LocalAgentLauncherProperties;
import com.netflix.genie.web.properties.TitusAgentLauncherProperties;
import com.netflix.genie.web.util.ExecutorFactory;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

import java.util.concurrent.TimeUnit;

/**
 * Auto configuration for beans responsible ofor launching Genie Agent instances.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Configuration
@EnableConfigurationProperties(
    {
        LocalAgentLauncherProperties.class,
        TitusAgentLauncherProperties.class
    }
)
public class AgentLaunchersAutoConfiguration {

    /**
     * Provide a {@link TitusAgentLauncherImpl} implementation which launches agent processes in a dedicated Titus
     * container if enabled via property.
     *
     * @param restTemplate                 the rest template
     * @param genieHostInfo                the metadata about the local server and host
     * @param titusAgentLauncherProperties the configuration properties
     * @param registry                     the metric registry
     * @return a {@link TitusAgentLauncherImpl}
     */
    @Bean
    @ConditionalOnProperty(name = TitusAgentLauncherProperties.ENABLE_PROPERTY, havingValue = "true")
    public TitusAgentLauncherImpl titusAgentLauncher(
        @Qualifier("titusRestTemplate") final RestTemplate restTemplate,
        final GenieHostInfo genieHostInfo,
        final TitusAgentLauncherProperties titusAgentLauncherProperties,
        final MeterRegistry registry
    ) {
        final Cache<String, String> healthIndicatorCache = Caffeine.newBuilder()
            .maximumSize(titusAgentLauncherProperties.getHealthIndicatorMaxSize())
            .expireAfterWrite(
                titusAgentLauncherProperties.getHealthIndicatorExpiration().getSeconds(),
                TimeUnit.SECONDS
            )
            .build();

        return new TitusAgentLauncherImpl(
            restTemplate,
            healthIndicatorCache,
            genieHostInfo,
            titusAgentLauncherProperties,
            registry
        );
    }

    /**
     * Provide an {@link ExecutorFactory} instance if no other was defined.
     *
     * @return Instance of {@link ExecutorFactory}
     */
    @Bean
    @ConditionalOnMissingBean(ExecutorFactory.class)
    public ExecutorFactory processExecutorFactory() {
        return new ExecutorFactory();
    }

    /**
     * Provide a {@link AgentLauncher} implementation which launches local agent processes if enabled via property.
     *
     * @param genieWebHostInfo   The {@link GenieWebHostInfo} of this instance
     * @param genieWebRpcInfo    The {@link GenieWebRpcInfo} of this instance
     * @param dataServices       The {@link DataServices} instance to use
     * @param launcherProperties The properties related to launching an agent locally
     * @param executorFactory    The {@link ExecutorFactory} to use to launch agent processes
     * @param registry           The {@link MeterRegistry} to register metrics
     * @return A {@link LocalAgentLauncherImpl} instance
     */
    @Bean
    @ConditionalOnProperty(name = LocalAgentLauncherProperties.ENABLE_PROPERTY, havingValue = "true")
    public LocalAgentLauncherImpl localAgentLauncher(
        final GenieWebHostInfo genieWebHostInfo,
        final GenieWebRpcInfo genieWebRpcInfo,
        final DataServices dataServices,
        final LocalAgentLauncherProperties launcherProperties,
        final ExecutorFactory executorFactory,
        final MeterRegistry registry
    ) {
        return new LocalAgentLauncherImpl(
            genieWebHostInfo,
            genieWebRpcInfo,
            dataServices,
            launcherProperties,
            executorFactory,
            registry
        );
    }
}
