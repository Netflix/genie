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

import com.netflix.genie.web.agent.launchers.AgentLauncher;
import com.netflix.genie.web.agent.launchers.impl.LocalAgentLauncherImpl;
import com.netflix.genie.web.data.services.JobSearchService;
import com.netflix.genie.web.introspection.GenieWebHostInfo;
import com.netflix.genie.web.introspection.GenieWebRpcInfo;
import com.netflix.genie.web.properties.LocalAgentLauncherProperties;
import com.netflix.genie.web.util.ExecutorFactory;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Auto configuration for beans responsible ofor launching Genie Agent instances.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Configuration
@EnableConfigurationProperties(
    {
        LocalAgentLauncherProperties.class
    }
)
public class AgentLaunchersAutoConfiguration {

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
     * Provide a {@link AgentLauncher} implementation which launches local agent processes if no other implementation
     * is defined.
     *
     * @param genieWebHostInfo   The {@link GenieWebHostInfo} of this instance
     * @param genieWebRpcInfo    The {@link GenieWebRpcInfo} of this instance
     * @param jobSearchService   The {@link JobSearchService} instance to use
     * @param launcherProperties The properties related to launching an agent locally
     * @param executorFactory    The {@link ExecutorFactory} to use to launch agent processes
     * @param registry           The {@link MeterRegistry} to register metrics
     * @return A {@link LocalAgentLauncherImpl} instance
     */
    @Bean
    @ConditionalOnMissingBean(AgentLauncher.class)
    public LocalAgentLauncherImpl localAgentLauncher(
        final GenieWebHostInfo genieWebHostInfo,
        final GenieWebRpcInfo genieWebRpcInfo,
        final JobSearchService jobSearchService,
        final LocalAgentLauncherProperties launcherProperties,
        final ExecutorFactory executorFactory,
        final MeterRegistry registry
    ) {
        return new LocalAgentLauncherImpl(
            genieWebHostInfo,
            genieWebRpcInfo,
            jobSearchService,
            launcherProperties,
            executorFactory,
            registry
        );
    }
}
