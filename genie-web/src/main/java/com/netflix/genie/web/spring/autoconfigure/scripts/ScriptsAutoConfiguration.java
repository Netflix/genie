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
package com.netflix.genie.web.spring.autoconfigure.scripts;

import com.netflix.genie.common.util.GenieObjectMapper;
import com.netflix.genie.web.properties.ClusterLoadBalancerScriptProperties;
import com.netflix.genie.web.properties.ScriptManagerProperties;
import com.netflix.genie.web.scripts.ClusterLoadBalancerScript;
import com.netflix.genie.web.scripts.ScriptManager;
import com.netflix.genie.web.spring.autoconfigure.aws.AWSAutoConfiguration;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ResourceLoader;
import org.springframework.scheduling.TaskScheduler;

import javax.script.ScriptEngineManager;
import java.util.concurrent.Executors;

/**
 * Configuration for script extensions.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Configuration
@EnableConfigurationProperties(
    {
        ClusterLoadBalancerScriptProperties.class,
        ScriptManagerProperties.class,
    }
)
@AutoConfigureAfter({
    AWSAutoConfiguration.class // Make sure resource loader is configured for S3
})
public class ScriptsAutoConfiguration {

    /**
     * Create a {@link ScriptManager} unless one exists.
     *
     * @param scriptManagerProperties properties
     * @param taskScheduler           task scheduler
     * @param resourceLoader          resource loader
     * @param meterRegistry           meter registry
     * @return a {@link ScriptManager}
     */
    @Bean
    @ConditionalOnMissingBean(ScriptManager.class)
    ScriptManager scriptManager(
        final ScriptManagerProperties scriptManagerProperties,
        @Qualifier("genieTaskScheduler") final TaskScheduler taskScheduler,
        final ResourceLoader resourceLoader,
        final MeterRegistry meterRegistry
    ) {
        return new ScriptManager(
            scriptManagerProperties,
            taskScheduler,
            Executors.newCachedThreadPool(),
            new ScriptEngineManager(),
            resourceLoader,
            meterRegistry
        );
    }

    /**
     * Create a {@link ClusterLoadBalancerScript} unless one exists.
     *
     * @param scriptManager    script manager
     * @param scriptProperties script properties
     * @param meterRegistry    meter registry
     * @return a {@link ClusterLoadBalancerScript}
     */
    @Bean
    @ConditionalOnMissingBean(ClusterLoadBalancerScript.class)
    @ConditionalOnProperty(value = ClusterLoadBalancerScriptProperties.SOURCE_PROPERTY)
    ClusterLoadBalancerScript clusterLoadBalancerScript(
        final ScriptManager scriptManager,
        final ClusterLoadBalancerScriptProperties scriptProperties,
        final MeterRegistry meterRegistry
    ) {
        return new ClusterLoadBalancerScript(
            scriptManager,
            scriptProperties,
            GenieObjectMapper.getMapper(),
            meterRegistry
        );
    }
}
