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

import com.netflix.genie.web.properties.ClusterLoadBalancerScriptProperties;
import com.netflix.genie.web.properties.ExecutionModeFilterScriptProperties;
import com.netflix.genie.web.scripts.ClusterLoadBalancerScript;
import com.netflix.genie.web.scripts.ExecutionModeFilterScript;
import com.netflix.genie.web.scripts.ScriptManager;
import io.micrometer.core.instrument.MeterRegistry;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ResourceLoader;
import org.springframework.scheduling.TaskScheduler;

/**
 * Tests for {@link ScriptsAutoConfiguration}.
 *
 * @author mprimi
 * @since 4.0.0
 */
class ScriptsAutoConfigurationTest {

    private ApplicationContextRunner contextRunner =
        new ApplicationContextRunner()
            .withUserConfiguration(MocksConfiguration.class)
            .withConfiguration(
                AutoConfigurations.of(
                    ScriptsAutoConfiguration.class
                )
            );

    @Test
    void scriptManagerCreated() {
        this.contextRunner
            .run(
                context -> {
                    Assertions.assertThat(context).hasSingleBean(ScriptManager.class);
                }
            );
    }

    @Test
    void scriptsNotCreatedByDefault() {
        this.contextRunner
            .withPropertyValues()
            .run(
                context -> {
                    Assertions.assertThat(context).hasSingleBean(ClusterLoadBalancerScriptProperties.class);
                    Assertions.assertThat(context).doesNotHaveBean(ClusterLoadBalancerScript.class);
                    Assertions.assertThat(context).hasSingleBean(ExecutionModeFilterScriptProperties.class);
                    Assertions.assertThat(context).doesNotHaveBean(ExecutionModeFilterScript.class);
                    Assertions.assertThat(context).hasSingleBean(ScriptsAutoConfiguration.ManagedScriptPreLoader.class);
                }
            );
    }

    @Test
    void scriptsCreatedIfSourceIsConfigured() {
        this.contextRunner
            .withPropertyValues(
                ClusterLoadBalancerScriptProperties.SOURCE_PROPERTY + "=file:///script.js",
                ExecutionModeFilterScriptProperties.SOURCE_PROPERTY + "=file:///script.js"
            )
            .run(
                context -> {
                    Assertions.assertThat(context).hasSingleBean(ClusterLoadBalancerScriptProperties.class);
                    Assertions.assertThat(context).hasSingleBean(ClusterLoadBalancerScript.class);
                    Assertions.assertThat(context).hasSingleBean(ExecutionModeFilterScriptProperties.class);
                    Assertions.assertThat(context).hasSingleBean(ExecutionModeFilterScript.class);
                    Assertions.assertThat(context).hasSingleBean(ScriptsAutoConfiguration.ManagedScriptPreLoader.class);
                }
            );
    }

    static class MocksConfiguration {
        @Bean
        @Qualifier("genieTaskScheduler") TaskScheduler taskScheduler() {
            return Mockito.mock(TaskScheduler.class);
        }

        @Bean
        MeterRegistry meterRegistry() {
            return Mockito.mock(MeterRegistry.class);
        }

        @Bean
        ResourceLoader resourceLoader() {
            return Mockito.mock(ResourceLoader.class);
        }
    }
}

