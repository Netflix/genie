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

import com.netflix.genie.common.internal.util.PropertiesMapCache;
import com.netflix.genie.web.properties.AgentLauncherSelectorScriptProperties;
import com.netflix.genie.web.properties.ClusterSelectorScriptProperties;
import com.netflix.genie.web.properties.CommandSelectorManagedScriptProperties;
import com.netflix.genie.web.scripts.AgentLauncherSelectorManagedScript;
import com.netflix.genie.web.scripts.ClusterSelectorManagedScript;
import com.netflix.genie.web.scripts.CommandSelectorManagedScript;
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
    void scriptsNotCreatedByDefault() {
        this.contextRunner
            .run(
                context -> {
                    Assertions.assertThat(context).hasSingleBean(ClusterSelectorScriptProperties.class);
                    Assertions.assertThat(context).hasSingleBean(CommandSelectorManagedScriptProperties.class);
                    Assertions.assertThat(context).hasSingleBean(AgentLauncherSelectorScriptProperties.class);

                    Assertions.assertThat(context).hasSingleBean(ScriptManager.class);
                    Assertions.assertThat(context).doesNotHaveBean(ClusterSelectorManagedScript.class);
                    Assertions.assertThat(context).doesNotHaveBean(CommandSelectorManagedScript.class);
                    Assertions.assertThat(context).doesNotHaveBean(AgentLauncherSelectorManagedScript.class);
                    Assertions.assertThat(context).hasSingleBean(ScriptsAutoConfiguration.ManagedScriptPreLoader.class);
                }
            );
    }

    @Test
    void scriptsCreatedIfSourceIsConfigured() {
        this.contextRunner
            .withPropertyValues(
                ClusterSelectorScriptProperties.SOURCE_PROPERTY + "=file:///script.js",
                CommandSelectorManagedScriptProperties.SOURCE_PROPERTY + "=file:///script.groovy",
                AgentLauncherSelectorScriptProperties.SOURCE_PROPERTY + "=file:///script.groovy"
            )
            .run(
                context -> {
                    Assertions.assertThat(context).hasSingleBean(ClusterSelectorScriptProperties.class);
                    Assertions.assertThat(context).hasSingleBean(CommandSelectorManagedScriptProperties.class);
                    Assertions.assertThat(context).hasSingleBean(AgentLauncherSelectorScriptProperties.class);

                    Assertions.assertThat(context).hasSingleBean(ScriptManager.class);
                    Assertions.assertThat(context).hasSingleBean(ClusterSelectorManagedScript.class);
                    Assertions.assertThat(context).hasSingleBean(CommandSelectorManagedScript.class);
                    Assertions.assertThat(context).hasSingleBean(AgentLauncherSelectorManagedScript.class);
                    Assertions.assertThat(context).hasSingleBean(ScriptsAutoConfiguration.ManagedScriptPreLoader.class);
                }
            );
    }

    static class MocksConfiguration {
        @Bean
        @Qualifier("genieTaskScheduler")
        TaskScheduler taskScheduler() {
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

        @Bean
        PropertiesMapCache.Factory propertiesMapCacheFactory() {
            return Mockito.mock(PropertiesMapCache.Factory.class);
        }
    }
}

