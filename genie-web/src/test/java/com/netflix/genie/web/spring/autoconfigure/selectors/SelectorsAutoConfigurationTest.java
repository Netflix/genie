/*
 *
 *  Copyright 2020 Netflix, Inc.
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
package com.netflix.genie.web.spring.autoconfigure.selectors;

import com.netflix.genie.web.scripts.ClusterSelectorScript;
import com.netflix.genie.web.selectors.ClusterSelector;
import com.netflix.genie.web.selectors.impl.RandomClusterSelectorImpl;
import com.netflix.genie.web.selectors.impl.ScriptClusterSelectorImpl;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;

/**
 * Tests for {@link SelectorsAutoConfiguration}.
 *
 * @author tgianos
 * @since 4.0.0
 */
class SelectorsAutoConfigurationTest {

    private ApplicationContextRunner contextRunner =
        new ApplicationContextRunner()
            .withConfiguration(
                AutoConfigurations.of(
                    SelectorsAutoConfiguration.class
                )
            );

    @Test
    void canCreateDefaultBeans() {
        Assertions
            .assertThat(SelectorsAutoConfiguration.SCRIPT_CLUSTER_SELECTOR_PRECEDENCE)
            .isEqualTo(Ordered.LOWEST_PRECEDENCE - 50);

        this.contextRunner.run(
            context -> {
                Assertions.assertThat(context).hasSingleBean(ClusterSelector.class);
                Assertions.assertThat(context).hasSingleBean(RandomClusterSelectorImpl.class);
                Assertions.assertThat(context).doesNotHaveBean(ScriptClusterSelectorImpl.class);
            }
        );
    }

    @Test
    void canCreateConditionalBeans() {
        this.contextRunner
            .withUserConfiguration(UserConfig.class)
            .run(
                context -> {
                    Assertions
                        .assertThat(context)
                        .hasSingleBean(RandomClusterSelectorImpl.class)
                        .hasSingleBean(ScriptClusterSelectorImpl.class)
                        .getBeans(ClusterSelector.class)
                        .hasSize(2); // No real easy way to test the order
                }
            );
    }

    /**
     * Dummy user configuration for tests.
     */
    private static class UserConfig {

        /**
         * Dummy meter registry.
         *
         * @return {@link SimpleMeterRegistry} instance
         */
        @Bean
        public MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }

        /**
         * Dummy script based selector.
         */
        @Bean
        public ClusterSelectorScript clusterSelectorScript() {
            return Mockito.mock(ClusterSelectorScript.class);
        }
    }
}
