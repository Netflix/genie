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
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;

/**
 * Spring Auto Configuration for the {@literal selectors} module.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Configuration
public class SelectorsAutoConfiguration {

    /**
     * The relative order of the {@link ScriptClusterSelectorImpl} if one is enabled relative to other
     * {@link ClusterSelector} instances that may be in the context. This allows users to fit {@literal 50} more
     * selectors between the script selector and the default {@link RandomClusterSelectorImpl}. If
     * the user wants to place a selector implementation before the script one they only need to subtract from this
     * value.
     */
    public static final int SCRIPT_CLUSTER_SELECTOR_PRECEDENCE = Ordered.LOWEST_PRECEDENCE - 50;

    /**
     * Produce the {@link ScriptClusterSelectorImpl} instance to use for this Genie node if it was configured by the
     * user. This bean is only created if the script is configured.
     *
     * @param clusterSelectorScript the cluster selector script
     * @param registry              the metrics registry
     * @return a {@link ScriptClusterSelectorImpl}
     */
    @Bean
    @Order(SCRIPT_CLUSTER_SELECTOR_PRECEDENCE)
    @ConditionalOnBean(ClusterSelectorScript.class)
    public ScriptClusterSelectorImpl scriptClusterSelector(
        final ClusterSelectorScript clusterSelectorScript,
        final MeterRegistry registry
    ) {
        return new ScriptClusterSelectorImpl(clusterSelectorScript, registry);
    }

    /**
     * The default cluster selector if all others fail.
     * <p>
     * Defaults to {@link Ordered#LOWEST_PRECEDENCE}.
     *
     * @return A {@link RandomClusterSelectorImpl} instance
     */
    @Bean
    @Order
    public RandomClusterSelectorImpl randomizedClusterSelector() {
        return new RandomClusterSelectorImpl();
    }
}
