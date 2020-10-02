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

import com.netflix.genie.web.agent.launchers.AgentLauncher;
import com.netflix.genie.web.scripts.AgentLauncherSelectorManagedScript;
import com.netflix.genie.web.scripts.ClusterSelectorManagedScript;
import com.netflix.genie.web.scripts.CommandSelectorManagedScript;
import com.netflix.genie.web.selectors.AgentLauncherSelector;
import com.netflix.genie.web.selectors.ClusterSelector;
import com.netflix.genie.web.selectors.CommandSelector;
import com.netflix.genie.web.selectors.impl.RandomAgentLauncherSelectorImpl;
import com.netflix.genie.web.selectors.impl.RandomClusterSelectorImpl;
import com.netflix.genie.web.selectors.impl.RandomCommandSelectorImpl;
import com.netflix.genie.web.selectors.impl.ScriptAgentLauncherSelectorImpl;
import com.netflix.genie.web.selectors.impl.ScriptClusterSelectorImpl;
import com.netflix.genie.web.selectors.impl.ScriptCommandSelectorImpl;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;

import javax.validation.constraints.NotEmpty;
import java.util.Collection;
import java.util.Optional;

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
     * @param clusterSelectorManagedScript the cluster selector script
     * @param registry                     the metrics registry
     * @return a {@link ScriptClusterSelectorImpl}
     */
    @Bean
    @Order(SCRIPT_CLUSTER_SELECTOR_PRECEDENCE)
    @ConditionalOnBean(ClusterSelectorManagedScript.class)
    public ScriptClusterSelectorImpl scriptClusterSelector(
        final ClusterSelectorManagedScript clusterSelectorManagedScript,
        final MeterRegistry registry
    ) {
        return new ScriptClusterSelectorImpl(clusterSelectorManagedScript, registry);
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

    /**
     * Provide a default {@link CommandSelector} implementation if no other has been defined in the context already.
     *
     * @param commandSelectorManagedScriptOptional An {@link Optional} wrapping a {@link CommandSelectorManagedScript}
     *                                             instance if one is present in the context else
     *                                             {@link Optional#empty()}
     * @param registry                             The {@link MeterRegistry} instance to use
     * @return A {@link ScriptCommandSelectorImpl} if a {@link CommandSelectorManagedScript} instance as present else
     * a {@link RandomCommandSelectorImpl} instance
     */
    @Bean
    @ConditionalOnMissingBean(CommandSelector.class)
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public CommandSelector commandSelector(
        final Optional<CommandSelectorManagedScript> commandSelectorManagedScriptOptional,
        final MeterRegistry registry
    ) {
        if (commandSelectorManagedScriptOptional.isPresent()) {
            return new ScriptCommandSelectorImpl(commandSelectorManagedScriptOptional.get(), registry);
        } else {
            return new RandomCommandSelectorImpl();
        }
    }

    /**
     * Provide a default {@link AgentLauncherSelector} implementation if no other has been defined in the context.
     *
     * @param agentLauncherSelectorManagedScript An {@link Optional} {@link AgentLauncherSelectorManagedScript}
     *                                           instance if one is present in the context
     * @param agentLaunchers                     The available agent launchers
     * @param registry                           The {@link MeterRegistry} instance to use
     * @return A {@link ScriptAgentLauncherSelectorImpl} if a {@link AgentLauncherSelectorManagedScript} instance is
     * present, else a {@link RandomAgentLauncherSelectorImpl} instance
     */
    @Bean
    @ConditionalOnMissingBean(AgentLauncherSelector.class)
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public AgentLauncherSelector agentLauncherSelector(
        final Optional<AgentLauncherSelectorManagedScript> agentLauncherSelectorManagedScript,
        @NotEmpty final Collection<AgentLauncher> agentLaunchers,
        final MeterRegistry registry
    ) {
        if (agentLauncherSelectorManagedScript.isPresent()) {
            return new ScriptAgentLauncherSelectorImpl(
                agentLauncherSelectorManagedScript.get(),
                agentLaunchers,
                registry
            );
        } else {
            return new RandomAgentLauncherSelectorImpl(agentLaunchers);
        }
    }
}
