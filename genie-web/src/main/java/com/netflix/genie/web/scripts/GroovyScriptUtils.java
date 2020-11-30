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
package com.netflix.genie.web.scripts;

import com.google.common.collect.ImmutableMap;
import com.netflix.genie.common.external.dtos.v4.Cluster;
import com.netflix.genie.common.external.dtos.v4.Command;
import com.netflix.genie.web.selectors.AgentLauncherSelectionContext;
import com.netflix.genie.web.selectors.ClusterSelectionContext;
import com.netflix.genie.web.selectors.CommandSelectionContext;
import groovy.lang.Binding;

import java.util.Map;
import java.util.Set;

/**
 * Utility functions that can be used within Groovy scripts executed from Genie.
 *
 * @author tgianos
 * @since 4.0.0
 */
public final class GroovyScriptUtils {
    private static final Map<String, String> EMPTY_PROPERTIES_MAP = ImmutableMap.of();

    private GroovyScriptUtils() {
    }

    /**
     * Given the {@link Binding} that a script has attempt to extract the command selection context.
     *
     * @param binding The {@link Binding} for the script
     * @return The {@link CommandSelectionContext} instance
     * @throws IllegalArgumentException If there is no context parameter for the script or it is not a
     *                                  {@link CommandSelectionContext}
     */
    public static CommandSelectionContext getCommandSelectionContext(
        final Binding binding
    ) throws IllegalArgumentException {
        return getObjectVariable(binding, ResourceSelectorScript.CONTEXT_BINDING, CommandSelectionContext.class);
    }

    /**
     * Given the {@link Binding} that a script has attempt to extract the cluster selection context.
     *
     * @param binding The {@link Binding} for the script
     * @return The {@link ClusterSelectionContext} instance
     * @throws IllegalArgumentException If there is no context parameter for the script or it is not a
     *                                  {@link ClusterSelectionContext}
     */
    public static ClusterSelectionContext getClusterSelectionContext(
        final Binding binding
    ) throws IllegalArgumentException {
        return getObjectVariable(binding, ResourceSelectorScript.CONTEXT_BINDING, ClusterSelectionContext.class);
    }

    /**
     * Given the {@link Binding} that a script has attempt to extract the cluster selection context.
     *
     * @param binding The {@link Binding} for the script
     * @return The {@link ClusterSelectionContext} instance
     * @throws IllegalArgumentException If there is no context parameter for the script or it is not a
     *                                  {@link ClusterSelectionContext}
     */
    public static AgentLauncherSelectionContext getAgentLauncherSelectionContext(
        final Binding binding
    ) throws IllegalArgumentException {
        return getObjectVariable(binding, ResourceSelectorScript.CONTEXT_BINDING, AgentLauncherSelectionContext.class);
    }


    /**
     * Given the {@link Binding} that a script has attempt to extract the properties map.
     *
     * @param binding The {@link Binding} for the script
     * @return The map of properties
     */
    @SuppressWarnings("unchecked") // Due to cast of Map to Map<String, String>
    public static Map<String, String> getProperties(
        final Binding binding
    ) {
        try {
            if (binding.hasVariable(ResourceSelectorScript.PROPERTIES_MAP_BINDING)) {
                return (Map<String, String>) binding.getVariable(ResourceSelectorScript.PROPERTIES_MAP_BINDING);
            }
        } catch (ClassCastException e) {
            // Value bound is not Map<String, String> -- should never happen
        }
        // Properties not bound -- should never happen
        return EMPTY_PROPERTIES_MAP;
    }

    /**
     * Given the {@link Binding} that a script has attempt to extract the clusters parameter.
     *
     * @param binding The {@link Binding} of the script
     * @return The set of {@link Cluster}'s
     * @throws IllegalArgumentException If there is no clusters parameter, it isn't a set, is empty or doesn't contain
     *                                  all {@link Cluster} instances
     * @deprecated Use {@link #getClusterSelectionContext(Binding)} or {@link #getCommandSelectionContext(Binding)}
     * instead
     */
    @Deprecated
    public static Set<Cluster> getClusters(final Binding binding) throws IllegalArgumentException {
        return getSetVariable(binding, ClusterSelectorManagedScript.CLUSTERS_BINDING, Cluster.class);
    }

    /**
     * Given the {@link Binding} that a script has attempt to extract the commands parameter.
     *
     * @param binding The {@link Binding} of the script
     * @return The set of {@link Command}'s
     * @throws IllegalArgumentException If there is no commands parameter, it isn't a set, is empty or doesn't contain
     *                                  all {@link Command} instances
     * @deprecated Use {@link #getClusterSelectionContext(Binding)} or {@link #getCommandSelectionContext(Binding)}
     * instead
     */
    @Deprecated
    public static Set<Command> getCommands(final Binding binding) throws IllegalArgumentException {
        return getSetVariable(binding, CommandSelectorManagedScript.COMMANDS_BINDING, Command.class);
    }

    @SuppressWarnings("unchecked")
    private static <R> R getObjectVariable(
        final Binding binding,
        final String variableName,
        final Class<R> expectedType
    ) {
        if (!binding.hasVariable(variableName)
            || !(binding.getVariable(variableName).getClass().isAssignableFrom(expectedType))) {
            throw new IllegalArgumentException(variableName + " argument not instance of " + expectedType.getName());
        }
        return (R) binding.getVariable(variableName);
    }

    @SuppressWarnings("unchecked")
    private static <R> Set<R> getSetVariable(
        final Binding binding,
        final String bindingName,
        final Class<R> expectedType
    ) {
        if (!binding.hasVariable(bindingName) || !(binding.getVariable(bindingName) instanceof Set<?>)) {
            throw new IllegalArgumentException(
                "Expected " + bindingName + " to be instance of Set<" + expectedType.getName() + ">"
            );
        }
        final Set<?> set = (Set<?>) binding.getVariable(bindingName);
        if (set.isEmpty() || !set.stream().allMatch(expectedType::isInstance)) {
            throw new IllegalArgumentException(
                "Expected " + bindingName + " to be non-empty set of " + expectedType.getName()
            );
        }
        return (Set<R>) set;
    }
}
