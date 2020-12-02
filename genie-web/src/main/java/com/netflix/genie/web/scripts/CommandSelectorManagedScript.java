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

import com.netflix.genie.common.external.dtos.v4.Command;
import com.netflix.genie.common.internal.util.PropertiesMapCache;
import com.netflix.genie.web.exceptions.checked.ResourceSelectionException;
import com.netflix.genie.web.properties.CommandSelectorManagedScriptProperties;
import com.netflix.genie.web.selectors.CommandSelectionContext;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * An extension of {@link ResourceSelectorScript} which from a set of commands and the original job request will
 * attempt to determine the best command to use for execution.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Slf4j
public class CommandSelectorManagedScript extends ResourceSelectorScript<Command, CommandSelectionContext> {

    static final String COMMANDS_BINDING = "commandsParameter";

    /**
     * Constructor.
     *
     * @param scriptManager    The {@link ScriptManager} instance to use
     * @param properties       The {@link CommandSelectorManagedScriptProperties} instance to use
     * @param registry         The {@link MeterRegistry} instance to use
     * @param propertyMapCache The {@link PropertiesMapCache} instance to use
     */
    public CommandSelectorManagedScript(
        final ScriptManager scriptManager,
        final CommandSelectorManagedScriptProperties properties,
        final MeterRegistry registry,
        final PropertiesMapCache propertyMapCache
    ) {
        super(scriptManager, properties, registry, propertyMapCache);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResourceSelectorScriptResult<Command> selectResource(
        final CommandSelectionContext context
    ) throws ResourceSelectionException {
        log.debug(
            "Called to attempt to select a command from {} for job {}",
            context.getResources(),
            context.getJobId()
        );

        return super.selectResource(context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void addParametersForScript(
        final Map<String, Object> parameters,
        final CommandSelectionContext context
    ) {
        super.addParametersForScript(parameters, context);

        // TODO: Remove once internal scripts migrate to use context directly
        parameters.put(COMMANDS_BINDING, context.getResources());
    }
}
