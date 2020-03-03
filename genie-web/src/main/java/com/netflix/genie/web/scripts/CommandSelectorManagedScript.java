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
import com.netflix.genie.common.external.dtos.v4.Command;
import com.netflix.genie.common.external.dtos.v4.JobRequest;
import com.netflix.genie.web.exceptions.checked.ResourceSelectionException;
import com.netflix.genie.web.exceptions.checked.ScriptExecutionException;
import com.netflix.genie.web.exceptions.checked.ScriptNotConfiguredException;
import com.netflix.genie.web.properties.CommandSelectorManagedScriptProperties;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;

/**
 * An extension of {@link ManagedScript} which from a set of commands and the original job request will attempt to
 * determine the best command to use for execution.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Slf4j
public class CommandSelectorManagedScript extends ManagedScript implements ResourceSelectorScript<Command> {

    static final String JOB_REQUEST_BINDING = "jobRequestParameter";
    static final String COMMANDS_BINDING = "commandsParameter";

    /**
     * Constructor.
     *
     * @param scriptManager The {@link ScriptManager} instance to use
     * @param properties    The {@link CommandSelectorManagedScriptProperties} instance to use
     * @param registry      The {@link MeterRegistry} instance to use
     */
    public CommandSelectorManagedScript(
        final ScriptManager scriptManager,
        final CommandSelectorManagedScriptProperties properties,
        final MeterRegistry registry
    ) {
        super(scriptManager, properties, registry);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResourceSelectorScriptResult<Command> selectResource(
        final Set<Command> resources,
        final JobRequest jobRequest
    ) throws ResourceSelectionException {
        log.debug("Called to attempt to select a command from {} for job {}", resources, jobRequest);

        try {
            final Object evaluationResult = this.evaluateScript(
                ImmutableMap.of(
                    JOB_REQUEST_BINDING, jobRequest,
                    COMMANDS_BINDING, resources
                )
            );
            if (!(evaluationResult instanceof ResourceSelectorScriptResult)) {
                throw new ResourceSelectionException(
                    "Command selector evaluation returned invalid type: " + evaluationResult.getClass().getName()
                );
            }
            @SuppressWarnings("unchecked") final ResourceSelectorScriptResult<Command> result
                = (ResourceSelectorScriptResult<Command>) evaluationResult;

            // Validate that the selected resource is actually in the original set
            if (result.getResource().isPresent() && !resources.contains(result.getResource().get())) {
                throw new ResourceSelectionException(result.getResource().get() + " is not in original set");
            }

            return result;
        } catch (
            final ScriptExecutionException
                | ScriptNotConfiguredException
                | RuntimeException e
        ) {
            throw new ResourceSelectionException(e);
        }
    }
}
