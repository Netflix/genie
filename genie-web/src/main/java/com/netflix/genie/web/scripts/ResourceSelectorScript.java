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

import com.google.common.collect.Maps;
import com.netflix.genie.common.external.dtos.v4.JobRequest;
import com.netflix.genie.common.internal.util.PropertiesMapCache;
import com.netflix.genie.web.exceptions.checked.ResourceSelectionException;
import com.netflix.genie.web.exceptions.checked.ScriptExecutionException;
import com.netflix.genie.web.exceptions.checked.ScriptNotConfiguredException;
import com.netflix.genie.web.selectors.ResourceSelectionContext;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * Interface for defining the contract between the selection of a resource from a set of resources for a given
 * job request.
 *
 * @param <R> The type of resource this script is selecting from
 * @param <C> The context for resource selection which must extend {@link ResourceSelectionContext}
 * @author tgianos
 * @since 4.0.0
 */
@Slf4j
public class ResourceSelectorScript<R, C extends ResourceSelectionContext<R>> extends ManagedScript {

    static final String CONTEXT_BINDING = "contextParameter";
    static final String PROPERTIES_MAP_BINDING = "propertiesMap";
    private final PropertiesMapCache propertiesCache;

    /**
     * Constructor.
     *
     * @param scriptManager    The {@link ScriptManager} instance to use
     * @param properties       The {@link ManagedScriptBaseProperties} instance to use
     * @param registry         The {@link MeterRegistry} instance to use
     * @param propertyMapCache The {@link PropertiesMapCache} instance to use
     */
    protected ResourceSelectorScript(
        final ScriptManager scriptManager,
        final ManagedScriptBaseProperties properties,
        final MeterRegistry registry,
        final PropertiesMapCache propertyMapCache
    ) {
        super(scriptManager, properties, registry);
        this.propertiesCache = propertyMapCache;
    }

    /**
     * Given the {@link JobRequest} and an associated set of {@literal resources} which matched the request criteria
     * invoke the configured script to see if a preferred resource is selected based on the current logic.
     *
     * @param context The {@link ResourceSelectionContext} instance containing information about the context for this
     *                selection
     * @return A {@link ResourceSelectorScriptResult} instance
     * @throws ResourceSelectionException If an unexpected error occurs during selection
     */
    public ResourceSelectorScriptResult<R> selectResource(final C context) throws ResourceSelectionException {
        try {
            final Map<String, Object> parameters = Maps.newHashMap();
            parameters.put(PROPERTIES_MAP_BINDING, this.propertiesCache.get());
            this.addParametersForScript(parameters, context);
            final Object evaluationResult = this.evaluateScript(parameters);
            if (!(evaluationResult instanceof ResourceSelectorScriptResult)) {
                throw new ResourceSelectionException(
                    "Selector evaluation returned invalid type: " + evaluationResult.getClass().getName()
                        + " expected " + ResourceSelectorScriptResult.class.getName()
                );
            }
            @SuppressWarnings("unchecked") final ResourceSelectorScriptResult<R> result
                = (ResourceSelectorScriptResult<R>) evaluationResult;

            // Validate that the selected resource is actually in the original set
            if (result.getResource().isPresent() && !context.getResources().contains(result.getResource().get())) {
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

    /**
     * Add any implementation specific parameters to the map of parameters to send to the script.
     *
     * @param parameters The existing set of parameters for implementations to add to
     * @param context    The selection context
     */
    protected void addParametersForScript(final Map<String, Object> parameters, final C context) {
        parameters.put(CONTEXT_BINDING, context);
    }
}
