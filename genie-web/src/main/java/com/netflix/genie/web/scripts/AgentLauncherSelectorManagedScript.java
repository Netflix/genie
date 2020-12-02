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
package com.netflix.genie.web.scripts;

import com.netflix.genie.common.internal.util.PropertiesMapCache;
import com.netflix.genie.web.agent.launchers.AgentLauncher;
import com.netflix.genie.web.exceptions.checked.ResourceSelectionException;
import com.netflix.genie.web.properties.AgentLauncherSelectorScriptProperties;
import com.netflix.genie.web.selectors.AgentLauncherSelectionContext;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * Extension of {@link ResourceSelectorScript} that delegates selection of a job's agent launcher when more than one
 * choice is available. See also: {@link com.netflix.genie.web.selectors.AgentLauncherSelector}.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class AgentLauncherSelectorManagedScript
    extends ResourceSelectorScript<AgentLauncher, AgentLauncherSelectionContext> {

    static final String AGENT_LAUNCHERS_BINDING = "agentLaunchersParameter";

    /**
     * Constructor.
     *
     * @param scriptManager    script manager
     * @param properties       script manager properties
     * @param registry         meter registry
     * @param propertyMapCache dynamic properties map cache
     */
    public AgentLauncherSelectorManagedScript(
        final ScriptManager scriptManager,
        final AgentLauncherSelectorScriptProperties properties,
        final MeterRegistry registry,
        final PropertiesMapCache propertyMapCache
    ) {
        super(scriptManager, properties, registry, propertyMapCache);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResourceSelectorScriptResult<AgentLauncher> selectResource(
        final AgentLauncherSelectionContext context
    ) throws ResourceSelectionException {
        log.debug(
            "Called to attempt to select agent launcher from {} for job {}",
            context.getAgentLaunchers(),
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
        final AgentLauncherSelectionContext context
    ) {
        super.addParametersForScript(parameters, context);

        // TODO: Remove once internal scripts migrate to use context directly
        parameters.put(AGENT_LAUNCHERS_BINDING, context.getAgentLaunchers());
    }
}
