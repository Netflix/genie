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

import com.netflix.genie.common.external.dtos.v4.Cluster;
import com.netflix.genie.common.internal.util.PropertiesMapCache;
import com.netflix.genie.web.exceptions.checked.ResourceSelectionException;
import com.netflix.genie.web.properties.ClusterSelectorScriptProperties;
import com.netflix.genie.web.selectors.ClusterSelectionContext;
import com.netflix.genie.web.selectors.ClusterSelector;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * Extension of {@link ResourceSelectorScript} that delegates selection of a job's cluster when more than one choice is
 * available. See also: {@link ClusterSelector}.
 * <p>
 * The contract between the script and the Java code is that the script will be supplied global variables
 * {@code clusters} and {@code jobRequest} which will be a {@code Set} of {@link Cluster} instances
 * matching the cluster criteria and the job request that kicked off this evaluation respectively. The code expects the
 * script to return a {@link ResourceSelectorScriptResult} instance.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class ClusterSelectorManagedScript extends ResourceSelectorScript<Cluster, ClusterSelectionContext> {

    static final String CLUSTERS_BINDING = "clustersParameter";

    /**
     * Constructor.
     *
     * @param scriptManager    script manager
     * @param properties       script manager properties
     * @param registry         meter registry
     * @param propertyMapCache dynamic properties map cache
     */
    public ClusterSelectorManagedScript(
        final ScriptManager scriptManager,
        final ClusterSelectorScriptProperties properties,
        final MeterRegistry registry,
        final PropertiesMapCache propertyMapCache
    ) {
        super(scriptManager, properties, registry, propertyMapCache);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResourceSelectorScriptResult<Cluster> selectResource(
        final ClusterSelectionContext context
    ) throws ResourceSelectionException {
        log.debug(
            "Called to attempt to select a cluster from {} for job {}",
            context.getClusters(),
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
        final ClusterSelectionContext context
    ) {
        super.addParametersForScript(parameters, context);

        // TODO: Remove once internal scripts migrate to use context directly
        parameters.put(CLUSTERS_BINDING, context.getClusters());
    }
}
