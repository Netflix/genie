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
import com.netflix.genie.common.external.dtos.v4.JobRequest;
import com.netflix.genie.web.exceptions.checked.ResourceSelectionException;
import com.netflix.genie.web.properties.ClusterSelectorScriptProperties;
import com.netflix.genie.web.selectors.ClusterSelector;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Set;

/**
 * Extension of {@link ResourceSelectorScript} that delegates selection of a job's cluster when more than one choice is
 * available. See also: {@link ClusterSelector}.
 * <p>
 * The contract between the script and the Java code is that the script will be supplied global variables
 * {@code clusters} and {@code jobRequest} which will be JSON strings representing the list (array) of clusters
 * matching the cluster criteria tags and the job request that kicked off this evaluation. The code expects the script
 * to either return the id of the cluster if one is selected or null if none was selected.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class ClusterSelectorManagedScript extends ResourceSelectorScript<Cluster> {

    static final String CLUSTERS_BINDING = "clustersParameter";

    /**
     * Constructor.
     *
     * @param scriptManager script manager
     * @param properties    script manager properties
     * @param registry      meter registry
     */
    public ClusterSelectorManagedScript(
        final ScriptManager scriptManager,
        final ClusterSelectorScriptProperties properties,
        final MeterRegistry registry
    ) {
        super(scriptManager, properties, registry);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResourceSelectorScriptResult<Cluster> selectResource(
        final Set<Cluster> resources,
        final JobRequest jobRequest,
        final String jobId
    ) throws ResourceSelectionException {
        log.debug("Called to attempt to select a cluster from {} for job {}", resources, jobId);

        return super.selectResource(resources, jobRequest, jobId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void addParametersForScript(
        final Map<String, Object> parameters,
        final Set<Cluster> resources,
        final JobRequest jobRequest
    ) {
        super.addParametersForScript(parameters, resources, jobRequest);
        parameters.put(CLUSTERS_BINDING, resources);
    }
}
