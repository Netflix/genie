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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.internal.dto.v4.Cluster;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieClusterNotFoundException;
import com.netflix.genie.web.apis.rest.v3.controllers.DtoConverters;
import com.netflix.genie.web.exceptions.checked.ScriptExecutionException;
import com.netflix.genie.web.exceptions.checked.ScriptNotConfiguredException;
import com.netflix.genie.web.properties.ClusterSelectorScriptProperties;
import com.netflix.genie.web.services.ClusterSelector;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Implementation of {@link ManagedScript} that delegates selection of a job's cluster when more than one choice is
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
public class ClusterSelectorScript extends ManagedScript {
    private static final String CLUSTERS_BINDING = "clusters";
    private static final String JOB_REQUEST_BINDING = "jobRequest";

    /**
     * Constructor.
     *
     * @param scriptManager script manager
     * @param properties    script manager properties
     * @param mapper        object mapper
     * @param registry      meter registry
     */
    public ClusterSelectorScript(
        final ScriptManager scriptManager,
        final ClusterSelectorScriptProperties properties,
        final ObjectMapper mapper,
        final MeterRegistry registry
    ) {
        super(scriptManager, properties, mapper, registry);
    }

    /**
     * Evaluate the script to select a cluster.
     *
     * @param jobRequest the job request
     * @param clusters   the set of clusters that matched criteria
     * @return the selected cluster from the input list, or null to indicate no preference
     * @throws ScriptNotConfiguredException  if the script is not configured or not yet compiled
     * @throws ScriptExecutionException      if the script fails to execute
     * @throws GenieClusterNotFoundException if the script returns an id not in the input cluster list
     */
    @Nullable
    public Cluster selectCluster(
        final JobRequest jobRequest,
        final Set<Cluster> clusters
    ) throws ScriptNotConfiguredException, ScriptExecutionException, GenieClusterNotFoundException {

        // TODO: For now for backwards compatibility with selector scripts continue writing Clusters out in
        //       V3 format. Change to V4 once stabilize a bit more
        final Set<com.netflix.genie.common.dto.Cluster> v3Clusters = clusters
            .stream()
            .map(DtoConverters::toV3Cluster)
            .collect(Collectors.toSet());

        final Map<String, Object> scriptParameters = ImmutableMap.of(
            CLUSTERS_BINDING,
            v3Clusters,
            JOB_REQUEST_BINDING,
            jobRequest
        );

        final String selectedClusterId = (String) this.evaluateScript(scriptParameters);
        if (StringUtils.isBlank(selectedClusterId)) {
            return null;
        }

        for (final Cluster cluster : clusters) {
            if (selectedClusterId.equals(cluster.getId())) {
                return cluster;
            }
        }

        throw new GenieClusterNotFoundException("No such cluster in input list: " + selectedClusterId);
    }

}
