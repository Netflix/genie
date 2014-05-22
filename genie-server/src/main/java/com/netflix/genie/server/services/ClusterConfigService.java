/*
 *
 *  Copyright 2013 Netflix, Inc.
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
package com.netflix.genie.server.services;

import java.util.ArrayList;
import java.util.List;

import com.netflix.genie.common.messages.ClusterConfigRequest;
import com.netflix.genie.common.messages.ClusterConfigResponse;
import com.netflix.genie.common.model.ClusterCriteria;
import com.netflix.genie.common.model.Types;

/**
 * Abstraction layer to encapsulate data ClusterConfig functionality.<br>
 * Classes implementing this abstraction layer must be thread-safe
 *
 * @author skrishnan
 * @author amsharma
 */
public interface ClusterConfigService {

    /**
     * Gets the cluster config by id.
     *
     * @param id unique id of cluster config to return
     * @return successful response, or one with HTTP error code
     */
    ClusterConfigResponse getClusterConfig(String id);

    /**
     * Get the cluster config by various params.
     *
     * @param id unique id for cluster (can be a pattern)
     * @param name name of cluster (can be a pattern)
     * @param commandId commands supported by the cluster
     * @param tags tags allocated to this cluster
     * @param status status for cluster
     * @return successful response, or one with HTTP error code
     */
    ClusterConfigResponse getClusterConfig(String id, String name,
            String commandId, List<String> tags, Types.ClusterStatus status);

    /**
     * Get cluster info for various params Null parameters are ignored.
     *
     * @param id unique id for cluster
     * @param name cluster name
     * @param status valid types - Types.ClusterStatus
     * @param tags tags allocated to this cluster
     * @param minUpdateTime min time when cluster config was updated
     * @param maxUpdateTime max time when cluster config was updated
     * @param limit number of entries to return
     * @param page page number
     * @return successful response, or one with HTTP error code
     */
    ClusterConfigResponse getClusterConfig(String id, String name,
            List<String> status, List<String> tags, Long minUpdateTime,
            Long maxUpdateTime, Integer limit, Integer page);

    /**
     * Get the cluster config for various params.
     *
     * @param applicationId The application id
     * @param applicationName The application name
     * @param commandId The command identifier
     * @param commandName The command name
     * @param clusterCriteriaList List of cluster criteria
     * @return successful response, or one with HTTP error code
     */
    ClusterConfigResponse getClusterConfig(String applicationId,
            String applicationName, String commandId, String commandName,
            ArrayList<ClusterCriteria> clusterCriteriaList);

    /**
     * Create new cluster config.
     *
     * @param request
     * @return successful response, or one with HTTP error code
     */
    ClusterConfigResponse createClusterConfig(ClusterConfigRequest request);

    /**
     * Update/insert cluster config.
     *
     * @param request enscapsulates cluster config to upsert, must contain valid
     * id
     * @return successful response, or one with HTTP error code
     */
    ClusterConfigResponse updateClusterConfig(ClusterConfigRequest request);

    /**
     * Delete a cluster config by id.
     *
     * @param id unique id for cluster to delete
     * @return successful response, or one with HTTP error code
     */
    ClusterConfigResponse deleteClusterConfig(String id);
}
