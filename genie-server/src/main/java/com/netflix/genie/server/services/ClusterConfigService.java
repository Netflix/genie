/*
 *
 *  Copyright 2014 Netflix, Inc.
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

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.model.Cluster;
import com.netflix.genie.common.model.ClusterCriteria;
import java.util.List;
import java.util.Set;

/**
 * Abstraction layer to encapsulate data ClusterConfig functionality.<br>
 * Classes implementing this abstraction layer must be thread-safe
 *
 * @author skrishnan
 * @author amsharma
 * @author tgianos
 */
public interface ClusterConfigService {

    /**
     * Get the cluster configuration by id.
     *
     * @param id unique id of cluster configuration to return
     * @return The cluster configuration
     * @throws CloudServiceException
     */
    Cluster getClusterConfig(final String id) throws CloudServiceException;

    /**
     * Get cluster info for various parameters. Null or empty parameters are
     * ignored.
     *
     * @param name cluster name
     * @param statuses valid types - Types.ClusterStatus
     * @param tags tags allocated to this cluster
     * @param minUpdateTime min time when cluster configuration was updated
     * @param maxUpdateTime max time when cluster configuration was updated
     * @param limit number of entries to return
     * @param page page number
     * @return All the clusters matching the criteria
     * @throws CloudServiceException
     */
    //TODO: Combine the two getAlls into one if possible
    List<Cluster> getClusterConfigs(
            final String name,
            final List<String> statuses,
            final List<String> tags,
            final Long minUpdateTime,
            final Long maxUpdateTime,
            final Integer limit,
            final Integer page) throws CloudServiceException;

    /**
     * Get the cluster configurations for various parameters.
     *
     * @param applicationId The application id
     * @param applicationName The application name
     * @param commandId The command identifier
     * @param commandName The command name
     * @param clusterCriterias List of cluster criteria
     * @return successful response, or one with HTTP error code
     */
    List<Cluster> getClusterConfigs(
            final String applicationId,
            final String applicationName,
            final String commandId,
            final String commandName,
            final Set<ClusterCriteria> clusterCriterias);

    /**
     * Create new cluster configuration.
     *
     * @param cluster The cluster to create
     * @return The created cluster
     * @throws CloudServiceException
     */
    Cluster createClusterConfig(final Cluster cluster) throws CloudServiceException;

    /**
     * Update a cluster configuration.
     *
     * @param id The id of the cluster to update
     * @param updateCluster the information to update the cluster with
     * @return the updated cluster
     * @throws CloudServiceException
     */
    Cluster updateClusterConfig(
            final String id,
            final Cluster updateCluster) throws CloudServiceException;

    /**
     * Delete a cluster configuration by id.
     *
     * @param id unique id for cluster to delete
     * @return the deleted cluster
     * @throws CloudServiceException
     */
    Cluster deleteClusterConfig(final String id) throws CloudServiceException;
}
