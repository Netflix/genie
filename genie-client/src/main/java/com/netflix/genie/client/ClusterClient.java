/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.genie.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jsonpatch.JsonPatch;
import com.netflix.genie.client.apis.ClusterService;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Client library for the Cluster Service.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Slf4j
public class ClusterClient extends BaseGenieClient {

    private ClusterService clusterService;

    /**
     * Constructor.
     *
     * @param configuration The configuration object containing all information for instantiating the client.
     *
     * @throws GenieException If there is any problem.
     */
    public ClusterClient(
        final GenieConfiguration configuration
    ) throws GenieException {
        super(configuration);
        clusterService = retrofit.create(ClusterService.class);
    }

    /******************* CRUD Methods   ***************************/

    /**
     * Create a cluster ing genie.
     *
     * @param cluster A cluster object.
     *
     * @return The id of the cluster created.
     *
     * @throws GenieException For any other error.
     * @throws IOException If the response received is not 2xx.
     */
    public String createCluster(
        final Cluster cluster
    ) throws IOException, GenieException {
        if (cluster == null) {
            throw new GeniePreconditionException("Cluster cannot be null.");
        }
        return getIdFromLocation(clusterService.createCluster(cluster).execute().headers().get("location"));
    }

    /**
     * Method to get a list of all the clusters.
     *
     * @return A list of clusters.
     * @throws GenieException       For any other error.
     * @throws IOException If the response received is not 2xx.
     */
    public List<Cluster> getClusters() throws IOException, GenieException {
        return this.getClusters(Collections.emptyMap());
    }

    /**
     * Method to get a list of all the clusters from Genie for the query parameters specified.
     *
     * @param options A list of query options
     *
     * @return A list of clusters.
     * @throws GenieException       For any other error.
     * @throws IOException If the response received is not 2xx.
     */
    public List<Cluster> getClusters(final Map<String, String> options) throws IOException, GenieException {

        final List<Cluster> clusterList = new ArrayList<>();
        final JsonNode jnode =  clusterService.getClusters(options).execute().body()
            .get("_embedded");
        if (jnode != null) {
            for (final JsonNode objNode : jnode.get("clusterList")) {
                final Cluster cluster  = mapper.treeToValue(objNode, Cluster.class);
                clusterList.add(cluster);
            }
        }
        return clusterList;
    }

    /**
     * Method to get a Cluster from Genie.
     *
     * @param clusterId The id of the cluster to get.
     * @return The cluster details.
     * @throws GenieException       For any other error.
     * @throws IOException If the response received is not 2xx.
     */
    public Cluster getCluster(
        final String clusterId
    ) throws IOException, GenieException {
        if (StringUtils.isEmpty(clusterId)) {
            throw new GeniePreconditionException("Missing required parameter: clusterId.");
        }
        return clusterService.getCluster(clusterId).execute().body();
    }

    /**
     * Method to delete a cluster from Genie.
     *
     * @param clusterId The id of the cluster.
     * @throws GenieException       For any other error.
     * @throws IOException If the response received is not 2xx.
     */
    public void deleteCluster(final String clusterId) throws IOException, GenieException {
        if (StringUtils.isEmpty(clusterId)) {
            throw new GeniePreconditionException("Missing required parameter: clusterId.");
        }
        clusterService.deleteCluster(clusterId).execute();
    }

    /**
     * Method to delete all clusters from Genie.
     *
     * @throws GenieException       For any other error.
     * @throws IOException If the response received is not 2xx.
     */
    public void deleteAllClusters() throws IOException, GenieException {
        clusterService.deleteAllClusters().execute();
    }

    /**
     * Method to patch a cluster using json patch instructions.
     *
     * @param clusterId The id of the cluster.
     * @param patch The patch object specifying all the instructions.
     *
     * @throws GenieException       For any other error.
     * @throws IOException If the response received is not 2xx.
     */
    public void patchCluster(final String clusterId, final JsonPatch patch) throws IOException, GenieException {
        if (StringUtils.isEmpty(clusterId)) {
            throw new GeniePreconditionException("Missing required parameter: clusterId.");
        }

        if (patch == null) {
            throw new GeniePreconditionException("Patch cannot be null");
        }

        clusterService.patchCluster(clusterId, patch).execute();
    }

    /**
     * Method to updated a cluster.
     *
     * @param clusterId The id of the cluster.
     * @param cluster The updated cluster object to use.
     *
     * @throws GenieException       For any other error.
     * @throws IOException If the response received is not 2xx.
     */
    public void updateCluster(final String clusterId, final Cluster cluster) throws IOException, GenieException {
        if (StringUtils.isEmpty(clusterId)) {
            throw new GeniePreconditionException("Missing required parameter: clusterId.");
        }

        if (cluster == null) {
            throw new GeniePreconditionException("Patch cannot be null");
        }

        clusterService.updateCluster(clusterId, cluster).execute();
    }

    /****************** Methods to manipulate configs for a cluster   *********************/

    /**
     * Method to get all the configs for a cluster.
     *
     * @param clusterId The id of the cluster.
     *
     * @return The set of configs for the cluster.
     * @throws GenieException       For any other error.
     * @throws IOException If the response received is not 2xx.
     */
    public Set<String> getConfigsForCluster(final String clusterId) throws IOException, GenieException {
        if (StringUtils.isEmpty(clusterId)) {
            throw new GeniePreconditionException("Missing required parameter: clusterId.");
        }

        return clusterService.getConfigsForCluster(clusterId).execute().body();
    }

    /**
     * Method to add configs to a cluster.
     *
     * @param clusterId The id of the cluster.
     * @param configs The set of configs to add.
     *
     * @throws GenieException       For any other error.
     * @throws IOException If the response received is not 2xx.
     */
    public void addConfigsToCluster(
        final String clusterId, final Set<String> configs
    ) throws IOException, GenieException {
        if (StringUtils.isEmpty(clusterId)) {
            throw new GeniePreconditionException("Missing required parameter: clusterId.");
        }

        if (configs == null || configs.isEmpty()) {
            throw new GeniePreconditionException("Configs cannot be null or empty");
        }

        clusterService.addConfigsToCluster(clusterId, configs).execute();
    }

    /**
     * Method to update configs for a cluster.
     *
     * @param clusterId The id of the cluster.
     * @param configs The set of configs to add.
     *
     * @throws GenieException       For any other error.
     * @throws IOException If the response received is not 2xx.
     */
    public void updateConfigsForCluster(
        final String clusterId, final Set<String> configs
    ) throws IOException, GenieException {
        if (StringUtils.isEmpty(clusterId)) {
            throw new GeniePreconditionException("Missing required parameter: clusterId.");
        }

        if (configs == null || configs.isEmpty()) {
            throw new GeniePreconditionException("Configs cannot be null or empty");
        }

        clusterService.updateConfigsForCluster(clusterId, configs).execute();
    }

    /**
     * Remove all configs for this cluster.
     *
     * @param clusterId The id of the cluster.
     *
     * @throws GenieException       For any other error.
     * @throws IOException If the response received is not 2xx.
     */
    public void removeAllConfigsForCluster(
        final String clusterId
    ) throws IOException, GenieException {
        if (StringUtils.isEmpty(clusterId)) {
            throw new GeniePreconditionException("Missing required parameter: clusterId.");
        }

        clusterService.removeAllConfigsForCluster(clusterId).execute();
    }

    /****************** Methods to manipulate tags for a cluster   *********************/

    /**
     * Method to get all the tags for a cluster.
     *
     * @param clusterId The id of the cluster.
     *
     * @return The set of configs for the cluster.
     * @throws GenieException       For any other error.
     * @throws IOException If the response received is not 2xx.
     */
    public Set<String> getTagsForCluster(final String clusterId) throws IOException, GenieException {
        if (StringUtils.isEmpty(clusterId)) {
            throw new GeniePreconditionException("Missing required parameter: clusterId.");
        }

        return clusterService.getTagsForCluster(clusterId).execute().body();
    }

    /**
     * Method to add tags to a cluster.
     *
     * @param clusterId The id of the cluster.
     * @param tags The set of tags to add.
     *
     * @throws GenieException       For any other error.
     * @throws IOException If the response received is not 2xx.
     */
    public void addTagsToCluster(
        final String clusterId, final Set<String> tags
    ) throws IOException, GenieException {
        if (StringUtils.isEmpty(clusterId)) {
            throw new GeniePreconditionException("Missing required parameter: clusterId.");
        }

        if (tags == null || tags.isEmpty()) {
            throw new GeniePreconditionException("Tags cannot be null or empty");
        }

        clusterService.addTagsToCluster(clusterId, tags).execute();
    }

    /**
     * Method to update tags for a cluster.
     *
     * @param clusterId The id of the cluster.
     * @param tags The set of tags to add.
     *
     * @throws GenieException       For any other error.
     * @throws IOException If the response received is not 2xx.
     */
    public void updateTagsForCluster(
        final String clusterId, final Set<String> tags
    ) throws IOException, GenieException {
        if (StringUtils.isEmpty(clusterId)) {
            throw new GeniePreconditionException("Missing required parameter: clusterId.");
        }

        if (tags == null || tags.isEmpty()) {
            throw new GeniePreconditionException("Tags cannot be null or empty");
        }

        clusterService.updateTagsForCluster(clusterId, tags).execute();
    }

    /**
     * Remove a tag from a cluster.
     *
     * @param clusterId The id of the cluster.
     * @param tag The tag to remove.
     *
     * @throws GenieException       For any other error.
     * @throws IOException If the response received is not 2xx.
     */
    public void removeTagFromCluster(
        final String clusterId,
        final String tag
    ) throws IOException, GenieException {
        if (StringUtils.isEmpty(clusterId)) {
            throw new GeniePreconditionException("Missing required parameter: clusterId.");
        }

        if (StringUtils.isEmpty(tag)) {
            throw new GeniePreconditionException("Missing required parameter: tag.");
        }

        clusterService.removeTagForCluster(clusterId, tag).execute();
    }

    /**
     * Remove all tags for this cluster.
     *
     * @param clusterId The id of the cluster.
     *
     * @throws GenieException       For any other error.
     * @throws IOException If the response received is not 2xx.
     */
    public void removeAllTagsForCluster(
        final String clusterId
    ) throws IOException, GenieException {
        if (StringUtils.isEmpty(clusterId)) {
            throw new GeniePreconditionException("Missing required parameter: clusterId.");
        }

        clusterService.removeAllTagsForCluster(clusterId).execute();
    }
}
