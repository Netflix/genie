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
import com.netflix.genie.client.configs.GenieNetworkConfiguration;
import com.netflix.genie.client.exceptions.GenieClientException;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.Command;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Interceptor;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import javax.validation.constraints.NotEmpty;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
     * @param url                       The endpoint URL of the Genie API. Not null or empty
     * @param interceptors              Any interceptors to configure the client with, can include security ones
     * @param genieNetworkConfiguration The network configuration parameters. Could be null
     * @throws GenieClientException On error
     */
    public ClusterClient(
        @NotEmpty final String url,
        @Nullable final List<Interceptor> interceptors,
        @Nullable final GenieNetworkConfiguration genieNetworkConfiguration
    ) throws GenieClientException {
        super(url, interceptors, genieNetworkConfiguration);
        this.clusterService = this.getService(ClusterService.class);
    }

    /* CRUD Methods */

    /**
     * Create a cluster ing genie.
     *
     * @param cluster A cluster object.
     * @return The id of the cluster created.
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    public String createCluster(
        final Cluster cluster
    ) throws IOException, GenieClientException {
        if (cluster == null) {
            throw new IllegalArgumentException("Cluster cannot be null.");
        }
        return getIdFromLocation(clusterService.createCluster(cluster).execute().headers().get("location"));
    }

    /**
     * Method to get a list of all the clusters.
     *
     * @return A list of clusters.
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    public List<Cluster> getClusters() throws IOException, GenieClientException {
        return this.getClusters(
            null,
            null,
            null,
            null,
            null
        );
    }

    /**
     * Method to get a list of all the clusters from Genie for the query parameters specified.
     *
     * @param name          The name of the cluster.
     * @param statusList    The list of statuses.
     * @param tagList       The list of tags.
     * @param minUpdateTime Minimum Time after which cluster was updated.
     * @param maxUpdateTime Maximum Time before which cluster was updated.
     * @return A list of clusters.
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    public List<Cluster> getClusters(
        final String name,
        final List<String> statusList,
        final List<String> tagList,
        final Long minUpdateTime,
        final Long maxUpdateTime
    ) throws IOException, GenieClientException {

        final List<Cluster> clusterList = new ArrayList<>();
        final JsonNode jnode = clusterService.getClusters(
            name,
            statusList,
            tagList,
            minUpdateTime,
            maxUpdateTime
        ).execute().body()
            .get("_embedded");
        if (jnode != null) {
            for (final JsonNode objNode : jnode.get("clusterList")) {
                final Cluster cluster = this.treeToValue(objNode, Cluster.class);
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
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    public Cluster getCluster(
        final String clusterId
    ) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(clusterId)) {
            throw new IllegalArgumentException("Missing required parameter: clusterId.");
        }
        return clusterService.getCluster(clusterId).execute().body();
    }

    /**
     * Method to delete a cluster from Genie.
     *
     * @param clusterId The id of the cluster.
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    public void deleteCluster(final String clusterId) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(clusterId)) {
            throw new IllegalArgumentException("Missing required parameter: clusterId.");
        }
        clusterService.deleteCluster(clusterId).execute();
    }

    /**
     * Method to delete all clusters from Genie.
     *
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    public void deleteAllClusters() throws IOException, GenieClientException {
        clusterService.deleteAllClusters().execute();
    }

    /**
     * Method to patch a cluster using json patch instructions.
     *
     * @param clusterId The id of the cluster.
     * @param patch     The patch object specifying all the instructions.
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    public void patchCluster(final String clusterId, final JsonPatch patch) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(clusterId)) {
            throw new IllegalArgumentException("Missing required parameter: clusterId.");
        }

        if (patch == null) {
            throw new IllegalArgumentException("Patch cannot be null");
        }

        clusterService.patchCluster(clusterId, patch).execute();
    }

    /**
     * Method to updated a cluster.
     *
     * @param clusterId The id of the cluster.
     * @param cluster   The updated cluster object to use.
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    public void updateCluster(final String clusterId, final Cluster cluster) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(clusterId)) {
            throw new IllegalArgumentException("Missing required parameter: clusterId.");
        }

        if (cluster == null) {
            throw new IllegalArgumentException("Patch cannot be null");
        }

        clusterService.updateCluster(clusterId, cluster).execute();
    }

    /****************** Methods to manipulate configs for a cluster   *********************/

    /**
     * Method to get all the configs for a cluster.
     *
     * @param clusterId The id of the cluster.
     * @return The set of configs for the cluster.
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    public Set<String> getConfigsForCluster(final String clusterId) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(clusterId)) {
            throw new IllegalArgumentException("Missing required parameter: clusterId.");
        }

        return clusterService.getConfigsForCluster(clusterId).execute().body();
    }

    /**
     * Method to add configs to a cluster.
     *
     * @param clusterId The id of the cluster.
     * @param configs   The set of configs to add.
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    public void addConfigsToCluster(
        final String clusterId, final Set<String> configs
    ) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(clusterId)) {
            throw new IllegalArgumentException("Missing required parameter: clusterId.");
        }

        if (configs == null || configs.isEmpty()) {
            throw new IllegalArgumentException("Configs cannot be null or empty");
        }

        clusterService.addConfigsToCluster(clusterId, configs).execute();
    }

    /**
     * Method to update configs for a cluster.
     *
     * @param clusterId The id of the cluster.
     * @param configs   The set of configs to add.
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    public void updateConfigsForCluster(
        final String clusterId, final Set<String> configs
    ) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(clusterId)) {
            throw new IllegalArgumentException("Missing required parameter: clusterId.");
        }

        if (configs == null || configs.isEmpty()) {
            throw new IllegalArgumentException("Configs cannot be null or empty");
        }

        clusterService.updateConfigsForCluster(clusterId, configs).execute();
    }

    /**
     * Remove all configs for this cluster.
     *
     * @param clusterId The id of the cluster.
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    public void removeAllConfigsForCluster(
        final String clusterId
    ) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(clusterId)) {
            throw new IllegalArgumentException("Missing required parameter: clusterId.");
        }

        clusterService.removeAllConfigsForCluster(clusterId).execute();
    }

    /****************** Methods to manipulate dependencies for a cluster   *********************/

    /**
     * Method to get all the dependency files for an cluster.
     *
     * @param clusterId The id of the cluster.
     * @return The set of dependencies for the cluster.
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues
     */
    public Set<String> getDependenciesForCluster(final String clusterId) throws IOException,
        GenieClientException {
        if (StringUtils.isEmpty(clusterId)) {
            throw new IllegalArgumentException("Missing required parameter: clusterId.");
        }

        return clusterService.getDependenciesForCluster(clusterId).execute().body();
    }

    /**
     * Method to add dependencies to a cluster.
     *
     * @param clusterId    The id of the cluster.
     * @param dependencies The set of dependencies to add.
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues
     */
    public void addDependenciesToCluster(
        final String clusterId, final Set<String> dependencies
    ) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(clusterId)) {
            throw new IllegalArgumentException("Missing required parameter: clusterId.");
        }

        if (dependencies == null || dependencies.isEmpty()) {
            throw new IllegalArgumentException("Dependencies cannot be null or empty");
        }

        clusterService.addDependenciesToCluster(clusterId, dependencies).execute();
    }

    /**
     * Method to update dependencies for a cluster.
     *
     * @param clusterId    The id of the cluster.
     * @param dependencies The set of dependencies to add.
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues
     */
    public void updateDependenciesForCluster(
        final String clusterId, final Set<String> dependencies
    ) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(clusterId)) {
            throw new IllegalArgumentException("Missing required parameter: clusterId.");
        }

        if (dependencies == null || dependencies.isEmpty()) {
            throw new IllegalArgumentException("Dependencies cannot be null or empty");
        }

        clusterService.updateDependenciesForCluster(clusterId, dependencies).execute();
    }

    /**
     * Remove all dependencies for this cluster.
     *
     * @param clusterId The id of the cluster.
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues
     */
    public void removeAllDependenciesForCluster(
        final String clusterId
    ) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(clusterId)) {
            throw new IllegalArgumentException("Missing required parameter: clusterId.");
        }

        clusterService.removeAllDependenciesForCluster(clusterId).execute();
    }

    /****************** Methods to manipulate commands for a cluster   *********************/

    /**
     * Method to get all the commands for a cluster.
     *
     * @param clusterId The id of the cluster.
     * @return The set of commands for the cluster.
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    public List<Command> getCommandsForCluster(final String clusterId) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(clusterId)) {
            throw new IllegalArgumentException("Missing required parameter: clusterId.");
        }

        return clusterService.getCommandsForCluster(clusterId).execute().body();
    }

    /**
     * Method to add commands to a cluster.
     *
     * @param clusterId  The id of the cluster.
     * @param commandIds The list of commands to add.
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    public void addCommandsToCluster(
        final String clusterId, final List<String> commandIds
    ) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(clusterId)) {
            throw new IllegalArgumentException("Missing required parameter: clusterId.");
        }

        if (commandIds == null || commandIds.isEmpty()) {
            throw new IllegalArgumentException("Command Ids cannot be null or empty");
        }

        clusterService.addCommandsToCluster(clusterId, commandIds).execute();
    }

    /**
     * Method to update commands for a cluster.
     *
     * @param clusterId  The id of the cluster.
     * @param commandIds The set of commands to add.
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    public void updateCommandsForCluster(
        final String clusterId, final List<String> commandIds
    ) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(clusterId)) {
            throw new IllegalArgumentException("Missing required parameter: clusterId.");
        }

        if (commandIds == null || commandIds.isEmpty()) {
            throw new IllegalArgumentException("commandIds cannot be null or empty");
        }

        clusterService.setCommandsForCluster(clusterId, commandIds).execute();
    }

    /**
     * Remove a command from a cluster.
     *
     * @param clusterId The id of the cluster.
     * @param commandId The id of the command to remove.
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    public void removeCommandFromCluster(
        final String clusterId,
        final String commandId
    ) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(clusterId)) {
            throw new IllegalArgumentException("Missing required parameter: clusterId.");
        }

        if (StringUtils.isEmpty(commandId)) {
            throw new IllegalArgumentException("Missing required parameter: commandId.");
        }

        clusterService.removeCommandForCluster(clusterId, commandId).execute();
    }

    /**
     * Remove all commands for this cluster.
     *
     * @param clusterId The id of the cluster.
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    public void removeAllCommandsForCluster(
        final String clusterId
    ) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(clusterId)) {
            throw new IllegalArgumentException("Missing required parameter: clusterId.");
        }

        clusterService.removeAllCommandsForCluster(clusterId).execute();
    }

    /****************** Methods to manipulate tags for a cluster   *********************/

    /**
     * Method to get all the tags for a cluster.
     *
     * @param clusterId The id of the cluster.
     * @return The set of tags for the cluster.
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    public Set<String> getTagsForCluster(final String clusterId) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(clusterId)) {
            throw new IllegalArgumentException("Missing required parameter: clusterId.");
        }

        return clusterService.getTagsForCluster(clusterId).execute().body();
    }

    /**
     * Method to add tags to a cluster.
     *
     * @param clusterId The id of the cluster.
     * @param tags      The set of tags to add.
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    public void addTagsToCluster(
        final String clusterId, final Set<String> tags
    ) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(clusterId)) {
            throw new IllegalArgumentException("Missing required parameter: clusterId.");
        }

        if (tags == null || tags.isEmpty()) {
            throw new IllegalArgumentException("Tags cannot be null or empty");
        }

        clusterService.addTagsToCluster(clusterId, tags).execute();
    }

    /**
     * Method to update tags for a cluster.
     *
     * @param clusterId The id of the cluster.
     * @param tags      The set of tags to add.
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    public void updateTagsForCluster(
        final String clusterId, final Set<String> tags
    ) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(clusterId)) {
            throw new IllegalArgumentException("Missing required parameter: clusterId.");
        }

        if (tags == null || tags.isEmpty()) {
            throw new IllegalArgumentException("Tags cannot be null or empty");
        }

        clusterService.updateTagsForCluster(clusterId, tags).execute();
    }

    /**
     * Remove a tag from a cluster.
     *
     * @param clusterId The id of the cluster.
     * @param tag       The tag to remove.
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    public void removeTagFromCluster(
        final String clusterId,
        final String tag
    ) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(clusterId)) {
            throw new IllegalArgumentException("Missing required parameter: clusterId.");
        }

        if (StringUtils.isEmpty(tag)) {
            throw new IllegalArgumentException("Missing required parameter: tag.");
        }

        clusterService.removeTagForCluster(clusterId, tag).execute();
    }

    /**
     * Remove all tags for this cluster.
     *
     * @param clusterId The id of the cluster.
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    public void removeAllTagsForCluster(
        final String clusterId
    ) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(clusterId)) {
            throw new IllegalArgumentException("Missing required parameter: clusterId.");
        }

        clusterService.removeAllTagsForCluster(clusterId).execute();
    }
}
