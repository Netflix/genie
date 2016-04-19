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
        final JsonNode jnode =  clusterService.getClusters(options).execute().body()
            .get("_embedded")
            .get("clusterList");

        final List<Cluster> clusterList = new ArrayList<>();
        for (final JsonNode objNode : jnode) {
            final Cluster cluster  = mapper.treeToValue(objNode, Cluster.class);
            clusterList.add(cluster);
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
}
