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
package com.netflix.genie.client.apis;

import com.fasterxml.jackson.databind.JsonNode;
import com.netflix.genie.common.dto.Cluster;
import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.DELETE;
import retrofit2.http.GET;
import retrofit2.http.POST;
import retrofit2.http.PUT;
import retrofit2.http.Path;
import retrofit2.http.QueryMap;

import java.util.Map;
import java.util.Set;

/**
 * An interface that provides all methods needed for the Genie cluster client implementation.
 *
 * @author amsharma
 * @since 3.0.0
 */
public interface ClusterService {

    /**
     * Path to Clusters.
     */
    String CLUSTER_URL_SUFFIX = "/api/v3/clusters";

    /******************* CRUD Methods   ***************************/

    /**
     * Method to create a cluster in Genie.
     *
     * @param cluster The cluster object.
     * @return A callable object.
     */
    @POST(CLUSTER_URL_SUFFIX)
    Call<Void> createCluster(@Body final Cluster cluster);

    /**
     * Method to update a cluster in Genie.
     *
     * @param clusterId The id of the cluster to udpate.
     * @param cluster The cluster object.
     * @return A callable object.
     */
    @PUT(CLUSTER_URL_SUFFIX + "/{id}")
    Call<Void> updateCluster(@Path("id") final String clusterId, @Body final Cluster cluster);

    /**
     * Method to get all clusters from Genie.
     *
     * @param options A map of query parameters to be used to filter the clusters.
     * @return A callable object.
     */
    @GET(CLUSTER_URL_SUFFIX)
    Call<JsonNode> getClusters(@QueryMap final Map<String, String> options);

    /**
     * Method to fetch a single job from Genie.
     *
     * @param clusterId The id of the cluster to get.
     * @return A callable object.
     */
    @GET(CLUSTER_URL_SUFFIX + "/{id}")
    Call<Cluster> getCluster(@Path("id") final String clusterId);

    /**
     * Method to delete a cluster in Genie.
     *
     * @param clusterId The id of the cluster.
     * @return A callable object.
     */
    @DELETE(CLUSTER_URL_SUFFIX + "/{id}")
    Call<Void> deleteCluster(@Path("id") final String clusterId);

    /**
     * Method to delete all clusters in Genie.
     *
     * @return A callable object.
     */
    @DELETE(CLUSTER_URL_SUFFIX)
    Call<Void> deleteAllClusters();

    /****************** Methods to manipulate commands for a cluster   *********************/

    /****************** Methods to manipulate configs for a cluster   *********************/

    /**
     * Method to get configs for a cluster in Genie.
     *
     * @param clusterId The id of the cluster.
     * @return A callable object.
     */
    @GET(CLUSTER_URL_SUFFIX + "/{id}/configs")
    Call<Set<String>> getConfigsForCluster(@Path("id") final String clusterId);

    /**
     * Method to add configs to a cluster in Genie.
     *
     * @param clusterId The id of the cluster..
     * @param configs The configs to be added.
     * @return A callable object.
     */
    @POST(CLUSTER_URL_SUFFIX + "/{id}/configs")
    Call<Void> addConfigsToCluster(@Path("id") final String clusterId, @Body final Set<String> configs);

    /**
     * Method to update configs for a cluster in Genie.
     *
     * @param clusterId The id of the cluster..
     * @param configs The configs to be added.
     * @return A callable object.
     */
    @PUT(CLUSTER_URL_SUFFIX + "/{id}/configs")
    Call<Void> updateConfigsForCluster(@Path("id") final String clusterId, @Body final Set<String> configs);

    /**
     * Method to delete all configs for a cluster in Genie.
     *
     * @param clusterId The id of the cluster.
     * @return A callable object.
     */
    @DELETE(CLUSTER_URL_SUFFIX + "/{id}/configs")
    Call<Void> removeAllConfigsForCluster(@Path("id") final String clusterId);

    /****************** Methods to manipulate tags for a cluster   *********************/

    /**
     * Method to get tags for a cluster in Genie.
     *
     * @param clusterId The id of the cluster.
     * @return A callable object.
     */
    @GET(CLUSTER_URL_SUFFIX + "/{id}/tags")
    Call<Set<String>> getTagsForCluster(@Path("id") final String clusterId);

    /**
     * Method to add tags to a cluster in Genie.
     *
     * @param clusterId The id of the cluster..
     * @param tags The tags to be added.
     * @return A callable object.
     */
    @POST(CLUSTER_URL_SUFFIX + "/{id}/tags")
    Call<Void> addTagsToCluster(@Path("id") final String clusterId, @Body final Set<String> tags);

    /**
     * Method to update tags for a cluster in Genie.
     *
     * @param clusterId The id of the cluster..
     * @param tags The tags to be added.
     * @return A callable object.
     */
    @PUT(CLUSTER_URL_SUFFIX + "/{id}/tags")
    Call<Void> updateTagsForCluster(@Path("id") final String clusterId, @Body final Set<String> tags);

    /**
     * Method to delete a tag for a cluster in Genie.
     *
     * @param clusterId The id of the cluster.
     * @param tag The tag to delete.
     * @return A callable object.
     */
    @DELETE(CLUSTER_URL_SUFFIX + "/{id}/tags/{tag}")
    Call<Void> removeTagForCluster(@Path("id") final String clusterId, @Path("tag") final String tag);

    /**
     * Method to delete all tags for a cluster in Genie.
     *
     * @param clusterId The id of the cluster.
     * @return A callable object.
     */
    @DELETE(CLUSTER_URL_SUFFIX + "/{id}/tags")
    Call<Void> removeAllTagsForCluster(@Path("id") final String clusterId);
}
