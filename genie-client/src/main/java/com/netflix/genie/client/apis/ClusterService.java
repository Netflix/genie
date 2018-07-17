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
import com.github.fge.jsonpatch.JsonPatch;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.Command;
import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.DELETE;
import retrofit2.http.GET;
import retrofit2.http.PATCH;
import retrofit2.http.POST;
import retrofit2.http.PUT;
import retrofit2.http.Path;
import retrofit2.http.Query;

import java.util.List;
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
     * @param clusterId The id of the cluster to update.
     * @param cluster   The cluster object.
     * @return A callable object.
     */
    @PUT(CLUSTER_URL_SUFFIX + "/{id}")
    Call<Void> updateCluster(@Path("id") final String clusterId, @Body final Cluster cluster);

    /**
     * Method to get clusters from Genie based on filters specified.
     *
     * @param name          The name of the cluster.
     * @param statusList    The list of statuses.
     * @param tagList       The list of tags.
     * @param minUpdateTime Minimum Time after which cluster was updated.
     * @param maxUpdateTime Maximum Time before which cluster was updated.
     * @return A callable object.
     */
    @GET(CLUSTER_URL_SUFFIX)
    Call<JsonNode> getClusters(
        @Query("name") final String name,
        @Query("status") final List<String> statusList,
        @Query("tag") final List<String> tagList,
        @Query("minUpdateTime") final Long minUpdateTime,
        @Query("maxUpdateTime") final Long maxUpdateTime
    );

    /**
     *  getClusters(
     @RequestParam(value = "name", required = false) final String name,
     @RequestParam(value = "status", required = false) final Set<String> statuses,
     @RequestParam(value = "tag", required = false) final Set<String> tags,
     @RequestParam(value = "minUpdateTime", required = false) final Long minUpdateTime,
     @RequestParam(value = "maxUpdateTime", required = false) final Long maxUpdateTime,
     @PageableDefault(page = 0, size = 64, sort = {"updated"}, direction = Sort.Direction.DESC) final Pageable page,
     final PagedResourcesAssembler<Cluster> assembler
     )
     *
     */

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

    /**
     * Patch a cluster using JSON Patch.
     *
     * @param clusterId The id of the cluster to patch
     * @param patch     The JSON Patch instructions
     * @return A callable object.
     */
    @PATCH(CLUSTER_URL_SUFFIX + "/{id}")
    Call<Void> patchCluster(@Path("id") final String clusterId, @Body final JsonPatch patch);

    /****************** Methods to manipulate commands for a cluster   *********************/

    /**
     * Method to get commmands for a cluster in Genie.
     *
     * @param clusterId The id of the cluster.
     * @return A callable object.
     */
    @GET(CLUSTER_URL_SUFFIX + "/{id}/commands")
    Call<List<Command>> getCommandsForCluster(@Path("id") final String clusterId);

    /**
     * Method to add commands to a cluster in Genie.
     *
     * @param clusterId  The id of the cluster..
     * @param commandIds The command Ids to be added.
     * @return A callable object.
     */
    @POST(CLUSTER_URL_SUFFIX + "/{id}/commands")
    Call<Void> addCommandsToCluster(@Path("id") final String clusterId, @Body final List<String> commandIds);

    /**
     * Method to override and set commands for a cluster in Genie.
     *
     * @param clusterId  The id of the cluster..
     * @param commandIds The command Ids to be added.
     * @return A callable object.
     */
    @PUT(CLUSTER_URL_SUFFIX + "/{id}/commands")
    Call<Void> setCommandsForCluster(@Path("id") final String clusterId, @Body final List<String> commandIds);

    /**
     * Method to delete a command for a cluster in Genie.
     *
     * @param clusterId The id of the cluster.
     * @param commandId The command to delete.
     * @return A callable object.
     */
    @DELETE(CLUSTER_URL_SUFFIX + "/{id}/commands/{commandId}")
    Call<Void> removeCommandForCluster(@Path("id") final String clusterId, @Path("commandId") final String commandId);

    /**
     * Method to delete all commands for a cluster in Genie.
     *
     * @param clusterId The id of the cluster.
     * @return A callable object.
     */
    @DELETE(CLUSTER_URL_SUFFIX + "/{id}/commands")
    Call<Void> removeAllCommandsForCluster(@Path("id") final String clusterId);

    /****************** Methods to manipulate dependencies for a cluster   *********************/

    /**
     * Method to get dependency files for a cluster in Genie.
     *
     * @param clusterId The id of the cluster.
     * @return A callable object.
     */
    @GET(CLUSTER_URL_SUFFIX + "/{id}/dependencies")
    Call<Set<String>> getDependenciesForCluster(@Path("id") final String clusterId);

    /**
     * Method to add dependencies to a cluster in Genie.
     *
     * @param clusterId    The id of the cluster..
     * @param dependencies The dependencies to be added.
     * @return A callable object.
     */
    @POST(CLUSTER_URL_SUFFIX + "/{id}/dependencies")
    Call<Void> addDependenciesToCluster(
        @Path("id") final String clusterId,
        @Body final Set<String> dependencies
    );

    /**
     * Method to update dependencies for a cluster in Genie.
     *
     * @param clusterId    The id of the cluster..
     * @param dependencies The dependencies to be added.
     * @return A callable object.
     */
    @PUT(CLUSTER_URL_SUFFIX + "/{id}/dependencies")
    Call<Void> updateDependenciesForCluster(
        @Path("id") final String clusterId,
        @Body final Set<String> dependencies
    );

    /**
     * Method to delete all dependencies for a cluster in Genie.
     *
     * @param clusterId The id of the cluster.
     * @return A callable object.
     */
    @DELETE(CLUSTER_URL_SUFFIX + "/{id}/dependencies")
    Call<Void> removeAllDependenciesForCluster(@Path("id") final String clusterId);

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
     * @param configs   The configs to be added.
     * @return A callable object.
     */
    @POST(CLUSTER_URL_SUFFIX + "/{id}/configs")
    Call<Void> addConfigsToCluster(@Path("id") final String clusterId, @Body final Set<String> configs);

    /**
     * Method to update configs for a cluster in Genie.
     *
     * @param clusterId The id of the cluster..
     * @param configs   The configs to be added.
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
     * @param tags      The tags to be added.
     * @return A callable object.
     */
    @POST(CLUSTER_URL_SUFFIX + "/{id}/tags")
    Call<Void> addTagsToCluster(@Path("id") final String clusterId, @Body final Set<String> tags);

    /**
     * Method to update tags for a cluster in Genie.
     *
     * @param clusterId The id of the cluster..
     * @param tags      The tags to be added.
     * @return A callable object.
     */
    @PUT(CLUSTER_URL_SUFFIX + "/{id}/tags")
    Call<Void> updateTagsForCluster(@Path("id") final String clusterId, @Body final Set<String> tags);

    /**
     * Method to delete a tag for a cluster in Genie.
     *
     * @param clusterId The id of the cluster.
     * @param tag       The tag to delete.
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
