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
    Call<Void> createCluster(@Body Cluster cluster);

    /**
     * Method to update a cluster in Genie.
     *
     * @param clusterId The id of the cluster to update.
     * @param cluster   The cluster object.
     * @return A callable object.
     */
    @PUT(CLUSTER_URL_SUFFIX + "/{id}")
    Call<Void> updateCluster(@Path("id") String clusterId, @Body Cluster cluster);

    /**
     * Method to get clusters from Genie based on filters specified.
     *
     * @param name          The name of the cluster.
     * @param statusList    The list of statuses.
     * @param tagList       The list of tags.
     * @param minUpdateTime Minimum Time after which cluster was updated.
     * @param maxUpdateTime Maximum Time before which cluster was updated.
     * @param size          The maximum number of results in the page
     * @param sort          The sort order
     * @param page          The page index
     * @return A callable object.
     */
    @GET(CLUSTER_URL_SUFFIX)
    Call<JsonNode> getClusters(
        @Query("name") String name,
        @Query("status") List<String> statusList,
        @Query("tag") List<String> tagList,
        @Query("minUpdateTime") Long minUpdateTime,
        @Query("maxUpdateTime") Long maxUpdateTime,
        @Query("size") Integer size,
        @Query("sort") String sort,
        @Query("page") Integer page
    );

    /**
     *  getClusters(
     @RequestParam(value = "name", required = false) String name,
     @RequestParam(value = "status", required = false) Set<String> statuses,
     @RequestParam(value = "tag", required = false) Set<String> tags,
     @RequestParam(value = "minUpdateTime", required = false) Long minUpdateTime,
     @RequestParam(value = "maxUpdateTime", required = false) Long maxUpdateTime,
     @PageableDefault(page = 0, size = 64, sort = {"updated"}, direction = Sort.Direction.DESC) Pageable page,
     PagedResourcesAssembler<Cluster> assembler
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
    Call<Cluster> getCluster(@Path("id") String clusterId);

    /**
     * Method to delete a cluster in Genie.
     *
     * @param clusterId The id of the cluster.
     * @return A callable object.
     */
    @DELETE(CLUSTER_URL_SUFFIX + "/{id}")
    Call<Void> deleteCluster(@Path("id") String clusterId);

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
    Call<Void> patchCluster(@Path("id") String clusterId, @Body JsonPatch patch);

    /****************** Methods to manipulate commands for a cluster   *********************/

    /**
     * Method to get commmands for a cluster in Genie.
     *
     * @param clusterId The id of the cluster.
     * @return A callable object.
     */
    @GET(CLUSTER_URL_SUFFIX + "/{id}/commands")
    Call<List<Command>> getCommandsForCluster(@Path("id") String clusterId);

    /**
     * Method to add commands to a cluster in Genie.
     *
     * @param clusterId  The id of the cluster..
     * @param commandIds The command Ids to be added.
     * @return A callable object.
     */
    @POST(CLUSTER_URL_SUFFIX + "/{id}/commands")
    Call<Void> addCommandsToCluster(@Path("id") String clusterId, @Body List<String> commandIds);

    /**
     * Method to override and set commands for a cluster in Genie.
     *
     * @param clusterId  The id of the cluster..
     * @param commandIds The command Ids to be added.
     * @return A callable object.
     */
    @PUT(CLUSTER_URL_SUFFIX + "/{id}/commands")
    Call<Void> setCommandsForCluster(@Path("id") String clusterId, @Body List<String> commandIds);

    /**
     * Method to delete a command for a cluster in Genie.
     *
     * @param clusterId The id of the cluster.
     * @param commandId The command to delete.
     * @return A callable object.
     */
    @DELETE(CLUSTER_URL_SUFFIX + "/{id}/commands/{commandId}")
    Call<Void> removeCommandForCluster(@Path("id") String clusterId, @Path("commandId") String commandId);

    /**
     * Method to delete all commands for a cluster in Genie.
     *
     * @param clusterId The id of the cluster.
     * @return A callable object.
     */
    @DELETE(CLUSTER_URL_SUFFIX + "/{id}/commands")
    Call<Void> removeAllCommandsForCluster(@Path("id") String clusterId);

    /****************** Methods to manipulate dependencies for a cluster   *********************/

    /**
     * Method to get dependency files for a cluster in Genie.
     *
     * @param clusterId The id of the cluster.
     * @return A callable object.
     */
    @GET(CLUSTER_URL_SUFFIX + "/{id}/dependencies")
    Call<Set<String>> getDependenciesForCluster(@Path("id") String clusterId);

    /**
     * Method to add dependencies to a cluster in Genie.
     *
     * @param clusterId    The id of the cluster..
     * @param dependencies The dependencies to be added.
     * @return A callable object.
     */
    @POST(CLUSTER_URL_SUFFIX + "/{id}/dependencies")
    Call<Void> addDependenciesToCluster(
        @Path("id") String clusterId,
        @Body Set<String> dependencies
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
        @Path("id") String clusterId,
        @Body Set<String> dependencies
    );

    /**
     * Method to delete all dependencies for a cluster in Genie.
     *
     * @param clusterId The id of the cluster.
     * @return A callable object.
     */
    @DELETE(CLUSTER_URL_SUFFIX + "/{id}/dependencies")
    Call<Void> removeAllDependenciesForCluster(@Path("id") String clusterId);

    /****************** Methods to manipulate configs for a cluster   *********************/

    /**
     * Method to get configs for a cluster in Genie.
     *
     * @param clusterId The id of the cluster.
     * @return A callable object.
     */
    @GET(CLUSTER_URL_SUFFIX + "/{id}/configs")
    Call<Set<String>> getConfigsForCluster(@Path("id") String clusterId);

    /**
     * Method to add configs to a cluster in Genie.
     *
     * @param clusterId The id of the cluster..
     * @param configs   The configs to be added.
     * @return A callable object.
     */
    @POST(CLUSTER_URL_SUFFIX + "/{id}/configs")
    Call<Void> addConfigsToCluster(@Path("id") String clusterId, @Body Set<String> configs);

    /**
     * Method to update configs for a cluster in Genie.
     *
     * @param clusterId The id of the cluster..
     * @param configs   The configs to be added.
     * @return A callable object.
     */
    @PUT(CLUSTER_URL_SUFFIX + "/{id}/configs")
    Call<Void> updateConfigsForCluster(@Path("id") String clusterId, @Body Set<String> configs);

    /**
     * Method to delete all configs for a cluster in Genie.
     *
     * @param clusterId The id of the cluster.
     * @return A callable object.
     */
    @DELETE(CLUSTER_URL_SUFFIX + "/{id}/configs")
    Call<Void> removeAllConfigsForCluster(@Path("id") String clusterId);

    /****************** Methods to manipulate tags for a cluster   *********************/

    /**
     * Method to get tags for a cluster in Genie.
     *
     * @param clusterId The id of the cluster.
     * @return A callable object.
     */
    @GET(CLUSTER_URL_SUFFIX + "/{id}/tags")
    Call<Set<String>> getTagsForCluster(@Path("id") String clusterId);

    /**
     * Method to add tags to a cluster in Genie.
     *
     * @param clusterId The id of the cluster..
     * @param tags      The tags to be added.
     * @return A callable object.
     */
    @POST(CLUSTER_URL_SUFFIX + "/{id}/tags")
    Call<Void> addTagsToCluster(@Path("id") String clusterId, @Body Set<String> tags);

    /**
     * Method to update tags for a cluster in Genie.
     *
     * @param clusterId The id of the cluster..
     * @param tags      The tags to be added.
     * @return A callable object.
     */
    @PUT(CLUSTER_URL_SUFFIX + "/{id}/tags")
    Call<Void> updateTagsForCluster(@Path("id") String clusterId, @Body Set<String> tags);

    /**
     * Method to delete a tag for a cluster in Genie.
     *
     * @param clusterId The id of the cluster.
     * @param tag       The tag to delete.
     * @return A callable object.
     */
    @DELETE(CLUSTER_URL_SUFFIX + "/{id}/tags/{tag}")
    Call<Void> removeTagForCluster(@Path("id") String clusterId, @Path("tag") String tag);

    /**
     * Method to delete all tags for a cluster in Genie.
     *
     * @param clusterId The id of the cluster.
     * @return A callable object.
     */
    @DELETE(CLUSTER_URL_SUFFIX + "/{id}/tags")
    Call<Void> removeAllTagsForCluster(@Path("id") String clusterId);
}
