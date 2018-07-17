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
import com.netflix.genie.common.dto.Application;
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
 * An interface that provides all methods needed for the Genie command client implementation.
 *
 * @author amsharma
 * @since 3.0.0
 */
public interface CommandService {

    /**
     * Path to Commands.
     */
    String COMMAND_URL_SUFFIX = "/api/v3/commands";

    /******************* CRUD Methods   ***************************/

    /**
     * Method to create a command in Genie.
     *
     * @param command The command object.
     * @return A callable object.
     */
    @POST(COMMAND_URL_SUFFIX)
    Call<Void> createCommand(@Body final Command command);

    /**
     * Method to update a command in Genie.
     *
     * @param commandId The id of the command to update.
     * @param command   The command object.
     * @return A callable object.
     */
    @PUT(COMMAND_URL_SUFFIX + "/{id}")
    Call<Void> updateCommand(@Path("id") final String commandId, @Body final Command command);

    /**
     * Method to get all commands from Genie filtered based on query params.
     *
     * @param name       The name of the commands.
     * @param user       The user who created the command.
     * @param statusList The list of Command statuses.
     * @param tagList    The list of tags.
     * @return A callable object.
     */
    @GET(COMMAND_URL_SUFFIX)
    Call<JsonNode> getCommands(
        @Query("name") final String name,
        @Query("user") final String user,
        @Query("status") final List<String> statusList,
        @Query("tag") final List<String> tagList
    );

    /**
     * Method to fetch a single job from Genie.
     *
     * @param commandId The id of the command to get.
     * @return A callable object.
     */
    @GET(COMMAND_URL_SUFFIX + "/{id}")
    Call<Command> getCommand(@Path("id") final String commandId);

    /**
     * Method to delete a command in Genie.
     *
     * @param commandId The id of the command.
     * @return A callable object.
     */
    @DELETE(COMMAND_URL_SUFFIX + "/{id}")
    Call<Void> deleteCommand(@Path("id") final String commandId);

    /**
     * Method to delete all commands in Genie.
     *
     * @return A callable object.
     */
    @DELETE(COMMAND_URL_SUFFIX)
    Call<Void> deleteAllCommands();

    /**
     * Patch a command using JSON Patch.
     *
     * @param commandId The id of the command to patch
     * @param patch     The JSON Patch instructions
     * @return A callable object.
     */
    @PATCH(COMMAND_URL_SUFFIX + "/{id}")
    Call<Void> patchCommand(@Path("id") final String commandId, @Body final JsonPatch patch);

    /****************** Methods to manipulate applications and clusters for a command   *********************/

    /**
     * Method to get applications for a command in Genie.
     *
     * @param commandId The id of the command.
     * @return A callable object.
     */
    @GET(COMMAND_URL_SUFFIX + "/{id}/applications")
    Call<List<Application>> getApplicationsForCommand(@Path("id") final String commandId);

    /**
     * Method to get clusters for a command in Genie.
     *
     * @param commandId The id of the command.
     * @return A callable object.
     */
    @GET(COMMAND_URL_SUFFIX + "/{id}/clusters")
    Call<List<Cluster>> getClustersForCommand(@Path("id") final String commandId);

    /**
     * Method to add applications to a command in Genie.
     *
     * @param commandId      The id of the command..
     * @param applicationIds The application Ids to be added.
     * @return A callable object.
     */
    @POST(COMMAND_URL_SUFFIX + "/{id}/applications")
    Call<Void> addApplicationsToCommand(@Path("id") final String commandId, @Body final List<String> applicationIds);

    /**
     * Method to override and set applications for a command in Genie.
     *
     * @param commandId      The id of the command..
     * @param applicationIds The application Ids to be added.
     * @return A callable object.
     */
    @PUT(COMMAND_URL_SUFFIX + "/{id}/applications")
    Call<Void> setApplicationsForCommand(@Path("id") final String commandId, @Body final List<String> applicationIds);

    /**
     * Method to delete a application for a command in Genie.
     *
     * @param commandId     The id of the command.
     * @param applicationId The application to delete.
     * @return A callable object.
     */
    @DELETE(COMMAND_URL_SUFFIX + "/{id}/applications/{applicationId}")
    Call<Void> removeApplicationForCommand(
        @Path("id") final String commandId,
        @Path("applicationId") final String applicationId
    );

    /**
     * Method to delete all applications for a command in Genie.
     *
     * @param commandId The id of the command.
     * @return A callable object.
     */
    @DELETE(COMMAND_URL_SUFFIX + "/{id}/applications")
    Call<Void> removeAllApplicationsForCommand(@Path("id") final String commandId);

    /****************** Methods to manipulate configs for a command   *********************/

    /**
     * Method to get configs for a command in Genie.
     *
     * @param commandId The id of the command.
     * @return A callable object.
     */
    @GET(COMMAND_URL_SUFFIX + "/{id}/configs")
    Call<Set<String>> getConfigsForCommand(@Path("id") final String commandId);

    /**
     * Method to add configs to a command in Genie.
     *
     * @param commandId The id of the command..
     * @param configs   The configs to be added.
     * @return A callable object.
     */
    @POST(COMMAND_URL_SUFFIX + "/{id}/configs")
    Call<Void> addConfigsToCommand(@Path("id") final String commandId, @Body final Set<String> configs);

    /**
     * Method to update configs for a command in Genie.
     *
     * @param commandId The id of the command..
     * @param configs   The configs to be added.
     * @return A callable object.
     */
    @PUT(COMMAND_URL_SUFFIX + "/{id}/configs")
    Call<Void> updateConfigsForCommand(@Path("id") final String commandId, @Body final Set<String> configs);

    /**
     * Method to delete all configs for a command in Genie.
     *
     * @param commandId The id of the command.
     * @return A callable object.
     */
    @DELETE(COMMAND_URL_SUFFIX + "/{id}/configs")
    Call<Void> removeAllConfigsForCommand(@Path("id") final String commandId);

    /****************** Methods to manipulate dependencies for a command   *********************/

    /**
     * Method to get dependency files for a command in Genie.
     *
     * @param commandId The id of the command.
     * @return A callable object.
     */
    @GET(COMMAND_URL_SUFFIX + "/{id}/dependencies")
    Call<Set<String>> getDependenciesForCommand(@Path("id") final String commandId);

    /**
     * Method to add dependencies to a command in Genie.
     *
     * @param commandId    The id of the command..
     * @param dependencies The dependencies to be added.
     * @return A callable object.
     */
    @POST(COMMAND_URL_SUFFIX + "/{id}/dependencies")
    Call<Void> addDependenciesToCommand(
        @Path("id") final String commandId,
        @Body final Set<String> dependencies
    );

    /**
     * Method to update dependencies for a command in Genie.
     *
     * @param commandId    The id of the command..
     * @param dependencies The dependencies to be added.
     * @return A callable object.
     */
    @PUT(COMMAND_URL_SUFFIX + "/{id}/dependencies")
    Call<Void> updateDependenciesForCommand(
        @Path("id") final String commandId,
        @Body final Set<String> dependencies
    );

    /**
     * Method to delete all dependencies for a command in Genie.
     *
     * @param commandId The id of the command.
     * @return A callable object.
     */
    @DELETE(COMMAND_URL_SUFFIX + "/{id}/dependencies")
    Call<Void> removeAllDependenciesForCommand(@Path("id") final String commandId);

    /****************** Methods to manipulate tags for a command   *********************/

    /**
     * Method to get tags for a command in Genie.
     *
     * @param commandId The id of the command.
     * @return A callable object.
     */
    @GET(COMMAND_URL_SUFFIX + "/{id}/tags")
    Call<Set<String>> getTagsForCommand(@Path("id") final String commandId);

    /**
     * Method to add tags to a command in Genie.
     *
     * @param commandId The id of the command..
     * @param tags      The tags to be added.
     * @return A callable object.
     */
    @POST(COMMAND_URL_SUFFIX + "/{id}/tags")
    Call<Void> addTagsToCommand(@Path("id") final String commandId, @Body final Set<String> tags);

    /**
     * Method to update tags for a command in Genie.
     *
     * @param commandId The id of the command..
     * @param tags      The tags to be added.
     * @return A callable object.
     */
    @PUT(COMMAND_URL_SUFFIX + "/{id}/tags")
    Call<Void> updateTagsForCommand(@Path("id") final String commandId, @Body final Set<String> tags);

    /**
     * Method to delete a tag for a command in Genie.
     *
     * @param commandId The id of the command.
     * @param tag       The tag to delete.
     * @return A callable object.
     */
    @DELETE(COMMAND_URL_SUFFIX + "/{id}/tags/{tag}")
    Call<Void> removeTagForCommand(@Path("id") final String commandId, @Path("tag") final String tag);

    /**
     * Method to delete all tags for a command in Genie.
     *
     * @param commandId The id of the command.
     * @return A callable object.
     */
    @DELETE(COMMAND_URL_SUFFIX + "/{id}/tags")
    Call<Void> removeAllTagsForCommand(@Path("id") final String commandId);
}
