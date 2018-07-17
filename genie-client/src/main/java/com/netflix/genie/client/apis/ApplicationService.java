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
 * An interface that provides all methods needed for the Genie application client implementation.
 *
 * @author amsharma
 * @since 3.0.0
 */
public interface ApplicationService {

    /**
     * Path to Applications.
     */
    String APPLICATION_URL_SUFFIX = "/api/v3/applications";

    /******************* CRUD Methods   ***************************/

    /**
     * Method to create a application in Genie.
     *
     * @param application The application object.
     * @return A callable object.
     */
    @POST(APPLICATION_URL_SUFFIX)
    Call<Void> createApplication(@Body final Application application);

    /**
     * Method to update a application in Genie.
     *
     * @param applicationId The id of the application to update.
     * @param application   The application object.
     * @return A callable object.
     */
    @PUT(APPLICATION_URL_SUFFIX + "/{id}")
    Call<Void> updateApplication(@Path("id") final String applicationId, @Body final Application application);

    /**
     * Method to get all applications from Genie.
     *
     * @param name       The name of the commands.
     * @param user       The user who created the command.
     * @param statusList The list of Command statuses.
     * @param tagList    The list of tags.
     * @param type       The type of the application.
     * @return A callable object.
     */
    @GET(APPLICATION_URL_SUFFIX)
    Call<JsonNode> getApplications(
        @Query("name") final String name,
        @Query("user") final String user,
        @Query("status") final List<String> statusList,
        @Query("tag") final List<String> tagList,
        @Query("type") final String type
    );

    /**
     * Method to fetch a single job from Genie.
     *
     * @param applicationId The id of the application to get.
     * @return A callable object.
     */
    @GET(APPLICATION_URL_SUFFIX + "/{id}")
    Call<Application> getApplication(@Path("id") final String applicationId);

    /**
     * Method to delete a application in Genie.
     *
     * @param applicationId The id of the application.
     * @return A callable object.
     */
    @DELETE(APPLICATION_URL_SUFFIX + "/{id}")
    Call<Void> deleteApplication(@Path("id") final String applicationId);

    /**
     * Method to delete all applications in Genie.
     *
     * @return A callable object.
     */
    @DELETE(APPLICATION_URL_SUFFIX)
    Call<Void> deleteAllApplications();

    /**
     * Patch a application using JSON Patch.
     *
     * @param applicationId The id of the application to patch
     * @param patch         The JSON Patch instructions
     * @return A callable object.
     */
    @PATCH(APPLICATION_URL_SUFFIX + "/{id}")
    Call<Void> patchApplication(@Path("id") final String applicationId, @Body final JsonPatch patch);

    /**
     * Method to get commmands for a application in Genie.
     *
     * @param applicationId The id of the application.
     * @return A callable object.
     */
    @GET(APPLICATION_URL_SUFFIX + "/{id}/commands")
    Call<List<Command>> getCommandsForApplication(@Path("id") final String applicationId);

    /****************** Methods to manipulate dependencies for a application   *********************/

    /**
     * Method to get dependency files for a application in Genie.
     *
     * @param applicationId The id of the application.
     * @return A callable object.
     */
    @GET(APPLICATION_URL_SUFFIX + "/{id}/dependencies")
    Call<Set<String>> getDependenciesForApplication(@Path("id") final String applicationId);

    /**
     * Method to add dependencies to a application in Genie.
     *
     * @param applicationId The id of the application..
     * @param dependencies  The dependencies to be added.
     * @return A callable object.
     */
    @POST(APPLICATION_URL_SUFFIX + "/{id}/dependencies")
    Call<Void> addDependenciesToApplication(
        @Path("id") final String applicationId,
        @Body final Set<String> dependencies
    );

    /**
     * Method to update dependencies for a application in Genie.
     *
     * @param applicationId The id of the application..
     * @param dependencies  The dependencies to be added.
     * @return A callable object.
     */
    @PUT(APPLICATION_URL_SUFFIX + "/{id}/dependencies")
    Call<Void> updateDependenciesForApplication(
        @Path("id") final String applicationId,
        @Body final Set<String> dependencies
    );

    /**
     * Method to delete all dependencies for a application in Genie.
     *
     * @param applicationId The id of the application.
     * @return A callable object.
     */
    @DELETE(APPLICATION_URL_SUFFIX + "/{id}/dependencies")
    Call<Void> removeAllDependenciesForApplication(@Path("id") final String applicationId);

    /****************** Methods to manipulate configs for a application   *********************/

    /**
     * Method to get configs for a application in Genie.
     *
     * @param applicationId The id of the application.
     * @return A callable object.
     */
    @GET(APPLICATION_URL_SUFFIX + "/{id}/configs")
    Call<Set<String>> getConfigsForApplication(@Path("id") final String applicationId);

    /**
     * Method to add configs to a application in Genie.
     *
     * @param applicationId The id of the application..
     * @param configs       The configs to be added.
     * @return A callable object.
     */
    @POST(APPLICATION_URL_SUFFIX + "/{id}/configs")
    Call<Void> addConfigsToApplication(@Path("id") final String applicationId, @Body final Set<String> configs);

    /**
     * Method to update configs for a application in Genie.
     *
     * @param applicationId The id of the application..
     * @param configs       The configs to be added.
     * @return A callable object.
     */
    @PUT(APPLICATION_URL_SUFFIX + "/{id}/configs")
    Call<Void> updateConfigsForApplication(@Path("id") final String applicationId, @Body final Set<String> configs);

    /**
     * Method to delete all configs for a application in Genie.
     *
     * @param applicationId The id of the application.
     * @return A callable object.
     */
    @DELETE(APPLICATION_URL_SUFFIX + "/{id}/configs")
    Call<Void> removeAllConfigsForApplication(@Path("id") final String applicationId);

    /****************** Methods to manipulate tags for a application   *********************/

    /**
     * Method to get tags for a application in Genie.
     *
     * @param applicationId The id of the application.
     * @return A callable object.
     */
    @GET(APPLICATION_URL_SUFFIX + "/{id}/tags")
    Call<Set<String>> getTagsForApplication(@Path("id") final String applicationId);

    /**
     * Method to add tags to a application in Genie.
     *
     * @param applicationId The id of the application..
     * @param tags          The tags to be added.
     * @return A callable object.
     */
    @POST(APPLICATION_URL_SUFFIX + "/{id}/tags")
    Call<Void> addTagsToApplication(@Path("id") final String applicationId, @Body final Set<String> tags);

    /**
     * Method to update tags for a application in Genie.
     *
     * @param applicationId The id of the application..
     * @param tags          The tags to be added.
     * @return A callable object.
     */
    @PUT(APPLICATION_URL_SUFFIX + "/{id}/tags")
    Call<Void> updateTagsForApplication(@Path("id") final String applicationId, @Body final Set<String> tags);

    /**
     * Method to delete a tag for a application in Genie.
     *
     * @param applicationId The id of the application.
     * @param tag           The tag to delete.
     * @return A callable object.
     */
    @DELETE(APPLICATION_URL_SUFFIX + "/{id}/tags/{tag}")
    Call<Void> removeTagForApplication(@Path("id") final String applicationId, @Path("tag") final String tag);

    /**
     * Method to delete all tags for a application in Genie.
     *
     * @param applicationId The id of the application.
     * @return A callable object.
     */
    @DELETE(APPLICATION_URL_SUFFIX + "/{id}/tags")
    Call<Void> removeAllTagsForApplication(@Path("id") final String applicationId);
}
