/*
 *
 *  Copyright 2015 Netflix, Inc.
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

import com.google.common.collect.Multimap;
import com.netflix.client.http.HttpRequest;
import com.netflix.client.http.HttpRequest.Verb;
import com.netflix.genie.common.client.BaseGenieClient;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.model.Application;
import com.netflix.genie.common.model.Command;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

/**
 * Singleton class, which acts as the client library for the Application
 * Configuration Service.
 *
 * @author tgianos
 */
public final class ApplicationServiceClient extends BaseGenieClient {

    private static final String BASE_CONFIG_APPLICATION_REST_URL
            = BASE_REST_URL + "config/applications";

    // reference to the instance object
    private static ApplicationServiceClient instance;

    /**
     * Private constructor for singleton class.
     *
     * @throws IOException if there is any error during initialization
     */
    private ApplicationServiceClient() throws IOException {
        super(null);
    }

    /**
     * Returns the singleton instance that can be used by clients.
     *
     * @return ApplicationServiceClient instance
     * @throws IOException if there is an error instantiating client
     */
    public static synchronized ApplicationServiceClient getInstance() throws IOException {
        if (instance == null) {
            instance = new ApplicationServiceClient();
        }

        return instance;
    }

    /**
     * Create a new application configuration.
     *
     * @param application the object encapsulating the new application
     *                    configuration to create
     * @return The application that was created
     * @throws GenieException For any other error.
     */
    public Application createApplication(final Application application)
            throws GenieException {
        if (application == null) {
            throw new GeniePreconditionException("No application passed in. Unable to validate.");
        }

        final HttpRequest request = BaseGenieClient.buildRequest(
                Verb.POST,
                BASE_CONFIG_APPLICATION_REST_URL,
                null,
                application);
        return (Application) this.executeRequest(request, null, Application.class);
    }

    /**
     * Create or update an application configuration.
     *
     * @param id          the id for the application to create or update
     * @param application the object encapsulating the new application to create
     * @return extracted application configuration response
     * @throws GenieException For any other error.
     */
    public Application updateApplication(
            final String id,
            final Application application)
            throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("Required parameter id is missing. Unable to update.");
        }

        final HttpRequest request = BaseGenieClient.buildRequest(
                Verb.PUT,
                StringUtils.join(
                        new String[]{BASE_CONFIG_APPLICATION_REST_URL, id},
                        SLASH),
                null,
                application);
        return (Application) this.executeRequest(request, null, Application.class);
    }

    /**
     * Gets information for a given id.
     *
     * @param id the application id to get (can't be null or empty)
     * @return the application for this id
     * @throws GenieException For any other error.
     */
    public Application getApplication(final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("Required parameter id is missing. Unable to get.");
        }

        final HttpRequest request = BaseGenieClient.buildRequest(
                Verb.GET,
                StringUtils.join(
                        new String[]{BASE_CONFIG_APPLICATION_REST_URL, id},
                        SLASH),
                null,
                null);
        return (Application) this.executeRequest(request, null, Application.class);
    }

    /**
     * Gets a set of application configurations for the given parameters.
     *
     * @param params key/value pairs in a map object.<br>
     *               More details on the parameters can be found on the Genie User Guide on
     *               GitHub.
     * @return List of application configuration elements that match the filter
     * @throws GenieException For any other error.
     */
    public List<Application> getApplications(final Multimap<String, String> params)
            throws GenieException {
        final HttpRequest request = BaseGenieClient.buildRequest(
                Verb.GET,
                BASE_CONFIG_APPLICATION_REST_URL,
                params,
                null);

        @SuppressWarnings("unchecked")
        final List<Application> apps = (List<Application>) this.executeRequest(request, List.class, Application.class);
        return apps;
    }

    /**
     * Delete all the applications in the database.
     *
     * @return the should be empty set.
     * @throws GenieException For any other error.
     */
    public List<Application> deleteAllApplications() throws GenieException {
        final HttpRequest request = BaseGenieClient.buildRequest(
                Verb.DELETE,
                BASE_CONFIG_APPLICATION_REST_URL,
                null,
                null);

        @SuppressWarnings("unchecked")
        final List<Application> apps = (List<Application>) this.executeRequest(request, List.class, Application.class);
        return apps;
    }

    /**
     * Delete an application configuration using its id.
     *
     * @param id the id for the application to delete
     * @return the deleted application
     * @throws GenieException For any other error.
     */
    public Application deleteApplication(final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("Missing required parameter: id");
        }

        final HttpRequest request = BaseGenieClient.buildRequest(
                Verb.DELETE,
                StringUtils.join(
                        new String[]{BASE_CONFIG_APPLICATION_REST_URL, id},
                        SLASH),
                null,
                null);
        return (Application) this.executeRequest(request, null, Application.class);
    }

    /**
     * Add some more configuration files to a given application.
     *
     * @param id      The id of the application to add configurations to. Not
     *                Null/empty/blank.
     * @param configs The configuration files to add. Not null or empty.
     * @return The new set of configuration files for the given application.
     * @throws GenieException For any other error.
     */
    public Set<String> addConfigsToApplication(
            final String id,
            final Set<String> configs) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("Missing required parameter: id");
        }
        if (configs == null || configs.isEmpty()) {
            throw new GeniePreconditionException("Missing required parameter: configs");
        }

        final HttpRequest request = BaseGenieClient.buildRequest(
                Verb.POST,
                StringUtils.join(
                        new String[]{BASE_CONFIG_APPLICATION_REST_URL, id, "configs"},
                        SLASH),
                null,
                configs);

        @SuppressWarnings("unchecked")
        final Set<String> newConfigs = (Set<String>) this.executeRequest(request, Set.class, String.class);
        return newConfigs;
    }

    /**
     * Get the active set of configuration files for the given application.
     *
     * @param id The id of the application to get configurations for. Not
     *           Null/empty/blank.
     * @return The set of configuration files for the given application.
     * @throws GenieException For any other error.
     */
    public Set<String> getConfigsForApplication(final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("Missing required parameter: id");
        }

        final HttpRequest request = BaseGenieClient.buildRequest(
                Verb.GET,
                StringUtils.join(
                        new String[]{BASE_CONFIG_APPLICATION_REST_URL, id, "configs"},
                        SLASH),
                null,
                null);

        @SuppressWarnings("unchecked")
        final Set<String> configs = (Set<String>) this.executeRequest(request, Set.class, String.class);
        return configs;
    }

    /**
     * Update the configuration files for a given application.
     *
     * @param id      The id of the application to update the configuration files
     *                for. Not null/empty/blank.
     * @param configs The configuration files to replace existing configuration
     *                files with. Not null.
     * @return The new set of application configurations.
     * @throws GenieException For any other error.
     */
    public Set<String> updateConfigsForApplication(
            final String id,
            final Set<String> configs) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("Missing required parameter: id");
        }
        if (configs == null) {
            throw new GeniePreconditionException("Missing required parameter: configs");
        }

        final HttpRequest request = BaseGenieClient.buildRequest(
                Verb.PUT,
                StringUtils.join(
                        new String[]{BASE_CONFIG_APPLICATION_REST_URL, id, "configs"},
                        SLASH),
                null,
                configs);

        @SuppressWarnings("unchecked")
        final Set<String> newConfigs = (Set<String>) this.executeRequest(request, Set.class, String.class);
        return newConfigs;
    }

    /**
     * Delete the all configuration files from a given application.
     *
     * @param id The id of the application to delete the configuration files
     *           from. Not null/empty/blank.
     * @return Empty set if successful
     * @throws GenieException For any other error.
     */
    public Set<String> removeAllConfigsForApplication(
            final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("Missing required parameter: id");
        }

        final HttpRequest request = BaseGenieClient.buildRequest(
                Verb.DELETE,
                StringUtils.join(
                        new String[]{BASE_CONFIG_APPLICATION_REST_URL, id, "configs"},
                        SLASH),
                null,
                null);

        @SuppressWarnings("unchecked")
        final Set<String> configs = (Set<String>) this.executeRequest(request, Set.class, String.class);
        return configs;
    }

    /**
     * Add some more jar files to a given application.
     *
     * @param id   The id of the application to add jars to. Not
     *             Null/empty/blank.
     * @param jars The jar files to add. Not null or empty.
     * @return The new set of jar files for the given application.
     * @throws GenieException For any other error.
     */
    public Set<String> addJarsToApplication(
            final String id,
            final Set<String> jars) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("Missing required parameter: id");
        }
        if (jars == null || jars.isEmpty()) {
            throw new GeniePreconditionException("Missing required parameter: jars");
        }

        final HttpRequest request = BaseGenieClient.buildRequest(
                Verb.POST,
                StringUtils.join(
                        new String[]{BASE_CONFIG_APPLICATION_REST_URL, id, "jars"},
                        SLASH),
                null,
                jars);

        @SuppressWarnings("unchecked")
        final Set<String> newJars = (Set<String>) this.executeRequest(request, Set.class, String.class);
        return newJars;
    }

    /**
     * Get the active set of jar files for the given application.
     *
     * @param id The id of the application to get jars for. Not
     *           Null/empty/blank.
     * @return The set of jar files for the given application.
     * @throws GenieException For any other error.
     */
    public Set<String> getJarsForApplication(final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("Missing required parameter: id");
        }

        final HttpRequest request = BaseGenieClient.buildRequest(
                Verb.GET,
                StringUtils.join(
                        new String[]{BASE_CONFIG_APPLICATION_REST_URL, id, "jars"},
                        SLASH),
                null,
                null);

        @SuppressWarnings("unchecked")
        final Set<String> jars = (Set<String>) this.executeRequest(request, Set.class, String.class);
        return jars;
    }

    /**
     * Update the jar files for a given application.
     *
     * @param id   The id of the application to update the jar files
     *             for. Not null/empty/blank.
     * @param jars The jar files to replace existing jar
     *             files with. Not null.
     * @return The new set of application jars.
     * @throws GenieException For any other error.
     */
    public Set<String> updateJarsForApplication(
            final String id,
            final Set<String> jars) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("Missing required parameter: id");
        }
        if (jars == null) {
            throw new GeniePreconditionException("Missing required parameter: jars");
        }

        final HttpRequest request = BaseGenieClient.buildRequest(
                Verb.PUT,
                StringUtils.join(
                        new String[]{BASE_CONFIG_APPLICATION_REST_URL, id, "jars"},
                        SLASH),
                null,
                jars);

        @SuppressWarnings("unchecked")
        final Set<String> newJars = (Set<String>) this.executeRequest(request, Set.class, String.class);
        return newJars;
    }

    /**
     * Delete the all jar files from a given application.
     *
     * @param id The id of the application to delete the jar files
     *           from. Not null/empty/blank.
     * @return Empty set if successful
     * @throws GenieException For any other error.
     */
    public Set<String> removeAllJarsForApplication(
            final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("Missing required parameter: id");
        }

        final HttpRequest request = BaseGenieClient.buildRequest(
                Verb.DELETE,
                StringUtils.join(
                        new String[]{BASE_CONFIG_APPLICATION_REST_URL, id, "jars"},
                        SLASH),
                null,
                null);

        @SuppressWarnings("unchecked")
        final Set<String> jars = (Set<String>) this.executeRequest(request, Set.class, String.class);
        return jars;
    }

    /**
     * Get all the commands this application is associated with.
     *
     * @param id The id of the application to get the commands for. Not
     *           NULL/empty/blank.
     * @return The set of commands.
     * @throws GenieException For any error.
     */
    public Set<Command> getCommandsForApplication(
            final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("Missing required parameter: id");
        }

        final HttpRequest request = BaseGenieClient.buildRequest(
                Verb.GET,
                StringUtils.join(
                        new String[]{BASE_CONFIG_APPLICATION_REST_URL, id, "commands"},
                        SLASH),
                null,
                null);

        @SuppressWarnings("unchecked")
        final Set<Command> commands = (Set<Command>) this.executeRequest(request, Set.class, Command.class);
        return commands;
    }

    /**
     * Add some more tags to a given application.
     *
     * @param id   The id of the application to add tags to. Not
     *             Null/empty/blank.
     * @param tags The tags to add. Not null or empty.
     * @return The new set of tags for the given application.
     * @throws GenieException For any other error.
     */
    public Set<String> addTagsToApplication(
            final String id,
            final Set<String> tags) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("Missing required parameter: id");
        }
        if (tags == null || tags.isEmpty()) {
            throw new GeniePreconditionException("Missing required parameter: tags");
        }

        final HttpRequest request = BaseGenieClient.buildRequest(
                Verb.POST,
                StringUtils.join(
                        new String[]{BASE_CONFIG_APPLICATION_REST_URL, id, "tags"},
                        SLASH),
                null,
                tags);

        @SuppressWarnings("unchecked")
        final Set<String> newTags = (Set<String>) this.executeRequest(request, Set.class, String.class);
        return newTags;
    }

    /**
     * Get the active set of tags for the given application.
     *
     * @param id The id of the application to get tags for. Not
     *           Null/empty/blank.
     * @return The set of tags for the given application.
     * @throws GenieException For any other error.
     */
    public Set<String> getTagsForApplication(final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("Missing required parameter: id");
        }

        final HttpRequest request = BaseGenieClient.buildRequest(
                Verb.GET,
                StringUtils.join(
                        new String[]{BASE_CONFIG_APPLICATION_REST_URL, id, "tags"},
                        SLASH),
                null,
                null);

        @SuppressWarnings("unchecked")
        final Set<String> tags = (Set<String>) this.executeRequest(request, Set.class, String.class);
        return tags;
    }

    /**
     * Update the tags for a given application.
     *
     * @param id   The id of the application to update the tags for.
     *             Not null/empty/blank.
     * @param tags The tags to replace existing tag
     *             files with. Not null.
     * @return The new set of application tags.
     * @throws GenieException For any other error.
     */
    public Set<String> updateTagsForApplication(
            final String id,
            final Set<String> tags) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("Missing required parameter: id");
        }
        if (tags == null) {
            throw new GeniePreconditionException("Missing required parameter: tags");
        }

        final HttpRequest request = BaseGenieClient.buildRequest(
                Verb.PUT,
                StringUtils.join(
                        new String[]{BASE_CONFIG_APPLICATION_REST_URL, id, "tags"},
                        SLASH),
                null,
                tags);

        @SuppressWarnings("unchecked")
        final Set<String> newTags = (Set<String>) this.executeRequest(request, Set.class, String.class);
        return newTags;
    }

    /**
     * Delete all the tags from a given application.
     *
     * @param id The id of the application to delete the tags from.
     *           Not null/empty/blank.
     * @return Empty set if successful
     * @throws GenieException For any error.
     */
    public Set<String> removeAllTagsForApplication(
            final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("Missing required parameter: id");
        }

        final HttpRequest request = BaseGenieClient.buildRequest(
                Verb.DELETE,
                StringUtils.join(
                        new String[]{BASE_CONFIG_APPLICATION_REST_URL, id, "tags"},
                        SLASH),
                null,
                null);

        @SuppressWarnings("unchecked")
        final Set<String> tags = (Set<String>) this.executeRequest(request, Set.class, String.class);
        return tags;
    }

    /**
     * Remove tag from a given application.
     *
     * @param id  The id of the application to delete the tag from. Not
     *            null/empty/blank.
     * @param tag The tag to remove. Not null/empty/blank.
     * @return The tag for the application.
     * @throws GenieException For any error.
     */
    public Set<String> removeTagForApplication(
            final String id,
            final String tag) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("Missing required parameter: id");
        }

        final HttpRequest request = BaseGenieClient.buildRequest(
                Verb.DELETE,
                StringUtils.join(
                        new String[]{
                                BASE_CONFIG_APPLICATION_REST_URL,
                                id,
                                "tags",
                                tag,
                        },
                        SLASH),
                null,
                null);

        @SuppressWarnings("unchecked")
        final Set<String> tags = (Set<String>) this.executeRequest(request, Set.class, String.class);
        return tags;
    }
}
