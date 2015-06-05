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
import com.netflix.genie.common.model.Cluster;
import com.netflix.genie.common.model.Command;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

/**
 * Singleton class, which acts as the client library for the Command
 * Configuration Service.
 *
 * @author tgianos
 */
public final class CommandServiceClient extends BaseGenieClient {

    private static final String BASE_CONFIG_COMMAND_REST_URL
            = BASE_REST_URL + "config/commands";

    // reference to the instance object
    private static CommandServiceClient instance;

    /**
     * Private constructor for singleton class.
     *
     * @throws IOException if there is any error during initialization
     */
    private CommandServiceClient() throws IOException {
        super(null);
    }

    /**
     * Returns the singleton instance that can be used by clients.
     *
     * @return ExecutionServiceClient instance
     * @throws IOException if there is an error instantiating client
     */
    public static synchronized CommandServiceClient getInstance() throws IOException {
        if (instance == null) {
            instance = new CommandServiceClient();
        }

        return instance;
    }

    /**
     * Create a new command configuration.
     *
     * @param command the object encapsulating the new Cluster configuration to
     *                create
     * @return extracted command configuration response
     * @throws GenieException For any other error.
     */
    public Command createCommand(final Command command)
            throws GenieException {
        if (command == null) {
            throw new GeniePreconditionException("No command entered to validate");
        }

        final HttpRequest request = BaseGenieClient.buildRequest(
                Verb.POST,
                BASE_CONFIG_COMMAND_REST_URL,
                null,
                command);
        return (Command) this.executeRequest(request, null, Command.class);
    }

    /**
     * Create or update a command configuration.
     *
     * @param id      the id for the command configuration to create or update
     * @param command the object encapsulating the new Cluster configuration to
     *                create
     * @return extracted command configuration response
     * @throws GenieException For any other error.
     */
    public Command updateCommand(final String id, final Command command)
            throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("Required parameter id can't be null or empty.");
        }

        final HttpRequest request = BaseGenieClient.buildRequest(
                Verb.PUT,
                StringUtils.join(
                        new String[]{BASE_CONFIG_COMMAND_REST_URL, id},
                        SLASH),
                null,
                command);
        return (Command) this.executeRequest(request, null, Command.class);
    }

    /**
     * Gets information for a given configId.
     *
     * @param id the command configuration id to get (can't be null or empty)
     * @return the command configuration for this id
     * @throws GenieException For any other error.
     */
    public Command getCommand(final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("Missing required parameter: id");
        }

        final HttpRequest request = BaseGenieClient.buildRequest(
                Verb.GET,
                StringUtils.join(
                        new String[]{BASE_CONFIG_COMMAND_REST_URL, id},
                        SLASH),
                null,
                null);
        return (Command) this.executeRequest(request, null, Command.class);
    }

    /**
     * Gets a set of command configurations for the given parameters.
     *
     * @param params key/value pairs in a map object.<br>
     *               More details on the parameters can be found on the Genie User Guide on
     *               GitHub.
     * @return List of command configuration elements that match the filter
     * @throws GenieException For any other error.
     */
    public List<Command> getCommands(final Multimap<String, String> params)
            throws GenieException {
        final HttpRequest request = BaseGenieClient.buildRequest(
                Verb.GET,
                BASE_CONFIG_COMMAND_REST_URL,
                params,
                null);

        @SuppressWarnings("unchecked")
        final List<Command> commands = (List<Command>) this.executeRequest(request, List.class, Command.class);
        return commands;
    }

    /**
     * Delete all the commands in the database.
     *
     * @return the should be empty set.
     * @throws GenieException For any other error.
     */
    public List<Command> deleteAllCommands() throws GenieException {
        final HttpRequest request = BaseGenieClient.buildRequest(
                Verb.DELETE,
                BASE_CONFIG_COMMAND_REST_URL,
                null,
                null);

        @SuppressWarnings("unchecked")
        final List<Command> commands = (List<Command>) this.executeRequest(request, List.class, Command.class);
        return commands;
    }

    /**
     * Delete a configuration using its id.
     *
     * @param id the id for the command configuration to delete. Not null or
     *           empty.
     * @return the deleted command configuration
     * @throws GenieException For any other error.
     */
    public Command deleteCommand(final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("Missing required parameter: id");
        }

        final HttpRequest request = BaseGenieClient.buildRequest(
                Verb.DELETE,
                StringUtils.join(
                        new String[]{BASE_CONFIG_COMMAND_REST_URL, id},
                        SLASH),
                null,
                null);
        return (Command) this.executeRequest(request, null, Command.class);
    }

    /**
     * Add some more configuration files to a given command.
     *
     * @param id      The id of the command to add configurations to. Not
     *                Null/empty/blank.
     * @param configs The configuration files to add. Not null or empty.
     * @return The new set of configuration files for the given command.
     * @throws GenieException On any error.
     */
    public Set<String> addConfigsToCommand(
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
                        new String[]{BASE_CONFIG_COMMAND_REST_URL, id, "configs"},
                        SLASH),
                null,
                configs);

        @SuppressWarnings("unchecked")
        final Set<String> newConfigs = (Set<String>) this.executeRequest(request, Set.class, String.class);
        return newConfigs;
    }

    /**
     * Get the active set of configuration files for the given command.
     *
     * @param id The id of the command to get configurations for. Not
     *           Null/empty/blank.
     * @return The set of configuration files for the given command.
     * @throws GenieException For any other error.
     */
    public Set<String> getConfigsForCommand(final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("Missing required parameter: id");
        }

        final HttpRequest request = BaseGenieClient.buildRequest(
                Verb.GET,
                StringUtils.join(
                        new String[]{BASE_CONFIG_COMMAND_REST_URL, id, "configs"},
                        SLASH),
                null,
                null);

        @SuppressWarnings("unchecked")
        final Set<String> configs = (Set<String>) this.executeRequest(request, Set.class, String.class);
        return configs;
    }

    /**
     * Update the configuration files for a given command.
     *
     * @param id      The id of the command to update the configuration files for.
     *                Not null/empty/blank.
     * @param configs The configuration files to replace existing configuration
     *                files with. Not null.
     * @return The new set of command configurations.
     * @throws GenieException For any other error.
     */
    public Set<String> updateConfigsForCommand(
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
                        new String[]{BASE_CONFIG_COMMAND_REST_URL, id, "configs"},
                        SLASH),
                null,
                configs);

        @SuppressWarnings("unchecked")
        final Set<String> newConfigs = (Set<String>) this.executeRequest(request, Set.class, String.class);
        return newConfigs;
    }

    /**
     * Delete all the configuration files from a given command.
     *
     * @param id The id of the command to delete the configuration files from.
     *           Not null/empty/blank.
     * @return Empty set if successful
     * @throws GenieException For any other error.
     */
    public Set<String> removeAllConfigsForCommand(
            final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("Missing required parameter: id");
        }

        final HttpRequest request = BaseGenieClient.buildRequest(
                Verb.DELETE,
                StringUtils.join(
                        new String[]{BASE_CONFIG_COMMAND_REST_URL, id, "configs"},
                        SLASH),
                null,
                null);

        @SuppressWarnings("unchecked")
        final Set<String> configs = (Set<String>) this.executeRequest(request, Set.class, String.class);
        return configs;
    }

    /**
     * Add some more applications to a given command.
     *
     * @param id          The id of the command to add applications to. Not
     *                    Null/empty/blank.
     * @param application The application to set. Not null.
     * @return The new application for the given command.
     * @throws GenieException For any other error.
     */
    public Application setApplicationForCommand(
            final String id,
            final Application application) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("Missing required parameter: id");
        }
        if (application == null) {
            throw new GeniePreconditionException("Missing required parameter: application");
        }

        final HttpRequest request = BaseGenieClient.buildRequest(
                Verb.POST,
                StringUtils.join(
                        new String[]{
                                BASE_CONFIG_COMMAND_REST_URL,
                                id,
                                "application"
                        },
                        SLASH),
                null,
                application
        );
        return (Application) this.executeRequest(request, null, Application.class);
    }

    /**
     * Get the active application for the given command.
     *
     * @param id The id of the command to get application for. Not Null.
     * @return The application for the given command.
     * @throws GenieException For any other error.
     */
    public Application getApplicationForCommand(
            final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("Missing required parameter: id");
        }

        final HttpRequest request = BaseGenieClient.buildRequest(
                Verb.GET,
                StringUtils.join(
                        new String[]{
                                BASE_CONFIG_COMMAND_REST_URL,
                                id,
                                "application"
                        },
                        SLASH),
                null,
                null);
        return (Application) this.executeRequest(request, null, Application.class);
    }

    /**
     * Remove application from a given command.
     *
     * @param id The id of the command to delete the application from. Not
     *           null/empty/blank.
     * @return The active application for the command.
     * @throws GenieException For any other error.
     */
    public Application removeApplicationForCommand(
            final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("Missing required parameter: id");
        }

        final HttpRequest request = BaseGenieClient.buildRequest(
                Verb.DELETE,
                StringUtils.join(
                        new String[]{
                                BASE_CONFIG_COMMAND_REST_URL,
                                id,
                                "application"
                        },
                        SLASH),
                null,
                null);
        return (Application) this.executeRequest(request, null, Application.class);
    }

    /**
     * Get all the clusters this command is associated with.
     *
     * @param id The id of the command to get the clusters for. Not
     *           NULL/empty/blank.
     * @return The set of clusters.
     * @throws GenieException For any other error.
     */
    public Set<Cluster> getClustersForCommand(
            final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("Missing required parameter: id");
        }

        final HttpRequest request = BaseGenieClient.buildRequest(
                Verb.GET,
                StringUtils.join(
                        new String[]{BASE_CONFIG_COMMAND_REST_URL, id, "configs"},
                        SLASH),
                null,
                null);

        @SuppressWarnings("unchecked")
        final Set<Cluster> clusters = (Set<Cluster>) this.executeRequest(request, Set.class, Cluster.class);
        return clusters;
    }

    /**
     * Add some more tags to a given command.
     *
     * @param id   The id of the command to add tags to. Not
     *             Null/empty/blank.
     * @param tags The tags to add. Not null or empty.
     * @return The new set of tags for the given command.
     * @throws GenieException For any other error.
     */
    public Set<String> addTagsToCommand(
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
                        new String[]{BASE_CONFIG_COMMAND_REST_URL, id, "tags"},
                        SLASH),
                null,
                tags);

        @SuppressWarnings("unchecked")
        final Set<String> newTags = (Set<String>) this.executeRequest(request, Set.class, String.class);
        return newTags;
    }

    /**
     * Get the active set of tags for the given command.
     *
     * @param id The id of the command to get tags for. Not
     *           Null/empty/blank.
     * @return The set of tags for the given command.
     * @throws GenieException For any other error.
     */
    public Set<String> getTagsForCommand(final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("Missing required parameter: id");
        }

        final HttpRequest request = BaseGenieClient.buildRequest(
                Verb.GET,
                StringUtils.join(
                        new String[]{BASE_CONFIG_COMMAND_REST_URL, id, "tags"},
                        SLASH),
                null,
                null);

        @SuppressWarnings("unchecked")
        final Set<String> tags = (Set<String>) this.executeRequest(request, Set.class, String.class);
        return tags;
    }

    /**
     * Update the tags for a given command.
     *
     * @param id   The id of the command to update the tags for.
     *             Not null/empty/blank.
     * @param tags The tags to replace existing tag
     *             files with. Not null.
     * @return The new set of command tags.
     * @throws GenieException For any other error.
     */
    public Set<String> updateTagsForCommand(
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
                        new String[]{BASE_CONFIG_COMMAND_REST_URL, id, "tags"},
                        SLASH),
                null,
                tags);

        @SuppressWarnings("unchecked")
        final Set<String> newTags = (Set<String>) this.executeRequest(request, Set.class, String.class);
        return newTags;
    }

    /**
     * Delete all the tags from a given command.
     *
     * @param id The id of the command to delete the tags from.
     *           Not null/empty/blank.
     * @return Empty set if successful
     * @throws GenieException For any other error.
     */
    public Set<String> removeAllTagsForCommand(
            final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("Missing required parameter: id");
        }

        final HttpRequest request = BaseGenieClient.buildRequest(
                Verb.DELETE,
                StringUtils.join(
                        new String[]{BASE_CONFIG_COMMAND_REST_URL, id, "tags"},
                        SLASH),
                null,
                null);

        @SuppressWarnings("unchecked")
        final Set<String> tags = (Set<String>) this.executeRequest(request, Set.class, String.class);
        return tags;
    }

    /**
     * Remove tag from a given command.
     *
     * @param id  The id of the command to delete the tag from. Not
     *            null/empty/blank.
     * @param tag The tag to remove. Not null/empty/blank.
     * @return The tag for the command.
     * @throws GenieException For any other error.
     */
    public Set<String> removeTagForCommand(
            final String id,
            final String tag) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("Missing required parameter: id");
        }

        final HttpRequest request = BaseGenieClient.buildRequest(
                Verb.DELETE,
                StringUtils.join(
                        new String[]{
                                BASE_CONFIG_COMMAND_REST_URL,
                                id,
                                "tags",
                                tag
                        },
                        SLASH),
                null,
                null);

        @SuppressWarnings("unchecked")
        final Set<String> tags = (Set<String>) this.executeRequest(request, Set.class, String.class);
        return tags;
    }
}
