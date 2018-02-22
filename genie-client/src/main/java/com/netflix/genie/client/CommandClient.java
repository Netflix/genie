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
import com.netflix.genie.client.apis.CommandService;
import com.netflix.genie.client.configs.GenieNetworkConfiguration;
import com.netflix.genie.client.exceptions.GenieClientException;
import com.netflix.genie.common.dto.Application;
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
 * Client library for the Command Service.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Slf4j
public class CommandClient extends BaseGenieClient {

    private CommandService commandService;

    /**
     * Constructor.
     *
     * @param url                       The endpoint URL of the Genie API. Not null or empty
     * @param interceptors              Any interceptors to configure the client with, can include security ones
     * @param genieNetworkConfiguration The network configuration parameters. Could be null
     * @throws GenieClientException On error
     */
    public CommandClient(
        @NotEmpty final String url,
        @Nullable final List<Interceptor> interceptors,
        @Nullable final GenieNetworkConfiguration genieNetworkConfiguration
    ) throws GenieClientException {
        super(url, interceptors, genieNetworkConfiguration);
        this.commandService = this.getService(CommandService.class);
    }

    /* CRUD Methods */

    /**
     * Create a command ing genie.
     *
     * @param command A command object.
     *
     * @return id Id of the command created.
     *
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException For Network and other IO issues.
     */
    public String createCommand(
        final Command command
    ) throws IOException, GenieClientException {
        if (command == null) {
            throw new IllegalArgumentException("Command cannot be null.");
        }
        return getIdFromLocation(commandService.createCommand(command).execute().headers().get("location"));
    }

    /**
     * Method to get a list of all the commands.
     *
     * @return A list of commands.
     *
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException For Network and other IO issues.
     */
    public List<Command> getCommands() throws IOException, GenieClientException {
        return this.getCommands(null, null, null, null);
    }

    /**
     * Method to get a list of all the commands from Genie for the query parameters specified.
     *
     * @param name The name of the commands.
     * @param user The user who created the command.
     * @param statusList The list of Command statuses.
     * @param tagList The list of tags.
     *
     * @return A list of commands.
     *
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException For Network and other IO issues.
     */
    public List<Command> getCommands(
        final String name,
        final String user,
        final List<String> statusList,
        final List<String> tagList
    ) throws IOException, GenieClientException {
        final List<Command> commandList = new ArrayList<>();
        final JsonNode jNode =  commandService.getCommands(
            name,
            user,
            statusList,
            tagList
        ).execute().body()
            .get("_embedded");
        if (jNode != null) {
            for (final JsonNode objNode : jNode.get("commandList")) {
                final Command command  = this.treeToValue(objNode, Command.class);
                commandList.add(command);
            }
        }
        return commandList;
    }

    /**
     * Method to get a Command from Genie.
     *
     * @param commandId The id of the command to get.
     * @return The command details.
     *
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException For Network and other IO issues.
     */
    public Command getCommand(
        final String commandId
    ) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(commandId)) {
            throw new IllegalArgumentException("Missing required parameter: commandId.");
        }
        return commandService.getCommand(commandId).execute().body();
    }

    /**
     * Method to delete a command from Genie.
     *
     * @param commandId The id of the command.
     *
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException For Network and other IO issues.
     */
    public void deleteCommand(final String commandId) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(commandId)) {
            throw new IllegalArgumentException("Missing required parameter: commandId.");
        }
        commandService.deleteCommand(commandId).execute();
    }

    /**
     * Method to delete all commands from Genie.
     *
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException For Network and other IO issues.
     */
    public void deleteAllCommands() throws IOException, GenieClientException {
        commandService.deleteAllCommands().execute();
    }

    /**
     * Method to patch a command using json patch instructions.
     *
     * @param commandId The id of the command.
     * @param patch The patch object specifying all the instructions.
     *
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException For Network and other IO issues.
     */
    public void patchCommand(final String commandId, final JsonPatch patch) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(commandId)) {
            throw new IllegalArgumentException("Missing required parameter: commandId.");
        }

        if (patch == null) {
            throw new IllegalArgumentException("Patch cannot be null");
        }

        commandService.patchCommand(commandId, patch).execute();
    }

    /**
     * Method to updated a command.
     *
     * @param commandId The id of the command.
     * @param command The updated command object to use.
     *
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException For Network and other IO issues.
     */
    public void updateCommand(final String commandId, final Command command) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(commandId)) {
            throw new IllegalArgumentException("Missing required parameter: commandId.");
        }

        if (command == null) {
            throw new IllegalArgumentException("Patch cannot be null");
        }

        commandService.updateCommand(commandId, command).execute();
    }

    /****************** Methods to manipulate configs for a command   *********************/

    /**
     * Method to get all the configs for a command.
     *
     * @param commandId The id of the command.
     *
     * @return The set of configs for the command.
     *
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException For Network and other IO issues.
     */
    public Set<String> getConfigsForCommand(final String commandId) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(commandId)) {
            throw new IllegalArgumentException("Missing required parameter: commandId.");
        }

        return commandService.getConfigsForCommand(commandId).execute().body();
    }

    /**
     * Method to add configs to a command.
     *
     * @param commandId The id of the command.
     * @param configs The set of configs to add.
     *
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException For Network and other IO issues.
     */
    public void addConfigsToCommand(
        final String commandId, final Set<String> configs
    ) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(commandId)) {
            throw new IllegalArgumentException("Missing required parameter: commandId.");
        }

        if (configs == null || configs.isEmpty()) {
            throw new IllegalArgumentException("Configs cannot be null or empty");
        }

        commandService.addConfigsToCommand(commandId, configs).execute();
    }

    /**
     * Method to update configs for a command.
     *
     * @param commandId The id of the command.
     * @param configs The set of configs to add.
     *
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException For Network and other IO issues.
     */
    public void updateConfigsForCommand(
        final String commandId, final Set<String> configs
    ) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(commandId)) {
            throw new IllegalArgumentException("Missing required parameter: commandId.");
        }

        if (configs == null || configs.isEmpty()) {
            throw new IllegalArgumentException("Configs cannot be null or empty");
        }

        commandService.updateConfigsForCommand(commandId, configs).execute();
    }

    /**
     * Remove all configs for this command.
     *
     * @param commandId The id of the command.
     *
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException For Network and other IO issues.
     */
    public void removeAllConfigsForCommand(
        final String commandId
    ) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(commandId)) {
            throw new IllegalArgumentException("Missing required parameter: commandId.");
        }

        commandService.removeAllConfigsForCommand(commandId).execute();
    }

    /****************** Methods to manipulate dependencies for a command   *********************/

    /**
     * Method to get all the dependency files for an command.
     *
     * @param commandId The id of the command.
     * @return The set of dependencies for the command.
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues
     */
    public Set<String> getDependenciesForCommand(final String commandId) throws IOException,
        GenieClientException {
        if (StringUtils.isEmpty(commandId)) {
            throw new IllegalArgumentException("Missing required parameter: commandId.");
        }

        return commandService.getDependenciesForCommand(commandId).execute().body();
    }

    /**
     * Method to add dependencies to a command.
     *
     * @param commandId The id of the command.
     * @param dependencies  The set of dependencies to add.
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues
     */
    public void addDependenciesToCommand(
        final String commandId, final Set<String> dependencies
    ) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(commandId)) {
            throw new IllegalArgumentException("Missing required parameter: commandId.");
        }

        if (dependencies == null || dependencies.isEmpty()) {
            throw new IllegalArgumentException("Dependencies cannot be null or empty");
        }

        commandService.addDependenciesToCommand(commandId, dependencies).execute();
    }

    /**
     * Method to update dependencies for a command.
     *
     * @param commandId The id of the command.
     * @param dependencies  The set of dependencies to add.
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues
     */
    public void updateDependenciesForCommand(
        final String commandId, final Set<String> dependencies
    ) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(commandId)) {
            throw new IllegalArgumentException("Missing required parameter: commandId.");
        }

        if (dependencies == null || dependencies.isEmpty()) {
            throw new IllegalArgumentException("Dependencies cannot be null or empty");
        }

        commandService.updateDependenciesForCommand(commandId, dependencies).execute();
    }

    /**
     * Remove all dependencies for this command.
     *
     * @param commandId The id of the command.
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues
     */
    public void removeAllDependenciesForCommand(
        final String commandId
    ) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(commandId)) {
            throw new IllegalArgumentException("Missing required parameter: commandId.");
        }

        commandService.removeAllDependenciesForCommand(commandId).execute();
    }

    /****************** Methods to manipulate applications for a command   *********************/

    /**
     * Method to get all the applications for a command.
     *
     * @param commandId The id of the command.
     *
     * @return The set of applications for the command.
     *
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException For Network and other IO issues.
     */
    public List<Application> getApplicationsForCommand(final String commandId)
        throws IOException, GenieClientException {
        if (StringUtils.isEmpty(commandId)) {
            throw new IllegalArgumentException("Missing required parameter: commandId.");
        }

        return commandService.getApplicationsForCommand(commandId).execute().body();
    }

    /**
     * Method to get all the clusters for a command.
     *
     * @param commandId The id of the command.
     *
     * @return The set of clusters for the command.
     *
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException For Network and other IO issues.
     */
    public List<Cluster> getClustersForCommand(final String commandId) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(commandId)) {
            throw new IllegalArgumentException("Missing required parameter: commandId.");
        }

        return commandService.getClustersForCommand(commandId).execute().body();
    }

    /**
     * Method to add applications to a command.
     *
     * @param commandId The id of the command.
     * @param applicationIds The set of applications ids to add.
     *
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException For Network and other IO issues.
     */
    public void addApplicationsToCommand(
        final String commandId, final List<String> applicationIds
    ) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(commandId)) {
            throw new IllegalArgumentException("Missing required parameter: commandId.");
        }

        if (applicationIds == null || applicationIds.isEmpty()) {
            throw new IllegalArgumentException("applicationIds cannot be null or empty");
        }

        commandService.addApplicationsToCommand(commandId, applicationIds).execute();
    }

    /**
     * Method to update applications for a command.
     *
     * @param commandId The id of the command.
     * @param applicationIds The set of application ids to add.
     *
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException For Network and other IO issues.
     */
    public void updateApplicationsForCommand(
        final String commandId, final List<String> applicationIds
    ) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(commandId)) {
            throw new IllegalArgumentException("Missing required parameter: commandId.");
        }

        if (applicationIds == null || applicationIds.isEmpty()) {
            throw new IllegalArgumentException("applicationIds cannot be null or empty");
        }

        commandService.setApplicationsForCommand(commandId, applicationIds).execute();
    }

    /**
     * Remove an application from a command.
     *
     * @param commandId The id of the command.
     * @param applicationId The id of the application to remove.
     *
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException For Network and other IO issues.
     */
    public void removeApplicationFromCommand(
        final String commandId,
        final String applicationId
    ) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(commandId)) {
            throw new IllegalArgumentException("Missing required parameter: commandId.");
        }

        if (StringUtils.isEmpty(applicationId)) {
            throw new IllegalArgumentException("Missing required parameter: applicationId.");
        }

        commandService.removeApplicationForCommand(commandId, applicationId).execute();
    }

    /**
     * Remove all applications for this command.
     *
     * @param commandId The id of the command.
     *
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException For Network and other IO issues.
     */
    public void removeAllApplicationsForCommand(
        final String commandId
    ) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(commandId)) {
            throw new IllegalArgumentException("Missing required parameter: commandId.");
        }

        commandService.removeAllApplicationsForCommand(commandId).execute();
    }

    /****************** Methods to manipulate tags for a command   *********************/

    /**
     * Method to get all the tags for a command.
     *
     * @param commandId The id of the command.
     *
     * @return The set of configs for the command.
     *
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException For Network and other IO issues.
     */
    public Set<String> getTagsForCommand(final String commandId) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(commandId)) {
            throw new IllegalArgumentException("Missing required parameter: commandId.");
        }

        return commandService.getTagsForCommand(commandId).execute().body();
    }

    /**
     * Method to add tags to a command.
     *
     * @param commandId The id of the command.
     * @param tags The set of tags to add.
     *
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException For Network and other IO issues.
     */
    public void addTagsToCommand(
        final String commandId, final Set<String> tags
    ) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(commandId)) {
            throw new IllegalArgumentException("Missing required parameter: commandId.");
        }

        if (tags == null || tags.isEmpty()) {
            throw new IllegalArgumentException("Tags cannot be null or empty");
        }

        commandService.addTagsToCommand(commandId, tags).execute();
    }

    /**
     * Method to update tags for a command.
     *
     * @param commandId The id of the command.
     * @param tags The set of tags to add.
     *
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException For Network and other IO issues.
     */
    public void updateTagsForCommand(
        final String commandId, final Set<String> tags
    ) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(commandId)) {
            throw new IllegalArgumentException("Missing required parameter: commandId.");
        }

        if (tags == null || tags.isEmpty()) {
            throw new IllegalArgumentException("Tags cannot be null or empty");
        }

        commandService.updateTagsForCommand(commandId, tags).execute();
    }

    /**
     * Remove a tag from a command.
     *
     * @param commandId The id of the command.
     * @param tag The tag to remove.
     *
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException For Network and other IO issues.
     */
    public void removeTagFromCommand(
        final String commandId,
        final String tag
    ) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(commandId)) {
            throw new IllegalArgumentException("Missing required parameter: commandId.");
        }

        if (StringUtils.isEmpty(tag)) {
            throw new IllegalArgumentException("Missing required parameter: tag.");
        }

        commandService.removeTagForCommand(commandId, tag).execute();
    }

    /**
     * Remove all tags for this command.
     *
     * @param commandId The id of the command.
     *
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException For Network and other IO issues.
     */
    public void removeAllTagsForCommand(
        final String commandId
    ) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(commandId)) {
            throw new IllegalArgumentException("Missing required parameter: commandId.");
        }

        commandService.removeAllTagsForCommand(commandId).execute();
    }
}
