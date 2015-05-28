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
package com.netflix.genie.server.services;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.model.*;
import org.hibernate.validator.constraints.NotBlank;
import org.hibernate.validator.constraints.NotEmpty;
import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Set;

/**
 * Abstraction layer to encapsulate CommandConfig functionality.<br>
 * Classes implementing this abstraction layer must be thread-safe.
 *
 * @author amsharma
 * @author tgianos
 */
@Validated
public interface CommandConfigService {

    /**
     * Create new command configuration.
     *
     * @param command encapsulates the command configuration information to
     *                create. Not null. Valid.
     * @return The command created
     * @throws GenieException if there is an error
     */
    Command createCommand(
            @NotNull(message = "No command entered. Unable to create.")
            @Valid
            final Command command
    ) throws GenieException;

    /**
     * Gets command configuration for given id.
     *
     * @param id unique id for command configuration to get. Not null/empty.
     * @return The command configuration
     * @throws GenieException if there is an error
     */
    Command getCommand(
            @NotBlank(message = "No id entered unable to get.")
            final String id
    ) throws GenieException;

    /**
     * Get command configurations for given filter criteria.
     *
     * @param name       Name of command config
     * @param userName   The name of the user who created the configuration
     * @param statuses   The status of the applications to get. Can be null.
     * @param tags       tags allocated to this command
     * @param page       Page number to start results on
     * @param limit      Max number of results per page
     * @param descending Whether the results should be returned in descending or ascending order
     * @param orderBys   The fields to order the results by
     * @return All the commands matching the specified criteria
     */
    List<Command> getCommands(
            final String name,
            final String userName,
            final Set<CommandStatus> statuses,
            final Set<String> tags,
            final int page,
            final int limit,
            final boolean descending,
            final Set<String> orderBys);

    /**
     * Update command configuration.
     *
     * @param id            The id of the command configuration to update. Not null or
     *                      empty.
     * @param updateCommand contains the information to update the command with
     * @return The updated command
     * @throws GenieException if there is an error
     */
    Command updateCommand(
            @NotBlank(message = "No id entered. Unable to update.")
            final String id,
            @NotNull(message = "No command information entered. Unable to update.")
            final Command updateCommand
    ) throws GenieException;

    /**
     * Delete all commands from database.
     *
     * @return The deleted commands
     * @throws GenieException if there is an error
     */
    List<Command> deleteAllCommands() throws GenieException;

    /**
     * Delete a command configuration from database.
     *
     * @param id unique if of the command configuration to delete
     * @return The deleted command configuration
     * @throws GenieException if there is an error
     */
    Command deleteCommand(
            @NotBlank(message = "No id entered. Unable to delete.")
            final String id
    ) throws GenieException;

    /**
     * Add a configuration files to the command.
     *
     * @param id      The id of the command to add the configuration file to. Not
     *                null/empty/blank.
     * @param configs The configuration files to add. Not null/empty.
     * @return The active set of configurations
     * @throws GenieException if there is an error
     */
    Set<String> addConfigsForCommand(
            @NotBlank(message = "No command id entered. Unable to add configurations.")
            final String id,
            @NotEmpty(message = "No configuration files entered. Unable to add.")
            final Set<String> configs
    ) throws GenieException;

    /**
     * Get the set of configuration files associated with the command with given
     * id.
     *
     * @param id The id of the command to get the configuration files for. Not
     *           null/empty/blank.
     * @return The set of configuration files as paths
     * @throws GenieException if there is an error
     */
    Set<String> getConfigsForCommand(
            @NotBlank(message = "No command id entered. Unable to get configs.")
            final String id
    ) throws GenieException;

    /**
     * Update the set of configuration files associated with the command with
     * given id.
     *
     * @param id      The id of the command to update the configuration files for.
     *                Not null/empty/blank.
     * @param configs The configuration files to replace existing configurations
     *                with. Not null/empty.
     * @return The active set of configurations
     * @throws GenieException if there is an error
     */
    Set<String> updateConfigsForCommand(
            @NotBlank(message = "No command id entered. Unable to update configurations.")
            final String id,
            @NotEmpty(message = "No configs entered. Unable to update.")
            final Set<String> configs
    ) throws GenieException;

    /**
     * Remove all configuration files from the command.
     *
     * @param id The id of the command to remove the configuration file from.
     *           Not null/empty/blank.
     * @return The active set of configurations
     * @throws GenieException if there is an error
     */
    Set<String> removeAllConfigsForCommand(
            @NotBlank(message = "No command id entered. Unable to remove configs.")
            final String id
    ) throws GenieException;

    /**
     * Remove a configuration file from the command.
     *
     * @param id     The id of the command to remove the configuration file from.
     *               Not null/empty/blank.
     * @param config The configuration file to remove. Not null/empty/blank.
     * @return The active set of configurations
     * @throws GenieException if there is an error
     */
    Set<String> removeConfigForCommand(
            @NotBlank(message = "No command id entered. Unable to remove configuration.")
            final String id,
            @NotBlank(message = "No config entered. Unable to remove.")
            final String config
    ) throws GenieException;

    /**
     * Set the application for the command.
     *
     * @param id          The id of the command to add the application file to. Not
     *                    null/empty/blank.
     * @param application The applications to set. Not null.
     * @return The application
     * @throws GenieException if there is an error
     */
    Application setApplicationForCommand(
            @NotBlank(message = "No command id entered. Unable to add applications.")
            final String id,
            @NotNull(message = "No application entered. Unable to set application.")
            final Application application
    ) throws GenieException;

    /**
     * Get the application for a given command.
     *
     * @param id The id of the command to get the application for. Not
     *           null/empty/blank.
     * @return The application or exception if none exists.
     * @throws GenieException if there is an error
     */
    Application getApplicationForCommand(
            @NotBlank(message = "No command id entered. Unable to get applications.")
            final String id
    ) throws GenieException;

    /**
     * Remove the application from the command.
     *
     * @param id The id of the command to remove the application from. Not
     *           null/empty/blank.
     * @return The removed application
     * @throws GenieException if there is an error
     */
    Application removeApplicationForCommand(
            @NotBlank(message = "No command id entered. Unable to remove application.")
            final String id
    ) throws GenieException;

    /**
     * Add tags to the command.
     *
     * @param id   The id of the command to add the tags to. Not
     *             null/empty/blank.
     * @param tags The tags to add. Not null/empty.
     * @return The active set of tags
     * @throws GenieException if there is an error
     */
    Set<String> addTagsForCommand(
            @NotBlank(message = "No command id entered. Unable to add tags.")
            final String id,
            @NotEmpty(message = "No tags entered. Unable to add.")
            final Set<String> tags
    ) throws GenieException;

    /**
     * Get the set of tags associated with the command with given
     * id.
     *
     * @param id The id of the command to get the tags for. Not
     *           null/empty/blank.
     * @return The set of tags as paths
     * @throws GenieException if there is an error
     */
    Set<String> getTagsForCommand(
            @NotBlank(message = "No command id sent. Cannot retrieve tags.")
            final String id
    ) throws GenieException;

    /**
     * Update the set of tags associated with the command with
     * given id.
     *
     * @param id   The id of the command to update the tags for.
     *             Not null/empty/blank.
     * @param tags The tags to replace existing tags
     *             with. Not null/empty.
     * @return The active set of tags
     * @throws GenieException if there is an error
     */
    Set<String> updateTagsForCommand(
            @NotBlank(message = "No command id entered. Unable to update tags.")
            final String id,
            @NotEmpty(message = "No tags entered. Unable to update.")
            final Set<String> tags
    ) throws GenieException;

    /**
     * Remove all tags from the command.
     *
     * @param id The id of the command to remove the tags from.
     *           Not null/empty/blank.
     * @return The active set of tags
     * @throws GenieException if there is an error
     */
    Set<String> removeAllTagsForCommand(
            @NotBlank(message = "No command id entered. Unable to remove tags.")
            final String id
    ) throws GenieException;

    /**
     * Remove a tag from the command.
     *
     * @param id  The id of the command to remove the tag from. Not
     *            null/empty/blank.
     * @param tag The tag to remove. Not null/empty/blank.
     * @return The active set of tags
     * @throws GenieException if there is an error
     */
    Set<String> removeTagForCommand(
            @NotBlank(message = "No command id entered. Unable to remove tag.")
            final String id,
            @NotBlank(message = "No tag entered. Unable to remove.")
            final String tag
    ) throws GenieException;

    /**
     * Get all the clusters the command with given id is associated with.
     *
     * @param id The id of the command to get the clusters for.
     * @param statuses The status of the clusters returned
     * @return The clusters the command is available on.
     * @throws GenieException if there is an error
     */
    List<Cluster> getClustersForCommand(
            @NotBlank(message = "No command id entered. Unable to get clusters.")
            final String id,
            final Set<ClusterStatus> statuses
    ) throws GenieException;
}
