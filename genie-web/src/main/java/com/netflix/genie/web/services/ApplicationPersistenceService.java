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
package com.netflix.genie.web.services;

import com.github.fge.jsonpatch.JsonPatch;
import com.netflix.genie.common.dto.ApplicationStatus;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.internal.dto.v4.Application;
import com.netflix.genie.common.internal.dto.v4.ApplicationRequest;
import com.netflix.genie.common.internal.dto.v4.Command;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.validation.annotation.Validated;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.Set;

/**
 * Application service interface.
 *
 * @author amsharma
 * @author tgianos
 */
@Validated
public interface ApplicationPersistenceService {

    /**
     * Create new application.
     *
     * @param applicationRequest The application request containing the metadata of the application to create
     * @return The id of the application that was created
     * @throws GenieException if there is an error
     */
    String createApplication(
        @NotNull(message = "No application entered to create.")
        @Valid final ApplicationRequest applicationRequest
    ) throws GenieException;

    /**
     * Gets application for given id.
     *
     * @param id unique id for application configuration to get. Not null/empty.
     * @return The application if one exists or null if not.
     * @throws GenieException if there is an error
     */
    Application getApplication(
        @NotBlank(message = "No id entered. Unable to get") final String id
    ) throws GenieException;

    /**
     * Get applications for given filter criteria.
     *
     * @param name     Name of application. Can be null or empty.
     * @param user     The user who created the application. Can be null/empty
     * @param statuses The statuses of the applications to find. Can be null.
     * @param tags     Tags allocated to this application
     * @param type     The type of the application to find
     * @param pageable The page requested from the search results
     * @return The page of found applications
     */
    Page<Application> getApplications(
        @Nullable final String name,
        @Nullable final String user,
        @Nullable final Set<ApplicationStatus> statuses,
        @Nullable final Set<String> tags,
        @Nullable final String type,
        final Pageable pageable
    );

    /**
     * Update an application.
     *
     * @param id        The id of the application configuration to update
     * @param updateApp Information to update for the application configuration
     *                  with
     * @throws GenieException if there is an error
     */
    void updateApplication(
        @NotBlank(message = "No application id entered. Unable to update.") final String id,
        @NotNull(message = "No application information entered. Unable to update.")
        @Valid final Application updateApp
    ) throws GenieException;

    /**
     * Patch an application with the given json patch.
     *
     * @param id    The id of the application to update
     * @param patch The json patch to use to update the given application
     * @throws GenieException if there is an error
     */
    void patchApplication(@NotBlank final String id, @NotNull final JsonPatch patch) throws GenieException;

    /**
     * Delete all applications from database.
     *
     * @throws GenieException if there is an error
     */
    void deleteAllApplications() throws GenieException;

    /**
     * Delete an application configuration from database.
     *
     * @param id unique id of application configuration to delete
     * @throws GenieException if there is an error
     */
    void deleteApplication(
        @NotBlank(message = "No application id entered. Unable to delete.") final String id
    ) throws GenieException;

    // TODO: Look into removing all these extraneous APIs

    /**
     * Add a configuration file to the application.
     *
     * @param id      The id of the application to add the configuration file to. Not
     *                null/empty/blank.
     * @param configs The configuration files to add. Not null/empty.
     * @throws GenieException if there is an error
     */
    void addConfigsToApplication(
        @NotBlank(message = "No application id entered. Unable to add configurations.") final String id,
        @NotEmpty(message = "No configuration files entered.") final Set<String> configs
    ) throws GenieException;

    /**
     * Get the set of configuration files associated with the application with
     * given id.
     *
     * @param id The id of the application to get the configuration files for.
     *           Not null/empty/blank.
     * @return The set of configuration files as paths
     * @throws GenieException if there is an error
     */
    Set<String> getConfigsForApplication(
        @NotBlank(message = "No application id entered. Unable to get configs.") final String id
    ) throws GenieException;

    /**
     * Update the set of configuration files associated with the application
     * with given id.
     *
     * @param id      The id of the application to update the configuration files
     *                for. Not null/empty/blank.
     * @param configs The configuration files to replace existing configurations
     *                with. Not null/empty.
     * @throws GenieException if there is an error
     */
    void updateConfigsForApplication(
        @NotBlank(message = "No application id entered. Unable to update configurations.") final String id,
        @NotNull(
            message = "No configs entered. Unable to update. If you want, use delete API."
        ) final Set<String> configs
    ) throws GenieException;

    /**
     * Remove all configuration files from the application.
     *
     * @param id The id of the application to remove the configuration file
     *           from. Not null/empty/blank.
     * @throws GenieException if there is an error
     */
    void removeAllConfigsForApplication(
        @NotBlank(message = "No application id entered. Unable to remove configs.") final String id
    ) throws GenieException;

    /**
     * Remove a configuration file from the application.
     *
     * @param id     The id of the application to remove the configuration file
     *               from. Not null/empty/blank.
     * @param config The configuration file to remove. Not null/empty/blank.
     * @throws GenieException if there is an error
     */
    void removeConfigForApplication(
        @NotBlank(message = "No application id entered. Unable to remove configuration.") final String id,
        @NotBlank(message = "No config entered. Unable to remove.") final String config
    ) throws GenieException;

    /**
     * Add dependency files to the application.
     *
     * @param id           The id of the application to add the dependency file to. Not
     *                     null/empty/blank.
     * @param dependencies The dependency files to add. Not null.
     * @throws GenieException if there is an error
     */
    void addDependenciesForApplication(
        @NotBlank(message = "No application id entered. Unable to add dependencies.") final String id,
        @NotEmpty(message = "No dependencies entered. Unable to add dependencies.") final Set<String> dependencies
    ) throws GenieException;

    /**
     * Get the set of dependency files associated with the application with given id.
     *
     * @param id The id of the application to get the dependency files for. Not
     *           null/empty/blank.
     * @return The set of dependency files as paths
     * @throws GenieException if there is an error
     */
    Set<String> getDependenciesForApplication(
        @NotBlank(message = "No application id entered. Unable to get dependencies.") final String id
    ) throws GenieException;

    /**
     * Update the set of dependency files associated with the application with given
     * id.
     *
     * @param id           The id of the application to update the dependency files for. Not
     *                     null/empty/blank.
     * @param dependencies The dependency files to replace existing dependencies with. Not null/empty.
     * @throws GenieException if there is an error
     */
    void updateDependenciesForApplication(
        @NotBlank(message = "No application id entered. Unable to update dependencies.") final String id,
        @NotNull(message = "No dependencies entered. Unable to update.") final Set<String> dependencies
    ) throws GenieException;

    /**
     * Remove all dependency files from the application.
     *
     * @param id The id of the application to remove the configuration file
     *           from. Not null/empty/blank.
     * @throws GenieException if there is an error
     */
    void removeAllDependenciesForApplication(
        @NotBlank(message = "No application id entered. Unable to remove dependencies.") final String id
    ) throws GenieException;

    /**
     * Remove a dependency file from the application.
     *
     * @param id         The id of the application to remove the dependency file from. Not
     *                   null/empty/blank.
     * @param dependency The dependency file to remove. Not null/empty/blank.
     * @throws GenieException if there is an error
     */
    void removeDependencyForApplication(
        @NotBlank(message = "No application id entered. Unable to remove dependency.") final String id,
        @NotBlank(message = "No dependency entered. Unable to remove dependency.") final String dependency
    ) throws GenieException;

    /**
     * Add tags to the application.
     *
     * @param id   The id of the application to add the tags to. Not
     *             null/empty/blank.
     * @param tags The tags to add. Not null/empty.
     * @throws GenieException if there is an error
     */
    void addTagsForApplication(
        @NotBlank(message = "No application id entered. Unable to add tags.") final String id,
        @NotEmpty(message = "No tags entered. Unable to add.") final Set<String> tags
    ) throws GenieException;

    /**
     * Get the set of tags associated with the application with given
     * id.
     *
     * @param id The id of the application to get the tags for. Not
     *           null/empty/blank.
     * @return The set of tags as paths
     * @throws GenieException if there is an error
     */
    Set<String> getTagsForApplication(
        @NotBlank(message = "No application id entered. Cannot retrieve tags.") final String id
    ) throws GenieException;

    /**
     * Update the set of tags associated with the application with
     * given id.
     *
     * @param id   The id of the application to update the tags for.
     *             Not null/empty/blank.
     * @param tags The tags to replace existing tags
     *             with. Not null/empty.
     * @throws GenieException if there is an error
     */
    void updateTagsForApplication(
        @NotBlank(message = "No application id entered. Unable to update tags.") final String id,
        @NotNull(message = "No tags entered unable to update tags.") final Set<String> tags
    ) throws GenieException;

    /**
     * Remove all tags from the application.
     *
     * @param id The id of the application to remove the tags from.
     *           Not null/empty/blank.
     * @throws GenieException if there is an error
     */
    void removeAllTagsForApplication(
        @NotBlank(message = "No application id entered. Unable to remove tags.") final String id
    ) throws GenieException;

    /**
     * Remove a tag from the application.
     *
     * @param id  The id of the application to remove the tag from. Not
     *            null/empty/blank.
     * @param tag The tag to remove. Not null/empty/blank.
     * @throws GenieException if there is an error
     */
    void removeTagForApplication(
        @NotBlank(message = "No application id entered. Unable to remove tag.") final String id,
        @NotBlank(message = "No tag entered. Unable to remove.") final String tag
    ) throws GenieException;

    /**
     * Get all the commands the application with given id is associated with.
     *
     * @param id       The id of the application to get the commands for.
     * @param statuses The status of the commands returned
     * @return The commands the application is a part of.
     * @throws GenieException if there is an error
     */
    Set<Command> getCommandsForApplication(
        @NotBlank(message = "No application id entered. Unable to get commands.") final String id,
        @Nullable final Set<CommandStatus> statuses
    ) throws GenieException;
}
