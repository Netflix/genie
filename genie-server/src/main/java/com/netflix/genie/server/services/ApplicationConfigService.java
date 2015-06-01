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
import com.netflix.genie.common.model.Application;
import com.netflix.genie.common.model.ApplicationStatus;
import com.netflix.genie.common.model.CommandStatus;
import com.netflix.genie.common.model.Command;
import org.hibernate.validator.constraints.NotBlank;
import org.hibernate.validator.constraints.NotEmpty;
import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Set;

/**
 * Abstraction layer to encapsulate ApplicationConfig functionality.<br>
 * Classes implementing this abstraction layer must be thread-safe.
 *
 * @author amsharma
 * @author tgianos
 */
@Validated
public interface ApplicationConfigService {

    /**
     * Create new application.
     *
     * @param app The application configuration to create
     * @return The application that was created
     * @throws GenieException if there is an error
     */
    Application createApplication(
            @NotNull(message = "No application entered to create.")
            @Valid
            final Application app
    ) throws GenieException;

    /**
     * Gets application for given id.
     *
     * @param id unique id for application configuration to get. Not null/empty.
     * @return The application if one exists or null if not.
     * @throws GenieException if there is an error
     */
    Application getApplication(
            @NotBlank(message = "No id entered. Unable to get")
            final String id
    ) throws GenieException;

    /**
     * Get applications for given filter criteria.
     *
     * @param name     name of application. Can be null or empty.
     * @param userName The user who created the application. Can be null/empty
     * @param statuses The statuses of the applications to find. Can be null.
     * @param tags     tags allocated to this application
     * @param page     Page number to start results on
     * @param limit    Max number of results per page
     * @return successful response, or one with HTTP error code
     */
    List<Application> getApplications(final String name,
                                      final String userName,
                                      final Set<ApplicationStatus> statuses,
                                      final Set<String> tags,
                                      final int page,
                                      final int limit,
                                      final boolean descending,
                                      final Set<String> orderBys);

    /**
     * Update an application.
     *
     * @param id        The id of the application configuration to update
     * @param updateApp Information to update for the application configuration
     *                  with
     * @return The updated application
     * @throws GenieException if there is an error
     */
    Application updateApplication(
            @NotBlank(message = "No application id entered. Unable to update.")
            final String id,
            @NotNull(message = "No application information entered. Unable to update.")
            final Application updateApp
    ) throws GenieException;

    /**
     * Delete all applications from database.
     *
     * @return The deleted applications
     * @throws GenieException if there is an error
     */
    List<Application> deleteAllApplications() throws GenieException;

    /**
     * Delete an application configuration from database.
     *
     * @param id unique id of application configuration to delete
     * @return The deleted application
     * @throws GenieException if there is an error
     */
    Application deleteApplication(
            @NotBlank(message = "No application id entered. Unable to delete.")
            final String id
    ) throws GenieException;

    /**
     * Add a configuration file to the application.
     *
     * @param id      The id of the application to add the configuration file to. Not
     *                null/empty/blank.
     * @param configs The configuration files to add. Not null/empty.
     * @return The active set of configurations
     * @throws GenieException if there is an error
     */
    Set<String> addConfigsToApplication(
            @NotBlank(message = "No application id entered. Unable to add configurations.")
            final String id,
            @NotEmpty(message = "No configuration files entered.")
            final Set<String> configs
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
            @NotBlank(message = "No application id entered. Unable to get configs.")
            final String id
    ) throws GenieException;

    /**
     * Update the set of configuration files associated with the application
     * with given id.
     *
     * @param id      The id of the application to update the configuration files
     *                for. Not null/empty/blank.
     * @param configs The configuration files to replace existing configurations
     *                with. Not null/empty.
     * @return The active set of configurations
     * @throws GenieException if there is an error
     */
    Set<String> updateConfigsForApplication(
            @NotBlank(message = "No application id entered. Unable to update configurations.")
            final String id,
            @NotNull(message = "No configs entered. Unable to update. If you want, use delete API.")
            final Set<String> configs
    ) throws GenieException;

    /**
     * Remove all configuration files from the application.
     *
     * @param id The id of the application to remove the configuration file
     *           from. Not null/empty/blank.
     * @return The active set of configurations
     * @throws GenieException if there is an error
     */
    Set<String> removeAllConfigsForApplication(
            @NotBlank(message = "No application id entered. Unable to remove configs.")
            final String id
    ) throws GenieException;

    /**
     * Remove a configuration file from the application.
     *
     * @param id     The id of the application to remove the configuration file
     *               from. Not null/empty/blank.
     * @param config The configuration file to remove. Not null/empty/blank.
     * @return The active set of configurations
     * @throws GenieException if there is an error
     */
    Set<String> removeConfigForApplication(
            @NotBlank(message = "No application id entered. Unable to remove configuration.")
            final String id,
            @NotBlank(message = "No config entered. Unable to remove.")
            final String config
    ) throws GenieException;

    /**
     * Add a jar file to the application.
     *
     * @param id   The id of the application to add the jar file to. Not
     *             null/empty/blank.
     * @param jars The jar files to add. Not null.
     * @return The active set of configurations
     * @throws GenieException if there is an error
     */
    Set<String> addJarsForApplication(
            @NotBlank(message = "No application id entered. Unable to add jars.")
            final String id,
            @NotEmpty(message = "No jars entered. Unable to add jars.")
            final Set<String> jars
    ) throws GenieException;

    /**
     * Get the set of jar files associated with the application with given id.
     *
     * @param id The id of the application to get the jar files for. Not
     *           null/empty/blank.
     * @return The set of jar files as paths
     * @throws GenieException if there is an error
     */
    Set<String> getJarsForApplication(
            @NotBlank(message = "No application id entered. Unable to get jars.")
            final String id
    ) throws GenieException;

    /**
     * Update the set of jar files associated with the application with given
     * id.
     *
     * @param id   The id of the application to update the jar files for. Not
     *             null/empty/blank.
     * @param jars The jar files to replace existing jars with. Not null/empty.
     * @return The active set of configurations
     * @throws GenieException if there is an error
     */
    Set<String> updateJarsForApplication(
            @NotBlank(message = "No application id entered. Unable to update jars.")
            final String id,
            @NotNull(message = "No jars entered. Unable to update.")
            final Set<String> jars
    ) throws GenieException;

    /**
     * Remove all jar files from the application.
     *
     * @param id The id of the application to remove the configuration file
     *           from. Not null/empty/blank.
     * @return The active set of configurations
     * @throws GenieException if there is an error
     */
    Set<String> removeAllJarsForApplication(
            @NotBlank(message = "No application id entered. Unable to remove jars.")
            final String id
    ) throws GenieException;

    /**
     * Remove a jar file from the application.
     *
     * @param id  The id of the application to remove the jar file from. Not
     *            null/empty/blank.
     * @param jar The jar file to remove. Not null/empty/blank.
     * @return The active set of configurations
     * @throws GenieException if there is an error
     */
    Set<String> removeJarForApplication(
            @NotBlank(message = "No application id entered. Unable to remove jar.")
            final String id,
            @NotBlank(message = "No jar entiered. Unable to remove jar.")
            final String jar
    ) throws GenieException;

    /**
     * Add tags to the application.
     *
     * @param id   The id of the application to add the tags to. Not
     *             null/empty/blank.
     * @param tags The tags to add. Not null/empty.
     * @return The active set of tags
     * @throws GenieException if there is an error
     */
    Set<String> addTagsForApplication(
            @NotBlank(message = "No application id entered. Unable to add tags.")
            final String id,
            @NotEmpty(message = "No tags entered. Unable to add.")
            final Set<String> tags
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
            @NotBlank(message = "No application id entered. Cannot retrieve tags.")
            final String id
    ) throws GenieException;

    /**
     * Update the set of tags associated with the application with
     * given id.
     *
     * @param id   The id of the application to update the tags for.
     *             Not null/empty/blank.
     * @param tags The tags to replace existing tags
     *             with. Not null/empty.
     * @return The active set of tags
     * @throws GenieException if there is an error
     */
    Set<String> updateTagsForApplication(
            @NotBlank(message = "No application id entered. Unable to update tags.")
            final String id,
            @NotNull(message = "No tags entered unable to update tags.")
            final Set<String> tags
    ) throws GenieException;

    /**
     * Remove all tags from the application.
     *
     * @param id The id of the application to remove the tags from.
     *           Not null/empty/blank.
     * @return The active set of tags
     * @throws GenieException if there is an error
     */
    Set<String> removeAllTagsForApplication(
            @NotBlank(message = "No application id entered. Unable to remove tags.")
            final String id
    ) throws GenieException;

    /**
     * Remove a tag from the application.
     *
     * @param id  The id of the application to remove the tag from. Not
     *            null/empty/blank.
     * @param tag The tag to remove. Not null/empty/blank.
     * @return The active set of tags
     * @throws GenieException if there is an error
     */
    Set<String> removeTagForApplication(
            @NotBlank(message = "No application id entered. Unable to remove tag.")
            final String id,
            @NotBlank(message = "No tag entered. Unable to remove.")
            final String tag
    ) throws GenieException;

    /**
     * Get all the commands the application with given id is associated with.
     *
     * @param id The id of the application to get the commands for.
     * @param statuses The status of the commands returned
     * @return The commands the application is a part of.
     * @throws GenieException if there is an error
     */
    List<Command> getCommandsForApplication(
            @NotBlank(message = "No application id entered. Unable to get commands.")
            final String id,
            final Set<CommandStatus> statuses
    ) throws GenieException;
}
