/*
 *
 *  Copyright 2014 Netflix, Inc.
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

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.model.Application;
import com.netflix.genie.common.model.Command;
import java.util.List;
import java.util.Set;

/**
 * Abstraction layer to encapsulate ApplicationConfig functionality.<br>
 * Classes implementing this abstraction layer must be thread-safe.
 *
 * @author amsharma
 * @author tgianos
 */
public interface ApplicationConfigService {

    /**
     * Gets application for given id.
     *
     * @param id unique id for application configuration to get. Not null/empty.
     * @return The application if one exists or null if not.
     * @throws CloudServiceException
     */
    Application getApplication(final String id) throws CloudServiceException;

    /**
     * Get applications for given filter criteria.
     *
     * @param name name of application. Can be null or empty.
     * @param userName The user who created the application. Can be null/empty
     * @param page Page number to start results on
     * @param limit Max number of results per page
     * @return successful response, or one with HTTP error code
     */
    List<Application> getApplications(final String name,
            final String userName,
            final int page,
            final int limit);

    /**
     * Create new application.
     *
     * @param app The application configuration to create
     * @return The application that was created
     * @throws CloudServiceException
     */
    Application createApplication(
            final Application app) throws CloudServiceException;

    /**
     * Update an application.
     *
     * @param id The id of the application configuration to update
     * @param updateApp Information to update for the application configuration
     * with
     * @return The updated application
     * @throws CloudServiceException
     */
    Application updateApplication(
            final String id,
            final Application updateApp) throws CloudServiceException;

    /**
     * Delete all applications from database.
     *
     * @return The deleted applications
     * @throws CloudServiceException
     */
    List<Application> deleteAllApplications() throws CloudServiceException;

    /**
     * Delete an application configuration from database.
     *
     * @param id unique id of application configuration to delete
     * @return The deleted application
     * @throws CloudServiceException
     */
    Application deleteApplication(final String id) throws CloudServiceException;

    /**
     * Add a configuration file to the application.
     *
     * @param id The id of the application to add the configuration file to. Not
     * null/empty/blank.
     * @param configs The configuration files to add. Not null/empty.
     * @return The active set of configurations
     * @throws CloudServiceException
     */
    Set<String> addConfigsToApplication(
            final String id,
            final Set<String> configs) throws CloudServiceException;

    /**
     * Get the set of configuration files associated with the application with
     * given id.
     *
     * @param id The id of the application to get the configuration files for.
     * Not null/empty/blank.
     * @return The set of configuration files as paths
     * @throws CloudServiceException
     */
    Set<String> getConfigsForApplication(
            final String id) throws CloudServiceException;

    /**
     * Update the set of configuration files associated with the application
     * with given id.
     *
     * @param id The id of the application to update the configuration files
     * for. Not null/empty/blank.
     * @param configs The configuration files to replace existing configurations
     * with. Not null/empty.
     * @return The active set of configurations
     * @throws CloudServiceException
     */
    Set<String> updateConfigsForApplication(
            final String id,
            final Set<String> configs) throws CloudServiceException;

    /**
     * Remove all configuration files from the application.
     *
     * @param id The id of the application to remove the configuration file
     * from. Not null/empty/blank.
     * @return The active set of configurations
     * @throws CloudServiceException
     */
    Set<String> removeAllConfigsForApplication(
            final String id) throws CloudServiceException;

    /**
     * Remove a configuration file from the application.
     *
     * @param id The id of the application to remove the configuration file
     * from. Not null/empty/blank.
     * @param config The configuration file to remove. Not null/empty/blank.
     * @return The active set of configurations
     * @throws CloudServiceException
     */
    Set<String> removeApplicationConfig(
            final String id,
            final String config) throws CloudServiceException;

    /**
     * Add a jar file to the application.
     *
     * @param id The id of the application to add the jar file to. Not
     * null/empty/blank.
     * @param jars The jar files to add. Not null.
     * @return The active set of configurations
     * @throws CloudServiceException
     */
    Set<String> addJarsForApplication(
            final String id,
            final Set<String> jars) throws CloudServiceException;

    /**
     * Get the set of jar files associated with the application with given id.
     *
     * @param id The id of the application to get the jar files for. Not
     * null/empty/blank.
     * @return The set of jar files as paths
     * @throws CloudServiceException
     */
    Set<String> getJarsForApplication(
            final String id) throws CloudServiceException;

    /**
     * Update the set of jar files associated with the application with given
     * id.
     *
     * @param id The id of the application to update the jar files for. Not
     * null/empty/blank.
     * @param jars The jar files to replace existing jars with. Not null/empty.
     * @return The active set of configurations
     * @throws CloudServiceException
     */
    Set<String> updateJarsForApplication(
            final String id,
            final Set<String> jars) throws CloudServiceException;

    /**
     * Remove all jar files from the application.
     *
     * @param id The id of the application to remove the configuration file
     * from. Not null/empty/blank.
     * @return The active set of configurations
     * @throws CloudServiceException
     */
    Set<String> removeAllJarsForApplication(
            final String id) throws CloudServiceException;

    /**
     * Remove a jar file from the application.
     *
     * @param id The id of the application to remove the jar file from. Not
     * null/empty/blank.
     * @param jar The jar file to remove. Not null/empty/blank.
     * @return The active set of configurations
     * @throws CloudServiceException
     */
    Set<String> removeJarForApplication(
            final String id,
            final String jar) throws CloudServiceException;

    /**
     * Get all the commands the application with given id is associated with.
     *
     * @param id The id of the application to get the commands for.
     * @return The commands the application is a part of.
     * @throws CloudServiceException
     */
    Set<Command> getCommandsForApplication(
            final String id) throws CloudServiceException;
}
