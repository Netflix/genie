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
import java.util.List;

/**
 * Abstraction layer to encapsulate ApplicationConfig functionality.<br>
 * Classes implementing this abstraction layer must be thread-safe.
 *
 * @author amsharma
 * @author tgianos
 */
public interface ApplicationConfigService {

    /**
     * Gets application configuration for given id.
     *
     * @param id unique id for application configuration to get. Not null/empty.
     * @return The application if one exists or null if not.
     * @throws CloudServiceException
     */
    Application getApplicationConfig(final String id) throws CloudServiceException;

    /**
     * Get application configuration for given filter criteria.
     *
     * @param name name of application. Can be null or empty.
     * @param userName The user who created the application. Can be null/empty
     * @param page Page number to start results on
     * @param limit Max number of results per page
     * @return successful response, or one with HTTP error code
     */
    List<Application> getApplicationConfigs(final String name,
            final String userName,
            final int page,
            final int limit);

    /**
     * Create new application configuration.
     *
     * @param app The application configuration to create
     * @return The application that was created
     * @throws CloudServiceException
     */
    Application createApplicationConfig(final Application app) throws CloudServiceException;

    /**
     * Update an application configuration.
     *
     * @param id The id of the application configuration to update
     * @param updateApp Information to update for the application configuration
     * with
     * @return The updated application
     * @throws CloudServiceException
     */
    Application updateApplicationConfig(
            final String id,
            final Application updateApp) throws CloudServiceException;

    /**
     * Delete an application configuration from database.
     *
     * @param id unique id of application configuration to delete
     * @return The deleted application
     * @throws CloudServiceException
     */
    Application deleteApplicationConfig(final String id) throws CloudServiceException;
}
