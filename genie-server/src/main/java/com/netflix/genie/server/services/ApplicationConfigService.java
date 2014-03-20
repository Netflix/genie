/*
 *
 *  Copyright 2013 Netflix, Inc.
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

import com.netflix.genie.common.messages.ApplicationConfigRequest;
import com.netflix.genie.common.messages.ApplicationConfigResponse;

/**
 * Abstraction layer to encapsulate ApplicationConfig functionality.<br>
 * Classes implementing this abstraction layer must be thread-safe.
 *
 * @author amsharma
 */
public interface ApplicationConfigService {

    /**
     * Gets application configuration for given id.
     *
     * @param id
     *            unique id for application configuration to get
     * @return successful response, or one with HTTP error code
     */
    ApplicationConfigResponse getApplicationConfig(String id);

    /**
     * Get application configuration for given filter criteria.
     *
     * @param id
     *            unique id for application config (can be null)
     * @param name
     *            name of application config (can be null)
     * @return successful response, or one with HTTP error code
     */
    ApplicationConfigResponse getApplicationConfig(String id, String name);

    /**
     * Create new application configuration.
     *
     * @param request
     *            encapsulates the application config element to create
     * @return successful response, or one with HTTP error code
     */
    ApplicationConfigResponse createApplicationConfig(ApplicationConfigRequest request);

    /**
     * Update application configuration.
     *
     * @param request
     *            encapsulates the application config element to upsert, must contain
     *            valid id
     * @return successful response, or one with HTTP error code
     */
    ApplicationConfigResponse updateApplicationConfig(ApplicationConfigRequest request);

    /**
     * Delete a application configuration from database.
     *
     * @param id
     *            unique if of application configuration to delete
     * @return successful response, or one with HTTP error code
     */
    ApplicationConfigResponse deleteApplicationConfig(String id);
}
