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

import com.netflix.genie.common.messages.PigConfigRequest;
import com.netflix.genie.common.messages.PigConfigResponse;

/**
 * Abstraction layer to encapsulate PigConfig functionality.<br>
 * Classes implementing this abstraction layer must be thread-safe.
 *
 * @author skrishnan
 */
public interface PigConfigService {

    /**
     * Gets pig configuration for given id.
     *
     * @param id
     *            unique id for pig configuration to get
     * @return successful response, or one with HTTP error code
     */
    PigConfigResponse getPigConfig(String id);

    /**
     * Get pig configuration for given filter criteria.
     *
     * @param id
     *            unique id for pig config (can be null)
     * @param name
     *            name of pig config (can be null)
     * @param type
     *            type of config - possible values: Types.Configuration (can be
     *            null)
     * @return successful response, or one with HTTP error code
     */
    PigConfigResponse getPigConfig(String id, String name, String type);

    /**
     * Create new pig configuration.
     *
     * @param request
     *            encapsulates the pig config element to create
     * @return successful response, or one with HTTP error code
     */
    PigConfigResponse createPigConfig(PigConfigRequest request);

    /**
     * Update pig configuration.
     *
     * @param request
     *            encapsulates the pig config element to upsert, must contain
     *            valid id
     * @return successful response, or one with HTTP error code
     */
    PigConfigResponse updatePigConfig(PigConfigRequest request);

    /**
     * Delete a pig configuration from database.
     *
     * @param id
     *            unique if of pig configuration to delete
     * @return successful response, or one with HTTP error code
     */
    PigConfigResponse deletePigConfig(String id);
}
