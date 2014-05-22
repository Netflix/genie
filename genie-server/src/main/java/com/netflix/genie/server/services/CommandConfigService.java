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

import com.netflix.genie.common.messages.CommandConfigRequest;
import com.netflix.genie.common.messages.CommandConfigResponse;

/**
 * Abstraction layer to encapsulate CommandConfig functionality.<br>
 * Classes implementing this abstraction layer must be thread-safe.
 *
 * @author amsharma
 */
public interface CommandConfigService {

    /**
     * Gets command configuration for given id.
     *
     * @param id
     *            unique id for command configuration to get
     * @return successful response, or one with HTTP error code
     */
    CommandConfigResponse getCommandConfig(String id);

    /**
     * Get command configuration for given filter criteria.
     *
     * @param id
     *            unique id for command config (can be null)
     * @param name
     *            name of command config (can be null)
     * @return successful response, or one with HTTP error code
     */
    CommandConfigResponse getCommandConfig(String id, String name);

    /**
     * Create new command configuration.
     *
     * @param request
     *            encapsulates the command config element to create
     * @return successful response, or one with HTTP error code
     */
    CommandConfigResponse createCommandConfig(CommandConfigRequest request);

    /**
     * Update command configuration.
     *
     * @param request
     *            encapsulates the command config element to upsert, must contain
     *            valid id
     * @return successful response, or one with HTTP error code
     */
    CommandConfigResponse updateCommandConfig(CommandConfigRequest request);

    /**
     * Delete a command configuration from database.
     *
     * @param id
     *            unique if of command configuration to delete
     * @return successful response, or one with HTTP error code
     */
    CommandConfigResponse deleteCommandConfig(String id);
}