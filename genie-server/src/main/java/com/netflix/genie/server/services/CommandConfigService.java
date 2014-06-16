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
import com.netflix.genie.common.model.Command;
import java.util.List;

/**
 * Abstraction layer to encapsulate CommandConfig functionality.<br>
 * Classes implementing this abstraction layer must be thread-safe.
 *
 * @author amsharma
 * @author tgianos
 */
public interface CommandConfigService {

    /**
     * Gets command configuration for given id.
     *
     * @param id unique id for command configuration to get. Not null/empty.
     * @return The command configuration
     * @throws CloudServiceException
     */
    Command getCommandConfig(final String id) throws CloudServiceException;

    /**
     * Get command configurations for given filter criteria.
     *
     * @param name name of command config (can be null)
     * @param userName the name of the user who created the configuration (can
     * be null)
     * @param page Page number to start results on
     * @param limit Max number of results per page
     * @return All the commands matching the specified criteria
     */
    List<Command> getCommandConfigs(
            final String name,
            final String userName,
            final int page,
            final int limit);

    /**
     * Create new command configuration.
     *
     * @param command encapsulates the command configuration information to
     * create
     * @return The command created
     * @throws CloudServiceException
     */
    Command createCommandConfig(final Command command) throws CloudServiceException;

    /**
     * Update command configuration.
     *
     * @param id The id of the command configuration to update. Not null or
     * empty.
     * @param updateCommand contains the information to update the command with
     * @return The updated command
     * @throws CloudServiceException
     */
    Command updateCommandConfig(final String id, final Command updateCommand) throws CloudServiceException;

    /**
     * Delete a command configuration from database.
     *
     * @param id unique if of the command configuration to delete
     * @return The deleted command configuration
     * @throws CloudServiceException
     */
    Command deleteCommandConfig(final String id) throws CloudServiceException;
}
