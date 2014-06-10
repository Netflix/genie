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
package com.netflix.genie.client;

import com.google.common.collect.Multimap;
import com.netflix.client.http.HttpRequest.Verb;
import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.model.Command;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Singleton class, which acts as the client library for the Command
 * Configuration Service.
 *
 * @author tgianos
 */
public final class CommandServiceClient extends BaseGenieClient {

    private static final Logger LOG = LoggerFactory
            .getLogger(CommandServiceClient.class);

    private static final String BASE_CONFIG_COMMAND_REST_URI
            = BASE_REST_URI + "config/commands";

    // reference to the instance object
    private static CommandServiceClient instance;

    /**
     * Private constructor for singleton class.
     *
     * @throws IOException if there is any error during initialization
     */
    private CommandServiceClient() throws IOException {
        super();
    }

    /**
     * Returns the singleton instance that can be used by clients.
     *
     * @return ExecutionServiceClient instance
     * @throws IOException if there is an error instantiating client
     */
    public static synchronized CommandServiceClient getInstance() throws IOException {
        if (instance == null) {
            instance = new CommandServiceClient();
        }

        return instance;
    }

    /**
     * Create a new command configuration.
     *
     * @param command the object encapsulating the new Cluster configuration to
     * create
     *
     * @return extracted command configuration response
     * @throws CloudServiceException
     */
    public Command createCommand(final Command command)
            throws CloudServiceException {
        Command.validate(command);

        return executeRequestForSingleEntity(
                Verb.POST,
                BASE_CONFIG_COMMAND_REST_URI,
                null,
                null,
                command,
                Command.class);
    }

    /**
     * Create or update a command configuration.
     *
     * @param id the id for the command configuration to create or update
     * @param command the object encapsulating the new Cluster configuration to
     * create
     *
     * @return extracted command configuration response
     * @throws CloudServiceException
     */
    public Command updateCommand(final String id, final Command command)
            throws CloudServiceException {
        if (StringUtils.isEmpty(id)) {
            String msg = "Required parameter id can't be null or empty.";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }
        Command.validate(command);

        return executeRequestForSingleEntity(
                Verb.PUT,
                BASE_CONFIG_COMMAND_REST_URI,
                id,
                null,
                command,
                Command.class);
    }

    /**
     * Gets information for a given configId.
     *
     * @param id the command configuration id to get (can't be null or empty)
     * @return the command configuration for this id
     * @throws CloudServiceException
     */
    public Command getCommand(final String id) throws CloudServiceException {
        if (StringUtils.isEmpty(id)) {
            final String msg = "Missing required parameter: id";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        return executeRequestForSingleEntity(
                Verb.GET,
                BASE_CONFIG_COMMAND_REST_URI,
                id,
                null,
                null,
                Command.class);
    }

    /**
     * Gets a set of command configurations for the given parameters.
     *
     * @param params key/value pairs in a map object.<br>
     *
     * More details on the parameters can be found on the Genie User Guide on
     * GitHub.
     * @return List of command configuration elements that match the filter
     * @throws CloudServiceException
     */
    public List<Command> getCommands(final Multimap<String, String> params)
            throws CloudServiceException {
        return executeRequestForListOfEntities(
                Verb.GET,
                BASE_CONFIG_COMMAND_REST_URI,
                null,
                params,
                null,
                Command.class);
    }

    /**
     * Delete a configuration using its id.
     *
     * @param id the id for the command configuration to delete. Not null or
     * empty.
     * @return the deleted command configuration
     * @throws CloudServiceException
     */
    public Command deleteCommand(final String id) throws CloudServiceException {
        if (StringUtils.isEmpty(id)) {
            String msg = "Missing required parameter: id";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        return executeRequestForSingleEntity(
                Verb.DELETE,
                BASE_CONFIG_COMMAND_REST_URI,
                id,
                null,
                null,
                Command.class);
    }
}
