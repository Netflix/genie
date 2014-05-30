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
package com.netflix.genie.client;

import com.google.common.collect.Multimap;
import com.netflix.client.http.HttpRequest.Verb;
import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.messages.CommandConfigRequest;
import com.netflix.genie.common.messages.CommandConfigResponse;
import com.netflix.genie.common.model.CommandConfig;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Arrays;
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
public final class CommandConfigServiceClient extends BaseGenieClient {

    private static final Logger LOG = LoggerFactory
            .getLogger(CommandConfigServiceClient.class);

    private static final String BASE_CONFIG_COMMAND_REST_URI
            = BASE_REST_URI + "config/command";

    // reference to the instance object
    private static CommandConfigServiceClient instance;

    /**
     * Private constructor for singleton class.
     *
     * @throws IOException if there is any error during initialization
     */
    private CommandConfigServiceClient() throws IOException {
        super();
    }

    /**
     * Returns the singleton instance that can be used by clients.
     *
     * @return ExecutionServiceClient instance
     * @throws IOException if there is an error instantiating client
     */
    public static synchronized CommandConfigServiceClient getInstance() throws IOException {
        if (instance == null) {
            instance = new CommandConfigServiceClient();
        }

        return instance;
    }

    /**
     * Create a new command configuration.
     *
     * @param config the object encapsulating the new Cluster configuration to
     * create
     *
     * @return extracted command configuration response
     * @throws CloudServiceException
     */
    public CommandConfig createCommandConfig(final CommandConfig config)
            throws CloudServiceException {
        checkErrorConditions(config);

        final CommandConfigRequest request = new CommandConfigRequest();
        request.setCommandConfig(config);

        CommandConfigResponse ccr = executeRequest(
                Verb.POST,
                BASE_CONFIG_COMMAND_REST_URI,
                null,
                null,
                request,
                CommandConfigResponse.class);

        if (ccr.getCommandConfigs() == null || ccr.getCommandConfigs().length == 0) {
            String msg = "Unable to parse command config from response";
            LOG.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // return the first (only) command config
        return ccr.getCommandConfigs()[0];
    }

    /**
     * Create or update a command configuration.
     *
     * @param id the id for the command configuration to create or update
     * @param config the object encapsulating the new Cluster configuration to
     * create
     *
     * @return extracted command configuration response
     * @throws CloudServiceException
     */
    public CommandConfig updateCommandConfig(final String id, final CommandConfig config)
            throws CloudServiceException {
        if (StringUtils.isEmpty(id)) {
            String msg = "Required parameter id can't be null or empty.";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }
        checkErrorConditions(config);

        final CommandConfigRequest request = new CommandConfigRequest();
        request.setCommandConfig(config);

        CommandConfigResponse ccr = executeRequest(
                Verb.PUT,
                BASE_CONFIG_COMMAND_REST_URI,
                id,
                null,
                request,
                CommandConfigResponse.class);

        if (ccr.getCommandConfigs() == null || ccr.getCommandConfigs().length == 0) {
            String msg = "Unable to parse command config from response";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // return the first (only) command config
        return ccr.getCommandConfigs()[0];
    }

    /**
     * Gets information for a given configId.
     *
     * @param id the command configuration id to get (can't be null or empty)
     * @return the command configuration for this id
     * @throws CloudServiceException
     */
    public CommandConfig getCommandConfig(final String id) throws CloudServiceException {
        if (StringUtils.isEmpty(id)) {
            final String msg = "Missing required parameter: id";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        final CommandConfigResponse ccr = executeRequest(
                Verb.GET,
                BASE_CONFIG_COMMAND_REST_URI,
                id,
                null,
                null,
                CommandConfigResponse.class);

        if (ccr.getCommandConfigs() == null || ccr.getCommandConfigs().length == 0) {
            final String msg = "Unable to parse command config from response";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // return the first (only) command config
        return ccr.getCommandConfigs()[0];
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
    public List<CommandConfig> getCommandConfigs(final Multimap<String, String> params)
            throws CloudServiceException {
        final CommandConfigResponse ccr = executeRequest(
                Verb.GET,
                BASE_CONFIG_COMMAND_REST_URI,
                null,
                params,
                null,
                CommandConfigResponse.class);

        // this will only happen if 200 is returned, and parsing fails for some
        // reason
        if (ccr.getCommandConfigs() == null || ccr.getCommandConfigs().length == 0) {
            final String msg = "Unable to parse command config from response";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // if we get here, there are non-zero command config elements - return all
        return Arrays.asList(ccr.getCommandConfigs());
    }

    /**
     * Delete a configuration using its id.
     *
     * @param id the id for the command configuration to delete. Not null or
     * empty.
     * @return the deleted command configuration
     * @throws CloudServiceException
     */
    public CommandConfig deleteCommandConfig(final String id) throws CloudServiceException {
        if (StringUtils.isEmpty(id)) {
            String msg = "Missing required parameter: id";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        CommandConfigResponse ccr = executeRequest(
                Verb.DELETE,
                BASE_CONFIG_COMMAND_REST_URI,
                id,
                null,
                null,
                CommandConfigResponse.class);

        if (ccr.getCommandConfigs() == null || ccr.getCommandConfigs().length == 0) {
            String msg = "Unable to parse command config from response";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // return the first (only) command config
        return ccr.getCommandConfigs()[0];
    }

    /**
     * Check to make sure that the required parameters exist.
     *
     * @param config The configuration to check
     * @throws CloudServiceException
     */
    private void checkErrorConditions(final CommandConfig config) throws CloudServiceException {
        if (config == null) {
            final String msg = "Required parameter config can't be NULL";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        final List<String> messages = new ArrayList<String>();
        if (StringUtils.isEmpty(config.getUser())) {
            messages.add("User name is missing and required.\n");
        }
        if (StringUtils.isEmpty(config.getName())) {
            messages.add("The command name is empty but is required.\n");
        }
        if (config.getStatus() == null) {
            messages.add("The command status is null and is required.\n");
        }

        if (!messages.isEmpty()) {
            final StringBuilder builder = new StringBuilder();
            builder.append("Command configuration errors:\n");
            for (final String message : messages) {
                builder.append(message);
            }
            final String msg = builder.toString();
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }
    }
}
