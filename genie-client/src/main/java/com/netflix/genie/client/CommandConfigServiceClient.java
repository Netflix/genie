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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Singleton class, which acts as the client library for the Command Config
 * Service.
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
     * Create a new cluster config.
     *
     * @param commandConfig the object encapsulating the new Cluster
     * config to create
     *
     * @return extracted cluster config response
     * @throws CloudServiceException
     */
    public CommandConfig createCommandConfig(final CommandConfig commandConfig)
            throws CloudServiceException {
        //TODO: Fix required elements
        if (commandConfig == null) {
            final String msg = "Required parameter commandConfig can't be NULL";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    msg);
        }
        if (commandConfig.getUser() == null) {
            final String msg = "User name is missing";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    msg);
        }
        if (commandConfig.getConfigs().isEmpty()) {
            final String msg = "At least one configuration file is required for the cluster.";
            LOG.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    msg);
        }

        final CommandConfigRequest request = new CommandConfigRequest();
        request.setCommandConfig(commandConfig);

        CommandConfigResponse ccr = executeRequest(
                Verb.POST,
                BASE_CONFIG_COMMAND_REST_URI,
                null,
                null,
                request,
                CommandConfigResponse.class);

        if ((ccr.getCommandConfigs() == null) || (ccr.getCommandConfigs().length == 0)) {
            String msg = "Unable to parse command config from response";
            LOG.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // return the first (only) cluster config
        return ccr.getCommandConfigs()[0];
    }

    /**
     * Create or update a cluster config.
     *
     * @param commandConfigId the id for the cluster config to create or update
     * @param commandConfig the object encapsulating the new Cluster
     * config to create
     *
     * @return extracted cluster config response
     * @throws CloudServiceException
     */
    public CommandConfig updateCommandConfig(final String commandConfigId,
            final CommandConfig commandConfig) throws CloudServiceException {
        if (commandConfig == null) {
            String msg = "Required parameter commandConfig can't be NULL";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    msg);
        }
        if (commandConfig.getUser() == null) {
            String msg = "User name is missing";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    msg);
        }

        final CommandConfigRequest request = new CommandConfigRequest();
        request.setCommandConfig(commandConfig);

        CommandConfigResponse ccr = executeRequest(Verb.PUT, BASE_CONFIG_COMMAND_REST_URI, commandConfigId, null, request, CommandConfigResponse.class);

        if ((ccr.getCommandConfigs() == null) || (ccr.getCommandConfigs().length == 0)) {
            String msg = "Unable to parse cluster config from response";
            LOG.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // return the first (only) cluster config
        return ccr.getCommandConfigs()[0];
    }

    /**
     * Gets information for a given clusterConfigId.
     *
     * @param clusterConfigId the cluster config id to get (can't be null)
     * @return the cluster config for this clusterConfigId
     * @throws CloudServiceException
     */
    public CommandConfig getCommandConfig(String clusterConfigId) throws CloudServiceException {
        if (clusterConfigId == null) {
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    "Missing required parameter: clusterConfigId");
        }

        CommandConfigResponse ccr = executeRequest(
                Verb.GET,
                BASE_CONFIG_COMMAND_REST_URI,
                clusterConfigId,
                null,
                null,
                CommandConfigResponse.class);

        if ((ccr.getCommandConfigs() == null)
                || (ccr.getCommandConfigs().length == 0)) {
            String msg = "Unable to parse cluster config from response";
            LOG.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // return the first (only) cluster config
        return ccr.getCommandConfigs()[0];
    }

    /**
     * Gets a set of cluster configs for the given parameters.
     *
     * @param params key/value pairs in a map object.<br>
     *
     * More details on the parameters can be found on the Genie User Guide on
     * GitHub.
     * @return array of cluster config elements that match the filter
     * @throws CloudServiceException
     */
    public CommandConfig[] getCommandConfigs(
            Multimap<String, String> params) throws CloudServiceException {
        CommandConfigResponse ccr = executeRequest(Verb.GET, BASE_CONFIG_COMMAND_REST_URI,
                null, params, null, CommandConfigResponse.class);

        // this will only happen if 200 is returned, and parsing fails for some
        // reason
        if ((ccr.getCommandConfigs() == null) || (ccr.getCommandConfigs().length == 0)) {
            String msg = "Unable to parse cluster config from response";
            LOG.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // if we get here, there are non-zero cluster config elements - return all
        return ccr.getCommandConfigs();
    }

    /**
     * Delete a clusterConfig using its id.
     *
     * @param clusterConfigId the id for the cluster config to delete
     * @return the deleted cluster config
     * @throws CloudServiceException
     */
    public CommandConfig deleteCommandConfig(String clusterConfigId) throws CloudServiceException {
        if (clusterConfigId == null) {
            String msg = "Missing required parameter: clusterConfigId";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    msg);
        }

        CommandConfigResponse ccr = executeRequest(Verb.DELETE, BASE_CONFIG_COMMAND_REST_URI,
                clusterConfigId, null, null, CommandConfigResponse.class);

        if ((ccr.getCommandConfigs() == null) || (ccr.getCommandConfigs().length == 0)) {
            String msg = "Unable to parse cluster config from response";
            LOG.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // return the first (only) cluster config
        return ccr.getCommandConfigs()[0];
    }
}
