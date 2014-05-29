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
import com.netflix.genie.common.messages.ClusterConfigRequest;
import com.netflix.genie.common.messages.ClusterConfigResponse;
import com.netflix.genie.common.model.ClusterConfig;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Singleton class, which acts as the client library for the Cluster
 * Configuration Service.
 *
 * @author skrishnan
 * @author tgianos
 */
public final class ClusterConfigServiceClient extends BaseGenieClient {

    private static final Logger LOG = LoggerFactory
            .getLogger(ClusterConfigServiceClient.class);

    private static final String BASE_CONFIG_CLUSTER_REST_URI
            = BASE_REST_URI + "config/cluster";

    // reference to the instance object
    private static ClusterConfigServiceClient instance;

    /**
     * Private constructor for singleton class.
     *
     * @throws IOException if there is any error during initialization
     */
    private ClusterConfigServiceClient() throws IOException {
        super();
    }

    /**
     * Returns the singleton instance that can be used by clients.
     *
     * @return ExecutionServiceClient instance
     * @throws IOException if there is an error instantiating client
     */
    public static synchronized ClusterConfigServiceClient getInstance() throws IOException {
        if (instance == null) {
            instance = new ClusterConfigServiceClient();
        }

        return instance;
    }

    /**
     * Create a new cluster configuration.
     *
     * @param config the object encapsulating the new Cluster configuration to
     * create
     *
     * @return extracted cluster configuration response
     * @throws CloudServiceException
     */
    public ClusterConfig createClusterConfig(final ClusterConfig config)
            throws CloudServiceException {
        checkErrorConditions(config);

        final ClusterConfigRequest request = new ClusterConfigRequest();
        request.setClusterConfig(config);

        final ClusterConfigResponse ccr = executeRequest(
                Verb.POST,
                BASE_CONFIG_CLUSTER_REST_URI,
                null,
                null,
                request,
                ClusterConfigResponse.class);

        if (ccr.getClusterConfigs() == null || ccr.getClusterConfigs().length == 0) {
            String msg = "Unable to parse cluster config from response";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // return the first (only) cluster config
        return ccr.getClusterConfigs()[0];
    }

    /**
     * Create or update a cluster configuration.
     *
     * @param id the id for the cluster configuration to create or update
     * @param config the object encapsulating the new Cluster configuration to
     * create
     *
     * @return extracted cluster configuration response
     * @throws CloudServiceException
     */
    public ClusterConfig updateClusterConfig(final String id, final ClusterConfig config)
            throws CloudServiceException {
        if (StringUtils.isEmpty(id)) {
            String msg = "Required parameter id can't be null or empty.";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }
        checkErrorConditions(config);

        ClusterConfigRequest request = new ClusterConfigRequest();
        request.setClusterConfig(config);

        ClusterConfigResponse ccr = executeRequest(
                Verb.PUT,
                BASE_CONFIG_CLUSTER_REST_URI,
                id,
                null,
                request,
                ClusterConfigResponse.class);

        if (ccr.getClusterConfigs() == null || ccr.getClusterConfigs().length == 0) {
            String msg = "Unable to parse cluster config from response";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // return the first (only) cluster config
        return ccr.getClusterConfigs()[0];
    }

    /**
     * Gets information for a given id.
     *
     * @param id the cluster configuration id to get (can't be null or empty)
     * @return the cluster configuration for this id
     * @throws CloudServiceException
     */
    public ClusterConfig getClusterConfig(final String id) throws CloudServiceException {
        if (StringUtils.isEmpty(id)) {
            final String msg = "Missing required parameter: id";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        ClusterConfigResponse ccr = executeRequest(
                Verb.GET,
                BASE_CONFIG_CLUSTER_REST_URI,
                id,
                null,
                null,
                ClusterConfigResponse.class);

        if (ccr.getClusterConfigs() == null || ccr.getClusterConfigs().length == 0) {
            String msg = "Unable to parse cluster config from response";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // return the first (only) cluster config
        return ccr.getClusterConfigs()[0];
    }

    /**
     * Gets a set of cluster configurations for the given parameters.
     *
     * @param params key/value pairs in a map object.<br>
     *
     * More details on the parameters can be found on the Genie User Guide on
     * GitHub.
     * @return List of cluster configuration elements that match the filter
     * @throws CloudServiceException
     */
    public List<ClusterConfig> getClusterConfigs(final Multimap<String, String> params)
            throws CloudServiceException {
        final ClusterConfigResponse ccr = executeRequest(
                Verb.GET,
                BASE_CONFIG_CLUSTER_REST_URI,
                null,
                params,
                null,
                ClusterConfigResponse.class);

        // this will only happen if 200 is returned, and parsing fails for some
        // reason
        if (ccr.getClusterConfigs() == null || ccr.getClusterConfigs().length == 0) {
            String msg = "Unable to parse cluster config from response";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // if we get here, there are non-zero cluster config elements - return all
        return Arrays.asList(ccr.getClusterConfigs());
    }

    /**
     * Delete a config using its id.
     *
     * @param id the id for the cluster config to delete
     * @return the deleted cluster config
     * @throws CloudServiceException
     */
    public ClusterConfig deleteClusterConfig(final String id) throws CloudServiceException {
        if (StringUtils.isEmpty(id)) {
            String msg = "Missing required parameter: id";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        final ClusterConfigResponse ccr = executeRequest(
                Verb.DELETE,
                BASE_CONFIG_CLUSTER_REST_URI,
                id,
                null,
                null,
                ClusterConfigResponse.class);

        if (ccr.getClusterConfigs() == null || ccr.getClusterConfigs().length == 0) {
            String msg = "Unable to parse cluster config from response";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // return the first (only) cluster config
        return ccr.getClusterConfigs()[0];
    }

    /**
     * Check to make sure that the required parameters exist.
     *
     * @param config The configuration to check
     * @throws CloudServiceException
     */
    private void checkErrorConditions(final ClusterConfig config) throws CloudServiceException {
        if (config == null) {
            final String msg = "Required parameter config can't be NULL";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        final List<String> messages = new ArrayList<String>();
        if (StringUtils.isEmpty(config.getUser())) {
            messages.add("User name is missing. Unable to continue.\n");
        }
        if (StringUtils.isEmpty(config.getName())) {
            messages.add("Cluster name is missing. Unable to continue.\n");
        }
        if (config.getStatus() == null) {
            messages.add("No application status entered. Required to create.\n");
        }
        if (config.getConfigs().isEmpty()) {
            messages.add("At least one configuration file is required for the cluster.\n");
        }

        if (!messages.isEmpty()) {
            final StringBuilder builder = new StringBuilder();
            builder.append("Cluster configuration errors:\n");
            for (final String message : messages) {
                builder.append(message);
            }
            final String msg = builder.toString();
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }
    }
}
