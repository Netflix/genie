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
import com.netflix.genie.common.model.Cluster;
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
public final class ClusterServiceClient extends BaseGenieClient {

    private static final Logger LOG = LoggerFactory
            .getLogger(ClusterServiceClient.class);

    private static final String BASE_CONFIG_CLUSTER_REST_URI
            = BASE_REST_URI + "config/cluster";

    // reference to the instance object
    private static ClusterServiceClient instance;

    /**
     * Private constructor for singleton class.
     *
     * @throws IOException if there is any error during initialization
     */
    private ClusterServiceClient() throws IOException {
        super();
    }

    /**
     * Returns the singleton instance that can be used by clients.
     *
     * @return ExecutionServiceClient instance
     * @throws IOException if there is an error instantiating client
     */
    public static synchronized ClusterServiceClient getInstance() throws IOException {
        if (instance == null) {
            instance = new ClusterServiceClient();
        }

        return instance;
    }

    /**
     * Create a new cluster configuration.
     *
     * @param cluster the object encapsulating the new Cluster configuration to
     * create
     *
     * @return extracted cluster configuration response
     * @throws CloudServiceException
     */
    public Cluster createCluster(final Cluster cluster)
            throws CloudServiceException {
        checkErrorConditions(cluster);

        final ClusterConfigRequest request = new ClusterConfigRequest();
        request.setClusterConfig(cluster);

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
     * @param cluster the object encapsulating the new Cluster configuration to
     * create
     *
     * @return extracted cluster configuration response
     * @throws CloudServiceException
     */
    public Cluster updateCluster(final String id, final Cluster cluster)
            throws CloudServiceException {
        if (StringUtils.isEmpty(id)) {
            String msg = "Required parameter id can't be null or empty.";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }
        checkErrorConditions(cluster);

        ClusterConfigRequest request = new ClusterConfigRequest();
        request.setClusterConfig(cluster);

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
    public Cluster getCluster(final String id) throws CloudServiceException {
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
            String msg = "Unable to parse cluster from response";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // return the first (only) cluster
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
    public List<Cluster> getClusterConfigs(final Multimap<String, String> params)
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
            String msg = "Unable to parse cluster from response";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // if we get here, there are non-zero cluster cluster elements - return all
        return Arrays.asList(ccr.getClusterConfigs());
    }

    /**
     * Delete a cluster using its id.
     *
     * @param id the id for the cluster cluster to delete
     * @return the deleted cluster cluster
     * @throws CloudServiceException
     */
    public Cluster deleteClusterConfig(final String id) throws CloudServiceException {
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
            String msg = "Unable to parse cluster from response";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // return the first (only) cluster
        return ccr.getClusterConfigs()[0];
    }

    /**
     * Check to make sure that the required parameters exist.
     *
     * @param cluster The configuration to check
     * @throws CloudServiceException
     */
    private void checkErrorConditions(final Cluster cluster) throws CloudServiceException {
        if (cluster == null) {
            final String msg = "Required parameter cluster can't be NULL";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        final List<String> messages = new ArrayList<String>();
        if (StringUtils.isEmpty(cluster.getUser())) {
            messages.add("User name is missing. Unable to continue.\n");
        }
        if (StringUtils.isEmpty(cluster.getName())) {
            messages.add("Cluster name is missing. Unable to continue.\n");
        }
        if (cluster.getStatus() == null) {
            messages.add("No cluster status entered. Unable to continue.\n");
        }
        if (cluster.getConfigs().isEmpty()) {
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
