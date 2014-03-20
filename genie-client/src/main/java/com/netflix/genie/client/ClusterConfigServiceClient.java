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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;

import com.netflix.genie.common.exceptions.CloudServiceException;

import com.netflix.genie.common.messages.ClusterConfigRequest;
import com.netflix.genie.common.messages.ClusterConfigResponse;
import com.netflix.genie.common.model.ClusterConfigElementOld;

import com.netflix.client.http.HttpRequest.Verb;

import com.google.common.collect.Multimap;

/**
 * Singleton class, which acts as the client library for the Cluster Config
 * Service.
 *
 * @author skrishnan
 *
 */
public final class ClusterConfigServiceClient extends BaseGenieClient {

    private static Logger logger = LoggerFactory
            .getLogger(ClusterConfigServiceClient.class);

    private static final String BASE_REST_URI = "/genie/v0/config/cluster";

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
     * Create a new cluster config.
     *
     * @param ClusterConfigElementOld the object encapsulating the new Cluster config to create
     *
     * @return extracted cluster config response
     * @throws CloudServiceException
     */
    public ClusterConfigElementOld createClusterConfig(ClusterConfigElementOld ClusterConfigElementOld)
            throws CloudServiceException {
        if (ClusterConfigElementOld == null) {
            String msg = "Required parameter clusterConfig can't be NULL";
            logger.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    msg);
        }
        if (ClusterConfigElementOld.getUser() == null) {
            String msg = "User name is missing";
            logger.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    msg);
        }
        if ((ClusterConfigElementOld.getS3CoreSiteXml() == null)
                || (ClusterConfigElementOld.getS3HdfsSiteXml() == null)
                || (ClusterConfigElementOld.getS3MapredSiteXml() == null)
                || (ClusterConfigElementOld.getName() == null)) {
            String msg = "Missing required Hadoop parameters for creating clusterConfig: "
                    + "{name, s3MapredSiteXml, s3HdfsSiteXml, s3CoreSiteXml}";
            logger.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    msg);
        }

        ClusterConfigRequest request = new ClusterConfigRequest();
        request.setClusterConfig(ClusterConfigElementOld);

        ClusterConfigResponse ccr = executeRequest(Verb.POST, BASE_REST_URI,
                null, null, request, ClusterConfigResponse.class);

        if ((ccr.getClusterConfigs() == null) || (ccr.getClusterConfigs().length == 0)) {
            String msg = "Unable to parse cluster config from response";
            logger.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // return the first (only) cluster config
        return ccr.getClusterConfigs()[0];
    }

    /**
     * Create or update a cluster config.
     *
     * @param clusterConfigId the id for the cluster config to create or update
     * @param ClusterConfigElementOld the object encapsulating the new Cluster config to create
     *
     * @return extracted cluster config response
     * @throws CloudServiceException
     */
    public ClusterConfigElementOld updateClusterConfig(String clusterConfigId,
            ClusterConfigElementOld ClusterConfigElementOld)
            throws CloudServiceException {
        if (ClusterConfigElementOld == null) {
            String msg = "Required parameter clusterConfig can't be NULL";
            logger.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    msg);
        }
        if (ClusterConfigElementOld.getUser() == null) {
            String msg = "User name is missing";
            logger.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    msg);
        }

        ClusterConfigRequest request = new ClusterConfigRequest();
        request.setClusterConfig(ClusterConfigElementOld);

        ClusterConfigResponse ccr = executeRequest(Verb.PUT, BASE_REST_URI,
                clusterConfigId, null, request, ClusterConfigResponse.class);

        if ((ccr.getClusterConfigs() == null) || (ccr.getClusterConfigs().length == 0)) {
            String msg = "Unable to parse cluster config from response";
            logger.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // return the first (only) cluster config
        return ccr.getClusterConfigs()[0];
    }

    /**
     * Gets information for a given clusterConfigId.
     *
     * @param clusterConfigId
     *            the cluster config id to get (can't be null)
     * @return the cluster config for this clusterConfigId
     * @throws CloudServiceException
     */
    public ClusterConfigElementOld getClusterConfig(String clusterConfigId) throws CloudServiceException {
        if (clusterConfigId == null) {
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    "Missing required parameter: clusterConfigId");
        }

        ClusterConfigResponse ccr = executeRequest(Verb.GET, BASE_REST_URI,
                clusterConfigId, null, null, ClusterConfigResponse.class);

        if ((ccr.getClusterConfigs() == null) || (ccr.getClusterConfigs().length == 0)) {
            String msg = "Unable to parse cluster config from response";
            logger.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // return the first (only) cluster config
        return ccr.getClusterConfigs()[0];
    }

    /**
     * Gets a set of cluster configs for the given parameters.
     *
     * @param params
     *            key/value pairs in a map object.<br>
     *
     *            More details on the parameters can be found
     *            on the Genie User Guide on GitHub.
     * @return array of cluster config elements that match the filter
     * @throws CloudServiceException
     */
    public ClusterConfigElementOld[] getClusterConfigs(
            Multimap<String, String> params) throws CloudServiceException {
        ClusterConfigResponse ccr = executeRequest(Verb.GET, BASE_REST_URI,
                null, params, null, ClusterConfigResponse.class);

        // this will only happen if 200 is returned, and parsing fails for some
        // reason
        if ((ccr.getClusterConfigs() == null) || (ccr.getClusterConfigs().length == 0)) {
            String msg = "Unable to parse cluster config from response";
            logger.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // if we get here, there are non-zero cluster config elements - return all
        return ccr.getClusterConfigs();
    }

    /**
     * Delete a clusterConfig using its id.
     *
     * @param clusterConfigId
     *            the id for the cluster config to delete
     * @return the deleted cluster config
     * @throws CloudServiceException
     */
    public ClusterConfigElementOld deleteClusterConfig(String clusterConfigId) throws CloudServiceException {
        if (clusterConfigId == null) {
            String msg = "Missing required parameter: clusterConfigId";
            logger.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    msg);
        }

        ClusterConfigResponse ccr = executeRequest(Verb.DELETE, BASE_REST_URI,
                clusterConfigId, null, null, ClusterConfigResponse.class);

        if ((ccr.getClusterConfigs() == null) || (ccr.getClusterConfigs().length == 0)) {
            String msg = "Unable to parse cluster config from response";
            logger.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // return the first (only) cluster config
        return ccr.getClusterConfigs()[0];
    }
}
