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

import com.netflix.genie.common.messages.HiveConfigRequest;
import com.netflix.genie.common.messages.HiveConfigResponse;
import com.netflix.genie.common.model.HiveConfigElement;
import com.netflix.genie.common.model.Types;

import com.netflix.niws.client.http.HttpClientRequest.Verb;

import javax.ws.rs.core.MultivaluedMap;

/**
 * Singleton class, which acts as the client library for the Hive Config
 * Service.
 *
 * @author skrishnan
 *
 */
public final class HiveConfigServiceClient extends BaseGenieClient {

    private static Logger logger = LoggerFactory
            .getLogger(HiveConfigServiceClient.class);

    private static final String BASE_REST_URI = "/genie/v0/config/hive";

    // reference to the instance object
    private static HiveConfigServiceClient instance;

    /**
     * Private constructor for singleton class.
     *
     * @throws IOException if there is any error during initialization
     */
    private HiveConfigServiceClient() throws IOException {
        super();
    }

    /**
     * Returns the singleton instance that can be used by clients.
     *
     * @return ExecutionServiceClient instance
     * @throws IOException if there is an error instantiating client
     */
    public static synchronized HiveConfigServiceClient getInstance() throws IOException {
        if (instance == null) {
            instance = new HiveConfigServiceClient();
        }

        return instance;
    }

    /**
     * Create a new Hive config.
     *
     * @param hiveConfigElement the object encapsulating the new Hive config to create
     *
     * @return extracted hive config response
     * @throws CloudServiceException
     */
    public HiveConfigElement createHiveConfig(HiveConfigElement hiveConfigElement)
            throws CloudServiceException {
        if (hiveConfigElement == null) {
            String msg = "Required parameter hiveConfig can't be NULL";
            logger.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    msg);
        }
        if (hiveConfigElement.getUser() == null) {
            String msg = "User name is missing";
            logger.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    msg);
        }
        String name = hiveConfigElement.getName();
        String s3HiveSiteXml = hiveConfigElement.getS3HiveSiteXml();
        Types.Configuration type = Types.Configuration.parse(hiveConfigElement
                .getType());

        if ((name == null) || (s3HiveSiteXml == null) || (type == null)) {
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    "Need all required params: {name, s3HiveSiteXml, type}");
        }

        HiveConfigRequest request = new HiveConfigRequest();
        request.setHiveConfig(hiveConfigElement);

        HiveConfigResponse hcr = executeRequest(Verb.POST, BASE_REST_URI,
                null, null, request, HiveConfigResponse.class);

        if ((hcr.getHiveConfigs() == null) || (hcr.getHiveConfigs().length == 0)) {
            String msg = "Unable to parse hive config from response";
            logger.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // return the first (only) hive config
        return hcr.getHiveConfigs()[0];
    }

    /**
     * Create or update a new Hive config.
     *
     * @param hiveConfigId the id for the hive config to create or update
     * @param hiveConfigElement the object encapsulating the new Hive config to create
     *
     * @return extracted hive config response
     * @throws CloudServiceException
     */
    public HiveConfigElement updateHiveConfig(String hiveConfigId,
            HiveConfigElement hiveConfigElement)
            throws CloudServiceException {
        if (hiveConfigElement == null) {
            String msg = "Required parameter hiveConfig can't be NULL";
            logger.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    msg);
        }
        if (hiveConfigElement.getUser() == null) {
            String msg = "User name is missing";
            logger.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    msg);
        }

        HiveConfigRequest request = new HiveConfigRequest();
        request.setHiveConfig(hiveConfigElement);

        HiveConfigResponse hcr = executeRequest(Verb.PUT, BASE_REST_URI,
                hiveConfigId, null, request, HiveConfigResponse.class);

        if ((hcr.getHiveConfigs() == null) || (hcr.getHiveConfigs().length == 0)) {
            String msg = "Unable to parse hive config from response";
            logger.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // return the first (only) hive config
        return hcr.getHiveConfigs()[0];
    }

    /**
     * Gets information for a given hiveConfigId.
     *
     * @param hiveConfigId
     *            the hive config id to get (can't be null)
     * @return the hive config for this hiveConfigId
     * @throws CloudServiceException
     */
    public HiveConfigElement getHiveConfig(String hiveConfigId) throws CloudServiceException {
        if (hiveConfigId == null) {
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    "Missing required parameter: hiveConfigId");
        }

        HiveConfigResponse hcr = executeRequest(Verb.GET, BASE_REST_URI,
                hiveConfigId, null, null, HiveConfigResponse.class);

        if ((hcr.getHiveConfigs() == null) || (hcr.getHiveConfigs().length == 0)) {
            String msg = "Unable to parse hive config from response";
            logger.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // return the first (only) hive config
        return hcr.getHiveConfigs()[0];
    }

    /**
     * Gets a set of hive configs for the given parameters.
     *
     * @param params
     *            key/value pairs in a map object.<br>
     *
     *            More details on the parameters can be found
     *            on the Genie User Guide on GitHub.
     * @return array of hive config elements that match the filter
     * @throws CloudServiceException
     */
    public HiveConfigElement[] getHiveConfigs(
            MultivaluedMap<String, String> params) throws CloudServiceException {
        HiveConfigResponse hcr = executeRequest(Verb.GET, BASE_REST_URI,
                null, params, null, HiveConfigResponse.class);

        // this will only happen if 200 is returned, and parsing fails for some
        // reason
        if ((hcr.getHiveConfigs() == null) || (hcr.getHiveConfigs().length == 0)) {
            String msg = "Unable to parse hive config from response";
            logger.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // if we get here, there are non-zero hive config elements - return all
        return hcr.getHiveConfigs();
    }

    /**
     * Delete a hiveConfig using its id.
     *
     * @param hiveConfigId
     *            the id for the hive config to delete
     * @return the deleted hive config
     * @throws CloudServiceException
     */
    public HiveConfigElement deleteHiveConfig(String hiveConfigId) throws CloudServiceException {
        if (hiveConfigId == null) {
            String msg = "Missing required parameter: hiveConfigId";
            logger.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    msg);
        }

        HiveConfigResponse hcr = executeRequest(Verb.DELETE, BASE_REST_URI,
                hiveConfigId, null, null, HiveConfigResponse.class);

        if ((hcr.getHiveConfigs() == null) || (hcr.getHiveConfigs().length == 0)) {
            String msg = "Unable to parse hive config from response";
            logger.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // return the first (only) hive config
        return hcr.getHiveConfigs()[0];
    }
}
