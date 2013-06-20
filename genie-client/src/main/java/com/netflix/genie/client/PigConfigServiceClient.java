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

import com.netflix.genie.common.messages.PigConfigRequest;
import com.netflix.genie.common.messages.PigConfigResponse;
import com.netflix.genie.common.model.PigConfigElement;
import com.netflix.genie.common.model.Types;

import com.netflix.niws.client.http.HttpClientRequest.Verb;
import com.netflix.niws.client.http.HttpClientResponse;

import javax.ws.rs.core.MultivaluedMap;

/**
 * Singleton class, which acts as the client library for the Pig Config
 * Service.
 *
 * @author skrishnan
 *
 */
public final class PigConfigServiceClient extends BaseGenieClient {

    private static Logger logger = LoggerFactory
            .getLogger(PigConfigServiceClient.class);

    private static final String BASE_REST_URI = "/genie/v0/config/pig";

    // reference to the instance object
    private static PigConfigServiceClient instance;

    /**
     * Private constructor for singleton class.
     *
     * @throws IOException if there is any error during initialization
     */
    private PigConfigServiceClient() throws IOException {
        super();
    }

    /**
     * Converts a response to a PigConfigResponse object.
     *
     * @param response
     *            generic response from REST service
     * @return extracted pig config response
     */
    private PigConfigResponse responseToPigConfig(HttpClientResponse response)
            throws CloudServiceException {
        return extractEntityFromClientResponse(response, PigConfigResponse.class);
    }

    /**
     * Returns the singleton instance that can be used by clients.
     *
     * @return ExecutionServiceClient instance
     * @throws IOException if there is an error instantiating client
     */
    public static synchronized PigConfigServiceClient getInstance() throws IOException {
        if (instance == null) {
            instance = new PigConfigServiceClient();
        }

        return instance;
    }

    /**
     * Create a new Pig config.
     *
     * @param pigConfigElement the object encapsulating the new Pig config to create
     *
     * @return extracted pig config response
     * @throws CloudServiceException
     */
    public PigConfigElement createPigConfig(PigConfigElement pigConfigElement)
            throws CloudServiceException {
        if (pigConfigElement == null) {
            String msg = "Required parameter pigConfig can't be NULL";
            logger.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    msg);
        }
        if (pigConfigElement.getUser() == null) {
            String msg = "User name is missing";
            logger.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    msg);
        }
        String name = pigConfigElement.getName();
        String s3PigProperties = pigConfigElement.getS3PigProperties();
        Types.Configuration type = Types.Configuration.parse(pigConfigElement
                .getType());

        if ((name == null) || (s3PigProperties == null) || (type == null)) {
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    "Need all required params: {name, s3PipProps, type}");
        }

        PigConfigRequest request = new PigConfigRequest();
        request.setPigConfig(pigConfigElement);

        HttpClientResponse response = executeRequest(Verb.POST, BASE_REST_URI,
                null, null, request);
        PigConfigResponse hcr = responseToPigConfig(response);

        if ((hcr.getPigConfigs() == null) || (hcr.getPigConfigs().length == 0)) {
            String msg = "Unable to parse pig config from response";
            logger.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // return the first (only) pig config
        return hcr.getPigConfigs()[0];
    }

    /**
     * Create or update a new Pig config.
     *
     * @param pigConfigId the id for the pig config to create or update
     * @param pigConfigElement the object encapsulating the new Pig config to create
     *
     * @return extracted pig config response
     * @throws CloudServiceException
     */
    public PigConfigElement updatePigConfig(String pigConfigId,
            PigConfigElement pigConfigElement)
            throws CloudServiceException {
        if (pigConfigElement == null) {
            String msg = "Required parameter pigConfig can't be NULL";
            logger.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    msg);
        }
        if (pigConfigElement.getUser() == null) {
            String msg = "User name is missing";
            logger.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    msg);
        }

        PigConfigRequest request = new PigConfigRequest();
        request.setPigConfig(pigConfigElement);

        HttpClientResponse response = executeRequest(Verb.PUT, BASE_REST_URI,
                pigConfigId, null, request);
        PigConfigResponse hcr = responseToPigConfig(response);

        if ((hcr.getPigConfigs() == null) || (hcr.getPigConfigs().length == 0)) {
            String msg = "Unable to parse pig config from response";
            logger.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // return the first (only) pig config
        return hcr.getPigConfigs()[0];
    }

    /**
     * Gets information for a given pigConfigId.
     *
     * @param pigConfigId
     *            the pig config id to get (can't be null)
     * @return the pig config for this pigConfigId
     * @throws CloudServiceException
     */
    public PigConfigElement getPigConfig(String pigConfigId) throws CloudServiceException {
        if (pigConfigId == null) {
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    "Missing required parameter: pigConfigId");
        }

        HttpClientResponse response = executeRequest(Verb.GET, BASE_REST_URI,
                pigConfigId, null, null);
        PigConfigResponse hcr = responseToPigConfig(response);

        if ((hcr.getPigConfigs() == null) || (hcr.getPigConfigs().length == 0)) {
            String msg = "Unable to parse pig config from response";
            logger.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // return the first (only) pig config
        return hcr.getPigConfigs()[0];
    }

    /**
     * Gets a set of pig configs for the given parameters.
     *
     * @param params
     *            key/value pairs in a map object.<br>
     *
     *            More details on the parameters can be found
     *            on the Genie User Guide on GitHub.
     * @return array of pig config elements that match the filter
     * @throws CloudServiceException
     */
    public PigConfigElement[] getPigConfigs(
            MultivaluedMap<String, String> params) throws CloudServiceException {
        HttpClientResponse response = executeRequest(Verb.GET, BASE_REST_URI,
                null, params, null);
        PigConfigResponse hcr = responseToPigConfig(response);

        // this will only happen if 200 is returned, and parsing fails for some
        // reason
        if ((hcr.getPigConfigs() == null) || (hcr.getPigConfigs().length == 0)) {
            String msg = "Unable to parse pig config from response";
            logger.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // if we get here, there are non-zero pig config elements - return all
        return hcr.getPigConfigs();
    }

    /**
     * Delete a pigConfig using its id.
     *
     * @param pigConfigId
     *            the id for the pig config to delete
     * @return the deleted pig config
     * @throws CloudServiceException
     */
    public PigConfigElement deletePigConfig(String pigConfigId) throws CloudServiceException {
        if (pigConfigId == null) {
            String msg = "Missing required parameter: pigConfigId";
            logger.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    msg);
        }

        HttpClientResponse response = executeRequest(Verb.DELETE, BASE_REST_URI,
                pigConfigId, null, null);
        PigConfigResponse hcr = responseToPigConfig(response);

        if ((hcr.getPigConfigs() == null) || (hcr.getPigConfigs().length == 0)) {
            String msg = "Unable to parse pig config from response";
            logger.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // return the first (only) pig config
        return hcr.getPigConfigs()[0];
    }
}
