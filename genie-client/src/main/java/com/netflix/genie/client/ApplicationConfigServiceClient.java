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
import com.netflix.genie.common.messages.ApplicationConfigRequest;
import com.netflix.genie.common.messages.ApplicationConfigResponse;
import com.netflix.genie.common.model.ApplicationConfig;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Singleton class, which acts as the client library for the Application
 * Configuration Service.
 *
 * @author tgianos
 */
public final class ApplicationConfigServiceClient extends BaseGenieClient {

    private static final Logger LOG = LoggerFactory
            .getLogger(ApplicationConfigServiceClient.class);

    private static final String BASE_CONFIG_APPLICATION_REST_URI
            = BASE_REST_URI + "config/application";

    // reference to the instance object
    private static ApplicationConfigServiceClient instance;

    /**
     * Private constructor for singleton class.
     *
     * @throws IOException if there is any error during initialization
     */
    private ApplicationConfigServiceClient() throws IOException {
        super();
    }

    /**
     * Returns the singleton instance that can be used by clients.
     *
     * @return ApplicationConfigServiceClient instance
     * @throws IOException if there is an error instantiating client
     */
    public static synchronized ApplicationConfigServiceClient getInstance() throws IOException {
        if (instance == null) {
            instance = new ApplicationConfigServiceClient();
        }

        return instance;
    }

    /**
     * Create a new application configuration.
     *
     * @param config the object encapsulating the new application config to
     * create
     *
     * @return extracted application config response
     * @throws CloudServiceException
     */
    public ApplicationConfig createApplicationConfig(final ApplicationConfig config)
            throws CloudServiceException {
        checkErrorConditions(config);

        final ApplicationConfigRequest request = new ApplicationConfigRequest();
        request.setApplicationConfig(config);

        final ApplicationConfigResponse ccr = executeRequest(
                Verb.POST,
                BASE_CONFIG_APPLICATION_REST_URI,
                null,
                null,
                request,
                ApplicationConfigResponse.class);

        if (ccr.getApplicationConfigs() == null || ccr.getApplicationConfigs().length == 0) {
            String msg = "Unable to parse application config from response";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // return the first (only) application config
        return ccr.getApplicationConfigs()[0];
    }

    /**
     * Create or update an application configuration.
     *
     * @param id the id for the application configuration to create or update
     * @param config the object encapsulating the new application configuration
     * to create
     *
     * @return extracted application configuration response
     * @throws CloudServiceException
     */
    public ApplicationConfig updateApplicationConfig(final String id, final ApplicationConfig config)
            throws CloudServiceException {
        if (StringUtils.isEmpty(id)) {
            final String msg = "Required parameter id is missing. Unable to update.";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }
        //Check to make sure we have all the parameters we need for a valid
        //application config
        checkErrorConditions(config);

        final ApplicationConfigRequest request = new ApplicationConfigRequest();
        request.setApplicationConfig(config);

        ApplicationConfigResponse ccr = executeRequest(
                Verb.PUT,
                BASE_CONFIG_APPLICATION_REST_URI,
                id,
                null,
                request,
                ApplicationConfigResponse.class);

        if (ccr.getApplicationConfigs() == null || ccr.getApplicationConfigs().length == 0) {
            String msg = "Unable to parse application config from response";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // return the first (only) application config
        return ccr.getApplicationConfigs()[0];
    }

    /**
     * Gets information for a given id.
     *
     * @param id the application configuration id to get (can't be null or
     * empty)
     * @return the application configuration for this id
     * @throws CloudServiceException
     */
    public ApplicationConfig getApplicationConfig(final String id) throws CloudServiceException {
        if (StringUtils.isEmpty(id)) {
            final String msg = "Required parameter id is missing. Unable to get.";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        ApplicationConfigResponse ccr = executeRequest(
                Verb.GET,
                BASE_CONFIG_APPLICATION_REST_URI,
                id,
                null,
                null,
                ApplicationConfigResponse.class);

        if (ccr.getApplicationConfigs() == null || ccr.getApplicationConfigs().length == 0) {
            final String msg = "Unable to parse application config from response";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // return the first (only) application config
        return ccr.getApplicationConfigs()[0];
    }

    /**
     * Gets a set of application configurations for the given parameters.
     *
     * @param params key/value pairs in a map object.<br>
     *
     * More details on the parameters can be found on the Genie User Guide on
     * GitHub.
     * @return List of application configuration elements that match the filter
     * @throws CloudServiceException
     */
    public List<ApplicationConfig> getApplicationConfigs(final Multimap<String, String> params)
            throws CloudServiceException {
        final ApplicationConfigResponse ccr = executeRequest(
                Verb.GET,
                BASE_CONFIG_APPLICATION_REST_URI,
                null,
                params,
                null,
                ApplicationConfigResponse.class);

        // this will only happen if 200 is returned, and parsing fails for some
        // reason
        if (ccr.getApplicationConfigs() == null || ccr.getApplicationConfigs().length == 0) {
            String msg = "Unable to parse application config from response";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // if we get here, there are non-zero application config elements - return all
        return Arrays.asList(ccr.getApplicationConfigs());
    }

    /**
     * Delete an application configuration using its id.
     *
     * @param id the id for the application configuration to delete
     * @return the deleted application configuration
     * @throws CloudServiceException
     */
    public ApplicationConfig deleteApplicationConfig(final String id) throws CloudServiceException {
        if (StringUtils.isEmpty(id)) {
            String msg = "Missing required parameter: id";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        ApplicationConfigResponse ccr = executeRequest(
                Verb.DELETE,
                BASE_CONFIG_APPLICATION_REST_URI,
                id,
                null,
                null,
                ApplicationConfigResponse.class);

        if (ccr.getApplicationConfigs() == null || ccr.getApplicationConfigs().length == 0) {
            String msg = "Unable to parse application config from response";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // return the first (only) application config
        return ccr.getApplicationConfigs()[0];
    }

    /**
     * Check to make sure that the required parameters exist.
     *
     * @param config The configuration to check
     * @throws CloudServiceException
     */
    private void checkErrorConditions(final ApplicationConfig config) throws CloudServiceException {
        if (config == null) {
            final String msg = "Required parameter config can't be NULL";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        final List<String> messages = new ArrayList<String>();
        if (StringUtils.isEmpty(config.getUser())) {
            messages.add("User name is missing and is required. Unable to create.\n");
        }
        if (StringUtils.isEmpty(config.getName())) {
            messages.add("Application name is missing and is required. Unable to create.\n");
        }
        if (config.getStatus() == null) {
            messages.add("No application status entered. Required to create\n");
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
