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
import com.netflix.genie.common.model.Application;
import java.io.IOException;
import java.net.HttpURLConnection;
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
public final class ApplicationServiceClient extends BaseGenieClient {

    private static final Logger LOG = LoggerFactory
            .getLogger(ApplicationServiceClient.class);

    private static final String BASE_CONFIG_APPLICATION_REST_URI
            = BASE_REST_URI + "config/applications";

    // reference to the instance object
    private static ApplicationServiceClient instance;

    /**
     * Private constructor for singleton class.
     *
     * @throws IOException if there is any error during initialization
     */
    private ApplicationServiceClient() throws IOException {
        super();
    }

    /**
     * Returns the singleton instance that can be used by clients.
     *
     * @return ApplicationServiceClient instance
     * @throws IOException if there is an error instantiating client
     */
    public static synchronized ApplicationServiceClient getInstance() throws IOException {
        if (instance == null) {
            instance = new ApplicationServiceClient();
        }

        return instance;
    }

    /**
     * Create a new application configuration.
     *
     * @param application the object encapsulating the new application
     * configuration to create
     *
     * @return extracted application config response
     * @throws CloudServiceException
     */
    public Application createApplication(final Application application)
            throws CloudServiceException {
        Application.validate(application);

        return executeRequestForSingleEntity(
                Verb.POST,
                BASE_CONFIG_APPLICATION_REST_URI,
                null,
                null,
                application,
                Application.class);
    }

    /**
     * Create or update an application configuration.
     *
     * @param id the id for the application to create or update
     * @param application the object encapsulating the new application to create
     *
     * @return extracted application configuration response
     * @throws CloudServiceException
     */
    public Application updateApplication(
            final String id,
            final Application application)
            throws CloudServiceException {
        if (StringUtils.isEmpty(id)) {
            final String msg = "Required parameter id is missing. Unable to update.";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }
        //Check to make sure we have all the parameters we need for a valid
        //application config
        Application.validate(application);

        return executeRequestForSingleEntity(
                Verb.PUT,
                BASE_CONFIG_APPLICATION_REST_URI,
                id,
                null,
                application,
                Application.class);
    }

    /**
     * Gets information for a given id.
     *
     * @param id the application id to get (can't be null or empty)
     * @return the application for this id
     * @throws CloudServiceException
     */
    public Application getApplication(final String id) throws CloudServiceException {
        if (StringUtils.isEmpty(id)) {
            final String msg = "Required parameter id is missing. Unable to get.";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        return executeRequestForSingleEntity(
                Verb.GET,
                BASE_CONFIG_APPLICATION_REST_URI,
                id,
                null,
                null,
                Application.class);
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
    public List<Application> getApplications(final Multimap<String, String> params)
            throws CloudServiceException {
        return executeRequestForListOfEntities(
                Verb.GET,
                BASE_CONFIG_APPLICATION_REST_URI,
                null,
                params,
                null,
                Application.class);
    }

    /**
     * Delete an application configuration using its id.
     *
     * @param id the id for the application to delete
     * @return the deleted application
     * @throws CloudServiceException
     */
    public Application deleteApplication(final String id) throws CloudServiceException {
        if (StringUtils.isEmpty(id)) {
            String msg = "Missing required parameter: id";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        return executeRequestForSingleEntity(
                Verb.DELETE,
                BASE_CONFIG_APPLICATION_REST_URI,
                id,
                null,
                null,
                Application.class);
    }
}
