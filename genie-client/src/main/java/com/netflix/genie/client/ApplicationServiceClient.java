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
import com.netflix.client.http.HttpRequest;
import com.netflix.client.http.HttpRequest.Verb;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.model.Application;
import com.netflix.genie.common.model.Command;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.List;
import java.util.Set;
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

    private static final String BASE_CONFIG_APPLICATION_REST_URL
            = BASE_REST_URL + "config/applications";

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
     * @return The application that was created
     * @throws GenieException
     */
    public Application createApplication(final Application application)
            throws GenieException {
        if (application == null) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No application passed in. Unable to validate.");
        }

        application.validate();
        final HttpRequest request = this.buildRequest(
                Verb.POST,
                BASE_CONFIG_APPLICATION_REST_URL,
                null,
                application);
        return (Application) this.executeRequest(request, null, Application.class);
    }

    /**
     * Create or update an application configuration.
     *
     * @param id the id for the application to create or update
     * @param application the object encapsulating the new application to create
     *
     * @return extracted application configuration response
     * @throws GenieException
     */
    public Application updateApplication(
            final String id,
            final Application application)
            throws GenieException {
        if (StringUtils.isBlank(id)) {
            final String msg = "Required parameter id is missing. Unable to update.";
            LOG.error(msg);
            throw new GenieException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        final HttpRequest request = this.buildRequest(
                Verb.PUT,
                StringUtils.join(
                        new String[]{BASE_CONFIG_APPLICATION_REST_URL, id},
                        SLASH),
                null,
                application);
        return (Application) this.executeRequest(request, null, Application.class);
    }

    /**
     * Gets information for a given id.
     *
     * @param id the application id to get (can't be null or empty)
     * @return the application for this id
     * @throws GenieException
     */
    public Application getApplication(final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            final String msg = "Required parameter id is missing. Unable to get.";
            LOG.error(msg);
            throw new GenieException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        final HttpRequest request = this.buildRequest(
                Verb.GET,
                StringUtils.join(
                        new String[]{BASE_CONFIG_APPLICATION_REST_URL, id},
                        SLASH),
                null,
                null);
        return (Application) this.executeRequest(request, null, Application.class);
    }

    /**
     * Gets a set of application configurations for the given parameters.
     *
     * @param params key/value pairs in a map object.<br>
     *
     * More details on the parameters can be found on the Genie User Guide on
     * GitHub.
     * @return List of application configuration elements that match the filter
     * @throws GenieException
     */
    public List<Application> getApplications(final Multimap<String, String> params)
            throws GenieException {
        final HttpRequest request = this.buildRequest(
                Verb.GET,
                BASE_CONFIG_APPLICATION_REST_URL,
                params,
                null);
        return (List<Application>) this.executeRequest(request, List.class, Application.class);
    }

    /**
     * Delete all the applications in the database.
     *
     * @return the should be empty set.
     * @throws GenieException
     */
    public List<Application> deleteAllApplications() throws GenieException {
        final HttpRequest request = this.buildRequest(
                Verb.DELETE,
                BASE_CONFIG_APPLICATION_REST_URL,
                null,
                null);
        return (List<Application>) this.executeRequest(request, List.class, Application.class);
    }

    /**
     * Delete an application configuration using its id.
     *
     * @param id the id for the application to delete
     * @return the deleted application
     * @throws GenieException
     */
    public Application deleteApplication(final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            String msg = "Missing required parameter: id";
            LOG.error(msg);
            throw new GenieException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        final HttpRequest request = this.buildRequest(
                Verb.DELETE,
                StringUtils.join(
                        new String[]{BASE_CONFIG_APPLICATION_REST_URL, id},
                        SLASH),
                null,
                null);
        return (Application) this.executeRequest(request, null, Application.class);
    }

    /**
     * Add some more configuration files to a given application.
     *
     * @param id The id of the application to add configurations to. Not
     * Null/empty/blank.
     * @param configs The configuration files to add. Not null or empty.
     * @return The new set of configuration files for the given application.
     * @throws GenieException
     */
    public Set<String> addConfigsToApplication(
            final String id,
            final Set<String> configs) throws GenieException {
        if (StringUtils.isBlank(id)) {
            final String msg = "Missing required parameter: id";
            LOG.error(msg);
            throw new GenieException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }
        if (configs == null || configs.isEmpty()) {
            final String msg = "Missing required parameter: configs";
            LOG.error(msg);
            throw new GenieException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        final HttpRequest request = this.buildRequest(
                Verb.POST,
                StringUtils.join(
                        new String[]{BASE_CONFIG_APPLICATION_REST_URL, id, "configs"},
                        SLASH),
                null,
                configs);
        return (Set<String>) this.executeRequest(request, Set.class, String.class);
    }

    /**
     * Get the active set of configuration files for the given application.
     *
     * @param id The id of the application to get configurations for. Not
     * Null/empty/blank.
     * @return The set of configuration files for the given application.
     * @throws GenieException
     */
    public Set<String> getConfigsForApplication(final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            String msg = "Missing required parameter: id";
            LOG.error(msg);
            throw new GenieException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        final HttpRequest request = this.buildRequest(
                Verb.GET,
                StringUtils.join(
                        new String[]{BASE_CONFIG_APPLICATION_REST_URL, id, "configs"},
                        SLASH),
                null,
                null);
        return (Set<String>) this.executeRequest(request, Set.class, String.class);
    }

    /**
     * Update the configuration files for a given application.
     *
     * @param id The id of the application to update the configuration files
     * for. Not null/empty/blank.
     * @param configs The configuration files to replace existing configuration
     * files with. Not null.
     * @return The new set of application configurations.
     * @throws GenieException
     */
    public Set<String> updateConfigsForApplication(
            final String id,
            final Set<String> configs) throws GenieException {
        if (StringUtils.isBlank(id)) {
            final String msg = "Missing required parameter: id";
            LOG.error(msg);
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }
        if (configs == null) {
            final String msg = "Missing required parameter: configs";
            LOG.error(msg);
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        final HttpRequest request = this.buildRequest(
                Verb.PUT,
                StringUtils.join(
                        new String[]{BASE_CONFIG_APPLICATION_REST_URL, id, "configs"},
                        SLASH),
                null,
                configs);
        return (Set<String>) this.executeRequest(request, Set.class, String.class);
    }

    /**
     * Delete the all configuration files from a given application.
     *
     * @param id The id of the application to delete the configuration files
     * from. Not null/empty/blank.
     * @return Empty set if successful
     * @throws GenieException
     */
    public Set<String> removeAllConfigsForApplication(
            final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            final String msg = "Missing required parameter: id";
            LOG.error(msg);
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        final HttpRequest request = this.buildRequest(
                Verb.DELETE,
                StringUtils.join(
                        new String[]{BASE_CONFIG_APPLICATION_REST_URL, id, "configs"},
                        SLASH),
                null,
                null);
        return (Set<String>) this.executeRequest(request, Set.class, String.class);
    }

    /**
     * Add some more jar files to a given application.
     *
     * @param id The id of the application to add jars to. Not
     * Null/empty/blank.
     * @param jars The jar files to add. Not null or empty.
     * @return The new set of jar files for the given application.
     * @throws GenieException
     */
    public Set<String> addJarsToApplication(
            final String id,
            final Set<String> jars) throws GenieException {
        if (StringUtils.isBlank(id)) {
            final String msg = "Missing required parameter: id";
            LOG.error(msg);
            throw new GenieException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }
        if (jars == null || jars.isEmpty()) {
            final String msg = "Missing required parameter: jars";
            LOG.error(msg);
            throw new GenieException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        final HttpRequest request = this.buildRequest(
                Verb.POST,
                StringUtils.join(
                        new String[]{BASE_CONFIG_APPLICATION_REST_URL, id, "jars"},
                        SLASH),
                null,
                jars);
        return (Set<String>) this.executeRequest(request, Set.class, String.class);
    }

    /**
     * Get the active set of jar files for the given application.
     *
     * @param id The id of the application to get jars for. Not
     * Null/empty/blank.
     * @return The set of jar files for the given application.
     * @throws GenieException
     */
    public Set<String> getJarsForApplication(final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            String msg = "Missing required parameter: id";
            LOG.error(msg);
            throw new GenieException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        final HttpRequest request = this.buildRequest(
                Verb.GET,
                StringUtils.join(
                        new String[]{BASE_CONFIG_APPLICATION_REST_URL, id, "jars"},
                        SLASH),
                null,
                null);
        return (Set<String>) this.executeRequest(request, Set.class, String.class);
    }

    /**
     * Update the jar files for a given application.
     *
     * @param id The id of the application to update the jar files
     * for. Not null/empty/blank.
     * @param jars The jar files to replace existing jar
     * files with. Not null.
     * @return The new set of application jars.
     * @throws GenieException
     */
    public Set<String> updateJarsForApplication(
            final String id,
            final Set<String> jars) throws GenieException {
        if (StringUtils.isBlank(id)) {
            final String msg = "Missing required parameter: id";
            LOG.error(msg);
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }
        if (jars == null) {
            final String msg = "Missing required parameter: jars";
            LOG.error(msg);
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        final HttpRequest request = this.buildRequest(
                Verb.PUT,
                StringUtils.join(
                        new String[]{BASE_CONFIG_APPLICATION_REST_URL, id, "jars"},
                        SLASH),
                null,
                jars);
        return (Set<String>) this.executeRequest(request, Set.class, String.class);
    }

    /**
     * Delete the all jar files from a given application.
     *
     * @param id The id of the application to delete the jar files
     * from. Not null/empty/blank.
     * @return Empty set if successful
     * @throws GenieException
     */
    public Set<String> removeAllJarsForApplication(
            final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            final String msg = "Missing required parameter: id";
            LOG.error(msg);
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        final HttpRequest request = this.buildRequest(
                Verb.DELETE,
                StringUtils.join(
                        new String[]{BASE_CONFIG_APPLICATION_REST_URL, id, "jars"},
                        SLASH),
                null,
                null);
        return (Set<String>) this.executeRequest(request, Set.class, String.class);
    }

    /**
     * Get all the commands this application is associated with.
     *
     * @param id The id of the application to get the commands for. Not
     * NULL/empty/blank.
     * @return The set of commands.
     * @throws GenieException
     */
    public Set<Command> getCommandsForApplication(
            final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            final String msg = "Missing required parameter: id";
            LOG.error(msg);
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }

        final HttpRequest request = this.buildRequest(
                Verb.GET,
                StringUtils.join(
                        new String[]{BASE_CONFIG_APPLICATION_REST_URL, id, "commands"},
                        SLASH),
                null,
                null);
        return (Set<Command>) this.executeRequest(request, Set.class, Command.class);
    }
}
