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

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.genie.common.messages.BaseResponse;
import com.netflix.genie.common.exceptions.CloudServiceException;


import com.netflix.niws.client.http.HttpClientRequest;
import com.netflix.niws.client.http.HttpClientRequest.Verb;
import com.netflix.niws.client.http.HttpClientResponse;
import com.netflix.niws.client.http.RestClient;
import com.netflix.appinfo.CloudInstanceConfig;
import com.netflix.appinfo.EurekaInstanceConfig;
import com.netflix.appinfo.MyDataCenterInstanceConfig;
import com.netflix.client.ClientFactory;
import com.netflix.config.ConfigurationManager;
import com.netflix.discovery.DefaultEurekaClientConfig;
import com.netflix.discovery.DiscoveryManager;

import javax.ws.rs.core.MultivaluedMap;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * Abstract REST client class that is extended to implement specific clients.
 *
 * @author skrishnan
 *
 */
public abstract class BaseGenieClient {

    private static final String CLOUD = "cloud";

    private static Logger logger = LoggerFactory
            .getLogger(BaseGenieClient.class);

    /**
     * The name with this this REST client is registered.
     */
    protected static final String NIWS_CLIENT_NAME_GENIE = "genieClient";

    /**
     * The name of the server application.
     */
    protected static final String NIWS_APP_NAME_GENIE = "genie";

    /**
     * Protected constructor for singleton class.
     * @throws IOException if properties can't be loaded
     */
    protected BaseGenieClient() throws IOException {
        ConfigurationManager.loadPropertiesFromResources(NIWS_CLIENT_NAME_GENIE + ".properties");
    }

    /**
     * Initializes Eureka for the given environment, if it is being used -
     * should only be used if the user of library hasn't initialized Eureka already.
     *
     * @param env "prod" or "test" or "dev"
     */
    public static synchronized void initEureka(String env) {
        if (env != null) {
            System.setProperty("eureka.environment", env);
        }

        EurekaInstanceConfig config;
        if (CLOUD.equals(ConfigurationManager.getDeploymentContext()
                .getDeploymentDatacenter())) {
            config = new CloudInstanceConfig();
        } else {
            config = new MyDataCenterInstanceConfig();
        }
        DiscoveryManager.getInstance().initComponent(config,
                new DefaultEurekaClientConfig());
    }

    /**
     * Executes HTTP request based on user params, and performs
     * marshaling/unmarshaling.
     *
     * @param verb
     *            GET, POST or DELETE
     * @param baseRestUri
     *            the base Uri to use in the request, e.g. genie/v0/jobs
     * @param uuid
     *            the id to append to the baseRestUri, if any (e.g. job ID)
     * @param params
     *            HTTP params (e.g. userName="foo")
     * @param request
     *            Genie request if applicable (for POST), null otherwise
     * @return response from the Genie Execution Service
     * @throws CloudServiceException
     */
    protected HttpClientResponse executeRequest(Verb verb, String baseRestUri, String uuid,
            MultivaluedMap<String, String> params, Object request)
            throws CloudServiceException {
        HttpClientResponse response = null;
        String requestUri = buildRequestUri(baseRestUri, uuid);
        try {
            MultivaluedMapImpl headers = new MultivaluedMapImpl();
            headers.add("Accept", "application/json");

            RestClient genieClient = (RestClient) ClientFactory
                    .getNamedClient(NIWS_CLIENT_NAME_GENIE);

            HttpClientRequest req = HttpClientRequest.newBuilder()
                    .setVerb(verb).setHeaders(headers).setQueryParams(params)
                    .setUri(new URI(requestUri)).setEntity(request).build();
            // logger.debug("Load balancer: " + genieClient.getLoadBalancer());
            response = genieClient.executeWithLoadBalancer(req);
            if (response != null) {
                return response;
            } else {
                String msg = "Received NULL response from Genie service";
                logger.error(msg);
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
            }
        } catch (Exception e) {
            logger.error("Exception caught while executing request", e);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, e);
        }
    }

    /**
     * Converts a response to a specific response object which must extend
     * BaseResponse.
     *
     * @param response
     *            response from REST service
     * @param responseClass
     *            class name of expected response
     * @return specific response class extracted
     */
    protected <T extends BaseResponse> T extractEntityFromClientResponse(
            HttpClientResponse response, Class<T> responseClass)
            throws CloudServiceException {
        if (response == null) {
            String msg = "Received null response from Genie Service";
            logger.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        int status = response.getStatus();
        logger.debug("Response Status:" + status);
        try {
            // check if entity exists within response
            if (!response.hasEntity()) {
                // assuming no Genie bug, this should only happen if the request
                // didn't get to Genie
                String msg = "Received status " + status
                        + " for Genie/NIWS call, but no entity/body";
                logger.error(msg);
                throw new CloudServiceException(
                        (status != HttpURLConnection.HTTP_OK) ? status
                                : HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
            }

            // if Genie sent a response back, then we can marshal it to a
            // response class
            T templateResponse = response.getEntity(responseClass);
            if (templateResponse == null) {
                String msg = "Received status " + status
                        + " - can't deserialize response from Genie Service";
                logger.error(msg);
                throw new CloudServiceException(
                        (status != HttpURLConnection.HTTP_OK) ? status
                                : HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
            }

            // got an http error code from Genie, throw exception
            if (status != HttpURLConnection.HTTP_OK) {
                logger.error("Received error for job: "
                        + templateResponse.getErrorMsg());
                throw new CloudServiceException(status,
                        templateResponse.getErrorMsg());
            }

            // all good
            return templateResponse;
        } catch (Exception e) {
            logger.error(
                    "Exception caught while extracting entity from response", e);
            throw new CloudServiceException(
                    (status != HttpURLConnection.HTTP_OK) ? status
                            : HttpURLConnection.HTTP_INTERNAL_ERROR, e);
        } finally {
            if (response != null) {
                response.releaseResources(); // this is really really important
            }
        }
    }

    /**
     * Given a urlPath such as genie/v0/jobs and a uuid, constructs the request
     * uri.
     *
     * @param baseRestUri
     *            e.g. genie/v0/jobs
     * @param uuid
     *            the uuid for the REST resource, if it exists
     * @return request URI for the service
     */
    private String buildRequestUri(String baseRestUri, String uuid) {
        return (uuid == null || uuid.isEmpty()) ? baseRestUri : baseRestUri
                + "/" + uuid;
    }
}
