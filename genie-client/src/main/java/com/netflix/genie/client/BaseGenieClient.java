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
import com.netflix.appinfo.CloudInstanceConfig;
import com.netflix.appinfo.EurekaInstanceConfig;
import com.netflix.appinfo.MyDataCenterInstanceConfig;
import com.netflix.client.ClientException;
import com.netflix.client.ClientFactory;
import com.netflix.client.http.HttpRequest;
import com.netflix.client.http.HttpRequest.Verb;
import com.netflix.client.http.HttpResponse;
import com.netflix.config.ConfigurationManager;
import com.netflix.discovery.DefaultEurekaClientConfig;
import com.netflix.discovery.DiscoveryManager;
import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.messages.BaseResponse;
import com.netflix.niws.client.http.RestClient;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map.Entry;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract REST client class that is extended to implement specific clients.
 *
 * @author skrishnan
 * @author tgianos
 */
public class BaseGenieClient {

    private static final String CLOUD = "cloud";

    private static final Logger LOG = LoggerFactory
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
     * Standard root for all rest services.
     */
    protected static final String BASE_REST_URI = "/genie/v1/";

    /**
     * Protected constructor for singleton class.
     *
     * @throws IOException if properties can't be loaded
     */
    protected BaseGenieClient() throws IOException {
        ConfigurationManager.loadPropertiesFromResources(NIWS_CLIENT_NAME_GENIE + ".properties");
    }

    /**
     * Initializes Eureka for the given environment, if it is being used -
     * should only be used if the user of library hasn't initialized Eureka
     * already.
     *
     * @param env "prod" or "test" or "dev"
     */
    public static synchronized void initEureka(final String env) {
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
     * @param <T>
     * @param verb GET, POST or DELETE
     * @param baseRestUri the base Uri to use in the request, e.g. genie/v0/jobs
     * @param uuid the id to append to the baseRestUri, if any (e.g. job ID)
     * @param params HTTP params (e.g. userName="foo")
     * @param request Genie request if applicable (for POST), null otherwise
     * @param responseClass class name of expected response to be used for
     * unmarshalling
     *
     * @return extracted and unmarshalled response from the Genie Execution
     * Service
     * @throws CloudServiceException
     */
    protected <T extends BaseResponse> T executeRequest(
            final Verb verb,
            final String baseRestUri,
            final String uuid,
            final Multimap<String, String> params,
            final Object request,
            final Class<T> responseClass)
            throws CloudServiceException {
        HttpResponse response = null;
        String requestUri = buildRequestUri(baseRestUri, uuid);
        try {
            // execute an HTTP request on Genie using load balancer
            RestClient genieClient = (RestClient) ClientFactory
                    .getNamedClient(NIWS_CLIENT_NAME_GENIE);
            HttpRequest.Builder builder = HttpRequest.newBuilder()
                    .verb(verb).header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON)
                    .uri(new URI(requestUri)).entity(request);
            if (params != null) {
                for (Entry<String, String> param : params.entries()) {
                    builder.queryParams(param.getKey(), param.getValue());
                }
            }
            HttpRequest req = builder.build();
            response = genieClient.executeWithLoadBalancer(req);

            // basic error checking
            if (response == null) {
                String msg = "Received NULL response from Genie service";
                LOG.error(msg);
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
            }

            // extract/cast/unmarshal and return entity
            return extractEntityFromClientResponse(response, responseClass);
        } catch (URISyntaxException e) {
            LOG.error("Exception caught while executing request", e);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, e);
        } catch (ClientException e) {
            LOG.error("Exception caught while executing request", e);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, e);
        } finally {
            // release resources after we are done
            // this is really really important - or we run out of connections
            if (response != null) {
                response.close();
            }
        }
    }

    /**
     * Converts a response to a specific response object which must extend
     * BaseResponse.
     *
     * @param response response from REST service
     * @param responseClass class name of expected response
     * @return specific response class extracted
     */
    private <T extends BaseResponse> T extractEntityFromClientResponse(
            HttpResponse response, Class<T> responseClass)
            throws CloudServiceException {
        if (response == null) {
            String msg = "Received null response from Genie Service";
            LOG.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        int status = response.getStatus();
        LOG.debug("Response Status:" + status);
        try {
            // check if entity exists within response
            if (!response.hasEntity()) {
                // assuming no Genie bug, this should only happen if the request
                // didn't get to Genie
                String msg = "Received status " + status
                        + " for Genie/NIWS call, but no entity/body";
                LOG.error(msg);
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
                LOG.error(msg);
                throw new CloudServiceException(
                        (status != HttpURLConnection.HTTP_OK) ? status
                        : HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
            }

            // got an http error code from Genie, throw exception
            if (status != HttpURLConnection.HTTP_OK) {
                LOG.error("Received error for job: "
                        + templateResponse.getErrorMsg());
                throw new CloudServiceException(status,
                        templateResponse.getErrorMsg());
            }

            // all good
            return templateResponse;
        } catch (Exception e) {
            LOG.error(
                    "Exception caught while extracting entity from response", e);
            throw new CloudServiceException(
                    (status != HttpURLConnection.HTTP_OK) ? status
                    : HttpURLConnection.HTTP_INTERNAL_ERROR, e);
        }
    }

    /**
     * Given a urlPath such as genie/v1/jobs and a uuid, constructs the request
     * uri.
     *
     * @param baseRestUri e.g. genie/v1/jobs
     * @param uuid the uuid for the REST resource, if it exists
     * @return request URI for the service
     */
    private String buildRequestUri(final String baseRestUri, final String uuid) {
        if (StringUtils.isEmpty(uuid)) {
            return baseRestUri;
        } else {
            final StringBuilder builder = new StringBuilder();
            builder.append(baseRestUri).append("/").append(uuid);
            return builder.toString();
        }
    }
}
