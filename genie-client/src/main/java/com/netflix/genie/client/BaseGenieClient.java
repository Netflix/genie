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
import com.netflix.genie.common.model.Auditable;
import com.netflix.niws.client.http.RestClient;
import java.io.IOException;
import java.lang.reflect.Array;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang3.StringUtils;
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

        final EurekaInstanceConfig config;
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
     * Executes HTTP request based on user parameters, and performs
     * marshaling/un-marshaling for an expected single result.
     *
     * @param <T> The entity type for this request
     * @param verb GET, POST or DELETE
     * @param baseRestUri the base Uri to use in the request, e.g. genie/v0/jobs
     * @param id the id to append to the baseRestUri, if any (e.g. job ID)
     * @param params HTTP query parameters (e.g. userName="foo")
     * @param entity Genie resource entity if applicable (for POST), null
     * otherwise
     * @param entityClass class name of expected response to be used for
     * un-marshaling
     *
     * @return extracted and un-marshaled response entity from Genie
     * @throws CloudServiceException
     */
    protected <T extends Auditable> T executeRequestForSingleEntity(
            final Verb verb,
            final String baseRestUri,
            final String id,
            final Multimap<String, String> params,
            final T entity,
            final Class<T> entityClass)
            throws CloudServiceException {
        return (T) executeRequest(verb, baseRestUri, id, params, entity, entityClass, false);
    }

    /**
     * Executes HTTP request based on user parameters, and performs
     * marshaling/un-marshaling for an expected list of results.
     *
     * @param <T> The entity type for this request
     * @param verb GET, POST or DELETE
     * @param baseRestUri the base Uri to use in the request, e.g. genie/v0/jobs
     * @param id the id to append to the baseRestUri, if any (e.g. job ID)
     * @param params HTTP query parameters (e.g. userName="foo")
     * @param entity Genie resource entity if applicable (for POST), null
     * otherwise
     * @param entityClass class name of expected response to be used for
     * un-marshaling
     *
     * @return extracted and un-marshaled response entity from Genie
     * @throws CloudServiceException
     */
    protected <T extends Auditable> List<T> executeRequestForListOfEntities(
            final Verb verb,
            final String baseRestUri,
            final String id,
            final Multimap<String, String> params,
            final T entity,
            final Class<T> entityClass)
            throws CloudServiceException {
        return (List<T>) executeRequest(verb, baseRestUri, id, params, entity, entityClass, true);
    }

    /**
     * Executes HTTP request based on user parameters, and performs
     * marshaling/un-marshaling.
     *
     * @param <T> The entity type for this request
     * @param verb GET, POST or DELETE
     * @param baseRestUri the base Uri to use in the request, e.g. genie/v0/jobs
     * @param id the id to append to the baseRestUri, if any (e.g. job ID)
     * @param params HTTP query parameters (e.g. userName="foo")
     * @param entity Genie resource entity if applicable (for POST), null
     * otherwise
     * @param entityClass class name of expected response to be used for
     * un-marshaling
     * @param isList Whether a list is expected as the result
     *
     * @return extracted and un-marshaled response entity from Genie
     * @throws CloudServiceException
     */
    private <T extends Auditable> Object executeRequest(
            final Verb verb,
            final String baseRestUri,
            final String id,
            final Multimap<String, String> params,
            final Object entity,
            final Class<T> entityClass,
            final boolean isList)
            throws CloudServiceException {
        HttpResponse response = null;
        final String requestUri = buildRequestUri(baseRestUri, id);
        try {
            // execute an HTTP request on Genie using load balancer
            final RestClient genieClient = (RestClient) ClientFactory
                    .getNamedClient(NIWS_CLIENT_NAME_GENIE);
            final HttpRequest.Builder builder = HttpRequest.newBuilder()
                    .verb(verb)
                    //                    .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                    //                    .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON)
                    .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_XML)
                    .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_XML)
                    .uri(new URI(requestUri)).entity(entity);
            if (params != null) {
                for (final Entry<String, String> param : params.entries()) {
                    builder.queryParams(param.getKey(), param.getValue());
                }
            }
            final HttpRequest req = builder.build();
            response = genieClient.executeWithLoadBalancer(req);

            // extract/cast/unmarshal and return entity
            return extractEntityFromClientResponse(response, isList, entityClass);
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
     * @param isList Whether the entity is expected to be a list or not
     * @param entityClass class name of expected response
     * @return specific response class extracted
     */
    private <T extends Auditable> Object extractEntityFromClientResponse(
            final HttpResponse response,
            final boolean isList,
            final Class<T> entityClass)
            throws CloudServiceException {
        if (response == null) {
            final String msg = "Received null response from Genie Service";
            LOG.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        try {
            final int status = response.getStatus();
            LOG.debug("Response Status:" + status);
            if (!response.isSuccess()) {
                throw new CloudServiceException(status, response.getEntity(String.class));
            }
            // check if entity exists within response
            if (!response.hasEntity()) {
                // assuming no Genie bug, this should only happen if the request
                // didn't get to Genie
                final String msg = "Received status " + status
                        + " for Genie/NIWS call, but no entity/body";
                LOG.error(msg);
                throw new CloudServiceException(
                        (status != HttpURLConnection.HTTP_OK) ? status
                        : HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
            }
            if (isList) {
                final Class clazz = Array.newInstance(entityClass, 0).getClass();
                final T[] collection = (T[]) response.getEntity(clazz);
                return Arrays.asList(collection);
            } else {
                return response.getEntity(entityClass);
            }
//            return response.getEntity(entityClass);
//            if (isList) {
//                return response.getEntity(new TypeToken<List<Application>>() {
//                });
//            } else {
//                return response.getEntity(entityClass);
//            }
        } catch (final Exception e) {
            if (e instanceof CloudServiceException) {
                throw (CloudServiceException) e;
            } else {
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_INTERNAL_ERROR, e);
            }
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
