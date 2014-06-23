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
import com.netflix.client.ClientFactory;
import com.netflix.client.http.HttpRequest;
import com.netflix.client.http.HttpRequest.Verb;
import com.netflix.client.http.HttpResponse;
import com.netflix.config.ConfigurationManager;
import com.netflix.discovery.DefaultEurekaClientConfig;
import com.netflix.discovery.DiscoveryManager;
import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.niws.client.http.RestClient;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Map.Entry;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.CollectionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * REST client class that is extended to implement specific clients.
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
    protected static final String BASE_REST_URL = "/genie/v1/";

    /**
     * Reusable String for /.
     */
    protected static final String SLASH = "/";

    /**
     * The rest client to use for all requests.
     */
    private final RestClient client;

    /**
     * Protected constructor for singleton class.
     *
     * @throws IOException if properties can't be loaded
     */
    protected BaseGenieClient() throws IOException {
        ConfigurationManager.loadPropertiesFromResources(NIWS_CLIENT_NAME_GENIE + ".properties");

        // execute an HTTP request on Genie using load balancer
        this.client = (RestClient) ClientFactory
                .getNamedClient(NIWS_CLIENT_NAME_GENIE);

        //Force jersey to properly serialize JSON
        final ClientConfig clientConfig = new DefaultClientConfig();
        clientConfig.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
        final Client jerseyClient = Client.create(clientConfig);
        this.client.setJerseyClient(jerseyClient);
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

    protected HttpRequest buildRequest(
            final Verb verb,
            final String requestUri,
            final Multimap<String, String> params,
            final Object entity) throws CloudServiceException {
        try {
            final HttpRequest.Builder builder = HttpRequest.newBuilder()
                    .verb(verb)
                    .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                    .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON)
                    .uri(new URI(requestUri)).entity(entity);
            if (params != null) {
                for (final Entry<String, String> param : params.entries()) {
                    builder.queryParams(param.getKey(), param.getValue());
                }
            }
            return builder.build();
        } catch (final URISyntaxException use) {
            LOG.error(use.getMessage(), use);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST, use);
        }
    }

    protected <C extends Collection> Object executeRequest(
            final HttpRequest request,
            final Class<C> collectionClass,
            final Class entityClass) throws CloudServiceException {
        HttpResponse response = null;
        try {
            response = this.client.executeWithLoadBalancer(request);
            if (response.isSuccess()) {
                if (collectionClass != null) {
                    final ObjectMapper mapper = new ObjectMapper();
                    final CollectionType type = mapper.
                            getTypeFactory().
                            constructCollectionType(collectionClass, entityClass);
                    return mapper.readValue(response.getInputStream(), type);
                } else if (entityClass != null) {
                    return response.getEntity(entityClass);
                } else {
                    throw new CloudServiceException(
                            HttpURLConnection.HTTP_BAD_REQUEST,
                            "No return type entered.");
                }
            } else {
                throw new CloudServiceException(
                        response.getStatus(),
                        response.getEntity(String.class));
            }
        } catch (final Exception e) {
            if (e instanceof CloudServiceException) {
                throw (CloudServiceException) e;
            } else {
                LOG.error(e.getMessage(), e);
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_INTERNAL_ERROR, e);
            }
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }
}
