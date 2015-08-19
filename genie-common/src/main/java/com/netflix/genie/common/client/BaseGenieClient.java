/*
 *
 *  Copyright 2015 Netflix, Inc.
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
package com.netflix.genie.common.client;

/**
 * REST client class that is extended to implement specific clients.
 *
 * @author skrishnan
 * @author tgianos
 */
@Deprecated
public class BaseGenieClient {
//    /**
//     * Standard root for all rest services.
//     */
//    protected static final String BASE_REST_URL = "/genie/v2/";
//    /**
//     * The eureka environment.
//     */
//    protected static final String EUREKA_ENVIRONMENT_PROPERTY = "eureka.environment";
//
//    /**
//     * Reusable String for /.
//     */
//    protected static final String SLASH = "/";
//
//    private static final Logger LOGGER = LoggerFactory.getLogger(BaseGenieClient.class);
//
//    private static final String CLOUD = "cloud";
//
//    /**
//     * The name with this this REST client is registered.
//     */
//    private static final String CLIENT_NAME_GENIE = "genie2Client";
//
//    /**
//     * The rest client to use for all requests.
//     */
//    private final RestClient client;
//
//    /**
//     * Protected constructor for singleton class.
//     *
//     * @param client the Rest client to use
//     * @throws IOException if properties can't be loaded
//     */
//    public BaseGenieClient(final RestClient client) throws IOException {
//        ConfigurationManager.loadPropertiesFromResources(CLIENT_NAME_GENIE + ".properties");
//
//        if (client == null) {
//            this.client = (RestClient) ClientFactory
//                    .getNamedClient(CLIENT_NAME_GENIE);
//        } else {
//            this.client = client;
//        }
//
//        //Force jersey to properly serialize JSON
//        final Set<Class<?>> providers = new HashSet<>();
//        providers.add(JacksonJaxbJsonProvider.class);
//        providers.add(JacksonJsonProvider.class);
//        final ClientConfig clientConfig = new DefaultClientConfig(providers);
//        final Client jerseyClient = Client.create(clientConfig);
//        this.client.setJerseyClient(jerseyClient);
//    }
//
//    /**
//     * Execute a HTTP request.
//     *
//     * @param <C>             The collection class if a collection is the expected response
//     *                        entity.
//     * @param request         The request to send. Not null.
//     * @param collectionClass The collection class. Null if none expected.
//     * @param entityClass     The entity class. Not null.
//     * @return The response entity.
//     * @throws GenieException On any error.
//     */
//    public <C extends Collection> Object executeRequest(
//            final HttpRequest request,
//            final Class<C> collectionClass,
//            final Class entityClass) throws GenieException {
//        if (collectionClass == null && entityClass == null) {
//            throw new GeniePreconditionException("No return type entered. Unable to continue.");
//        }
//        if (collectionClass != null && entityClass == null) {
//            throw new GeniePreconditionException(
//                    "No entity class for collection class " + collectionClass + " entered.");
//        }
//        if (request == null) {
//            throw new GeniePreconditionException("No request entered. Unable to continue..");
//        }
//        try (final HttpResponse response = this.client.executeWithLoadBalancer(request)) {
//            if (response.isSuccess()) {
//                LOGGER.debug("Response returned success.");
//                final ObjectMapper mapper = new ObjectMapper();
//                if (collectionClass != null) {
//                    final CollectionType type = mapper.
//                            getTypeFactory().
//                            constructCollectionType(collectionClass, entityClass);
//                    return mapper.readValue(response.getInputStream(), type);
//                } else {
//                    return mapper.readValue(response.getInputStream(), entityClass);
//                }
//            } else {
//                throw new GenieException(
//                        response.getStatus(),
//                        response.getEntity(String.class));
//            }
//        } catch (final Exception e) {
//            if (e instanceof GenieException) {
//                throw (GenieException) e;
//            } else {
//                LOGGER.error(e.getMessage(), e);
//                throw new GenieServerException(e);
//            }
//        }
//    }
//
//    /**
//     * Build a HTTP request from the given parameters.
//     *
//     * @param verb       The type of HTTP request to use. Not null.
//     * @param requestUri The URI to send the request to. Not null/empty/blank.
//     * @param params     Any query parameters to send along with the request.
//     * @param entity     An entity. Required for POST or PUT.
//     * @return The HTTP request.
//     * @throws GenieException On any error.
//     */
//    public static HttpRequest buildRequest(
//            final Verb verb,
//            final String requestUri,
//            final Multimap<String, String> params,
//            final Object entity) throws GenieException {
//        if (verb == null) {
//            throw new GeniePreconditionException("No http verb entered unable to continue.");
//        }
//        if ((verb == Verb.POST || verb == Verb.PUT) && entity == null) {
//            throw new GeniePreconditionException("Must have an entity to perform a post or a put.");
//        }
//        if (StringUtils.isBlank(requestUri)) {
//            throw new GeniePreconditionException("No request uri entered. Unable to continue.");
//        }
//        try {
//            final HttpRequest.Builder builder = HttpRequest.newBuilder()
//                    .verb(verb)
//                    .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
//                    .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON)
//                    .uri(new URI(requestUri))
//                    .entity(entity);
//            if (params != null) {
//                for (final Entry<String, String> param : params.entries()) {
//                    builder.queryParams(param.getKey(), param.getValue());
//                }
//            }
//            return builder.build();
//        } catch (final URISyntaxException use) {
//            LOGGER.error(use.getMessage(), use);
//            throw new GenieBadRequestException(use);
//        }
//    }
//
//    /**
//     * Initializes Eureka for the given environment, if it is being used -
//     * should only be used if the user of library hasn't initialized Eureka
//     * already.
//     *
//     * @param env "prod" or "test" or "dev"
//     */
//    public static synchronized void initEureka(final String env) {
//        if (env != null) {
//            System.setProperty(EUREKA_ENVIRONMENT_PROPERTY, env);
//        }
//
//        final EurekaInstanceConfig config;
//        if (CLOUD.equals(ConfigurationManager.getDeploymentContext()
//                .getDeploymentDatacenter())) {
//            config = new CloudInstanceConfig();
//        } else {
//            config = new MyDataCenterInstanceConfig();
//        }
//        DiscoveryManager.getInstance().initComponent(config,
//                new DefaultEurekaClientConfig());
//    }
//
//    /**
//     * Get the rest client used by this Genie Client.
//     *
//     * @return The rest client
//     */
//    protected RestClient getRestClient() {
//        return this.client;
//    }
}
