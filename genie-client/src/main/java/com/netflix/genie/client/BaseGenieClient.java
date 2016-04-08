/*
 *
 *  Copyright 2016 Netflix, Inc.
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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.genie.client.interceptor.SecurityHeaderInterceptor;
import com.netflix.genie.client.retrofit.GenieService;
import com.netflix.genie.client.security.TokenFetcher;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;

import okhttp3.OkHttpClient;
import org.apache.commons.configuration2.Configuration;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;


/**
 * Base class for the clients for Genie Services.
 *
 * @author amsharma
 * @since 3.0.0
 */
public abstract class BaseGenieClient {

    protected static final String FILE_PATH_DELIMITER = "/";

    private static final String SERVICE_BASE_URL_KEY = "genie.service.base.url";

    private static final String GENIE_SECURITY_ENABLED_KEY = "genie.security.enabled";
    private static final String GENIE_SECURITY_OAUTH_URL_KEY = "genie.security.oauth.url";
    private static final String GENIE_SECURITY_OAUTH_CLIENT_ID_KEY = "genie.security.oauth.clientId";
    private static final String GENIE_SECURITY_OAUTH_CLIENT_SECRET_KEY = "genie.security.oauth.clientSecret";
    private static final String GENIE_SECURITY_OAUTH_GRANT_TYPE_KEY = "genie.security.oauth.grantType";
    private static final String GENIE_SECURITY_OAUTH_SCOPE_KEY = "genie.security.oauth.scope";

    protected GenieService genieService;

    private TokenFetcher tokenFetcher;

    /**
     * Constructor.
     *
     * @param configuration The configuration to use for instantiating the client.
     *
     * @throws GenieException If there is any problem.
     */
    public BaseGenieClient(
        final Configuration configuration
        ) throws GenieException {

        // Only do this if security is enabled
        if (configuration.getBoolean(GENIE_SECURITY_ENABLED_KEY)) {
            try {
                this.tokenFetcher = new TokenFetcher(
                    configuration.getString(GENIE_SECURITY_OAUTH_URL_KEY),
                    configuration.getString(GENIE_SECURITY_OAUTH_CLIENT_ID_KEY),
                    configuration.getString(GENIE_SECURITY_OAUTH_CLIENT_SECRET_KEY),
                    configuration.getString(GENIE_SECURITY_OAUTH_GRANT_TYPE_KEY),
                    configuration.getString(GENIE_SECURITY_OAUTH_SCOPE_KEY)
                );
            } catch (Exception e) {
                throw new GenieServerException("Could not instantiate client", e);
            }
        }

        final OkHttpClient.Builder builder = new OkHttpClient.Builder();

        // If security is enabled tokenFetcher should never be null.
        if (this.tokenFetcher != null) {
            builder.interceptors().add(new SecurityHeaderInterceptor(tokenFetcher));
        }

        final OkHttpClient client = builder.build();

        final ObjectMapper mapper = new ObjectMapper().
            configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        final Retrofit retrofit = new Retrofit.Builder()
            .baseUrl(configuration.getString(SERVICE_BASE_URL_KEY))
            .addConverterFactory(JacksonConverterFactory.create(mapper))
            .client(client)
            .build();

        genieService = retrofit.create(GenieService.class);
    }

    protected String getIdFromLocation(final String location) {
        return location.substring(location.lastIndexOf("/") + 1);
    }
}
