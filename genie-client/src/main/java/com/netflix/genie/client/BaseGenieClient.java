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
import com.netflix.genie.client.interceptor.ResponseMappingInterceptor;
import com.netflix.genie.client.interceptor.SecurityHeaderInterceptor;
import com.netflix.genie.client.security.TokenFetcher;
import com.netflix.genie.common.exceptions.GenieException;
import okhttp3.OkHttpClient;
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
    protected static final String STATUS = "status";

    protected Retrofit retrofit;
    protected final ObjectMapper mapper;

    private TokenFetcher tokenFetcher;

    /**
     * Constructor.
     *
     * @param configuration The configuration to use for instantiating the client.
     *
     * @throws GenieException If there is any problem.
     */
    public BaseGenieClient(
        final GenieClientConfiguration configuration
        ) throws GenieException {

        final OkHttpClient.Builder builder = new OkHttpClient.Builder();

        // If security is enabled we instantiate the Token Fetcher object to get credentials when needed
        // and add the SecurityHeaderInterceptor to the request builder.
        if (configuration.isSecurityEnabled()) {
                this.tokenFetcher = new TokenFetcher(
                    configuration.getSecurityOauthUrl(),
                    configuration.getSecurityClientId(),
                    configuration.getSecurityClientSecret(),
                    configuration.getSecurityGrantType(),
                    configuration.getSecurityScope()
                );

            builder.interceptors().add(new SecurityHeaderInterceptor(tokenFetcher));
        }

        // Add the interceptor to map the retrofit response code to corresponding Genie Exceptions in case of
        // 4xx and 5xx errors.
        builder.addInterceptor(new ResponseMappingInterceptor());

        final OkHttpClient client = builder.build();

        mapper = new ObjectMapper().
            configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        retrofit = new Retrofit.Builder()
            .baseUrl(configuration.getServiceUrl())
            .addConverterFactory(JacksonConverterFactory.create(mapper))
            .client(client)
            .build();
    }

    /**
     * Helper method to parse the id out of the location string in the Header.
     *
     * @param location The location string in the header.
     * @return The id of the entity embedded in the location.
     */
    protected String getIdFromLocation(final String location) {
        return location.substring(location.lastIndexOf("/") + 1);
    }
}
