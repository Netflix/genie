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
import com.netflix.genie.client.security.SecurityInterceptor;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import okhttp3.OkHttpClient;
import org.apache.commons.lang3.StringUtils;
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
    protected ObjectMapper mapper;

    /**
     * Constructor that takes the service url and a security interceptor implementation.
     *
     * @param url The url of the Genie Service.
     * @param securityInterceptor An implementation of the Security Interceptor.
     * @throws GenieException If there is any problem creating the constructor.
     */
    public BaseGenieClient(
        final String url,
        final SecurityInterceptor securityInterceptor
        ) throws GenieException {

        if (StringUtils.isBlank(url)) {
            throw new GeniePreconditionException("Service URL cannot be empty or null");
        }

        final OkHttpClient.Builder builder = new OkHttpClient.Builder();
        mapper = new ObjectMapper().
            configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        // Add the interceptor to map the retrofit response code to corresponding Genie Exceptions in case of
        // 4xx and 5xx errors.
        builder.addInterceptor(new ResponseMappingInterceptor());

        // Add the security interceptor if provided to add credentials to a request.
        if (securityInterceptor != null) {
            builder.addInterceptor(securityInterceptor);
        }
        final OkHttpClient client = builder.build();

        retrofit = new Retrofit.Builder()
            .baseUrl(url)
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
