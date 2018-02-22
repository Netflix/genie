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

import com.fasterxml.jackson.databind.JsonNode;
import com.netflix.genie.client.configs.GenieNetworkConfiguration;
import com.netflix.genie.client.exceptions.GenieClientException;
import com.netflix.genie.client.interceptors.ResponseMappingInterceptor;
import com.netflix.genie.common.util.GenieObjectMapper;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import org.apache.commons.lang3.StringUtils;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import javax.annotation.Nullable;
import javax.validation.constraints.NotEmpty;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Base class for the clients for Genie Services.
 *
 * @author amsharma
 * @since 3.0.0
 */
abstract class BaseGenieClient {

    private Retrofit retrofit;

    /**
     * Constructor that takes the service url and a security interceptor implementation.
     *
     * @param url                       The url of the Genie Service.
     * @param interceptors              All desired interceptors for the client to be created
     * @param genieNetworkConfiguration A configuration object that provides network settings for HTTP calls.
     * @throws GenieClientException If there is any problem creating the constructor.
     */
    BaseGenieClient(
        @NotEmpty final String url,
        @Nullable final List<Interceptor> interceptors,
        @Nullable final GenieNetworkConfiguration genieNetworkConfiguration
    ) throws GenieClientException {

        if (StringUtils.isBlank(url)) {
            throw new GenieClientException("Service URL cannot be empty or null");
        }

        final OkHttpClient.Builder builder = new OkHttpClient.Builder();

        if (genieNetworkConfiguration != null) {
            this.addConfigParamsFromConfig(builder, genieNetworkConfiguration);
        }

        // Add the interceptor to map the retrofit response code to corresponding Genie Exceptions in case of
        // 4xx and 5xx errors.
        builder.addInterceptor(new ResponseMappingInterceptor());
        if (interceptors != null) {
            interceptors.forEach(builder::addInterceptor);
        }
        final OkHttpClient client = builder.build();

        this.retrofit = new Retrofit.Builder()
            .baseUrl(url)
            .addConverterFactory(JacksonConverterFactory.create(GenieObjectMapper.getMapper()))
            .client(client)
            .build();
    }

    // Private helper method to add network configurations to okhttp builder
    private void addConfigParamsFromConfig(
        final OkHttpClient.Builder builder,
        final GenieNetworkConfiguration genieNetworkConfiguration
    ) {
        if (genieNetworkConfiguration.getConnectTimeout() != GenieNetworkConfiguration.DEFAULT_TIMEOUT) {
            builder.connectTimeout(genieNetworkConfiguration.getConnectTimeout(), TimeUnit.MILLISECONDS);
        }

        if (genieNetworkConfiguration.getReadTimeout() != GenieNetworkConfiguration.DEFAULT_TIMEOUT) {
            builder.readTimeout(genieNetworkConfiguration.getReadTimeout(), TimeUnit.MILLISECONDS);
        }

        if (genieNetworkConfiguration.getWriteTimeout() != GenieNetworkConfiguration.DEFAULT_TIMEOUT) {
            builder.writeTimeout(genieNetworkConfiguration.getWriteTimeout(), TimeUnit.MILLISECONDS);
        }

        builder.retryOnConnectionFailure(genieNetworkConfiguration.isRetryOnConnectionFailure());
    }

    /**
     * Helper method to parse the id out of the location string in the Header.
     *
     * @param location The location string in the header.
     * @return The id of the entity embedded in the location.
     */
    String getIdFromLocation(final String location) {
        return location.substring(location.lastIndexOf("/") + 1);
    }

    <T> T getService(final Class<T> clazz) {
        return this.retrofit.create(clazz);
    }

    <T> T treeToValue(final JsonNode node, final Class<T> clazz) throws IOException {
        return GenieObjectMapper.getMapper().treeToValue(node, clazz);
    }
}
