/*
 *
 *  Copyright 2019 Netflix, Inc.
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
import javax.validation.constraints.NotBlank;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Utility methods for the Genie client.
 *
 * @author tgianos
 * @since 4.0.0
 */
final class GenieClientUtils {

    static final String LOCATION_HEADER = "location";
    private static final String SLASH = "/";

    /**
     * Utility class doesn't need a public constructor.
     */
    private GenieClientUtils() {
    }

    /**
     * Get a {@link Retrofit} instance given the parameters.
     *
     * @param url                       The url of the Genie Service.
     * @param interceptors              All desired interceptors for the client to be created
     * @param genieNetworkConfiguration A configuration object that provides network settings for HTTP calls.
     * @return A {@link Retrofit} instance configured with the given information
     * @throws GenieClientException If there is any problem creating the constructor
     */
    static Retrofit createRetrofitInstance(
        @NotBlank final String url,
        @Nullable final List<Interceptor> interceptors,
        @Nullable final GenieNetworkConfiguration genieNetworkConfiguration
    ) throws GenieClientException {
        if (StringUtils.isBlank(url)) {
            throw new GenieClientException("Service URL cannot be empty or null");
        }

        final OkHttpClient.Builder builder = new OkHttpClient.Builder();

        if (genieNetworkConfiguration != null) {
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

        // Add the interceptor to map the retrofit response code to corresponding Genie Exceptions in case of
        // 4xx and 5xx errors.
        builder.addInterceptor(new ResponseMappingInterceptor());
        if (interceptors != null) {
            interceptors.forEach(builder::addInterceptor);
        }

        return new Retrofit
            .Builder()
            .baseUrl(url)
            .addConverterFactory(JacksonConverterFactory.create(GenieObjectMapper.getMapper()))
            .client(builder.build())
            .build();
    }

    /**
     * Bind JSON to a Java POJO.
     *
     * @param node  The JSON to convert
     * @param clazz The clazz to bind to
     * @param <T>   The type of the POJO returned
     * @return An instance of {@code T}
     * @throws IOException If the JSON can't bind
     */
    static <T> T treeToValue(final JsonNode node, final Class<T> clazz) throws IOException {
        return GenieObjectMapper.getMapper().treeToValue(node, clazz);
    }

    /**
     * Helper method to parse the id out of the location string in the Header.
     *
     * @param location The location string in the header.
     * @return The id of the entity embedded in the location.
     */
    static String getIdFromLocation(final String location) {
        return location.substring(location.lastIndexOf(SLASH) + 1);
    }
}
