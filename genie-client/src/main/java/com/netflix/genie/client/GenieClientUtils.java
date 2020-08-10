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
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.google.common.collect.Lists;
import com.netflix.genie.client.apis.SortAttribute;
import com.netflix.genie.client.apis.SortDirection;
import com.netflix.genie.client.configs.GenieNetworkConfiguration;
import com.netflix.genie.client.exceptions.GenieClientException;
import com.netflix.genie.client.interceptors.ResponseMappingInterceptor;
import com.netflix.genie.common.external.util.GenieObjectMapper;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import org.apache.commons.lang3.StringUtils;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import javax.annotation.Nullable;
import javax.validation.constraints.NotBlank;
import java.io.IOException;
import java.util.ArrayList;
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

    /**
     * Given a response from a Genie search API parse the results from the list.
     *
     * @param response        The response JSON from the server
     * @param searchResultKey The JSON key the search result list is expected to exist under
     * @param clazz           The expected response type to bind to
     * @param <T>             The type of POJO to bind to
     * @return A {@link List} of {@literal T} or empty if no results
     * @throws IOException          On error reading the body
     * @throws GenieClientException On unsuccessful query
     */
    static <T> List<T> parseSearchResultsResponse(
        final Response<JsonNode> response,
        final String searchResultKey,
        final Class<T> clazz
    ) throws IOException {
        if (!response.isSuccessful()) {
            throw new GenieClientException(
                "Search failed due to "
                    + (response.errorBody() == null ? response.message() : response.errorBody().toString())
            );
        }

        // Request returned some 2xx
        final JsonNode body = response.body();
        if (body == null || body.getNodeType() != JsonNodeType.OBJECT) {
            return Lists.newArrayList();
        }
        final JsonNode embedded = body.get("_embedded");
        if (embedded == null || embedded.getNodeType() != JsonNodeType.OBJECT) {
            // Kind of an invalid response? Could return error or just swallow?
            return Lists.newArrayList();
        }
        final JsonNode searchResultsJson = embedded.get(searchResultKey);
        if (searchResultsJson == null || searchResultsJson.getNodeType() != JsonNodeType.ARRAY) {
            return Lists.newArrayList();
        }

        final List<T> searchList = new ArrayList<>();
        for (final JsonNode searchResultJson : searchResultsJson) {
            final T searchResult = GenieClientUtils.treeToValue(searchResultJson, clazz);
            searchList.add(searchResult);
        }
        return searchList;
    }

    @Nullable
    public static String getSortParameter(
        @Nullable final SortAttribute sortAttribute,
        @Nullable final SortDirection sortDirection
    ) {
        if (sortAttribute != null || sortDirection != null) {
            return ""
                + StringUtils.lowerCase((sortAttribute != null ? sortAttribute : SortAttribute.DEFAULT).name())
                + ","
                + (sortDirection != null ? sortDirection : SortDirection.DEFAULT);
        } else {
            return null;
        }
    }
}
