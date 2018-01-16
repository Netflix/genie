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
package com.netflix.genie.client.interceptors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.netflix.genie.client.exceptions.GenieClientException;
import com.netflix.genie.common.util.GenieObjectMapper;
import okhttp3.Interceptor;
import okhttp3.Response;
import okhttp3.ResponseBody;

import java.io.IOException;
import java.io.Reader;

/**
 * Class that evaluates the retrofit response code and maps it to an appropriate Genie Exception.
 *
 * @author amsharma
 * @since 3.0.0
 */
public class ResponseMappingInterceptor implements Interceptor {

    private static final String EMPTY_STRING = "";
    private static final String ERROR_MESSAGE_KEY = "message";

    /**
     * Constructor.
     */
    public ResponseMappingInterceptor() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Response intercept(final Chain chain) throws IOException {
        final Response response = chain.proceed(chain.request());

        if (response.isSuccessful()) {
            return response;
        } else {
            final ResponseBody body = response.body();
            if (body != null) {
                final Reader bodyReader = body.charStream();

                if (bodyReader != null) {
                    try {
                        final JsonNode responseBody = GenieObjectMapper.getMapper().readTree(bodyReader);
                        final String errorMessage =
                            responseBody == null || !responseBody.has(ERROR_MESSAGE_KEY)
                                ? EMPTY_STRING
                                : responseBody.get(ERROR_MESSAGE_KEY).asText();
                        throw new GenieClientException(
                            response.code(),
                            response.message() + " : " + errorMessage
                        );
                    } catch (final JsonProcessingException jpe) {
                        throw new GenieClientException(response.code(), response.message() + " " + jpe.getMessage());
                    }
                }
            }

            throw new GenieClientException(response.code(), "Unable to find response body to parse error message from");
        }
    }
}
