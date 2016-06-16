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
package com.netflix.genie.client.interceptor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.genie.client.exceptions.GenieClientException;
import okhttp3.Interceptor;
import okhttp3.Response;

import java.io.IOException;

/**
 * Class that evaluates the retrofit response code and maps it to an gitappropriate Genie Exception.
 *
 * @author amsharma
 * @since 3.0.0
 */
public class ResponseMappingInterceptor implements Interceptor {

    private final ObjectMapper mapper;

    /**
     * Constructor.
     *
     */
    public ResponseMappingInterceptor() {
        mapper = new ObjectMapper();
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
            final String bodyContent = response.body().string();

            try {
                final JsonNode responseBody = mapper.readTree(bodyContent);
                throw new GenieClientException(response.code(), response.message()
                    + " : " + responseBody.get("message"));
            } catch (JsonProcessingException jpe) {
                throw new GenieClientException(response.code(), response.message() + bodyContent);
            }
        }
    }
}
