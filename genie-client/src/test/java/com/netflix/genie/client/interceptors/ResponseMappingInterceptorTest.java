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

import com.netflix.genie.client.exceptions.GenieClientException;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.UUID;

/**
 * Unit tests for the {@link ResponseMappingInterceptor} class.
 *
 * @author tgianos
 * @since 3.0.0
 */
class ResponseMappingInterceptorTest {

    private MockWebServer server;
    private OkHttpClient client;
    private HttpUrl baseUrl;

    @BeforeEach
    void setup() throws IOException {
        this.server = new MockWebServer();
        this.server.start();
        this.baseUrl = this.server.url("/api/v3/jobs");

        final OkHttpClient.Builder builder = new OkHttpClient.Builder();
        builder.addInterceptor(new ResponseMappingInterceptor());
        this.client = builder.build();
    }

    @AfterEach
    void tearDown() throws IOException {
        this.server.shutdown();
    }

    @Test
    void canInterceptSuccess() throws IOException {
        this.server.enqueue(new MockResponse().setResponseCode(HttpURLConnection.HTTP_OK));
        final Request request = new Request.Builder().url(this.baseUrl).get().build();
        final Response response = this.client.newCall(request).execute();
        Assertions.assertThat(response.code()).isEqualTo(HttpURLConnection.HTTP_OK);
    }

    @Test
    void canInterceptFailure() {
        this.server.enqueue(new MockResponse().setResponseCode(HttpURLConnection.HTTP_INTERNAL_ERROR));
        final Request request = new Request.Builder().url(this.baseUrl).get().build();
        Assertions
            .assertThatExceptionOfType(GenieClientException.class)
            .isThrownBy(() -> this.client.newCall(request).execute())
            .satisfies(e -> Assertions.assertThat(e.getErrorCode()).isEqualTo(HttpURLConnection.HTTP_INTERNAL_ERROR));

        final String message = UUID.randomUUID().toString();
        final String bodyString = "{\"message\":\"" + message + "\"}";
        this.server.enqueue(
            new MockResponse()
                .setResponseCode(HttpURLConnection.HTTP_UNAVAILABLE)
                .setBody(bodyString)
        );
        Assertions
            .assertThatExceptionOfType(GenieClientException.class)
            .isThrownBy(() -> this.client.newCall(request).execute())
            .satisfies(e -> Assertions.assertThat(e.getErrorCode()).isEqualTo(HttpURLConnection.HTTP_UNAVAILABLE))
            .withMessage(HttpURLConnection.HTTP_UNAVAILABLE + ": " + message);

        this.server.enqueue(
            new MockResponse()
                .setResponseCode(HttpURLConnection.HTTP_PRECON_FAILED)
                .setBody("this is not valid JSON")
        );
        Assertions
            .assertThatExceptionOfType(GenieClientException.class)
            .isThrownBy(() -> this.client.newCall(request).execute())
            .satisfies(e -> Assertions.assertThat(e.getErrorCode()).isEqualTo(-1))
            .withMessage("Failed to parse server response as JSON");

    }
}
