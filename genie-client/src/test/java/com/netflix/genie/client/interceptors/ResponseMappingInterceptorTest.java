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

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.Options;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.netflix.genie.client.exceptions.GenieClientException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.http.HttpStatus;

import java.io.IOException;
import java.util.UUID;

/**
 * Unit tests for the ResponseMappingInterceptor class.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class ResponseMappingInterceptorTest {

    /**
     * Create a mock server.
     */
    @Rule
    @SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
    public WireMockRule wireMock = new WireMockRule(Options.DYNAMIC_PORT);

    private OkHttpClient client;
    private String uri;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        final int port = this.wireMock.port();
        this.uri = "http://localhost:" + port + "/api/v3/jobs";

        final OkHttpClient.Builder builder = new OkHttpClient.Builder();
        builder.addInterceptor(new ResponseMappingInterceptor());
        this.client = builder.build();
    }

    /**
     * Test to make sure success just forwards on the response.
     *
     * @throws IOException on error
     */
    @Test
    public void canInterceptSuccess() throws IOException {
        WireMock
            .stubFor(
                WireMock
                    .get(WireMock.urlMatching("/api/.*"))
                    .willReturn(
                        WireMock
                            .aResponse()
                            .withStatus(HttpStatus.OK.value())
                    )
            );
        final Request request = new Request.Builder().url(this.uri).get().build();
        final Response response = this.client.newCall(request).execute();
        Assert.assertThat(response.code(), Matchers.is(HttpStatus.OK.value()));
    }

    /**
     * Test to make sure success just forwards on the response.
     *
     * @throws IOException on error
     */
    @Test
    public void canInterceptFailure() throws IOException {
        WireMock
            .stubFor(
                WireMock
                    .get(WireMock.urlMatching("/api/.*"))
                    .willReturn(
                        WireMock
                            .aResponse()
                            .withStatus(HttpStatus.INTERNAL_SERVER_ERROR.value())
                            .withBody((String) null)
                    )
            );

        try {
            final Request request = new Request.Builder().url(this.uri).get().build();
            this.client.newCall(request).execute();
            Assert.fail();
        } catch (final GenieClientException gce) {
            Assert.assertThat(gce.getErrorCode(), Matchers.is(HttpStatus.INTERNAL_SERVER_ERROR.value()));
        }

        final String message = UUID.randomUUID().toString();
        final String bodyString = "{\"message\":\"" + message + "\"}";
        final String errorMessage = UUID.randomUUID().toString();
        WireMock
            .stubFor(
                WireMock
                    .get(WireMock.urlMatching("/api/.*"))
                    .willReturn(
                        WireMock
                            .aResponse()
                            .withStatusMessage(errorMessage)
                            .withStatus(HttpStatus.SERVICE_UNAVAILABLE.value())
                            .withBody(bodyString)
                    )
            );

        try {
            final Request request = new Request.Builder().url(this.uri).get().build();
            this.client.newCall(request).execute();
            Assert.fail();
        } catch (final GenieClientException gce) {
            Assert.assertThat(gce.getErrorCode(), Matchers.is(HttpStatus.SERVICE_UNAVAILABLE.value()));
            Assert.assertThat(
                gce.getMessage(),
                Matchers.is(HttpStatus.SERVICE_UNAVAILABLE.value() + ": " + errorMessage + " : " + message)
            );
        }
    }
}
