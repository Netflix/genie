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
import com.netflix.genie.test.categories.UnitTest;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.UUID;

/**
 * Unit tests for the ResponseMappingInterceptor class.
 *
 * @author tgianos
 * @since 3.0.0
 */
// OkHTTP has final classes for Request and Response making it darn near impossible to mock...thanks
@Ignore
@Category(UnitTest.class)
public class ResponseMappingInterceptorUnitTests {

    private ResponseMappingInterceptor interceptor;
    private Interceptor.Chain chain;
    private Request request;

    /**
     * Setup for the tests.
     *
     * @throws IOException on error
     */
    @Before
    public void setup() throws IOException {
        this.request = new Request.Builder().url("http://localhost:8080/api/v3/jobs").build();
        this.interceptor = new ResponseMappingInterceptor();
        this.chain = Mockito.mock(Interceptor.Chain.class);
    }

    /**
     * Test to make sure success just forwards on the response.
     *
     * @throws IOException on error
     */
    @Test
    public void canInterceptSuccess() throws IOException {
        final Response response = new Response.Builder().code(200).build();
        Mockito.when(this.chain.proceed(this.request)).thenReturn(response);
        Assert.assertThat(this.interceptor.intercept(this.chain), Matchers.is(response));
    }

    /**
     * Test to make sure success just forwards on the response.
     *
     * @throws IOException on error
     */
    @Test
    public void canInterceptFailure() throws IOException {
        final String message = UUID.randomUUID().toString();
        final String bodyString = "{\"message\":\"" + message + "\"}";
        final ResponseBody body = Mockito.mock(ResponseBody.class);
        Mockito.when(body.string()).thenReturn(bodyString);
        final int errorCode = 503;
        final String errorMessage = UUID.randomUUID().toString();
        final Response response = new Response.Builder().code(errorCode).message(errorMessage).body(body).build();

        Mockito.when(this.chain.proceed(this.request)).thenReturn(response);
        try {
            this.interceptor.intercept(this.chain);
            Assert.fail();
        } catch (final GenieClientException gce) {
            Assert.assertThat(gce.getErrorCode(), Matchers.is(errorCode));
            Assert.assertThat(gce.getMessage(), Matchers.is(errorMessage + " : " + message));
        }
    }
}
