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
package com.netflix.genie.web.configs.aws;

import com.netflix.genie.test.categories.UnitTest;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.UUID;

/**
 * Unit tests for the AwsMvcConfig class.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class AwsMvcConfigUnitTests {

    private AwsMvcConfig awsMvcConfig;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.awsMvcConfig = new AwsMvcConfig();
    }

    /**
     * Make sure we attempt public hostname first.
     *
     * @throws IOException on any problem
     */
    @Test
    public void canGetPublicHostname() throws IOException {
        final HttpClient client = Mockito.mock(HttpClient.class);
        final HttpResponse response = Mockito.mock(HttpResponse.class);
        Mockito.when(client.execute(this.awsMvcConfig.publicHostNameGet)).thenReturn(response);

        final StatusLine statusLine = Mockito.mock(StatusLine.class);
        Mockito.when(response.getStatusLine()).thenReturn(statusLine);
        Mockito.when(statusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);

        final HttpEntity entity = Mockito.mock(HttpEntity.class);
        Mockito.when(response.getEntity()).thenReturn(entity);
        final String hostname = UUID.randomUUID().toString();
        final ByteArrayInputStream bis = new ByteArrayInputStream(hostname.getBytes(Charset.forName("UTF-8")));
        Mockito.when(entity.getContent()).thenReturn(bis);

        Assert.assertThat(this.awsMvcConfig.hostName(client), Matchers.is(hostname));
        Mockito.verify(client, Mockito.times(1)).execute(awsMvcConfig.publicHostNameGet);
        Mockito.verify(client, Mockito.never()).execute(awsMvcConfig.localIPV4HostNameGet);
    }

    /**
     * Make sure we get IPv4 hostname if public hostname fails.
     *
     * @throws IOException on any problem
     */
    @Test
    public void canGetIPv4Hostname() throws IOException {
        final HttpClient client = Mockito.mock(HttpClient.class);
        final HttpResponse response = Mockito.mock(HttpResponse.class);
        Mockito.when(client.execute(this.awsMvcConfig.publicHostNameGet)).thenReturn(response);

        final StatusLine statusLine = Mockito.mock(StatusLine.class);
        Mockito.when(response.getStatusLine()).thenReturn(statusLine);
        Mockito.when(statusLine.getStatusCode()).thenReturn(HttpStatus.SC_NOT_FOUND);

        final HttpResponse ip4Response = Mockito.mock(HttpResponse.class);
        Mockito.when(client.execute(this.awsMvcConfig.localIPV4HostNameGet)).thenReturn(ip4Response);

        final StatusLine ip4StatusLine = Mockito.mock(StatusLine.class);
        Mockito.when(ip4Response.getStatusLine()).thenReturn(ip4StatusLine);
        Mockito.when(ip4StatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);

        final HttpEntity entity = Mockito.mock(HttpEntity.class);
        Mockito.when(ip4Response.getEntity()).thenReturn(entity);
        final String hostname = UUID.randomUUID().toString();
        final ByteArrayInputStream bis = new ByteArrayInputStream(hostname.getBytes(Charset.forName("UTF-8")));
        Mockito.when(entity.getContent()).thenReturn(bis);

        Assert.assertThat(this.awsMvcConfig.hostName(client), Matchers.is(hostname));
        Mockito.verify(client, Mockito.times(1)).execute(awsMvcConfig.publicHostNameGet);
        Mockito.verify(client, Mockito.times(1)).execute(awsMvcConfig.localIPV4HostNameGet);
    }

    /**
     * Make sure if both fails we throw an exception.
     *
     * @throws IOException on any problem
     */
    @Test(expected = IOException.class)
    public void cantGetHostname() throws IOException {
        final HttpClient client = Mockito.mock(HttpClient.class);
        final HttpResponse response = Mockito.mock(HttpResponse.class);
        Mockito.when(client.execute(this.awsMvcConfig.publicHostNameGet)).thenReturn(response);

        final StatusLine statusLine = Mockito.mock(StatusLine.class);
        Mockito.when(response.getStatusLine()).thenReturn(statusLine);
        Mockito.when(statusLine.getStatusCode()).thenReturn(HttpStatus.SC_NOT_FOUND);

        final HttpResponse ip4Response = Mockito.mock(HttpResponse.class);
        Mockito.when(client.execute(this.awsMvcConfig.localIPV4HostNameGet)).thenReturn(ip4Response);

        final StatusLine ip4StatusLine = Mockito.mock(StatusLine.class);
        Mockito.when(ip4Response.getStatusLine()).thenReturn(ip4StatusLine);
        Mockito.when(ip4StatusLine.getStatusCode()).thenReturn(HttpStatus.SC_BAD_REQUEST);

        this.awsMvcConfig.hostName(client);
    }
}
