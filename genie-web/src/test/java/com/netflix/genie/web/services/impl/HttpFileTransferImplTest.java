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
package com.netflix.genie.web.services.impl;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.genie.web.util.MetricsUtils;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.test.web.client.match.MockRestRequestMatchers;
import org.springframework.test.web.client.response.MockRestResponseCreators;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Tests for the HttpFileTransferImpl class.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class HttpFileTransferImplTest {

    private static final String TEST_URL = "http://localhost/myFile.txt";

    /**
     * Used for reading and writing files during tests. Cleaned up at end.
     */
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private MockRestServiceServer server;
    private HttpFileTransferImpl httpFileTransfer;

    private Id downloadTimerId;
    private Id uploadTimerId;
    private Id metadataTimerId;
    private Timer downloadTimer;
    private Timer uploadTimer;
    private Timer metadataTimer;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        final RestTemplate restTemplate = new RestTemplate();
        this.server = MockRestServiceServer.createServer(restTemplate);
        this.downloadTimer = Mockito.mock(Timer.class);
        this.uploadTimer = Mockito.mock(Timer.class);
        this.metadataTimer = Mockito.mock(Timer.class);
        this.downloadTimerId = Mockito.mock(Id.class);
        this.uploadTimerId = Mockito.mock(Id.class);
        this.metadataTimerId = Mockito.mock(Id.class);
        final Registry registry = Mockito.mock(Registry.class);
        Mockito.when(registry.createId("genie.files.http.download.timer")).thenReturn(this.downloadTimerId);
        Mockito.when(registry.createId("genie.files.http.upload.timer")).thenReturn(this.uploadTimerId);
        Mockito.when(registry.createId("genie.files.http.getLastModified.timer")).thenReturn(this.metadataTimerId);
        Mockito
            .when(this.downloadTimerId.withTags(Mockito.anyMapOf(String.class, String.class)))
            .thenReturn(this.downloadTimerId);
        Mockito
            .when(this.uploadTimerId.withTags(Mockito.anyMapOf(String.class, String.class)))
            .thenReturn(this.uploadTimerId);
        Mockito
            .when(this.metadataTimerId.withTags(Mockito.anyMapOf(String.class, String.class)))
            .thenReturn(this.metadataTimerId);
        Mockito.when(registry.timer(this.downloadTimerId)).thenReturn(this.downloadTimer);
        Mockito.when(registry.timer(this.uploadTimerId)).thenReturn(this.uploadTimer);
        Mockito.when(registry.timer(this.metadataTimerId)).thenReturn(this.metadataTimer);
        this.httpFileTransfer = new HttpFileTransferImpl(restTemplate, registry);
    }

    /**
     * Make sure valid url's return true.
     *
     * @throws GenieException On error
     */
    @Test
    public void canValidate() throws GenieException {
        Assert.assertTrue(this.httpFileTransfer.isValid("http://netflix.github.io/genie"));
        Assert.assertTrue(this.httpFileTransfer.isValid("https://netflix.github.io/genie"));
        Assert.assertFalse(this.httpFileTransfer.isValid("ftp://netflix.github.io/genie"));
        Assert.assertFalse(this.httpFileTransfer.isValid("file:///tmp/blah"));
        Assert.assertTrue(this.httpFileTransfer.isValid("http://localhost/someFile.txt"));
        Assert.assertTrue(this.httpFileTransfer.isValid("https://localhost:8080/someFile.txt"));
    }

    /**
     * Make sure we can actually get a file.
     *
     * @throws GenieException On error
     * @throws IOException    On error
     */
    @Test
    public void canGet() throws GenieException, IOException {
        final File output = this.temporaryFolder.newFile();
        final String contents = UUID.randomUUID().toString();

        this.server
            .expect(MockRestRequestMatchers.requestTo(TEST_URL))
            .andExpect(MockRestRequestMatchers.method(HttpMethod.GET))
            .andRespond(
                MockRestResponseCreators
                    .withSuccess(contents.getBytes(Charset.forName("UTF-8")), MediaType.APPLICATION_OCTET_STREAM)
            );

        this.httpFileTransfer.getFile(TEST_URL, output.getCanonicalPath());

        this.server.verify();
        Mockito
            .verify(this.downloadTimerId, Mockito.times(1))
            .withTags(MetricsUtils.newSuccessTagsMap());
        Mockito
            .verify(this.downloadTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
    }

    /**
     * Make sure can't get a file if the intput isn't a valid url.
     *
     * @throws GenieException On Error
     * @throws IOException    On Error
     */
    @Test(expected = GenieServerException.class)
    public void cantGetWithInvalidUrl() throws GenieException, IOException {
        try {
            this.httpFileTransfer.getFile(
                UUID.randomUUID().toString(),
                this.temporaryFolder.getRoot().getCanonicalPath()
            );
        } finally {
            Mockito
                .verify(this.downloadTimerId, Mockito.times(1))
                .withTags(MetricsUtils.newFailureTagsMapForException(new GenieServerException("test")));
            Mockito
                .verify(this.downloadTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        }
    }

    /**
     * Make sure can't get a file if the output location is a directory.
     *
     * @throws GenieException On Error
     * @throws IOException    On Error
     */
    @Test(expected = ResourceAccessException.class)
    public void cantGetWithDirectoryAsOutput() throws GenieException, IOException {
        this.server
            .expect(MockRestRequestMatchers.requestTo(TEST_URL))
            .andExpect(MockRestRequestMatchers.method(HttpMethod.GET))
            .andRespond(
                MockRestResponseCreators
                    .withSuccess("junk".getBytes(Charset.forName("UTF-8")), MediaType.APPLICATION_OCTET_STREAM)
            );
        try {
            this.httpFileTransfer.getFile(TEST_URL, this.temporaryFolder.getRoot().getCanonicalPath());
        } finally {
            Mockito
                .verify(this.downloadTimerId, Mockito.times(1))
                .withTags(MetricsUtils.newFailureTagsMapForException(new ResourceAccessException("test")));
            Mockito
                .verify(this.downloadTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        }
    }

    /**
     * Make sure that there is no implementation of the putFile method.
     *
     * @throws GenieException on error
     */
    @Test(expected = UnsupportedOperationException.class)
    public void cantPutFile() throws GenieException {
        try {
            final String file = UUID.randomUUID().toString();
            this.httpFileTransfer.putFile(file, file);
        } finally {
            Mockito
                .verify(this.uploadTimerId, Mockito.times(1))
                .withTags(MetricsUtils.newFailureTagsMapForException(new UnsupportedOperationException("test")));
            Mockito
                .verify(this.uploadTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        }
    }

    /**
     * Make sure can get the last update time of a file.
     *
     * @throws GenieException On error
     */
    @Test
    public void canGetLastModifiedTime() throws GenieException {
        final long lastModified = 28424323000L;
        final HttpHeaders headers = new HttpHeaders();
        headers.setLastModified(lastModified);
        this.server
            .expect(MockRestRequestMatchers.requestTo(TEST_URL))
            .andExpect(MockRestRequestMatchers.method(HttpMethod.HEAD))
            .andRespond(MockRestResponseCreators.withSuccess().headers(headers));

        Assert.assertThat(this.httpFileTransfer.getLastModifiedTime(TEST_URL), Matchers.is(lastModified));
        this.server.verify();
        Mockito
            .verify(this.metadataTimerId, Mockito.times(1))
            .withTags(MetricsUtils.newSuccessTagsMap());
        Mockito
            .verify(this.metadataTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
    }

    /**
     * Make sure can get the last update time of a file.
     *
     * @throws GenieException On error
     */
    @Test
    public void canGetLastModifiedTimeIfNoHeader() throws GenieException {
        final long time = Instant.now().toEpochMilli() - 1;
        this.server
            .expect(MockRestRequestMatchers.requestTo(TEST_URL))
            .andExpect(MockRestRequestMatchers.method(HttpMethod.HEAD))
            .andRespond(MockRestResponseCreators.withSuccess());
        Assert.assertTrue(this.httpFileTransfer.getLastModifiedTime(TEST_URL) > time);
        Mockito
            .verify(this.metadataTimerId, Mockito.times(1))
            .withTags(MetricsUtils.newSuccessTagsMap());
        Mockito
            .verify(this.metadataTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
    }

    /**
     * Make sure can get the last update time of a file.
     *
     * @throws GenieException On error
     */
    @Test(expected = GenieServerException.class)
    public void cantGetLastModifiedTimeIfNotURL() throws GenieException {
        try {
            this.httpFileTransfer.getLastModifiedTime(UUID.randomUUID().toString());
        } catch (final GenieServerException e) {
            Assert.assertTrue(e.getCause() instanceof MalformedURLException);
            throw e;
        } finally {
            Mockito
                .verify(this.metadataTimerId, Mockito.times(1))
                .withTags(MetricsUtils.newFailureTagsMapForException(new MalformedURLException("test")));
            Mockito
                .verify(this.metadataTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        }
    }
}
