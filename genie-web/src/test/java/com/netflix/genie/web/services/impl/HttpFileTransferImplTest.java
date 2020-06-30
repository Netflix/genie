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
import com.netflix.genie.web.util.MetricsUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.test.web.client.match.MockRestRequestMatchers;
import org.springframework.test.web.client.response.MockRestResponseCreators;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Tests for the HttpFileTransferImpl class.
 *
 * @author tgianos
 * @since 3.0.0
 */
class HttpFileTransferImplTest {

    private static final String TEST_URL = "http://localhost/myFile.txt";

    /**
     * Used for reading and writing files during tests. Cleaned up at end.
     */
    @SuppressWarnings("checkstyle:VisibilityModifier")
    @TempDir
    Path temporaryFolder;

    private MockRestServiceServer server;
    private HttpFileTransferImpl httpFileTransfer;

    private MeterRegistry registry;
    private Timer downloadTimer;
    private Timer uploadTimer;
    private Timer metadataTimer;

    @BeforeEach
    void setup() {
        final RestTemplate restTemplate = new RestTemplate();
        this.server = MockRestServiceServer.createServer(restTemplate);
        this.downloadTimer = Mockito.mock(Timer.class);
        this.uploadTimer = Mockito.mock(Timer.class);
        this.metadataTimer = Mockito.mock(Timer.class);
        this.registry = Mockito.mock(MeterRegistry.class);
        Mockito
            .when(registry.timer(Mockito.eq(HttpFileTransferImpl.DOWNLOAD_TIMER_NAME), Mockito.anySet()))
            .thenReturn(this.downloadTimer);
        Mockito
            .when(registry.timer(Mockito.eq(HttpFileTransferImpl.UPLOAD_TIMER_NAME), Mockito.anySet()))
            .thenReturn(this.uploadTimer);
        Mockito
            .when(registry.timer(Mockito.eq(HttpFileTransferImpl.GET_LAST_MODIFIED_TIMER_NAME), Mockito.anySet()))
            .thenReturn(this.metadataTimer);
        this.httpFileTransfer = new HttpFileTransferImpl(restTemplate, this.registry);
    }

    @Test
    void canValidate() throws GenieException {
        Assertions.assertThat(this.httpFileTransfer.isValid("http://netflix.github.io/genie")).isTrue();
        Assertions.assertThat(this.httpFileTransfer.isValid("https://netflix.github.io/genie")).isTrue();
        Assertions.assertThat(this.httpFileTransfer.isValid("ftp://netflix.github.io/genie")).isFalse();
        Assertions.assertThat(this.httpFileTransfer.isValid("file:///tmp/blah")).isFalse();
        Assertions.assertThat(this.httpFileTransfer.isValid("http://localhost/someFile.txt")).isTrue();
        Assertions.assertThat(this.httpFileTransfer.isValid("https://localhost:8080/someFile.txt")).isTrue();
    }

    @Test
    void canGet() throws GenieException, IOException {
        final Path output = this.temporaryFolder.resolve(UUID.randomUUID().toString());
        final String contents = UUID.randomUUID().toString();

        this.server
            .expect(MockRestRequestMatchers.requestTo(TEST_URL))
            .andExpect(MockRestRequestMatchers.method(HttpMethod.GET))
            .andRespond(
                MockRestResponseCreators
                    .withSuccess(contents.getBytes(StandardCharsets.UTF_8), MediaType.APPLICATION_OCTET_STREAM)
            );

        this.httpFileTransfer.getFile(TEST_URL, output.toFile().getCanonicalPath());

        this.server.verify();
        Mockito
            .verify(this.registry, Mockito.times(1))
            .timer(HttpFileTransferImpl.DOWNLOAD_TIMER_NAME, MetricsUtils.newSuccessTagsSet());
        Mockito
            .verify(this.downloadTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
    }

    @Test
    void cantGetWithInvalidUrl() {
        Assertions
            .assertThatExceptionOfType(GenieServerException.class)
            .isThrownBy(
                () -> this.httpFileTransfer.getFile(
                    UUID.randomUUID().toString(),
                    this.temporaryFolder.toFile().getCanonicalPath()
                )
            );
        Mockito
            .verify(this.registry, Mockito.times(1))
            .timer(
                HttpFileTransferImpl.DOWNLOAD_TIMER_NAME,
                MetricsUtils.newFailureTagsSetForException(new GenieServerException("test"))
            );
        Mockito
            .verify(this.downloadTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
    }

    @Test
    void cantGetWithDirectoryAsOutput() {
        this.server
            .expect(MockRestRequestMatchers.requestTo(TEST_URL))
            .andExpect(MockRestRequestMatchers.method(HttpMethod.GET))
            .andRespond(
                MockRestResponseCreators
                    .withSuccess("junk".getBytes(StandardCharsets.UTF_8), MediaType.APPLICATION_OCTET_STREAM)
            );
        Assertions
            .assertThatExceptionOfType(ResourceAccessException.class)
            .isThrownBy(
                () -> this.httpFileTransfer.getFile(TEST_URL, this.temporaryFolder.toFile().getCanonicalPath())
            );
        Mockito
            .verify(this.registry, Mockito.times(1))
            .timer(
                HttpFileTransferImpl.DOWNLOAD_TIMER_NAME,
                MetricsUtils.newFailureTagsSetForException(new ResourceAccessException("test"))
            );
        Mockito
            .verify(this.downloadTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
    }

    @Test
    void cantPutFile() {
        final String file = UUID.randomUUID().toString();
        Assertions
            .assertThatExceptionOfType(UnsupportedOperationException.class)
            .isThrownBy(() -> this.httpFileTransfer.putFile(file, file));
        Mockito
            .verify(this.registry, Mockito.times(1))
            .timer(
                HttpFileTransferImpl.UPLOAD_TIMER_NAME,
                MetricsUtils.newFailureTagsSetForException(new UnsupportedOperationException("test"))
            );
        Mockito
            .verify(this.uploadTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
    }

    @Test
    void canGetLastModifiedTime() throws GenieException {
        final long lastModified = 28424323000L;
        final HttpHeaders headers = new HttpHeaders();
        headers.setLastModified(lastModified);
        this.server
            .expect(MockRestRequestMatchers.requestTo(TEST_URL))
            .andExpect(MockRestRequestMatchers.method(HttpMethod.HEAD))
            .andRespond(MockRestResponseCreators.withSuccess().headers(headers));

        Assertions.assertThat(this.httpFileTransfer.getLastModifiedTime(TEST_URL)).isEqualTo(lastModified);
        this.server.verify();
        Mockito
            .verify(this.registry, Mockito.times(1))
            .timer(HttpFileTransferImpl.GET_LAST_MODIFIED_TIMER_NAME, MetricsUtils.newSuccessTagsSet());
        Mockito
            .verify(this.metadataTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
    }

    @Test
    void canGetLastModifiedTimeIfNoHeader() throws GenieException {
        final long time = Instant.now().toEpochMilli() - 1;
        this.server
            .expect(MockRestRequestMatchers.requestTo(TEST_URL))
            .andExpect(MockRestRequestMatchers.method(HttpMethod.HEAD))
            .andRespond(MockRestResponseCreators.withSuccess());
        Assertions.assertThat(this.httpFileTransfer.getLastModifiedTime(TEST_URL)).isGreaterThan(time);
        Mockito
            .verify(this.registry, Mockito.times(1))
            .timer(HttpFileTransferImpl.GET_LAST_MODIFIED_TIMER_NAME, MetricsUtils.newSuccessTagsSet());
        Mockito
            .verify(this.metadataTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
    }

    @Test
    void cantGetLastModifiedTimeIfNotURL() {
        Assertions
            .assertThatExceptionOfType(GenieServerException.class)
            .isThrownBy(() -> this.httpFileTransfer.getLastModifiedTime(UUID.randomUUID().toString()))
            .withCauseInstanceOf(MalformedURLException.class);
        Mockito
            .verify(this.registry, Mockito.times(1))
            .timer(
                HttpFileTransferImpl.GET_LAST_MODIFIED_TIMER_NAME,
                MetricsUtils.newFailureTagsSetForException(new MalformedURLException("test"))
            );
        Mockito
            .verify(this.metadataTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
    }
}
