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

import com.google.common.collect.Lists;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.core.services.FileTransfer;
import com.netflix.genie.core.util.MetricsUtils;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.validator.routines.UrlValidator;
import org.hibernate.validator.constraints.NotBlank;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.ResponseExtractor;
import org.springframework.web.client.RestTemplate;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * An implementation of the FileTransferService interface in which the remote locations are available via http[s].
 *
 * @author tgianos
 * @since 3.0.0
 */
@Slf4j
public class HttpFileTransferImpl implements FileTransfer {

    private final UrlValidator validator
        = new UrlValidator(new String[]{"http", "https"}, UrlValidator.ALLOW_LOCAL_URLS);
    private final RestTemplate restTemplate;
    private final Registry registry;
    private final Id downloadTimerId;
    private final Id uploadTimerId;
    private final Id getLastModifiedTimerId;

    /**
     * Constructor.
     *
     * @param restTemplate The rest template to use
     * @param registry     The metrics registry to use
     */
    public HttpFileTransferImpl(@NotNull final RestTemplate restTemplate, @NotNull final Registry registry) {
        this.restTemplate = restTemplate;
        this.registry = registry;
        this.downloadTimerId = registry.createId("genie.files.http.download.timer");
        this.uploadTimerId = registry.createId("genie.files.http.upload.timer");
        this.getLastModifiedTimerId = registry.createId("genie.files.http.getLastModified.timer");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValid(final String fileName) throws GenieException {
        log.debug("Called with file name {}", fileName);
        return this.validator.isValid(fileName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getFile(
        @NotBlank(message = "Source file path cannot be empty.")
        final String srcRemotePath,
        @NotBlank(message = "Destination local path cannot be empty")
        final String dstLocalPath
    ) throws GenieException {
        final long start = System.nanoTime();
        final Map<String, String> tags = MetricsUtils.newSuccessTagsMap();
        log.debug("Called with src path {} and destination path {}", srcRemotePath, dstLocalPath);

        try {
            final File outputFile = new File(dstLocalPath);
            if (!this.isValid(srcRemotePath)) {
                throw new GenieServerException("Unable to download " + srcRemotePath + " not a valid URL");
            }
            this.restTemplate.execute(
                srcRemotePath,
                HttpMethod.GET,
                requestEntity ->
                    requestEntity.getHeaders().setAccept(Lists.newArrayList(MediaType.ALL)),
                new ResponseExtractor<Void>() {
                    @Override
                    public Void extractData(final ClientHttpResponse response) throws IOException {
                        // Documentation I could find pointed to the HttpEntity reading the bytes off
                        // the stream so this should resolve memory problems if the file returned is large
                        FileUtils.copyInputStreamToFile(response.getBody(), outputFile);
                        return null;
                    }
                }
            );
        } catch (GenieException | RuntimeException e) {
            MetricsUtils.addFailureTagsWithException(tags, e);
            throw e;
        } finally {
            this.registry.timer(
                downloadTimerId.withTags(tags)
            ).record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void putFile(
        @NotBlank(message = "Source local path cannot be empty.")
        final String srcLocalPath,
        @NotBlank(message = "Destination remote path cannot be empty")
        final String dstRemotePath
    ) throws GenieException {
        final long start = System.nanoTime();
        final Map<String, String> tags = MetricsUtils.newSuccessTagsMap();
        try {
            throw new UnsupportedOperationException(
                "Saving a file to an HttpEndpoint isn't implemented in this version"
            );
        } catch (Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw t;
        } finally {
            this.registry.timer(
                uploadTimerId.withTags(tags)
            ).record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getLastModifiedTime(final String path) throws GenieException {
        final long start = System.nanoTime();
        final Map<String, String> tags = MetricsUtils.newSuccessTagsMap();
        final long lastModtime;
        try {
            final URL url = new URL(path);
            final long time = this.restTemplate.headForHeaders(url.toURI()).getLastModified();
            // Returns now if there was no last modified header as best we can do is assume file is brand new
            lastModtime = time != -1 ? time : Instant.now().toEpochMilli();
        } catch (final MalformedURLException | URISyntaxException e) {
            log.error(e.getLocalizedMessage(), e);
            MetricsUtils.addFailureTagsWithException(tags, e);
            throw new GenieServerException(e);
        } catch (Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw t;
        } finally {
            this.registry.timer(
                getLastModifiedTimerId.withTags(tags)
            ).record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
        return lastModtime;
    }
}
