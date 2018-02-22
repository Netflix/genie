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
import com.google.common.collect.Sets;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.web.services.FileTransfer;
import com.netflix.genie.web.util.MetricsUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.validator.routines.UrlValidator;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.web.client.ResponseExtractor;
import org.springframework.web.client.RestTemplate;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * An implementation of the FileTransferService interface in which the remote locations are available via http[s].
 *
 * @author tgianos
 * @since 3.0.0
 */
@Slf4j
public class HttpFileTransferImpl implements FileTransfer {

    static final String DOWNLOAD_TIMER_NAME = "genie.files.http.download.timer";
    static final String UPLOAD_TIMER_NAME = "genie.files.http.upload.timer";
    static final String GET_LAST_MODIFIED_TIMER_NAME = "genie.files.http.getLastModified.timer";

    private final UrlValidator validator
        = new UrlValidator(new String[]{"http", "https"}, UrlValidator.ALLOW_LOCAL_URLS);
    private final RestTemplate restTemplate;
    private final MeterRegistry registry;

    /**
     * Constructor.
     *
     * @param restTemplate The rest template to use
     * @param registry     The metrics registry to use
     */
    public HttpFileTransferImpl(@NotNull final RestTemplate restTemplate, @NotNull final MeterRegistry registry) {
        this.restTemplate = restTemplate;
        this.registry = registry;
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
        @NotBlank(message = "Source file path cannot be empty.") final String srcRemotePath,
        @NotBlank(message = "Destination local path cannot be empty") final String dstLocalPath
    ) throws GenieException {
        final long start = System.nanoTime();
        final Set<Tag> tags = Sets.newHashSet();
        log.debug("Called with src path {} and destination path {}", srcRemotePath, dstLocalPath);

        try {
            final File outputFile = new File(dstLocalPath);
            if (!this.isValid(srcRemotePath)) {
                throw new GenieServerException("Unable to download " + srcRemotePath + " not a valid URL");
            }
            this.restTemplate.execute(
                srcRemotePath,
                HttpMethod.GET,
                requestEntity -> requestEntity.getHeaders().setAccept(Lists.newArrayList(MediaType.ALL)),
                (ResponseExtractor<Void>) response -> {
                    // Documentation I could find pointed to the HttpEntity reading the bytes off
                    // the stream so this should resolve memory problems if the file returned is large
                    FileUtils.copyInputStreamToFile(response.getBody(), outputFile);
                    return null;
                }
            );
            MetricsUtils.addSuccessTags(tags);
        } catch (final GenieException | RuntimeException e) {
            MetricsUtils.addFailureTagsWithException(tags, e);
            throw e;
        } finally {
            this.registry.timer(DOWNLOAD_TIMER_NAME, tags).record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void putFile(
        @NotBlank(message = "Source local path cannot be empty.") final String srcLocalPath,
        @NotBlank(message = "Destination remote path cannot be empty") final String dstRemotePath
    ) throws GenieException {
        final long start = System.nanoTime();
        final Set<Tag> tags = Sets.newHashSet();
        try {
            throw new UnsupportedOperationException(
                "Saving a file to an HttpEndpoint isn't implemented in this version"
            );
        } catch (final Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw t;
        } finally {
            this.registry.timer(UPLOAD_TIMER_NAME, tags).record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getLastModifiedTime(final String path) throws GenieException {
        final long start = System.nanoTime();
        final Set<Tag> tags = Sets.newHashSet();
        final long lastModtime;
        try {
            final URL url = new URL(path);
            final long time = this.restTemplate.headForHeaders(url.toURI()).getLastModified();
            // Returns now if there was no last modified header as best we can do is assume file is brand new
            lastModtime = time != -1 ? time : Instant.now().toEpochMilli();
            MetricsUtils.addSuccessTags(tags);
        } catch (final MalformedURLException | URISyntaxException e) {
            log.error(e.getLocalizedMessage(), e);
            MetricsUtils.addFailureTagsWithException(tags, e);
            throw new GenieServerException("Failed to get metadata for invalid URL", e);
        } catch (final Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw t;
        } finally {
            this.registry
                .timer(GET_LAST_MODIFIED_TIMER_NAME, tags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
        return lastModtime;
    }
}
