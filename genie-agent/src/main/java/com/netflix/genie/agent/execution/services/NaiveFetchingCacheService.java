/*
 *
 *  Copyright 2018 Netflix, Inc.
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

package com.netflix.genie.agent.execution.services;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Files;
import com.netflix.genie.agent.cli.ArgumentDelegates;
import com.netflix.genie.agent.execution.exceptions.DownloadException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;
import org.springframework.util.DigestUtils;
import org.springframework.util.FileCopyUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Set;

/**
 * A naive implementation of a file cache on local disk that uses URIs as keys and transparently downloads
 * missing entries.
 *
 * Does NOT handle other agents reading/writing/deleting in the same cache folder
 * Does NOT handle visioning or staleness of locally cached resources
 * Does NOT handle concurrency withing the same agent (not an issue at the moment)
 * Does NOT garbage-collect old items
 * TODO: implement a cache that handles the above
 *
 * @author mprimi
 * @since 4.0.0
 */
@Service
@Lazy
@Slf4j
class NaiveFetchingCacheService implements FetchingCacheService {

    private final ResourceLoader resourceLoader;
    private final File cacheDirectory;

    NaiveFetchingCacheService(
        final ResourceLoader resourceLoader,
        final ArgumentDelegates.CacheArguments cacheArguments
    ) {
        this.resourceLoader = resourceLoader;

        this.cacheDirectory = cacheArguments.getCacheDirectory();
        if (!cacheDirectory.exists()) {
            try {
                Files.createParentDirs(new File(cacheDirectory, "_"));
            } catch (final IOException e) {
                throw new RuntimeException("Failed to create cache directory: " + cacheDirectory.getAbsolutePath(), e);
            }
        } else if (!cacheDirectory.isDirectory()) {
            throw new RuntimeException("Cache location is not directory: " + cacheDirectory.getAbsolutePath());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void get(final URI sourceFileUri, final File destinationFile) throws DownloadException, IOException {
        final File cachedFile = lookupOrDownload(sourceFileUri);
        Files.copy(cachedFile, destinationFile);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void get(final Set<Pair<URI, File>> sourceDestinationPairs) throws DownloadException, IOException {
        for (final Pair<URI, File> sourceDestinationPair : sourceDestinationPairs) {
            get(sourceDestinationPair.getKey(), sourceDestinationPair.getValue());
        }
    }

    private File lookupOrDownload(final URI sourceFileUri) throws DownloadException, IOException {

        final String uriString = sourceFileUri.toASCIIString();

        log.info("Lookup: {}", uriString);

        // Unique id to store the resource on local disk
        // TODO: current scheme does not support versions -- items can get stale
        final String resourceCacheId = getResourceCacheId(sourceFileUri);

        // File where resource is already stored or will be downloaded to
        final File cacheFile = new File(cacheDirectory, resourceCacheId);

        if (cacheFile.exists()) {
            log.info(
                "Cache hit: {} (id: {})",
                uriString,
                resourceCacheId
            );
        } else {
            log.info(
                "Cache miss: {} (id: {})",
                uriString,
                resourceCacheId
            );

            // Get a handle to the resource (implementation specific, but probably does not make a remote request yet)
            final Resource resource = resourceLoader.getResource(uriString);

            // Check for existence
            if (!resource.exists()) {
                throw new DownloadException("Resource not found: " + uriString);
            }

            // Download the resource into the cache
            try (
                final InputStream in = resource.getInputStream();
                final OutputStream out = new FileOutputStream(cacheFile)
            ) {
                FileCopyUtils.copy(in, out);
            }
        }
        return cacheFile;
    }

    @VisibleForTesting
    String getResourceCacheId(final URI uri) {
        return DigestUtils.md5DigestAsHex(uri.toASCIIString().getBytes(StandardCharsets.UTF_8));
    }
}
