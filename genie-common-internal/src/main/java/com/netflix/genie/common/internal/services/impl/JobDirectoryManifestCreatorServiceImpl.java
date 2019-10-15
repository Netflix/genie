/*
 *
 *  Copyright 2019 Netflix, Inc.
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
package com.netflix.genie.common.internal.services.impl;

import com.github.benmanes.caffeine.cache.Cache;
import com.netflix.genie.common.internal.dto.DirectoryManifest;
import com.netflix.genie.common.internal.services.JobDirectoryManifestCreatorService;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Implementation of {@link JobDirectoryManifestCreatorService} that caches manifests produced by the factory for a few
 * seconds, thus avoiding re-calculating the same for subsequent requests (e.g. a user navigating a tree true the UI).
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class JobDirectoryManifestCreatorServiceImpl implements JobDirectoryManifestCreatorService {

    private final Cache<Path, DirectoryManifest> cache;
    private final DirectoryManifest.Factory factory;
    private final boolean includeChecksum;

    /**
     * Constructor.
     *
     * @param factory         the directory manifest factory
     * @param cache           the loading cache to use
     * @param includeChecksum whether to produce manifests that include checksums
     */
    public JobDirectoryManifestCreatorServiceImpl(
        final DirectoryManifest.Factory factory,
        final Cache<Path, DirectoryManifest> cache,
        final boolean includeChecksum
    ) {
        this.factory = factory;
        this.cache = cache;
        this.includeChecksum = includeChecksum;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DirectoryManifest getDirectoryManifest(final Path jobDirectoryPath) throws IOException {
        try {
            return cache.get(
                jobDirectoryPath.normalize().toAbsolutePath(),
                path -> {
                    try {
                        return this.factory.getDirectoryManifest(path, this.includeChecksum);
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to create manifest", e);
                    }
                }
            );
        } catch (RuntimeException e) {
            if (e.getCause() != null && e.getCause() instanceof IOException) {
                // Unwrap and throw the original IOException
                throw (IOException) e.getCause();
            }
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void invalidateCachedDirectoryManifest(final Path jobDirectoryPath) {
        this.cache.invalidate(jobDirectoryPath);
    }
}
