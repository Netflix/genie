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

package com.netflix.genie.agent.execution.services.impl;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.genie.agent.execution.exceptions.DownloadException;
import com.netflix.genie.agent.execution.services.DownloadService;
import com.netflix.genie.agent.execution.services.FetchingCacheService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Cache-backed download service to retrieve a set of files and place them at an expected location, according
 * to a given manifest.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Component
@Lazy
@Slf4j
class DownloadServiceImpl implements DownloadService {
    private final FetchingCacheService fetchingCacheService;

    DownloadServiceImpl(final FetchingCacheService fetchingCacheService) {
        this.fetchingCacheService = fetchingCacheService;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Manifest.Builder newManifestBuilder() {
        return new ManifestImpl.Builder();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void download(final Manifest downloadsManifest) throws DownloadException {

        // Validate all destination directories exist
        for (final File targetDirectory : downloadsManifest.getTargetDirectories()) {
            if (!targetDirectory.exists()) {
                throw new DownloadException(
                    "Target directory does not exist: " + targetDirectory.getAbsolutePath()
                );
            } else if (!targetDirectory.isDirectory()) {
                throw new DownloadException(
                    "Target directory is not a directory: " + targetDirectory.getAbsolutePath()
                );
            }
        }

        // Validate all target files don't exist
        for (final File targetFile : downloadsManifest.getTargetFiles()) {
            if (targetFile.exists()) {
                throw new DownloadException("Target file exists: " + targetFile.getAbsolutePath());
            }
        }

        try {
            fetchingCacheService.get(downloadsManifest.getEntries());
        } catch (final IOException e) {
            throw new DownloadException("Failed to download", e);
        }
    }

    @Getter
    private static final class ManifestImpl implements Manifest {
        private final Map<URI, File> uriFileMap;
        private final Set<File> targetDirectories;
        private final Set<File> targetFiles;
        private final Set<URI> sourceFileUris;
        private final Set<Pair<URI, File>> entries;

        private ManifestImpl(final Map<URI, File> uriFileMap) {

            this.uriFileMap = Collections.unmodifiableMap(uriFileMap);

            this.targetDirectories = Collections.unmodifiableSet(
                uriFileMap
                    .values()
                    .stream()
                    .map(File::getParentFile)
                    .collect(Collectors.toSet())
            );

            this.targetFiles = Collections.unmodifiableSet(
                Sets.newHashSet(uriFileMap.values())
            );

            if (targetFiles.size() < uriFileMap.values().size()) {
                throw new IllegalArgumentException("Two or more files are targeting the same destination location");
            }

            this.sourceFileUris = Collections.unmodifiableSet(uriFileMap.keySet());

            this.entries = Collections.unmodifiableSet(
                uriFileMap.entrySet()
                    .stream()
                    .map(entry -> new ImmutablePair<>(entry.getKey(), entry.getValue()))
                    .collect(Collectors.toSet())
            );
        }

        /**
         * {@inheritDoc}
         */
        @Nullable
        @Override
        public File getTargetLocation(final URI sourceFileUri) {
            return uriFileMap.get(sourceFileUri);
        }

        private static final class Builder implements Manifest.Builder {

            private final Map<URI, File> filesMap;

            Builder() {
                this.filesMap = Maps.newHashMap();
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public Builder addFileWithTargetDirectory(
                final URI sourceFileUri,
                final File targetDirectory
            ) {
                final String uriPath = sourceFileUri.getPath();
                if (StringUtils.isBlank(uriPath)) {
                    throw new IllegalArgumentException("Uri has empty path: " + sourceFileUri);
                }
                final String filename = new File(uriPath).getName();
                if (StringUtils.isBlank(filename)) {
                    throw new IllegalArgumentException("Could not determine filename for source: " + sourceFileUri);
                }
                final File targetFile = new File(targetDirectory, filename);
                return addFileWithTargetFile(sourceFileUri, targetFile);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public Builder addFileWithTargetFile(
                final URI sourceFileUri,
                final File targetFile
            ) {
                filesMap.put(sourceFileUri, targetFile);
                return this;
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public Manifest build() {
                return new ManifestImpl(filesMap);
            }
        }
    }
}
