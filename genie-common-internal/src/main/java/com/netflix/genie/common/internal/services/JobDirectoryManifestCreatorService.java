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
package com.netflix.genie.common.internal.services;

import com.netflix.genie.common.internal.dto.DirectoryManifest;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Factory-like service that produces {@link DirectoryManifest} for Genie jobs directories.
 * Implementations can have behaviors such as remotely fetching, or caching.
 *
 * @author mprimi
 * @since 4.0.0
 */
// TODO: Move this into the Agent codebase once v3 embedded execution no longer needed on server
public interface JobDirectoryManifestCreatorService {

    /**
     * Produces a {@link DirectoryManifest} for the given job.
     *
     * @param jobDirectoryPath the job directory
     * @return a {@link DirectoryManifest}
     * @throws IOException if the manifest cannot be created.
     */
    DirectoryManifest getDirectoryManifest(
        Path jobDirectoryPath
    ) throws IOException;

    /**
     * If the implementation caches manifests to avoid excessive I/O, then demand the given cache entry be dropped,
     * thus forcing a re-creation of manifest from scratch.
     *
     * @param jobDirectoryPath the job directory
     */
    void invalidateCachedDirectoryManifest(Path jobDirectoryPath);
}
