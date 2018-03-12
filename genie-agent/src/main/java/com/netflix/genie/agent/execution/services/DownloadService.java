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

import com.netflix.genie.agent.execution.exceptions.DownloadException;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nullable;
import java.io.File;
import java.net.URI;
import java.util.Set;

/**
 * Service to download a set of files to local disk.
 *
 * @author mprimi
 * @since 4.0.0
 */
public interface DownloadService {
    /**
     * Get a new manifest builder.
     * @return a new builder
     */
    Manifest.Builder newManifestBuilder();

    /**
     * Download all files as per provided manifest.
     * The destination directories present in the manifest are expected to exist.
     * Existing files will not be overwritten.
     *
     * @param downloadsManifest a download manifest
     * @throws DownloadException if at least one of the downloads fails
     */
    void download(final Manifest downloadsManifest) throws DownloadException;

    /**
     * Manifest containing source (URIs) and their expected destination on disk after download.
     * This abstraction is used by different services to set up the job working directory.
     *
     * Currently, source URIs must be unique. I.e. it is not possible to have a manifest with the same source
     * URI and two or more target destinations.
     */
    interface Manifest {

        /**
         * Get the set of files representing expected target location for files to be downloaded.
         * @return an immutable set of files objects
         */
        Set<File> getTargetFiles();

        /**
         * Get the set of directories that contain all the target files.
         * @return an immutable set of file objects
         */
        Set<File> getTargetDirectories();

        /**
         * Get the set of URI s (source files) that were added to the manifest.
         * @return an immutable set of URIs
         */
        Set<URI> getSourceFileUris();

        /**
         * Get the manifest entries.
         * @return an immutable set of entries in this manifest
         */
        Set<Pair<URI, File>> getEntries();

        /**
         * Return the target location for a given source URI.
         * @param sourceFileUri source file URI
         * @return a File target, or null if the source is not in the manifest
         */
        @Nullable
        File getTargetLocation(final URI sourceFileUri);

        /**
         * Builder for Manifest.
         */
        interface Builder {

            /**
             * Add a source file specifying a target directory.
             * The filename for the target is computed by using the last component of the resource path.
             * @param sourceFileUri source URISi
             * @param targetDirectory target directory
             * @return a builder for chaining
             */
            DownloadService.Manifest.Builder addFileWithTargetDirectory(
                final URI sourceFileUri,
                final File targetDirectory
            );

            /**
             * Add a source file specifying a target file destination.
             * @param sourceFileUri source URI
             * @param targetFile target file
             * @return a builder for chaining
             */
            Builder addFileWithTargetFile(
                final URI sourceFileUri,
                final File targetFile
            );

            /**
             * Build the manifest.
             * @return the manifest
             * @throws IllegalArgumentException if two or more source URIs point to the same target file
             */
            Manifest build();
        }
    }

}
