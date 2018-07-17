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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Set;

/**
 * Interface for a cache that downloads resources via URL and stores them on local disk for later reuse.
 *
 * @author mprimi
 * @since 4.0.0
 */
public interface FetchingCacheService {

    /**
     * Download a given resource (if not already cached) and copy it to the specified destination.
     *
     * @param sourceFileUri   the resource URI
     * @param destinationFile the location on disk where to copy the resource for use
     * @throws DownloadException if the resource is not found or fails to download
     * @throws IOException       if downloading or copying the file to destination fails
     */
    void get(final URI sourceFileUri, final File destinationFile) throws DownloadException, IOException;

    /**
     * Download a given set of resources (if not already cached) and copy them to the specified destinations.
     *
     * @param sourceDestinationPairs a set of resource URIs and their requested local target locations
     * @throws DownloadException if the resource is not found or fails to download
     * @throws IOException       if downloading or copying the file to destination fails
     */
    void get(final Set<Pair<URI, File>> sourceDestinationPairs) throws DownloadException, IOException;
}
