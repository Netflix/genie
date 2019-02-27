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

package com.netflix.genie.web.resources.agent;

import com.netflix.genie.common.internal.dto.JobDirectoryManifest;
import com.netflix.genie.web.services.AgentFileStreamService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.InputStreamSource;
import org.springframework.core.io.Resource;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;

/**
 * Implementation of {@link org.springframework.core.io.Resource} for files local to an agent running a job that can be
 * requested and streamed to the server (so they can be served via API).
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
class AgentFileResource implements InputStreamSource, Resource {

    private final URI uri;
    private final String filename;
    private final InputStream inputStream;
    private final String description;
    private final long lastModified;
    private final long contentLength;

    AgentFileResource(
        final URI uri,
        final AgentFileStreamService.ActiveStream fileStream,
        final JobDirectoryManifest.ManifestEntry manifestEntry
    ) {
        this.uri = uri;
        this.inputStream = fileStream.getInputStream();
        this.filename = manifestEntry.getName();
        this.lastModified = manifestEntry.getLastModifiedTime().toEpochMilli(); //TODO correct?
        this.contentLength = manifestEntry.getSize();
        this.description = String.format(
            "%s [job:%s path:%s]",
            this.getClass().getSimpleName(),
            fileStream.getJobId(),
            fileStream.getRelativePath()
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean exists() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isReadable() {
        return this.exists();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isOpen() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isFile() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public URL getURL() throws IOException {
        throw new IOException("Cannot convert resource URI to URL");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public URI getURI() {
        return this.uri;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public File getFile() throws IOException {
        throw new FileNotFoundException("Agent stream should not be resolved to a local file");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long contentLength() {
        return this.contentLength;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long lastModified() {
        return this.lastModified;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Resource createRelative(final String relativePath) throws IOException {
        throw new IOException("Unable to created a resource via relative path");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getFilename() {
        return this.filename;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDescription() {
        return description;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputStream getInputStream() {
        return this.inputStream;
    }
}
