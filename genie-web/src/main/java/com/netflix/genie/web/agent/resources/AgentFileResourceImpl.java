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
package com.netflix.genie.web.agent.resources;

import com.netflix.genie.web.agent.services.AgentFileStreamService;
import org.springframework.core.io.Resource;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.time.Instant;

/**
 * Implementation of {@link Resource} for files local to an agent running a job that can be
 * requested and streamed to the server (so they can be served via API).
 *
 * @author mprimi
 * @since 4.0.0
 */
public final class AgentFileResourceImpl implements AgentFileStreamService.AgentFileResource {
    private final boolean exists;
    private final URI uri;
    private final long contentLength;
    private final long lastModified;
    private final String filename;
    private final String description;
    private final InputStream inputStream;

    private AgentFileResourceImpl(
        final boolean exists,
        final URI uri,
        final long contentLength,
        final long lastModified,
        final String filename,
        final String description,
        final InputStream inputStream
    ) {
        this.exists = exists;
        this.uri = uri;
        this.contentLength = contentLength;
        this.lastModified = lastModified;
        this.filename = filename;
        this.description = description;
        this.inputStream = inputStream;
    }

    /**
     * Factory method to create a placeholder resource for a remote file that does not exist.
     *
     * @return a {@link AgentFileStreamService.AgentFileResource}
     */
    public static AgentFileResourceImpl forNonExistingResource() {

        final String description = AgentFileResourceImpl.class.getSimpleName()
            + " [ non-existent ]";

        return new AgentFileResourceImpl(
            false,
            null,
            -1,
            -1,
            null,
            description,
            null
        );
    }

    /**
     * Factory method to create a resource for a remote file.
     *
     * @param uri              the resource URI
     * @param size             the size of the file, as per latest manifest
     * @param lastModifiedTime the last modification time, as per latest manifest
     * @param relativePath     the path of the file relative to the root of the job directory
     * @param jobId            the id of the job this file belongs to
     * @param inputStream      the input stream to read this file content
     * @return a {@link AgentFileStreamService.AgentFileResource}
     */
    public static AgentFileStreamService.AgentFileResource forAgentFile(
        final URI uri,
        final long size,
        final Instant lastModifiedTime,
        final Path relativePath,
        final String jobId,
        final InputStream inputStream
    ) {
        final String description = AgentFileResourceImpl.class.getSimpleName()
            + " ["
            + " jobId:" + jobId + ", "
            + " relativePath: " + relativePath.toString()
            + " ]";

        final Path filenamePath = relativePath.getFileName();

        if (filenamePath == null) {
            throw new IllegalArgumentException("Invalid relative path, not a file");
        }

        return new AgentFileResourceImpl(
            true,
            uri,
            size,
            lastModifiedTime.toEpochMilli(),
            filenamePath.toString(),
            description,
            inputStream
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean exists() {
        return this.exists;
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
    public URL getURL() throws IOException {
        //TODO: Doable, could resolve to API URL. Unclear what value this would add.
        throw new IOException("Cannot resolve to a URL");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public URI getURI() throws IOException {
        if (!this.exists()) {
            throw new IOException("Cannot determine URI of non-existent resource");
        }
        return this.uri;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public File getFile() throws IOException {
        throw new FileNotFoundException("Resource is not a local file");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long contentLength() throws IOException {
        if (!this.exists()) {
            throw new IOException("Cannot determine size of non-existent resource");

        }
        return this.contentLength;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long lastModified() throws IOException {
        if (!this.exists()) {
            throw new IOException("Cannot determine modification time of non-existent resource");

        }
        return this.lastModified;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Resource createRelative(final String relativePath) throws IOException {
        //TODO: Doable if a reference to AgentFileStreamService is kept. Unclear what value this would add.
        throw new IOException("Cannot create resource from relative path");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getFilename() {
        if (!this.exists()) {
            return null;
        }
        return this.filename;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDescription() {
        return this.description;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputStream getInputStream() throws IOException {
        if (!this.exists()) {
            throw new FileNotFoundException("Resource does not exist");
        }
        return this.inputStream;
    }
}
