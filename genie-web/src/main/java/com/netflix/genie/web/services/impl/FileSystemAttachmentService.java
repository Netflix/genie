/*
 *
 *  Copyright 2015 Netflix, Inc.
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

import com.google.common.collect.ImmutableSet;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.web.exceptions.checked.SaveAttachmentException;
import com.netflix.genie.web.services.AttachmentService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.springframework.core.io.AbstractResource;
import org.springframework.core.io.Resource;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Implementation of the AttachmentService interface which saves and retrieves attachments from the local filesystem.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Slf4j
public class FileSystemAttachmentService implements AttachmentService {

    private final Path attachmentDirectory;

    /**
     * Constructor.
     *
     * @param attachmentsDirectory The directory to use or null if want to default to system temp directory
     */
    public FileSystemAttachmentService(final String attachmentsDirectory) {
        String attachmentsDirectoryPath = attachmentsDirectory;
        if (!attachmentsDirectoryPath.endsWith(File.separator)) {
            attachmentsDirectoryPath = attachmentsDirectory + File.separator;
        }

        try {
            this.attachmentDirectory = Files.createDirectories(Paths.get(new URI(attachmentsDirectoryPath)));
        } catch (final IOException | URISyntaxException e) {
            throw new IllegalArgumentException("Unable to create attachment directory: " + attachmentsDirectoryPath, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void save(
        final String jobId,
        final String filename,
        final InputStream content
    ) throws GenieException {
        try {
            this.writeAttachments(jobId, ImmutableSet.of(new AttachmentResource(filename, content)));
        } catch (final IOException ioe) {
            throw new GenieServerException("Failed to save attachment", ioe);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copy(final String jobId, final File destination) throws GenieException {
        if (destination.exists() && !destination.isDirectory()) {
            throw new GeniePreconditionException(destination + " is not a directory and it needs to be.");
        }
        try {
            this.copyAll(jobId, destination.toPath());
        } catch (final IOException ioe) {
            throw new GenieServerException("Failed to copy attachment directory", ioe);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(final String jobId) throws GenieException {
        try {
            this.deleteAll(jobId);
        } catch (final IOException ioe) {
            throw new GenieServerException("Failed to delete directory " + jobId, ioe);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String saveAll(final Map<String, InputStream> attachments) throws IOException {
        final String requestId = UUID.randomUUID().toString();
        this.writeAttachments(
            requestId,
            attachments
                .entrySet()
                .stream()
                .map(entry -> new AttachmentResource(entry.getKey(), entry.getValue()))
                .collect(Collectors.toSet()));
        return requestId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyAll(final String id, final Path destination) throws IOException {
        Files.createDirectories(destination);
        final Path attachmentDir = this.attachmentDirectory.resolve(id);
        if (Files.exists(attachmentDir) && Files.isDirectory(attachmentDir)) {
            // Lets use this cause I don't feel like writing a visitor
            FileUtils.copyDirectory(attachmentDir.toFile(), destination.toFile());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteAll(final String id) throws IOException {
        this.deleteAttachments(id);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<URI> saveAttachments(
        final String jobId,
        final Set<Resource> attachments
    ) throws SaveAttachmentException {
        try {
            return this.writeAttachments(jobId, attachments);
        } catch (final IOException ioe) {
            throw new SaveAttachmentException(ioe);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteAttachments(final String jobId) throws IOException {
        final Path attachmentDir = this.attachmentDirectory.resolve(jobId);
        FileUtils.deleteDirectory(attachmentDir.toFile());
    }

    private Set<URI> writeAttachments(final String id, final Set<Resource> attachments) throws IOException {
        if (attachments.isEmpty()) {
            return ImmutableSet.of();
        }
        final Path requestDir = Files.createDirectories(this.attachmentDirectory.resolve(id));
        final ImmutableSet.Builder<URI> uris = ImmutableSet.builder();

        for (final Resource attachment : attachments) {
            final String rawFilename = attachment.getFilename() == null
                ? UUID.randomUUID().toString()
                : attachment.getFilename();
            // Sanitize the filename
            final Path fileName = Paths.get(rawFilename).getFileName();
            final Path file = requestDir.resolve(fileName);
            try (InputStream contents = attachment.getInputStream()) {
                final long byteCount = Files.copy(contents, file);
                log.debug("Wrote {} bytes for attachment {} to {}", byteCount, fileName, file);
            }
            uris.add(file.toUri());
        }
        return uris.build();
    }

    /**
     * Temporary class for backwards compatibility till we delete all unused APIs.
     */
    @SuppressWarnings("FinalClass")
    private static class AttachmentResource extends AbstractResource {
        private final String filename;
        private final InputStream contents;

        private AttachmentResource(final String filename, final InputStream contents) {
            this.filename = filename;
            this.contents = contents;
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
        @Nonnull
        public String getDescription() {
            return "Temporary resource for " + this.filename;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        @Nonnull
        public InputStream getInputStream() throws IOException {
            return this.contents;
        }
    }
}
