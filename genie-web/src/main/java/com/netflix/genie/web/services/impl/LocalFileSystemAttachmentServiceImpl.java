/*
 *
 *  Copyright 2020 Netflix, Inc.
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
import com.google.common.collect.Sets;
import com.netflix.genie.web.exceptions.checked.IllegalAttachmentFileNameException;
import com.netflix.genie.web.exceptions.checked.AttachmentTooLargeException;
import com.netflix.genie.web.exceptions.checked.SaveAttachmentException;
import com.netflix.genie.web.properties.AttachmentServiceProperties;
import com.netflix.genie.web.services.AttachmentService;
import org.springframework.core.io.Resource;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.UUID;

/**
 * Implementation of {@link AttachmentService} that saves the files to a local directory.
 * <p>
 * N.B.: This implementation is currently used for integration tests and lacks some aspects that would make it usable in
 * production environments (e.g., garbage collection of old files, metrics, etc.).
 *
 * @author mprimi
 * @since 4.0.0
 */
public class LocalFileSystemAttachmentServiceImpl implements AttachmentService {
    private final Path attachmentsDirectoryPath;
    private final AttachmentServiceProperties attachmentServiceProperties;

    /**
     * Constructor.
     *
     * @param attachmentServiceProperties the service properties
     * @throws IOException when failing to create the attachments directory
     */
    public LocalFileSystemAttachmentServiceImpl(
        final AttachmentServiceProperties attachmentServiceProperties
    ) throws IOException {
        this.attachmentServiceProperties = attachmentServiceProperties;
        this.attachmentsDirectoryPath = Paths.get(attachmentServiceProperties.getLocationPrefix());

        // Create base attachments directory
        Files.createDirectories(this.attachmentsDirectoryPath);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<URI> saveAttachments(
        @Nullable final String jobId,
        final Set<Resource> attachments
    ) throws SaveAttachmentException {

        if (attachments.isEmpty()) {
            return Sets.newHashSet();
        }

        final Path attachmentsBasePath = this.attachmentsDirectoryPath.resolve(UUID.randomUUID().toString());
        try {
            Files.createDirectories(attachmentsBasePath);
        } catch (IOException e) {
            throw new SaveAttachmentException("Failed to create directory for attachments: " + e.getMessage(), e);
        }

        long totalSize = 0;

        final ImmutableSet.Builder<URI> setBuilder = ImmutableSet.builder();

        for (final Resource attachment : attachments) {
            try (InputStream inputStream = attachment.getInputStream()) {
                final long attachmentSize = attachment.contentLength();
                final String filename = attachment.getFilename();

                if (filename != null) {
                    if ((filename.contains("/") || filename.contains("\\")
                        || filename.equals(".") || filename.equals(".."))) {
                        throw new IllegalAttachmentFileNameException("Attachment filename " + filename + " is illegal. "
                            + "Filenames should not be . or .., or contain /, \\.");
                    }

                    final String attachmentCanonicalPath =
                        createTempFile(String.valueOf(attachmentsBasePath), filename).getCanonicalPath();

                    final String baseCanonicalPath =
                        new File(String.valueOf(attachmentsBasePath)).getCanonicalPath();

                    if (!attachmentCanonicalPath.startsWith(baseCanonicalPath)
                        || attachmentCanonicalPath.equals(baseCanonicalPath)) {
                        throw new IllegalAttachmentFileNameException("Attachment filename " + filename + " is illegal. "
                            + "Filenames should not be a relative path.");
                    }
                }

                if (attachmentSize > this.attachmentServiceProperties.getMaxSize().toBytes()) {
                    throw new AttachmentTooLargeException("Attachment is too large: " + filename);
                }

                totalSize += attachmentSize;

                if (totalSize > this.attachmentServiceProperties.getMaxTotalSize().toBytes()) {
                    throw new AttachmentTooLargeException("Attachments total size is too large");
                }

                final Path attachmentPath = attachmentsBasePath.resolve(
                    filename != null ? filename : UUID.randomUUID().toString()
                );

                Files.copy(inputStream, attachmentPath);

                setBuilder.add(attachmentPath.toUri());

            } catch (IOException e) {
                throw new SaveAttachmentException("Failed to save attachment: " + e.getMessage(), e);
            }
        }

        return setBuilder.build();
    }

    /* for testing purposes */
    File createTempFile(final String attachmentsBasePath, final String filename) {
        return new File(attachmentsBasePath, filename);
    }
}
