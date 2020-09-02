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
import com.netflix.genie.web.exceptions.checked.AttachmentTooLargeException;
import com.netflix.genie.web.exceptions.checked.SaveAttachmentException;
import com.netflix.genie.web.services.AttachmentService;
import org.springframework.core.io.Resource;
import org.springframework.util.unit.DataSize;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.UUID;

/**
 * Implementaion of {@link AttachmentService} that saves the files to a local directory.
 * <p>
 * N.B.: This implementation is currently used for integration tests and lacks some aspects that would make it usable in
 * production environments (e.g., garbage collection of old files, metrics, etc.).
 *
 * @author mprimi
 * @since 4.0.0
 */
public class LocalFileSystemAttachmentService implements AttachmentService {
    private final DataSize maxAttachmentSize;
    private final DataSize maxTotalSize;
    private final Path attachmentsDirectoryPath;

    /**
     * Constructor.
     *
     * @param maxAttachmentSize    the maximum attachments
     * @param maxTotalSize         the maximum size of all attachments combined
     * @param attachmentsDirectory the base directory where to store attachments
     */
    public LocalFileSystemAttachmentService(
        final DataSize maxAttachmentSize,
        final DataSize maxTotalSize,
        final File attachmentsDirectory
    ) {
        this.maxAttachmentSize = maxAttachmentSize;
        this.maxTotalSize = maxTotalSize;
        this.attachmentsDirectoryPath = attachmentsDirectory.toPath();
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

                if (attachmentSize > this.maxAttachmentSize.toBytes()) {
                    throw new AttachmentTooLargeException("Attachment is too large: " + filename);
                }

                totalSize += attachmentSize;

                if (totalSize > this.maxTotalSize.toBytes()) {
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
}
