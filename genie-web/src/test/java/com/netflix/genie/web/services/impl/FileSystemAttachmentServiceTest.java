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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.web.exceptions.checked.SaveAttachmentException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.springframework.core.io.FileSystemResource;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Tests for {@link FileSystemAttachmentService}.
 *
 * @author tgianos
 * @since 3.0.0
 */
class FileSystemAttachmentServiceTest {

    @SuppressWarnings("checkstyle:VisibilityModifier")
    @TempDir
    Path folder;

    private FileSystemAttachmentService service;

    @BeforeEach
    void setup() {
        this.service = new FileSystemAttachmentService("file://" + this.folder.toFile().getAbsolutePath());
    }

    @Test
    void canSaveAttachment() throws GenieException, IOException {
        final String jobId = UUID.randomUUID().toString();
        final Path original = this.saveAttachment(jobId);
        final Path saved = this.folder.resolve(jobId).resolve(original.getFileName());
        Assertions.assertThat(original).exists();
        Assertions.assertThat(saved).exists();
    }

    @Test
    void cantCopyIfDestinationIsNotADirectory() {
        final File destination = Mockito.mock(File.class);
        Mockito.when(destination.exists()).thenReturn(true);
        Mockito.when(destination.isDirectory()).thenReturn(false);
        Assertions
            .assertThatExceptionOfType(GeniePreconditionException.class)
            .isThrownBy(() -> this.service.copy(UUID.randomUUID().toString(), destination));
    }

    @Test
    void canCopyAttachments() throws GenieException, IOException {
        final String jobId = UUID.randomUUID().toString();
        final String finalDirName = UUID.randomUUID().toString();
        final Set<Path> originals = this.saveAttachments(jobId);
        final Path jobDir = this.folder.resolve(jobId);
        Assertions.assertThat(jobDir).exists();
        final Path finalDir = this.folder.resolve(finalDirName);
        this.service.copy(jobId, finalDir.toFile());
        Assertions.assertThat(jobDir).exists();
        Assertions.assertThat(finalDir).exists();
        for (final Path file : originals) {
            Assertions.assertThat(file).exists();
            final Path finalFile = finalDir.resolve(file.getFileName());
            Assertions.assertThat(finalFile).exists().usingCharset(StandardCharsets.UTF_8).hasSameContentAs(file);
        }
    }

    @Test
    void canDeleteAttachments() throws GenieException, IOException {
        final String jobId = UUID.randomUUID().toString();
        final Set<Path> attachments = this.saveAttachments(jobId);
        final Path jobDir = this.folder.resolve(jobId);
        Assertions.assertThat(jobDir).exists();
        this.service.delete(jobId);
        Assertions.assertThat(jobDir).doesNotExist();
        attachments.forEach(file -> Assertions.assertThat(file).doesNotExist());
    }

    @Test
    void testAttachmentLifecycle() throws IOException {
        final Path sourceDirectory = this.folder.resolve(UUID.randomUUID().toString());
        Files.createDirectory(sourceDirectory);
        final int numAttachments = 10;
        final Set<Path> sourceAttachments = this.createAttachments(sourceDirectory, numAttachments);
        final Map<String, InputStream> attachments = Maps.newHashMap();
        for (final Path attachment : sourceAttachments) {
            final Path filename = attachment.getFileName();
            if (filename != null) {
                attachments.put(filename.toString(), Files.newInputStream(attachment));
            }
        }
        final String id = this.service.saveAll(attachments);
        final Path expectedAttachmentDirectory = this.folder.resolve(id);
        Assertions.assertThat(expectedAttachmentDirectory).exists();
        Assertions.assertThat(Files.list(expectedAttachmentDirectory).count()).isEqualTo(numAttachments);
        for (final Path attachment : sourceAttachments) {
            Assertions
                .assertThat(attachment)
                .exists()
                .usingCharset(StandardCharsets.UTF_8)
                .hasSameContentAs(expectedAttachmentDirectory.resolve(attachment.getFileName()));
        }

        // Attachments saved successfully lets try to copy
        final Path copyDirectory = this.folder.resolve(UUID.randomUUID().toString());
        Files.createDirectory(copyDirectory);
        this.service.copyAll(id, copyDirectory);
        Assertions.assertThat(copyDirectory).exists();
        Assertions.assertThat(Files.list(copyDirectory).count()).isEqualTo(numAttachments);
        for (final Path attachment : sourceAttachments) {
            Assertions
                .assertThat(attachment)
                .usingCharset(StandardCharsets.UTF_8)
                .hasSameContentAs(copyDirectory.resolve(attachment.getFileName()));
        }

        // Delete Them
        this.service.deleteAll(id);
        Assertions.assertThat(expectedAttachmentDirectory).doesNotExist();
    }

    @Test
    void testSaveAndDeleteAttachments() throws SaveAttachmentException, IOException {
        final String jobId = UUID.randomUUID().toString();
        final Path sourceDirectory = this.folder.resolve(UUID.randomUUID().toString());
        Files.createDirectory(sourceDirectory);
        final int numAttachments = 5;
        final Set<Path> sourceAttachments = this.createAttachments(sourceDirectory, numAttachments);
        final Set<URI> attachmentURIs = this.service.saveAttachments(
            jobId,
            sourceAttachments.stream().map(FileSystemResource::new).collect(Collectors.toSet())
        );
        final Path expectedAttachmentDirectory = this.folder.resolve(jobId);
        Assertions.assertThat(expectedAttachmentDirectory).exists();
        Assertions
            .assertThat(Files.list(expectedAttachmentDirectory).count())
            .isEqualTo(numAttachments)
            .isEqualTo(attachmentURIs.size());
        for (final Path attachment : sourceAttachments) {
            Assertions
                .assertThat(attachment)
                .exists()
                .usingCharset(StandardCharsets.UTF_8)
                .hasSameContentAs(expectedAttachmentDirectory.resolve(attachment.getFileName()));
        }

        // Delete the attachments
        this.service.deleteAttachments(jobId);
        Assertions.assertThat(expectedAttachmentDirectory).doesNotExist();
    }

    /**
     * Make sure when there are no attachments sent in it does nothing to the file system.
     *
     * @throws SaveAttachmentException on error running service API
     * @throws IOException             listing file system resources
     */
    @Test
    void emptyAttachmentsIsANoOp() throws SaveAttachmentException, IOException {
        final Set<Path> currentContents = Files.list(this.folder).collect(Collectors.toSet());
        final Set<URI> uris = this.service.saveAttachments(
            UUID.randomUUID().toString(),
            Sets.newHashSet()
        );
        Assertions.assertThat(uris).isEmpty();
        Assertions
            .assertThat(Files.list(this.folder).collect(Collectors.toSet()))
            .isEqualTo(currentContents);
    }

    private Set<Path> saveAttachments(final String jobId) throws GenieException, IOException {
        final Set<Path> attachments = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            final Path original = this.saveAttachment(jobId);
            final Path saved = this.folder.resolve(jobId).resolve(original.getFileName());
            Assertions.assertThat(saved).exists();
            attachments.add(saved);
        }
        return attachments;
    }

    private Path saveAttachment(final String jobId) throws GenieException, IOException {
        final Path attachment = this.folder.resolve(UUID.randomUUID().toString() + ".q");
        Files.write(
            attachment,
            ("SELECT * FROM my_table WHERE id = '" + UUID.randomUUID().toString() + "';")
                .getBytes(StandardCharsets.UTF_8)
        );
        try (InputStream is = Files.newInputStream(attachment)) {
            this.service.save(jobId, attachment.getFileName().toString(), is);
        }
        return attachment;
    }

    private Set<Path> createAttachments(final Path targetDirectory, final int numAttachments) throws IOException {
        final Set<Path> attachments = Sets.newHashSet();
        for (int i = 0; i < numAttachments; i++) {
            final Path attachment = targetDirectory.resolve(UUID.randomUUID().toString());
            Files.write(attachment, ("Select * FROM my_table where id = " + i + ";").getBytes(StandardCharsets.UTF_8));
            attachments.add(attachment);
        }

        return attachments;
    }
}
