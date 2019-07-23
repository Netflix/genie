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
import org.apache.commons.io.FileUtils;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.springframework.core.io.FileSystemResource;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Tests for the file system implementation of the attachment service.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class FileSystemAttachmentServiceTest {

    /**
     * Creates a temporary folder to use for these tests that is cleaned up after tests are run.
     */
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private FileSystemAttachmentService service;

    /**
     * Setup for tests.
     */
    @Before
    public void setup() {
        this.service = new FileSystemAttachmentService("file://" + this.folder.getRoot().getAbsolutePath());
    }

    /**
     * Test whether we can successfully save an attachment to the file system.
     *
     * @throws GenieException on error
     * @throws IOException    if the attachment file can't be located
     */
    @Test
    public void canSaveAttachment() throws GenieException, IOException {
        final String jobId = UUID.randomUUID().toString();
        final File original = this.saveAttachment(jobId);
        final File saved = new File(
            this.folder.getRoot().getAbsolutePath() + "/" + jobId + "/" + original.getName()
        );
        Assert.assertTrue(original.exists());
        Assert.assertTrue(saved.exists());
    }

    /**
     * Make sure it can't copy if the destination isn't a directory.
     *
     * @throws GenieException on error
     */
    @Test(expected = GeniePreconditionException.class)
    public void cantCopyIfDestinationIsntDirectory() throws GenieException {
        final File destination = Mockito.mock(File.class);
        Mockito.when(destination.exists()).thenReturn(true);
        Mockito.when(destination.isDirectory()).thenReturn(false);
        this.service.copy(UUID.randomUUID().toString(), destination);
    }

    /**
     * Test whether we can successfully delete an attachment from the filesystem.
     *
     * @throws GenieException on error
     * @throws IOException    if the attachment file can't be located
     */
    @Test
    public void canCopyAttachments() throws GenieException, IOException {
        final String jobId = UUID.randomUUID().toString();
        final String finalDirName = UUID.randomUUID().toString();
        final Set<File> originals = this.saveAttachments(jobId);
        final File jobDir = new File(this.folder.getRoot().getAbsoluteFile(), jobId);
        Assert.assertTrue(jobDir.exists());
        final File finalDir = new File(this.folder.getRoot().getAbsoluteFile(), finalDirName);
        this.service.copy(jobId, finalDir);
        Assert.assertTrue(jobDir.exists());
        Assert.assertTrue(finalDir.exists());
        for (final File file : originals) {
            Assert.assertTrue(file.exists());
            final File finalFile = new File(finalDir, file.getName());
            Assert.assertTrue(finalFile.exists());
            Assert.assertEquals(file.length(), finalFile.length());
        }
    }

    /**
     * Test whether we can successfully delete an attachment from the filesystem.
     *
     * @throws GenieException on error
     * @throws IOException    if the attachment file can't be located
     */
    @Test
    public void canDeleteAttachments() throws GenieException, IOException {
        final String jobId = UUID.randomUUID().toString();
        final Set<File> attachments = this.saveAttachments(jobId);
        final File jobDir = new File(this.folder.getRoot().getAbsoluteFile(), jobId);
        Assert.assertTrue(jobDir.exists());
        this.service.delete(jobId);
        Assert.assertFalse(jobDir.exists());
        attachments.forEach(file -> Assert.assertFalse(file.exists()));
    }

    /**
     * Test that all attachments can be saved at once.
     *
     * @throws IOException on error
     */
    @Test
    public void testAttachmentLifecycle() throws IOException {
        final Path sourceDirectory = this.folder.newFolder().toPath();
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
        final Path expectedAttachmentDirectory = this.folder.getRoot().toPath().resolve(id);
        Assert.assertTrue(Files.exists(expectedAttachmentDirectory));
        Assert.assertThat(Files.list(expectedAttachmentDirectory).count(), Matchers.is((long) numAttachments));
        for (final Path attachment : sourceAttachments) {
            Assert.assertTrue(
                FileUtils.contentEquals(
                    attachment.toFile(),
                    expectedAttachmentDirectory.resolve(attachment.getFileName()).toFile()
                )
            );
        }

        // Attachments saved successfully lets try to copy
        final Path copyDirectory = this.folder.newFolder().toPath();
        this.service.copyAll(id, copyDirectory);
        Assert.assertTrue(Files.exists(copyDirectory));
        Assert.assertThat(Files.list(copyDirectory).count(), Matchers.is((long) numAttachments));
        for (final Path attachment : sourceAttachments) {
            Assert.assertTrue(
                FileUtils.contentEquals(
                    attachment.toFile(),
                    copyDirectory.resolve(attachment.getFileName()).toFile()
                )
            );
        }

        // Delete Them
        this.service.deleteAll(id);
        Assert.assertFalse(Files.exists(expectedAttachmentDirectory));
    }

    /**
     * Test that all attachments are saved successfully to disk and can be deleted.
     *
     * @throws SaveAttachmentException on error running service API
     * @throws IOException             on error creating test attachments
     */
    @Test
    public void testSaveAndDeleteAttachments() throws SaveAttachmentException, IOException {
        final String jobId = UUID.randomUUID().toString();
        final Path sourceDirectory = this.folder.newFolder().toPath();
        final int numAttachments = 5;
        final Set<Path> sourceAttachments = this.createAttachments(sourceDirectory, numAttachments);
        final Set<URI> attachmentURIs = this.service.saveAttachments(
            jobId,
            sourceAttachments.stream().map(FileSystemResource::new).collect(Collectors.toSet())
        );
        final Path expectedAttachmentDirectory = this.folder.getRoot().toPath().resolve(jobId);
        Assert.assertTrue(Files.exists(expectedAttachmentDirectory));
        Assert.assertThat(Files.list(expectedAttachmentDirectory).count(), Matchers.is((long) numAttachments));
        Assert.assertThat(attachmentURIs.size(), Matchers.is(numAttachments));
        for (final Path attachment : sourceAttachments) {
            Assert.assertTrue(
                FileUtils.contentEquals(
                    attachment.toFile(),
                    expectedAttachmentDirectory.resolve(attachment.getFileName()).toFile()
                )
            );
        }

        // Delete the attachments
        this.service.deleteAttachments(jobId);
        Assert.assertFalse(Files.exists(expectedAttachmentDirectory));
    }

    private Set<File> saveAttachments(final String jobId) throws GenieException, IOException {
        final Set<File> attachments = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            final File original = saveAttachment(jobId);
            final File saved = new File(
                this.folder.getRoot().getAbsolutePath() + "/" + jobId + "/" + original.getName()
            );
            Assert.assertTrue(saved.exists());
            attachments.add(saved);
        }
        return attachments;
    }

    private File saveAttachment(final String jobId) throws GenieException, IOException {
        FileInputStream fis = null;
        try {
            final File attachment = this.folder.newFile(UUID.randomUUID().toString() + ".q");
            FileUtils.write(
                attachment,
                "SELECT * FROM my_table WHERE id = '" + UUID.randomUUID().toString() + "';",
                Charset.forName("UTF-8")
            );
            fis = new FileInputStream(attachment);
            this.service.save(jobId, attachment.getName(), fis);
            return attachment;
        } finally {
            if (fis != null) {
                fis.close();
            }
        }
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
