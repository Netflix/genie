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
package com.netflix.genie.core.services.impl;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.core.services.AttachmentService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;

/**
 * Implementation of the AttachmentService interface which saves and retrieves attachments from the local filesystem.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Slf4j
public class FileSystemAttachmentService implements AttachmentService {

    private File attachmentDirectory;

    /**
     * Constructor.
     *
     * @param attachmentsDirectory The directory to use or null if want to default to system temp directory
     */
    public FileSystemAttachmentService(final String attachmentsDirectory) {
        this.createAttachmentDirectory(attachmentsDirectory);
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
        final File attachment = new File(attachmentDirectory, jobId + "/" + filename);
        try {
            FileUtils.copyInputStreamToFile(content, attachment);
            log.info("Saved " + filename + " to " + attachment.getAbsolutePath());
        } catch (final IOException ioe) {
            throw new GenieServerException(ioe);
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
        final File source = new File(attachmentDirectory, jobId);
        if (source.exists() && source.isDirectory()) {
            try {
                FileUtils.copyDirectory(source, destination);
            } catch (final IOException ioe) {
                throw new GenieServerException(ioe);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(final String jobId) throws GenieException {
        final File jobDir = new File(attachmentDirectory, jobId);
        if (jobDir.exists()) {
            try {
                FileUtils.deleteDirectory(jobDir);
            } catch (final IOException ioe) {
                throw new GenieServerException(ioe);
            }
        }
    }

    private void createAttachmentDirectory(final String attachmentsDirectory) {
        String attachmentsDirectoryPath = attachmentsDirectory;
        if (!attachmentsDirectoryPath.endsWith(File.separator)) {
            attachmentsDirectoryPath = attachmentsDirectory + File.separator;
        }
        try {
            final File dir = new File(new URI(attachmentsDirectoryPath));
            if (!dir.exists()) {
                Files.createDirectories(dir.toPath());
            }
            this.attachmentDirectory = dir;
        } catch (IOException | URISyntaxException e) {
            throw new IllegalArgumentException(
                    "Failed to create attachments directory " + attachmentsDirectoryPath
            );
        }
    }
}
