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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * Implementation of the AttachmentService interface which saves and retrieves attachments from the local filesystem.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Service
@Slf4j
public class FileSystemAttachmentService implements AttachmentService {

    private String attachmentsDirectory;

    /**
     * Constructor.
     *
     * @param attachmentsDirectory The directory to use or null if want to default to system temp directory
     */
    @Autowired
    public FileSystemAttachmentService(
        @Value("${genie.jobs.attachments.dir:#{null}}") final String attachmentsDirectory
    ) {
        this.attachmentsDirectory = attachmentsDirectory;
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
        final File attachment = new File(this.getAttachmentDirectory(), jobId + "/" + filename);
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
        final File source = new File(this.getAttachmentDirectory(), jobId);
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
        final File jobDir = new File(this.getAttachmentDirectory(), jobId);
        if (jobDir.exists()) {
            try {
                FileUtils.deleteDirectory(jobDir);
            } catch (final IOException ioe) {
                throw new GenieServerException(ioe);
            }
        }
    }

    private File getAttachmentDirectory() throws GenieException {
        if (this.attachmentsDirectory == null) {
            this.attachmentsDirectory = System.getProperty("java.io.tmpdir") + "/genie/attachments";
        }
        final File dir = new File(this.attachmentsDirectory);
        if (dir.exists() && !dir.isDirectory()) {
            throw new GenieServerException(
                "Attachment directory configured isn't actually a directory: " + this.attachmentsDirectory
            );
        }
        return dir;
    }
}
