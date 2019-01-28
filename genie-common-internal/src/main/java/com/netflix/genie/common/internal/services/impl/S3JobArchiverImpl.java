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
package com.netflix.genie.common.internal.services.impl;

import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.netflix.genie.common.internal.aws.s3.S3ClientFactory;
import com.netflix.genie.common.internal.exceptions.JobArchiveException;
import com.netflix.genie.common.internal.services.JobArchiveService;
import com.netflix.genie.common.internal.services.JobArchiver;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotNull;
import java.net.URI;
import java.nio.file.Path;

/**
 * Implementation of {@link JobArchiveService} for S3 destinations.
 *
 * @author standon
 * @author tgianos
 * @since 4.0.0
 */
@Slf4j
public class S3JobArchiverImpl implements JobArchiver {

    private final S3ClientFactory s3ClientFactory;

    /**
     * Constructor.
     *
     * @param s3ClientFactory The factory to use to get S3 client instances for a given S3 bucket.
     */
    public S3JobArchiverImpl(final S3ClientFactory s3ClientFactory) {
        this.s3ClientFactory = s3ClientFactory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean archiveDirectory(
        @NotNull final Path directory,
        @NotNull final URI target
    ) throws JobArchiveException {
        final String uriString = target.toString();
        final AmazonS3URI s3URI;
        try {
            s3URI = new AmazonS3URI(target);
        } catch (final IllegalArgumentException iae) {
            log.debug("{} is not a valid S3 URI", uriString);
            return false;
        }
        final String directoryString = directory.toString();
        log.debug(
            "{} is a valid S3 location. Proceeding to archiving {} to location: {}",
            directoryString,
            uriString
        );

        try {
            final TransferManager transferManager = this.s3ClientFactory.getTransferManager(s3URI);
            final MultipleFileUpload upload = transferManager.uploadDirectory(
                s3URI.getBucket(),
                s3URI.getKey(),
                directory.toFile(),
                true
            );

            upload.waitForCompletion();
            return true;
        } catch (final Exception e) {
            log.error("Error archiving to S3 location: {} ", uriString, e);
            throw new JobArchiveException("Error archiving " + directoryString, e);
        }
    }
}
