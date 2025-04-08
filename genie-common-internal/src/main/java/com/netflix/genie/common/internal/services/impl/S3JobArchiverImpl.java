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

import com.netflix.genie.common.internal.aws.s3.S3ClientFactory;
import com.netflix.genie.common.internal.exceptions.checked.JobArchiveException;
import com.netflix.genie.common.internal.services.JobArchiver;
import jakarta.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.S3Utilities;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.core.sync.RequestBody;

import java.io.File;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;

/**
 * Implementation of {@link JobArchiver} for S3 destinations.
 */
@Slf4j
public class S3JobArchiverImpl implements JobArchiver {

    private final S3ClientFactory s3ClientFactory;

    private final S3Utilities s3Utilities;

    /**
     * Constructor.
     *
     * @param s3ClientFactory The factory to use to get S3 client instances for a given S3 bucket.
     */
    public S3JobArchiverImpl(final S3ClientFactory s3ClientFactory) {
        this.s3ClientFactory = s3ClientFactory;
        this.s3Utilities = S3Utilities.builder().build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean archiveDirectory(
        @NotNull final Path directory,
        final List<File> filesList,
        @NotNull final URI target
    ) throws JobArchiveException {
        final String uriString = target.toString();
        final S3Uri s3Uri = s3Utilities.parseUri(target);

        final String bucketName = String.valueOf(s3Uri.bucket());
        final String keyPrefix = String.valueOf(s3Uri.key());

        log.debug(
            "{} is a valid S3 location. Proceeding to archive {} to location: {}",
            uriString,
            directory,
            uriString
        );

        try {
            final S3Client s3Client = this.s3ClientFactory.getClient(s3Uri);

            for (File file : filesList) {
                String key = keyPrefix + "/" + directory.relativize(file.toPath()).toString();
                PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();

                s3Client.putObject(putObjectRequest, RequestBody.fromFile(file.toPath()));
            }
            return true;
        } catch (final Exception e) {
            log.error("Error archiving to S3 location: {} ", uriString, e);
            throw new JobArchiveException("Error archiving " + directory.toString(), e);
        }
    }

    private String extractBucketName(URI uri) {
        // Implement logic to extract bucket name from URI
        String host = uri.getHost();
        if (host == null) {
            throw new IllegalArgumentException("Invalid S3 URI: " + uri);
        }
        return host;
    }

    private String extractKeyPrefix(URI uri) {
        // Implement logic to extract key prefix from URI
        String path = uri.getPath();
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        return path;
    }
}
