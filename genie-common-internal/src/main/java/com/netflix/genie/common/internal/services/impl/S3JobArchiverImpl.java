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

import com.netflix.genie.common.internal.aws.s3.S3TransferManagerFactory;
import com.netflix.genie.common.internal.exceptions.checked.JobArchiveException;
import com.netflix.genie.common.internal.services.JobArchiver;
import jakarta.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedFileUpload;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

import java.io.File;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Implementation of {@link JobArchiver} for S3 destinations.
 */
@Slf4j
public class S3JobArchiverImpl implements JobArchiver {

    private final S3TransferManagerFactory transferManagerFactory;

    /**
     * Constructor.
     *
     * @param transferManagerFactory The factory to use to get S3 transfer manager instances for a given S3 bucket.
     */
    public S3JobArchiverImpl(final S3TransferManagerFactory transferManagerFactory) {
        this.transferManagerFactory = transferManagerFactory;
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

        try {
            final S3Uri s3Uri = this.transferManagerFactory.getS3Uri(target);
            final String bucketName = s3Uri.bucket().orElseThrow(() ->
                new IllegalArgumentException("No bucket specified in URI: " + uriString));
            final String keyPrefix = s3Uri.key().orElse("");

            log.debug(
                "{} is a valid S3 location. Proceeding to archive {} to location: {}",
                uriString,
                directory,
                uriString
            );

            final S3TransferManager transferManager = this.transferManagerFactory.getTransferManager(s3Uri);

            // Create a list of upload futures
            List<CompletableFuture<CompletedFileUpload>> uploadFutures = filesList.stream()
                .map(file -> {
                    String key = keyPrefix + "/" + directory.relativize(file.toPath()).toString();
                    UploadFileRequest uploadFileRequest = UploadFileRequest.builder()
                        .putObjectRequest(b -> b.bucket(bucketName).key(key))
                        .source(file.toPath())
                        .build();

                    FileUpload fileUpload = transferManager.uploadFile(uploadFileRequest);
                    return fileUpload.completionFuture();
                })
                .toList();

            // Wait for all uploads to complete
            CompletableFuture.allOf(uploadFutures.toArray(new CompletableFuture[0])).join();

            // Check for any failures
            for (CompletableFuture<CompletedFileUpload> future : uploadFutures) {
                CompletedFileUpload result = future.get();
                if (result.response().sdkHttpResponse().isSuccessful()) {
                    log.debug("Successfully uploaded file: {}", result.response().eTag());
                } else {
                    log.error("Failed to upload file: {}", result.response().sdkHttpResponse().statusText());
                    return false;
                }
            }

            return true;
        } catch (final Exception e) {
            log.error("Error archiving to S3 location: {} ", uriString, e);
            throw new JobArchiveException("Error archiving " + directory.toString(), e);
        }
    }
}
