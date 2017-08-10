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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.annotations.VisibleForTesting;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.core.services.FileTransfer;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.NotBlank;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * An implementation of the FileTransferService interface in which the remote locations are on Amazon S3.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Slf4j
public class S3FileTransferImpl implements FileTransfer {

    private final Pattern s3PrefixPattern = Pattern.compile("^s3[n]?://.*$");
    private final Pattern s3BucketPattern = Pattern.compile("^[a-z0-9][a-z0-9.\\-]{1,61}[a-z0-9]$");
    private final Pattern s3KeyPattern = Pattern.compile("^[0-9a-zA-Z!\\-_.*'()]+(?:/[0-9a-zA-Z!\\-_.*'()]+)*$");
    private AmazonS3 s3Client;
    private Timer downloadTimer;
    private Timer uploadTimer;
    private Timer getTimer;

    /**
     * Constructor.
     *
     * @param amazonS3Client An amazon s3 client object
     * @param registry       The metrics registry to use
     */
    public S3FileTransferImpl(@NotNull final AmazonS3 amazonS3Client, @NotNull final Registry registry) {
        this.s3Client = amazonS3Client;
        this.downloadTimer = registry.timer("genie.files.s3.download.timer");
        this.uploadTimer = registry.timer("genie.files.s3.upload.timer");
        this.getTimer = registry.timer("genie.files.s3.getObjectMetadata.timer");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValid(final String fileName) throws GenieException {
        log.debug("Called with file name {}", fileName);
        try {
            getS3Uri(fileName);
            return true;
        } catch (GenieServerException e) {
            log.error("Invalid S3 path {} ({})", fileName, e.getMessage());
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getFile(
        @NotBlank(message = "Source file path cannot be empty.")
        final String srcRemotePath,
        @NotBlank(message = "Destination local path cannot be empty")
        final String dstLocalPath
    ) throws GenieException {
        final long start = System.nanoTime();
        try {
            log.debug("Called with src path {} and destination path {}", srcRemotePath, dstLocalPath);

            final AmazonS3URI s3Uri = getS3Uri(srcRemotePath);
            try {
                this.s3Client.getObject(
                    new GetObjectRequest(s3Uri.getBucket(), s3Uri.getKey()),
                    new File(dstLocalPath)
                );
            } catch (AmazonS3Exception ase) {
                log.error("Error fetching file {} from s3 due to exception {}", srcRemotePath, ase);
                throw new GenieServerException("Error downloading file from s3. Filename: " + srcRemotePath);
            }
        } finally {
            this.downloadTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void putFile(
        @NotBlank(message = "Source local path cannot be empty.")
        final String srcLocalPath,
        @NotBlank(message = "Destination remote path cannot be empty")
        final String dstRemotePath
    ) throws GenieException {
        final long start = System.nanoTime();
        try {
            log.debug("Called with src path {} and destination path {}", srcLocalPath, dstRemotePath);

            final AmazonS3URI s3Uri = getS3Uri(dstRemotePath);
            try {
                this.s3Client.putObject(s3Uri.getBucket(), s3Uri.getKey(), new File(srcLocalPath));
            } catch (AmazonS3Exception ase) {
                log.error("Error posting file {} to s3 due to exception {}", dstRemotePath, ase);
                throw new GenieServerException("Error uploading file to s3. Filename: " + dstRemotePath);
            }
        } finally {
            this.uploadTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getLastModifiedTime(final String path) throws GenieException {
        final long start = System.nanoTime();
        try {
            final AmazonS3URI s3Uri = getS3Uri(path);
            try {
                final ObjectMetadata o = s3Client.getObjectMetadata(s3Uri.getBucket(), s3Uri.getKey());
                return o.getLastModified().getTime();
            } catch (final Exception ase) {
                final String message = String.format("Failed getting the metadata of the s3 file %s", path);
                log.error(message);
                throw new GenieServerException(message, ase);
            }
        } finally {
            this.getTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    @VisibleForTesting
    AmazonS3URI getS3Uri(final String path) throws GenieServerException {
        if (!s3PrefixPattern.matcher(path).matches()) {
            throw new GenieServerException(String.format("Invalid prefix in path for s3 file %s", path));
        }
        // Delegate validation and parsing to AmazonS3URI.
        // However it cannot handle "s3n://", so strip the 'n'
        final String adjustedPath = path.replaceFirst("^s3n://", "s3://");
        final AmazonS3URI uri;
        try {
            uri = new AmazonS3URI(adjustedPath, false);
        } catch (IllegalArgumentException e) {
            throw new GenieServerException(String.format("Invalid path for s3 file %s", path), e);
        }
        if (StringUtils.isBlank(uri.getBucket()) || StringUtils.isBlank(uri.getKey())) {
            throw new GenieServerException(String.format("Invalid blank components in path for s3 file %s", path));
        }
        // Perform further validation on bucket name as per:
        // http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html#bucketnamingrules
        if (!s3BucketPattern.matcher(uri.getBucket()).matches()) {
            throw new GenieServerException(String.format("Invalid bucket in path for s3 file %s", path));
        }
        // Perform further validation on key as per:
        // http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html#object-keys
        if (!s3KeyPattern.matcher(uri.getKey()).matches()) {
            throw new GenieServerException(String.format("Invalid bucket in path for s3 file %s", path));
        }
        return uri;
    }
}
