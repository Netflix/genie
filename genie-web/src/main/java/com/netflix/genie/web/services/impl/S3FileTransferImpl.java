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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.netflix.genie.common.exceptions.GenieBadRequestException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.web.properties.S3FileTransferProperties;
import com.netflix.genie.web.services.FileTransfer;
import com.netflix.genie.web.util.MetricsUtils;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.io.File;
import java.util.Set;
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

    static final String DOWNLOAD_TIMER_NAME = "genie.files.s3.download.timer";
    static final String UPLOAD_TIMER_NAME = "genie.files.s3.upload.timer";
    static final String STRICT_VALIDATION_COUNTER_NAME = "genie.files.s3.failStrictValidation.counter";
    private static final String GET_METADATA_TIMER_NAME = "genie.files.s3.getObjectMetadata.timer";
    private static final Pattern S3_PREFIX_PATTERN = Pattern.compile("^s3[n]?://.*$");
    // http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html#bucketnamingrules
    private static final Pattern S3_BUCKET_PATTERN = Pattern.compile("^[a-z0-9][a-z0-9.\\-]{1,61}[a-z0-9]$");
    // http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html#object-keys
    private static final Pattern S3_KEY_PATTERN
        = Pattern.compile("^[0-9a-zA-Z!\\-_.*'()]+(?:/[0-9a-zA-Z!\\-_.*'()]+)*$");
    private final MeterRegistry registry;
    private final AmazonS3 amazonS3;
    private final S3FileTransferProperties s3FileTransferProperties;
    private final Counter urlFailingStrictValidationCounter;

    /**
     * Constructor.
     *
     * @param amazonS3                 The S3 client to use
     * @param registry                 The metrics registry to use
     * @param s3FileTransferProperties Options
     */
    public S3FileTransferImpl(
        @NotNull final AmazonS3 amazonS3,
        @NotNull final MeterRegistry registry,
        @NotNull final S3FileTransferProperties s3FileTransferProperties
    ) {
        this.amazonS3 = amazonS3;
        this.registry = registry;
        this.urlFailingStrictValidationCounter = registry.counter(STRICT_VALIDATION_COUNTER_NAME);
        this.s3FileTransferProperties = s3FileTransferProperties;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValid(final String fileName) throws GenieException {
        log.debug("Called with file name {}", fileName);
        try {
            this.getS3Uri(fileName);
            return true;
        } catch (final GenieBadRequestException e) {
            log.error("Invalid S3 path {} ({})", fileName, e.getMessage());
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getFile(
        @NotBlank(message = "Source file path cannot be empty.") final String srcRemotePath,
        @NotBlank(message = "Destination local path cannot be empty") final String dstLocalPath
    ) throws GenieException {
        final long start = System.nanoTime();
        final Set<Tag> tags = Sets.newHashSet();
        try {
            log.debug("Called with src path {} and destination path {}", srcRemotePath, dstLocalPath);

            final AmazonS3URI s3Uri = getS3Uri(srcRemotePath);
            try {
                this.amazonS3.getObject(
                    new GetObjectRequest(s3Uri.getBucket(), s3Uri.getKey()),
                    new File(dstLocalPath)
                );
            } catch (final AmazonS3Exception ase) {
                log.error("Error fetching file {} from s3 due to exception {}", srcRemotePath, ase.toString());
                throw new GenieServerException("Error downloading file from s3. Filename: " + srcRemotePath, ase);
            }
            MetricsUtils.addSuccessTags(tags);
        } catch (Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw t;
        } finally {
            this.registry.timer(DOWNLOAD_TIMER_NAME, tags).record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void putFile(
        @NotBlank(message = "Source local path cannot be empty.") final String srcLocalPath,
        @NotBlank(message = "Destination remote path cannot be empty") final String dstRemotePath
    ) throws GenieException {
        final long start = System.nanoTime();
        final Set<Tag> tags = Sets.newHashSet();
        try {
            log.debug("Called with src path {} and destination path {}", srcLocalPath, dstRemotePath);

            final AmazonS3URI s3Uri = getS3Uri(dstRemotePath);
            try {
                this.amazonS3.putObject(s3Uri.getBucket(), s3Uri.getKey(), new File(srcLocalPath));
            } catch (final AmazonS3Exception ase) {
                log.error("Error posting file {} to s3 due to exception {}", dstRemotePath, ase.toString());
                throw new GenieServerException("Error uploading file to s3. Filename: " + dstRemotePath, ase);
            }
            MetricsUtils.addSuccessTags(tags);
        } catch (Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw t;
        } finally {
            this.registry.timer(UPLOAD_TIMER_NAME, tags).record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getLastModifiedTime(final String path) throws GenieException {
        final long start = System.nanoTime();
        final long lastModTime;
        final Set<Tag> tags = Sets.newHashSet();
        try {
            final AmazonS3URI s3Uri = this.getS3Uri(path);
            try {
                final ObjectMetadata o = this.amazonS3.getObjectMetadata(s3Uri.getBucket(), s3Uri.getKey());
                lastModTime = o.getLastModified().getTime();
            } catch (final Exception ase) {
                final String message = String.format("Failed getting the metadata of the s3 file %s", path);
                log.error(message);
                throw new GenieServerException(message, ase);
            }
            MetricsUtils.addSuccessTags(tags);
        } catch (Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw t;
        } finally {
            this.registry.timer(GET_METADATA_TIMER_NAME, tags).record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
        return lastModTime;
    }

    @VisibleForTesting
    AmazonS3URI getS3Uri(final String path) throws GenieBadRequestException {
        if (!S3_PREFIX_PATTERN.matcher(path).matches()) {
            throw new GenieBadRequestException(String.format("Invalid prefix in path for s3 file %s", path));
        }
        // Delegate validation and parsing to AmazonS3URI.
        // However it cannot handle "s3n://", so strip the 'n'
        final String adjustedPath = path.replaceFirst("^s3n://", "s3://");
        final AmazonS3URI uri;
        try {
            uri = new AmazonS3URI(adjustedPath, false);
        } catch (IllegalArgumentException e) {
            throw new GenieBadRequestException(String.format("Invalid path for s3 file %s", path), e);
        }
        if (StringUtils.isBlank(uri.getBucket()) || StringUtils.isBlank(uri.getKey())) {
            throw new GenieBadRequestException(String.format("Invalid blank components in path for s3 file %s", path));
        }

        final boolean bucketPassesStrictValidation = S3_BUCKET_PATTERN.matcher(uri.getBucket()).matches();
        final boolean keyPassesStrictValidation = S3_KEY_PATTERN.matcher(uri.getKey()).matches();
        // URL fails strict validation check!
        if (!bucketPassesStrictValidation || !keyPassesStrictValidation) {
            if (this.s3FileTransferProperties.isStrictUrlCheckEnabled()) {
                throw new GenieBadRequestException(
                    String.format(
                        "Invalid bucket %s in path for s3 file %s",
                        uri.getBucket(),
                        path
                    )
                );
            } else {
                log.warn("S3 URL fails strict validation: \"{}\"", path);
                this.urlFailingStrictValidationCounter.increment();
            }
        }
        return uri;
    }
}
