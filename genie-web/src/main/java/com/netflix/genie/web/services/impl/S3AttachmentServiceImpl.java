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
import com.netflix.genie.common.internal.aws.s3.S3ClientFactory;
import com.netflix.genie.web.exceptions.checked.AttachmentTooLargeException;
import com.netflix.genie.web.exceptions.checked.SaveAttachmentException;
import com.netflix.genie.web.properties.AttachmentServiceProperties;
import com.netflix.genie.web.services.AttachmentService;
import com.netflix.genie.web.util.MetricsUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.io.Resource;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import jakarta.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of the AttachmentService interface which saves attachments to AWS S3.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class S3AttachmentServiceImpl implements AttachmentService {

    private static final String METRICS_PREFIX = "genie.jobs.attachments.s3";
    private static final String COUNT_DISTRIBUTION = METRICS_PREFIX + ".count.distribution";
    private static final String LARGEST_SIZE_DISTRIBUTION = METRICS_PREFIX + ".largest.distribution";
    private static final String TOTAL_SIZE_DISTRIBUTION = METRICS_PREFIX + ".totalSize.distribution";
    private static final String SAVE_TIMER = METRICS_PREFIX + ".upload.timer";
    private static final Set<URI> EMPTY_SET = ImmutableSet.of();
    private static final String SLASH = "/";
    private static final String S3 = "s3";
    private final S3ClientFactory s3ClientFactory;
    private final AttachmentServiceProperties properties;
    private final MeterRegistry meterRegistry;
    private final S3Uri s3BaseURI;

    /**
     * Constructor.
     *
     * @param s3ClientFactory             the s3 client factory
     * @param attachmentServiceProperties the service properties
     * @param meterRegistry               the meter registry
     */
    public S3AttachmentServiceImpl(
        final S3ClientFactory s3ClientFactory,
        final AttachmentServiceProperties attachmentServiceProperties,
        final MeterRegistry meterRegistry
    ) {
        this.s3ClientFactory = s3ClientFactory;
        this.properties = attachmentServiceProperties;
        this.meterRegistry = meterRegistry;
        this.s3BaseURI = this.s3ClientFactory.getS3Uri(attachmentServiceProperties.getLocationPrefix());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<URI> saveAttachments(
        @Nullable final String jobId,
        final Set<Resource> attachments
    ) throws SaveAttachmentException {

        // Track number of attachments, including zeroes
        this.meterRegistry.summary(COUNT_DISTRIBUTION).record(attachments.size());

        log.debug("Saving {} attachments for job request with id: {}", attachments.size(), jobId);

        if (attachments.size() == 0) {
            return EMPTY_SET;
        }

        // Check for attachment size limits
        this.checkLimits(attachments);

        final long start = System.nanoTime();
        final Set<Tag> tags = Sets.newHashSet();
        try {
            // Upload all to S3
            final Set<URI> attachmentURIs = this.uploadAllAttachments(jobId, attachments);
            MetricsUtils.addSuccessTags(tags);
            return attachmentURIs;
        } catch (SaveAttachmentException e) {
            log.error("Failed to save attachments (requested job id: {}): {}", jobId, e.getMessage(), e);
            MetricsUtils.addFailureTagsWithException(tags, e);
            throw e;
        } finally {
            this.meterRegistry
                .timer(SAVE_TIMER, tags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    private void checkLimits(final Set<Resource> attachments) throws SaveAttachmentException {

        final long singleSizeLimit = this.properties.getMaxSize().toBytes();
        final long totalSizeLimit = this.properties.getMaxTotalSize().toBytes();

        long totalSize = 0;
        long largestSize = 0;
        for (final Resource attachment : attachments) {
            final String filename = attachment.getFilename();

            final long attachmentSize;

            try {
                attachmentSize = attachment.contentLength();
            } catch (IOException e) {
                throw new SaveAttachmentException(
                    "Failed to get size of attachment: " + filename + ": " + e.getMessage(),
                    e
                );
            }

            if (attachmentSize > largestSize) {
                largestSize = attachmentSize;
            }
            totalSize += attachmentSize;
        }


        if (largestSize > singleSizeLimit) {
            throw new AttachmentTooLargeException(
                "Size of attachment exceeds the maximum allowed"
                    + " (" + largestSize + " > " + singleSizeLimit + ")"
            );
        }

        if (totalSize > totalSizeLimit) {
            throw new AttachmentTooLargeException(
                "Total size of attachments exceeds the maximum allowed"
                    + " (" + totalSize + " > " + totalSizeLimit + ")"
            );
        }

        this.meterRegistry.summary(LARGEST_SIZE_DISTRIBUTION).record(largestSize);
        this.meterRegistry.summary(TOTAL_SIZE_DISTRIBUTION).record(totalSize);
    }

    private Set<URI> uploadAllAttachments(
        @Nullable final String jobId,
        final Set<Resource> attachments
    ) throws SaveAttachmentException {
        final S3Client s3Client = this.s3ClientFactory.getClient(this.s3BaseURI);
        final String bundleId = UUID.randomUUID().toString();
        final String commonPrefix = this.s3BaseURI.key().orElse("") + SLASH + bundleId + SLASH;

        log.debug(
            "Uploading {} attachments for job request with id {} to: {}",
            attachments.size(),
            jobId,
            commonPrefix
        );

        final Set<URI> attachmentURIs = Sets.newHashSet();

        for (final Resource attachment : attachments) {
            final String filename = attachment.getFilename();
            if (StringUtils.isBlank(filename)) {
                throw new SaveAttachmentException("Attachment filename is missing");
            }
            final String objectBucket = this.s3BaseURI.bucket().get();
            final String objectKey = commonPrefix + filename;

            URI attachmentURI = null;

            try {
                attachmentURI = new URI(S3, objectBucket, SLASH + objectKey, null);

                final long contentLength = attachment.contentLength();

                try (InputStream inputStream = attachment.getInputStream()) {
                    s3Client.putObject(
                        PutObjectRequest.builder()
                            .bucket(objectBucket)
                            .key(objectKey)
                            .contentLength(contentLength)
                            .build(),
                        RequestBody.fromInputStream(inputStream, contentLength)
                    );

                    attachmentURIs.add(attachmentURI);
                    log.debug("Successfully uploaded attachment: {}", attachmentURI);
                }
            } catch (IllegalStateException e) {
                log.error("Content length mismatch for attachment {}: {}", filename, e.getMessage());
                throw new SaveAttachmentException(
                    "Content length mismatch for attachment " + filename + ": " + e.getMessage(), e
                );
            } catch (IOException | SdkClientException | URISyntaxException e) {
                log.error("Failed to upload attachment {}: {}", filename, e.getMessage());
                throw new SaveAttachmentException(
                    "Failed to upload attachment: " + (attachmentURI != null ? attachmentURI : filename)
                        + " - "
                        + e.getMessage(),
                    e
                );
            }
        }

        return attachmentURIs;
    }
}
