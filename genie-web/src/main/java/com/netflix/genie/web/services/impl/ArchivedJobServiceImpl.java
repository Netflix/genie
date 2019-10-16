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
package com.netflix.genie.web.services.impl;

import com.google.common.collect.Sets;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.internal.dto.DirectoryManifest;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieRuntimeException;
import com.netflix.genie.common.internal.services.JobArchiveService;
import com.netflix.genie.common.util.GenieObjectMapper;
import com.netflix.genie.web.data.services.JobPersistenceService;
import com.netflix.genie.web.dtos.ArchivedJobMetadata;
import com.netflix.genie.web.exceptions.checked.JobDirectoryManifestNotFoundException;
import com.netflix.genie.web.exceptions.checked.JobNotArchivedException;
import com.netflix.genie.web.exceptions.checked.JobNotFoundException;
import com.netflix.genie.web.services.ArchivedJobService;
import com.netflix.genie.web.util.MetricsUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;

/**
 * Default implementation of {@link ArchivedJobService}.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Slf4j
public class ArchivedJobServiceImpl implements ArchivedJobService {
    private static final String SLASH = "/";
    private static final String GET_ARCHIVED_JOB_METADATA_METRIC_NAME
        = "genie.web.services.archivedJobService.getArchivedJobMetadata.timer";

    private final JobPersistenceService jobPersistenceService;
    private final ResourceLoader resourceLoader;
    private final MeterRegistry meterRegistry;

    /**
     * Constructor.
     *
     * @param jobPersistenceService The {@link JobPersistenceService} implementation to use
     * @param resourceLoader        The {@link ResourceLoader} used to get resources
     * @param meterRegistry         The {@link MeterRegistry} used to collect metrics
     */
    public ArchivedJobServiceImpl(
        final JobPersistenceService jobPersistenceService,
        final ResourceLoader resourceLoader,
        final MeterRegistry meterRegistry
    ) {
        this.jobPersistenceService = jobPersistenceService;
        this.resourceLoader = resourceLoader;
        this.meterRegistry = meterRegistry;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ArchivedJobMetadata getArchivedJobMetadata(
        final String jobId
    ) throws JobNotFoundException, JobNotArchivedException, JobDirectoryManifestNotFoundException {
        final Instant startTime = Instant.now();
        log.debug("Attempting to fetch archived job metadata for job {}", jobId);
        final Set<Tag> tags = Sets.newHashSet();

        try {
            final String archiveLocation;

            try {
                archiveLocation = this.jobPersistenceService
                    .getJobArchiveLocation(jobId)
                    .orElseThrow(() -> new JobNotArchivedException("Job " + jobId + " wasn't archived"));
            } catch (final GenieNotFoundException nfe) {
                throw new JobNotFoundException(nfe);
            }

            final URI jobDirectoryRoot;

            try {
                jobDirectoryRoot = new URI(archiveLocation + SLASH).normalize();
            } catch (final URISyntaxException e) {
                throw new GenieRuntimeException("Unable to create URI from archive location: " + archiveLocation, e);
            }

            final URI manifestLocation;
            if (StringUtils.isBlank(JobArchiveService.MANIFEST_DIRECTORY)) {
                manifestLocation = jobDirectoryRoot.resolve(JobArchiveService.MANIFEST_NAME).normalize();
            } else {
                manifestLocation = jobDirectoryRoot
                    .resolve(JobArchiveService.MANIFEST_DIRECTORY + SLASH)
                    .resolve(JobArchiveService.MANIFEST_NAME)
                    .normalize();
            }

            final Resource manifestResource = this.resourceLoader.getResource(manifestLocation.toString());
            if (!manifestResource.exists()) {
                throw new JobDirectoryManifestNotFoundException(
                    "No job directory manifest exists at " + manifestLocation
                );
            }

            final DirectoryManifest manifest;
            try (InputStream manifestData = manifestResource.getInputStream()) {
                manifest = GenieObjectMapper
                    .getMapper()
                    .readValue(manifestData, DirectoryManifest.class);
            } catch (final IOException e) {
                throw new GenieRuntimeException("Unable to read job directory manifest from " + manifestLocation, e);
            }

            MetricsUtils.addSuccessTags(tags);
            return new ArchivedJobMetadata(jobId, manifest, jobDirectoryRoot);
        } catch (final Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw t;
        } finally {
            log.debug("Finished attempting to fetch archived job metadata for job {}", jobId);
            this.meterRegistry
                .timer(GET_ARCHIVED_JOB_METADATA_METRIC_NAME, tags)
                .record(Duration.between(startTime, Instant.now()));
        }
    }
}
