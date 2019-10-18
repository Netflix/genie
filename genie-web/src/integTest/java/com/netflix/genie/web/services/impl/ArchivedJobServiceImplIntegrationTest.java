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

import com.google.common.annotations.VisibleForTesting;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.internal.dto.DirectoryManifest;
import com.netflix.genie.common.util.GenieObjectMapper;
import com.netflix.genie.web.data.services.JobPersistenceService;
import com.netflix.genie.web.dtos.ArchivedJobMetadata;
import com.netflix.genie.web.exceptions.checked.JobDirectoryManifestNotFoundException;
import com.netflix.genie.web.exceptions.checked.JobNotArchivedException;
import com.netflix.genie.web.exceptions.checked.JobNotFoundException;
import com.netflix.genie.web.services.ArchivedJobService;
import com.netflix.genie.web.spring.autoconfigure.RetryAutoConfiguration;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import java.net.URI;
import java.util.Optional;
import java.util.UUID;

/**
 * Integration tests for {@link ArchivedJobServiceImpl}.
 *
 * @author tgianos
 * @since 4.0.0
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
    classes = {
        ArchivedJobServiceImplIntegrationTest.ArchivedJobServiceConfig.class,
        RetryAutoConfiguration.class
    },
    loader = AnnotationConfigContextLoader.class
)
@TestPropertySource(
    properties = {
        ArchivedJobServiceImpl.GET_METADATA_NUM_RETRY_PROPERTY_NAME
            + "="
            + ArchivedJobServiceImplIntegrationTest.NUM_GET_RETRIES
    }
)
public class ArchivedJobServiceImplIntegrationTest {

    @VisibleForTesting
    static final int NUM_GET_RETRIES = 2;
    private static final String JOB_ID = UUID.randomUUID().toString();
    private static final String ARCHIVE_LOCATION = "file:/tmp/" + UUID.randomUUID().toString();

    @Autowired
    private ArchivedJobService archivedJobService;

    @MockBean
    private JobPersistenceService jobPersistenceService;

    /**
     * Make sure that when the correct exception is thrown it retries the expected number of times.
     *
     * @throws Exception On unexpected error
     */
    @Test
    public void testRetryOnMissingManifest() throws Exception {
        Mockito
            .when(this.jobPersistenceService.getJobArchiveLocation(JOB_ID))
            .thenReturn(Optional.of(ARCHIVE_LOCATION));

        try {
            this.archivedJobService.getArchivedJobMetadata(JOB_ID);
        } catch (final JobDirectoryManifestNotFoundException ignored) {
            Mockito
                .verify(this.jobPersistenceService, Mockito.times(NUM_GET_RETRIES))
                .getJobArchiveLocation(JOB_ID);
        }
    }

    /**
     * Make sure that when the job isn't found the method isn't retried.
     *
     * @throws Exception On unexpected error
     */
    @Test
    public void testNoRetryOnJobNotFound() throws Exception {
        Mockito
            .when(this.jobPersistenceService.getJobArchiveLocation(JOB_ID))
            .thenThrow(new GenieNotFoundException("blah"));

        try {
            this.archivedJobService.getArchivedJobMetadata(JOB_ID);
        } catch (final JobNotFoundException ignored) {
            Mockito
                .verify(this.jobPersistenceService, Mockito.times(1))
                .getJobArchiveLocation(JOB_ID);
        }
    }

    /**
     * Make sure that when a runtime exception is thrown it doesn't retry.
     *
     * @throws GenieNotFoundException If this happens something really bizarre happened
     */
    @Test
    public void testNoRetryOnUnexpectedException() throws GenieNotFoundException {
        Mockito
            .when(this.jobPersistenceService.getJobArchiveLocation(JOB_ID))
            .thenReturn(Optional.of("Not a valid URI"));

        try {
            this.archivedJobService.getArchivedJobMetadata(JOB_ID);
        } catch (final Exception ignored) {
            Mockito
                .verify(this.jobPersistenceService, Mockito.times(1))
                .getJobArchiveLocation(JOB_ID);
        }
    }

    /**
     * Make sure that when the job wasn't archived the method doesn't retry.
     *
     * @throws Exception On unexpected error
     */
    @Test
    public void testNoRetryOnJobNotArchived() throws Exception {
        Mockito
            .when(this.jobPersistenceService.getJobArchiveLocation(JOB_ID))
            .thenReturn(Optional.empty());

        try {
            this.archivedJobService.getArchivedJobMetadata(JOB_ID);
        } catch (final JobNotArchivedException ignored) {
            Mockito
                .verify(this.jobPersistenceService, Mockito.times(1))
                .getJobArchiveLocation(JOB_ID);
        }
    }

    /**
     * Make sure requesting a valid manifest successfully returns the right data.
     *
     * @throws Exception On unexpected error
     */
    @Test
    public void canSuccessfullyGetArchivedJobMetadata() throws Exception {
        final String archiveLocation = new ClassPathResource("archivedJobServiceImpl", this.getClass())
            .getURI()
            .toString();
        final URI expectedJobDirectoryRoot = new URI(archiveLocation + "/").normalize();

        final DirectoryManifest expectedManifest = GenieObjectMapper.getMapper().readValue(
            new ClassPathResource("archivedJobServiceImpl/genie/manifest.json", this.getClass()).getFile(),
            DirectoryManifest.class
        );

        Mockito
            .when(this.jobPersistenceService.getJobArchiveLocation(JOB_ID))
            .thenReturn(Optional.of(archiveLocation));

        final ArchivedJobMetadata metadata = this.archivedJobService.getArchivedJobMetadata(JOB_ID);

        Assertions.assertThat(metadata.getJobId()).isEqualTo(JOB_ID);
        Assertions.assertThat(metadata.getManifest()).isEqualTo(expectedManifest);
        Assertions.assertThat(metadata.getJobDirectoryRoot()).isEqualTo(expectedJobDirectoryRoot);
        Mockito
            .verify(this.jobPersistenceService, Mockito.times(1))
            .getJobArchiveLocation(JOB_ID);
    }

    @Configuration
    static class ArchivedJobServiceConfig {

        @Bean
        public ArchivedJobServiceImpl archivedJobService(
            final JobPersistenceService jobPersistenceService,
            final ResourceLoader resourceLoader,
            final MeterRegistry meterRegistry
        ) {
            return new ArchivedJobServiceImpl(jobPersistenceService, resourceLoader, meterRegistry);
        }

        @Bean
        public MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }
    }
}
