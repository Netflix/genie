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
package com.netflix.genie.web.services.impl

import com.google.common.collect.Sets
import com.netflix.genie.common.exceptions.GenieNotFoundException
import com.netflix.genie.common.internal.dto.DirectoryManifest
import com.netflix.genie.common.internal.exceptions.unchecked.GenieRuntimeException
import com.netflix.genie.common.util.GenieObjectMapper
import com.netflix.genie.web.data.services.JobPersistenceService
import com.netflix.genie.web.exceptions.checked.JobDirectoryManifestNotFoundException
import com.netflix.genie.web.exceptions.checked.JobNotArchivedException
import com.netflix.genie.web.exceptions.checked.JobNotFoundException
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.springframework.core.io.Resource
import org.springframework.core.io.ResourceLoader
import spock.lang.Specification

import java.nio.charset.StandardCharsets
import java.time.Instant

/**
 * Specifications for {@link ArchivedJobServiceImpl}.
 *
 * @author tgianos
 */
class ArchivedJobServiceImplSpec extends Specification {

    JobPersistenceService jobPersistenceService
    ResourceLoader resourceLoader
    MeterRegistry meterRegistry
    ArchivedJobServiceImpl service

    def setup() {
        this.jobPersistenceService = Mock(JobPersistenceService)
        this.resourceLoader = Mock(ResourceLoader)
        this.meterRegistry = new SimpleMeterRegistry()
        this.service = new ArchivedJobServiceImpl(this.jobPersistenceService, this.resourceLoader, this.meterRegistry)
    }

    def "expected exceptions are thrown when conditions exist"() {
        def jobId = UUID.randomUUID().toString()
        def archiveLocation = "file:/tmp/genie/jobs/archives/" + jobId
        def manifestResource = Mock(Resource)
        def badInputStream = new ByteArrayInputStream(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8))

        when: "A job doesn't exist"
        this.service.getArchivedJobMetadata(jobId)

        then: "A job not found exception is thrown"
        1 * this.jobPersistenceService.getJobArchiveLocation(jobId) >> {
            throw new GenieNotFoundException("job not found")
        }
        thrown(JobNotFoundException)

        when: "The job exists but doesn't have an archive location associated with it"
        this.service.getArchivedJobMetadata(jobId)

        then: "A job not archived exception is thrown"
        1 * this.jobPersistenceService.getJobArchiveLocation(jobId) >> Optional.empty()
        thrown(JobNotArchivedException)

        when: "When the archive location isn't valid"
        this.service.getArchivedJobMetadata(jobId)

        then: "A runtime exception is thrown"
        1 * this.jobPersistenceService.getJobArchiveLocation(jobId) >> Optional.of("I'm not a valid URI")
        thrown(GenieRuntimeException)

        when: "The manifest doesn't exist in the expected location"
        this.service.getArchivedJobMetadata(jobId)

        then: "A manifest not found exception is thrown"
        1 * this.jobPersistenceService.getJobArchiveLocation(jobId) >> Optional.of(archiveLocation)
        1 * this.resourceLoader.getResource(_ as String) >> manifestResource
        1 * manifestResource.exists() >> false
        thrown(JobDirectoryManifestNotFoundException)

        when: "The manifest can't be deserialized"
        this.service.getArchivedJobMetadata(jobId)

        then: "A runtime exception is thrown"
        1 * this.jobPersistenceService.getJobArchiveLocation(jobId) >> Optional.of(archiveLocation)
        1 * this.resourceLoader.getResource(_ as String) >> manifestResource
        1 * manifestResource.exists() >> true
        1 * manifestResource.getInputStream() >> badInputStream
        thrown(GenieRuntimeException)

        cleanup:
        try {
            badInputStream.close()
        } catch (IOException ignored) {
            // well we tried
        }
    }

    def "Successfully retrieving a manifest returns a valid metadata object"() {
        def jobId = UUID.randomUUID().toString()
        def archiveLocation = "file:/tmp/genie/jobs/archives/" + jobId + "/"
        def manifestResource = Mock(Resource)
        def manifest = new DirectoryManifest(
            Sets.newHashSet(
                new DirectoryManifest.ManifestEntry(
                    "",
                    "stdout",
                    Instant.now(),
                    Instant.now(),
                    Instant.now(),
                    false,
                    52L,
                    null,
                    null,
                    null,
                    Sets.newHashSet()
                )
            )
        )
        def manifestByteStream = new ByteArrayInputStream(
            GenieObjectMapper.getMapper().writeValueAsString(manifest).getBytes(StandardCharsets.UTF_8)
        )

        when:
        def metadata = this.service.getArchivedJobMetadata(jobId)

        then:
        1 * this.jobPersistenceService.getJobArchiveLocation(jobId) >> Optional.of(archiveLocation)
        1 * this.resourceLoader.getResource(_ as String) >> manifestResource
        1 * manifestResource.exists() >> true
        1 * manifestResource.getInputStream() >> manifestByteStream
        metadata.getJobId() == jobId
        metadata.getManifest() == manifest
        metadata.getJobDirectoryRoot() == new URI(archiveLocation)

        cleanup:
        try {
            manifestByteStream.close()
        } catch (IOException ignored) {
            // oh well
        }
    }
}
