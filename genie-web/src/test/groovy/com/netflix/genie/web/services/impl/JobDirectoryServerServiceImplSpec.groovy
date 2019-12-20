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

import com.netflix.genie.common.dto.JobStatus
import com.netflix.genie.common.exceptions.GenieNotFoundException
import com.netflix.genie.common.exceptions.GeniePreconditionException
import com.netflix.genie.common.exceptions.GenieServerException
import com.netflix.genie.common.exceptions.GenieServerUnavailableException
import com.netflix.genie.common.internal.dto.DirectoryManifest
import com.netflix.genie.common.internal.services.JobDirectoryManifestCreatorService
import com.netflix.genie.web.agent.resources.AgentFileProtocolResolver
import com.netflix.genie.web.agent.services.AgentFileStreamService
import com.netflix.genie.web.data.services.JobPersistenceService
import com.netflix.genie.web.exceptions.checked.JobDirectoryManifestNotFoundException
import com.netflix.genie.web.exceptions.checked.JobNotArchivedException
import com.netflix.genie.web.exceptions.checked.JobNotFoundException
import com.netflix.genie.web.services.ArchivedJobService
import com.netflix.genie.web.services.JobDirectoryServerService
import com.netflix.genie.web.services.JobFileService
import io.micrometer.core.instrument.MeterRegistry
import org.junit.Ignore
import org.springframework.core.io.Resource
import org.springframework.core.io.ResourceLoader
import org.springframework.http.MediaType
import spock.lang.Specification
import spock.lang.Unroll

import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import java.nio.file.Paths

//TODO serving of a directory entry is not covered by this test due to the usage of static resources.

/**
 * Specifications for {@link JobDirectoryServerServiceImpl}.
 */
class JobDirectoryServerServiceImplSpec extends Specification {
    static final String JOB_ID = "123456"
    static final String REL_PATH = "bar/foo.txt"
    static final URL BASE_URL = new URL("https", "genie.com", 8080, "/jobs/" + JOB_ID + "/output/" + REL_PATH)
    static final URI EXPECTED_V4_FILE_URI = AgentFileProtocolResolver.createUri(JOB_ID, "/" + REL_PATH)
    static final URI EXPECTED_V3_JOB_DIR_URI = new URI("file:/tmp/genie/jobs/" + JOB_ID)

    ResourceLoader resourceLoader
    JobPersistenceService jobPersistenceService
    JobFileService jobFileService
    AgentFileStreamService agentFileStreamService
    MeterRegistry meterRegistry
    JobDirectoryServerService service
    HttpServletRequest request
    HttpServletResponse response
    DirectoryManifest manifest
    DirectoryManifest.ManifestEntry manifestEntry
    Resource resource
    JobDirectoryServerServiceImpl.GenieResourceHandler.Factory handlerFactory
    JobDirectoryServerServiceImpl.GenieResourceHandler handler
    JobDirectoryManifestCreatorService jobDirectoryManifestService
    ArchivedJobService archivedJobService

    void setup() {
        this.resourceLoader = Mock(ResourceLoader)
        this.jobPersistenceService = Mock(JobPersistenceService)
        this.jobFileService = Mock(JobFileService)
        this.agentFileStreamService = Mock(AgentFileStreamService)
        this.meterRegistry = Mock(MeterRegistry)
        this.handlerFactory = Mock(JobDirectoryServerServiceImpl.GenieResourceHandler.Factory)
        this.handler = Mock(JobDirectoryServerServiceImpl.GenieResourceHandler)
        this.jobDirectoryManifestService = Mock(JobDirectoryManifestCreatorService)
        this.archivedJobService = Mock(ArchivedJobService)
        this.service = new JobDirectoryServerServiceImpl(
            this.resourceLoader,
            this.jobPersistenceService,
            this.agentFileStreamService,
            this.archivedJobService,
            this.handlerFactory,
            this.meterRegistry,
            this.jobFileService,
            this.jobDirectoryManifestService
        )

        this.request = Mock(HttpServletRequest)
        this.response = Mock(HttpServletResponse)
        this.manifest = Mock(DirectoryManifest)
        this.manifestEntry = Mock(DirectoryManifest.ManifestEntry)
        this.resource = Mock(Resource)
    }

    def "ServeResource -- job not found (common code)"() {
        setup:

        when: "Not found when doing status lookup"
        this.service.serveResource(JOB_ID, BASE_URL, REL_PATH, this.request, this.response)

        then:
        1 * this.jobPersistenceService.getJobStatus(JOB_ID) >> { throw new GenieNotFoundException("...") }
        thrown(GenieNotFoundException)

        when: "Not found when doing v4 lookup"
        this.service.serveResource(JOB_ID, BASE_URL, REL_PATH, request, response)

        then:
        1 * this.jobPersistenceService.getJobStatus(JOB_ID) >> JobStatus.RUNNING
        1 * this.jobPersistenceService.isV4(JOB_ID) >> { throw new GenieNotFoundException("...") }
        thrown(GenieNotFoundException)
    }

    def "ServeResource -- Active V4 job"() {
        setup:
        DirectoryManifest directoryManifest = Mock(DirectoryManifest)

        when: "Manifest not found"
        this.service.serveResource(JOB_ID, BASE_URL, REL_PATH, this.request, this.response)

        then:
        1 * this.jobPersistenceService.getJobStatus(JOB_ID) >> JobStatus.RUNNING
        1 * this.jobPersistenceService.isV4(JOB_ID) >> true
        1 * this.agentFileStreamService.getManifest(JOB_ID) >> Optional.empty()
        thrown(GenieServerUnavailableException)

        // TODO: No easy way to to make agent URI creation fail

        when: "Manifest entry not found"
        service.serveResource(JOB_ID, BASE_URL, REL_PATH, this.request, this.response)

        then:
        1 * this.jobPersistenceService.getJobStatus(JOB_ID) >> JobStatus.RUNNING
        1 * this.jobPersistenceService.isV4(JOB_ID) >> true
        1 * this.agentFileStreamService.getManifest(JOB_ID) >> Optional.of(this.manifest)
        1 * this.manifest.getEntry(REL_PATH) >> Optional.empty()
        thrown(GenieNotFoundException)

        when: "Success"
        this.service.serveResource(JOB_ID, BASE_URL, REL_PATH, this.request, this.response)

        then:
        1 * this.jobPersistenceService.getJobStatus(JOB_ID) >> JobStatus.RUNNING
        1 * this.jobPersistenceService.isV4(JOB_ID) >> true
        1 * this.agentFileStreamService.getManifest(JOB_ID) >> Optional.of(this.manifest)
        1 * this.manifest.getEntry(REL_PATH) >> Optional.of(this.manifestEntry)
        1 * this.manifestEntry.isDirectory() >> false
        1 * this.manifestEntry.getPath() >> REL_PATH
        1 * this.resourceLoader.getResource(EXPECTED_V4_FILE_URI.toString()) >> this.resource
        1 * this.manifestEntry.getMimeType() >> Optional.of(MediaType.TEXT_PLAIN_VALUE)
        1 * this.handlerFactory.get(MediaType.TEXT_PLAIN_VALUE, resource) >> this.handler
        1 * this.handler.handleRequest(this.request, this.response)
    }

    def "ServeResource -- Active V3 job"() {
        setup:
        DirectoryManifest directoryManifest = Mock(DirectoryManifest)

        when: "Job directory not found"
        this.service.serveResource(JOB_ID, BASE_URL, REL_PATH, this.request, this.response)

        then:
        1 * this.jobPersistenceService.getJobStatus(JOB_ID) >> JobStatus.RUNNING
        1 * this.jobPersistenceService.isV4(JOB_ID) >> false
        1 * this.jobFileService.getJobFileAsResource(JOB_ID, "") >> resource
        1 * this.resource.exists() >> false
        thrown(GenieNotFoundException)

        // TODO no easy way to cause URI syntax exception

        when: "Manifest creation exception"
        this.service.serveResource(JOB_ID, BASE_URL, REL_PATH, this.request, this.response)

        then:
        1 * this.jobPersistenceService.getJobStatus(JOB_ID) >> JobStatus.RUNNING
        1 * this.jobPersistenceService.isV4(JOB_ID) >> false
        1 * this.jobFileService.getJobFileAsResource(JOB_ID, "") >> resource
        1 * this.resource.exists() >> true
        1 * this.resource.getURI() >> EXPECTED_V3_JOB_DIR_URI
        1 * this.jobDirectoryManifestService.getDirectoryManifest(_) >> { throw new IOException() }
        thrown(GenieServerException)

        when: "Success"
        this.service.serveResource(JOB_ID, BASE_URL, REL_PATH, this.request, this.response)

        then:
        1 * this.jobPersistenceService.getJobStatus(JOB_ID) >> JobStatus.RUNNING
        1 * this.jobPersistenceService.isV4(JOB_ID) >> false
        1 * this.jobFileService.getJobFileAsResource(JOB_ID, "") >> resource
        1 * this.resource.exists() >> true
        1 * this.resource.getURI() >> EXPECTED_V3_JOB_DIR_URI
        1 * this.jobDirectoryManifestService.getDirectoryManifest(Paths.get(EXPECTED_V3_JOB_DIR_URI.getPath())) >> manifest
        1 * this.manifest.getEntry(REL_PATH) >> Optional.of(manifestEntry)

        1 * this.manifestEntry.isDirectory() >> false
        1 * this.manifestEntry.getPath() >> REL_PATH
        1 * this.resourceLoader.getResource(EXPECTED_V3_JOB_DIR_URI.toString() + "/" + REL_PATH) >> this.resource
        1 * this.manifestEntry.getMimeType() >> Optional.of(MediaType.TEXT_PLAIN_VALUE)
        1 * this.handlerFactory.get(MediaType.TEXT_PLAIN_VALUE, resource) >> this.handler
        1 * this.handler.handleRequest(this.request, this.response)
    }

    @Unroll
    def "ServeResource -- Finished job manifest error: #exception"() {
        when:
        this.service.serveResource(JOB_ID, BASE_URL, REL_PATH, this.request, this.response)

        then:
        1 * this.jobPersistenceService.getJobStatus(JOB_ID) >> JobStatus.SUCCEEDED
        1 * this.archivedJobService.getArchivedJobMetadata(JOB_ID) >> {
            throw exception
        }
        thrown(expectedExceptionClass)

        where:
        exception | expectedExceptionClass
        new JobNotArchivedException("...")               | GeniePreconditionException
        new JobNotFoundException("...")                  | GenieNotFoundException
        new JobDirectoryManifestNotFoundException("...") | GenieNotFoundException
        new IOException("...")                           | GenieServerException
        new RuntimeException("...")                      | GenieServerException
    }
}
