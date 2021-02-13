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

import com.netflix.genie.common.exceptions.GenieNotFoundException
import com.netflix.genie.common.exceptions.GeniePreconditionException
import com.netflix.genie.common.exceptions.GenieServerUnavailableException
import com.netflix.genie.common.external.dtos.v4.ArchiveStatus
import com.netflix.genie.common.internal.dtos.DirectoryManifest
import com.netflix.genie.common.internal.services.JobDirectoryManifestCreatorService
import com.netflix.genie.web.agent.resources.AgentFileProtocolResolver
import com.netflix.genie.web.agent.services.AgentFileStreamService
import com.netflix.genie.web.agent.services.AgentRoutingService
import com.netflix.genie.web.data.services.DataServices
import com.netflix.genie.web.data.services.PersistenceService
import com.netflix.genie.web.dtos.ArchivedJobMetadata
import com.netflix.genie.web.exceptions.checked.JobDirectoryManifestNotFoundException
import com.netflix.genie.web.exceptions.checked.JobNotArchivedException
import com.netflix.genie.web.exceptions.checked.JobNotFoundException
import com.netflix.genie.web.exceptions.checked.NotFoundException
import com.netflix.genie.web.services.ArchivedJobService
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer
import org.springframework.core.io.Resource
import org.springframework.core.io.ResourceLoader
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import spock.lang.Specification
import spock.lang.Unroll

import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import java.util.concurrent.TimeUnit

//TODO serving of a directory entry is not covered by this test due to the usage of static resources.
//(Integration tests do cover most of it)

/**
 * Specifications for {@link JobDirectoryServerServiceImpl}.
 */
@SuppressWarnings("GroovyAccessibility")
class JobDirectoryServerServiceImplSpec extends Specification {
    static final String JOB_ID = "123456"
    static final String REL_PATH = "bar/foo.txt"
    static final URL BASE_URL = new URL("https", "genie.com", 8080, "/jobs/" + JOB_ID + "/output/" + REL_PATH)
    static final URI EXPECTED_V4_FILE_URI = AgentFileProtocolResolver.createUri(JOB_ID, "/" + REL_PATH, null)
    static final URI ARCHIVE_BASE_URI = new URI("s3://genie-bucket/genie-archived/" + JOB_ID + "/")
    static final String EXPECTED_ARCHIVE_FILE_LOCATION = ARCHIVE_BASE_URI.toString() + REL_PATH

    static final String TIMER_NAME = JobDirectoryServerServiceImpl.SERVE_RESOURCE_TIMER

    ResourceLoader resourceLoader
    PersistenceService persistenceService
    AgentFileStreamService agentFileStreamService
    MeterRegistry meterRegistry
    Timer timer
    JobDirectoryServerServiceImpl service
    HttpServletRequest request
    HttpServletResponse response
    DirectoryManifest manifest
    DirectoryManifest.ManifestEntry manifestEntry
    Resource resource
    JobDirectoryServerServiceImpl.GenieResourceHandler.Factory handlerFactory
    JobDirectoryServerServiceImpl.GenieResourceHandler handler
    JobDirectoryManifestCreatorService jobDirectoryManifestService
    ArchivedJobService archivedJobService
    AgentRoutingService agentRoutingService

    void setup() {
        this.resourceLoader = Mock(ResourceLoader)
        this.persistenceService = Mock(PersistenceService)
        this.agentFileStreamService = Mock(AgentFileStreamService)
        this.timer = Mock(Timer)
        this.meterRegistry = Mock(MeterRegistry) {
            timer(TIMER_NAME, _ as Iterable<Tag>) >> {
                String timerName, Iterable<Tag> tags ->
                    print("Timer: " + timerName + " tags: ")
                    tags.forEach({ tag -> print(tag.getKey() + "=" + tag.getValue() + ", ") })
                    println()
                    return timer
            }
        }
        this.handlerFactory = Mock(JobDirectoryServerServiceImpl.GenieResourceHandler.Factory)
        this.handler = Mock(JobDirectoryServerServiceImpl.GenieResourceHandler)
        this.jobDirectoryManifestService = Mock(JobDirectoryManifestCreatorService)
        this.archivedJobService = Mock(ArchivedJobService)
        this.agentRoutingService = Mock(AgentRoutingService)
        def dataServices = Mock(DataServices) {
            getPersistenceService() >> this.persistenceService
        }
        this.service = new JobDirectoryServerServiceImpl(
            this.resourceLoader,
            dataServices,
            this.agentFileStreamService,
            this.archivedJobService,
            this.handlerFactory,
            this.meterRegistry
            ,
            this.agentRoutingService
        )

        this.request = Mock(HttpServletRequest)
        this.response = Mock(HttpServletResponse)
        this.manifest = Mock(DirectoryManifest)
        this.manifestEntry = Mock(DirectoryManifest.ManifestEntry)
        this.resource = Mock(Resource)
    }

    def "ServeResource -- job not found"() {
        setup:

        when: "Not found when doing status lookup"
        this.service.serveResource(JOB_ID, BASE_URL, REL_PATH, this.request, this.response)

        then:
        1 * this.persistenceService.getJobArchiveStatus(JOB_ID) >> { throw new NotFoundException("...") }
        1 * this.timer.record(_ as Long, TimeUnit.NANOSECONDS)
        thrown(GenieNotFoundException)
    }

    @Unroll
    def "ServeResource -- Status: #archiveStatus throws #expectedException"() {
        when:
        this.service.serveResource(JOB_ID, BASE_URL, REL_PATH, request, response)

        then:
        1 * this.persistenceService.getJobArchiveStatus(JOB_ID) >> archiveStatus
        1 * this.timer.record(_ as Long, TimeUnit.NANOSECONDS)
        thrown(expectedException)

        where:
        archiveStatus          | expectedException
        ArchiveStatus.FAILED   | GenieNotFoundException
        ArchiveStatus.NO_FILES | GenieNotFoundException
        ArchiveStatus.DISABLED | GeniePreconditionException
    }

    @Unroll
    def "ServeResource -- Serve from archive (status: #archiveStatus)"() {
        setup:
        ArchivedJobMetadata archivedJobMetadata = Mock(ArchivedJobMetadata)

        when:
        this.service.serveResource(JOB_ID, BASE_URL, REL_PATH, request, response)

        then:
        1 * this.persistenceService.getJobArchiveStatus(JOB_ID) >> archiveStatus
        1 * this.archivedJobService.getArchivedJobMetadata(JOB_ID) >> archivedJobMetadata
        1 * this.request.getHeader(HttpHeaders.RANGE) >> null
        1 * archivedJobMetadata.getManifest() >> manifest
        1 * archivedJobMetadata.getArchiveBaseUri() >> ARCHIVE_BASE_URI
        1 * this.manifest.getEntry(REL_PATH) >> Optional.of(this.manifestEntry)
        1 * this.manifestEntry.isDirectory() >> false
        1 * this.manifestEntry.getPath() >> REL_PATH
        1 * this.resourceLoader.getResource(EXPECTED_ARCHIVE_FILE_LOCATION) >> this.resource
        1 * this.manifestEntry.getMimeType() >> Optional.of(MediaType.TEXT_PLAIN_VALUE)
        1 * this.handlerFactory.get(MediaType.TEXT_PLAIN_VALUE, resource) >> this.handler
        1 * this.handler.handleRequest(this.request, this.response)
        1 * this.timer.record(_ as Long, TimeUnit.NANOSECONDS)

        where:
        archiveStatus          | _
        ArchiveStatus.ARCHIVED | _
        ArchiveStatus.ARCHIVED | _
        ArchiveStatus.UNKNOWN  | _
        ArchiveStatus.UNKNOWN  | _
    }

    @Unroll
    def "ServeResource -- Serve from archive with range #rangeHeader"() {
        setup:
        ArchivedJobMetadata archivedJobMetadata = Mock(ArchivedJobMetadata)
        String expectedResourceLocation = EXPECTED_ARCHIVE_FILE_LOCATION + (rangeHeader != null ? ("#" + rangeHeader) : "")

        when:
        this.service.serveResource(JOB_ID, BASE_URL, REL_PATH, request, response)

        then:
        1 * this.persistenceService.getJobArchiveStatus(JOB_ID) >> ArchiveStatus.ARCHIVED
        1 * this.archivedJobService.getArchivedJobMetadata(JOB_ID) >> archivedJobMetadata
        1 * this.request.getHeader(HttpHeaders.RANGE) >> rangeHeader
        1 * archivedJobMetadata.getManifest() >> manifest
        1 * archivedJobMetadata.getArchiveBaseUri() >> ARCHIVE_BASE_URI
        1 * this.manifest.getEntry(REL_PATH) >> Optional.of(this.manifestEntry)
        1 * this.manifestEntry.isDirectory() >> false
        1 * this.manifestEntry.getPath() >> REL_PATH
        1 * this.resourceLoader.getResource(expectedResourceLocation) >> this.resource
        1 * this.manifestEntry.getMimeType() >> Optional.of(MediaType.TEXT_PLAIN_VALUE)
        1 * this.handlerFactory.get(MediaType.TEXT_PLAIN_VALUE, resource) >> this.handler
        1 * this.handler.handleRequest(this.request, this.response)
        1 * this.timer.record(_ as Long, TimeUnit.NANOSECONDS)

        where:
        rangeHeader   | _
        null          | _
        "bytes=10-20" | _
        "bytes=10-"   | _
        "bytes=-20"   | _
    }

    @Unroll
    def "ServeResource -- Serve from archive errors"() {
        when:
        this.service.serveResource(JOB_ID, BASE_URL, REL_PATH, request, response)

        then:
        1 * this.persistenceService.getJobArchiveStatus(JOB_ID) >> ArchiveStatus.ARCHIVED
        1 * this.archivedJobService.getArchivedJobMetadata(JOB_ID) >> { throw exception }
        1 * this.timer.record(_ as Long, TimeUnit.NANOSECONDS)
        thrown(expectedException)

        where:
        exception                                        | expectedException
        new JobNotArchivedException("...")               | GeniePreconditionException
        new JobNotFoundException("...")                  | GenieNotFoundException
        new JobDirectoryManifestNotFoundException("...") | GenieNotFoundException
    }

    @Unroll
    def "ServeResource -- PENDING job with range #rangeHeader"() {
        setup:
        String expectedResourceUri = EXPECTED_V4_FILE_URI.toString() + (rangeHeader != null ? ("#" + rangeHeader) : "")

        when:
        this.service.serveResource(JOB_ID, BASE_URL, REL_PATH, this.request, this.response)

        then:
        1 * this.persistenceService.getJobArchiveStatus(JOB_ID) >> ArchiveStatus.PENDING
        1 * this.agentRoutingService.isAgentConnectionLocal(JOB_ID) >> true
        1 * this.agentFileStreamService.getManifest(JOB_ID) >> Optional.of(this.manifest)
        1 * this.request.getHeader(HttpHeaders.RANGE) >> rangeHeader
        1 * this.manifest.getEntry(REL_PATH) >> Optional.of(this.manifestEntry)
        1 * this.manifestEntry.isDirectory() >> false
        1 * this.manifestEntry.getPath() >> REL_PATH
        1 * this.resourceLoader.getResource(expectedResourceUri) >> this.resource
        1 * this.manifestEntry.getMimeType() >> Optional.of(MediaType.TEXT_PLAIN_VALUE)
        1 * this.handlerFactory.get(MediaType.TEXT_PLAIN_VALUE, resource) >> this.handler
        1 * this.handler.handleRequest(this.request, this.response)
        1 * this.timer.record(_ as Long, TimeUnit.NANOSECONDS)

        where:
        rangeHeader   | _
        null          | _
        "bytes=10-20" | _
        "bytes=10-"   | _
        "bytes=-20"   | _
    }

    def "ServeResource -- PENDING job errors"() {
        when: "Agent is not connected to local node"
        this.service.serveResource(JOB_ID, BASE_URL, REL_PATH, this.request, this.response)

        then:
        1 * this.persistenceService.getJobArchiveStatus(JOB_ID) >> ArchiveStatus.PENDING
        1 * this.agentRoutingService.isAgentConnectionLocal(JOB_ID) >> false
        1 * this.timer.record(_ as Long, TimeUnit.NANOSECONDS)
        thrown(GenieServerUnavailableException)

        when: "Manifest not found"
        this.service.serveResource(JOB_ID, BASE_URL, REL_PATH, this.request, this.response)

        then:
        1 * this.persistenceService.getJobArchiveStatus(JOB_ID) >> ArchiveStatus.PENDING
        1 * this.agentRoutingService.isAgentConnectionLocal(JOB_ID) >> true
        1 * this.agentFileStreamService.getManifest(JOB_ID) >> Optional.empty()
        1 * this.timer.record(_ as Long, TimeUnit.NANOSECONDS)
        thrown(GenieServerUnavailableException)
    }
}
