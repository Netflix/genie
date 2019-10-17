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
import com.netflix.genie.common.internal.dto.DirectoryManifest
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobNotFoundException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieRuntimeException
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
import org.springframework.core.io.Resource
import org.springframework.core.io.ResourceLoader
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import spock.lang.Specification

import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

/**
 * Specifications for {@link JobDirectoryServerServiceImpl}.
 *
 * @author mprimi
 */
class JobDirectoryServerServiceImplSpec extends Specification {
    static final String JOB_ID = "123456"
    static final String REL_PATH = "bar/foo.txt"
    static final URL BASE_URL = new URL("https", "genie.com", 8080, "/jobs/" + JOB_ID + "/output/" + REL_PATH)
    static final URI EXPECTED_V4_FILE_URI = AgentFileProtocolResolver.createUri(JOB_ID, "/" + REL_PATH)

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

    def "ServeResource -- job not found (status)"() {
        setup:
        Exception e = new GenieNotFoundException("...")

        when:
        this.service.serveResource(JOB_ID, BASE_URL, REL_PATH, this.request, this.response)

        then:
        1 * this.jobPersistenceService.getJobStatus(JOB_ID) >> { throw e }
        1 * this.response.sendError(404, e.getMessage())
    }

    def "ServeResource -- job not found (v4)"() {
        setup:
        Exception e = new GenieJobNotFoundException("...")

        when:
        this.service.serveResource(JOB_ID, BASE_URL, REL_PATH, request, response)

        then:
        1 * this.jobPersistenceService.getJobStatus(JOB_ID) >> JobStatus.RUNNING
        1 * this.jobPersistenceService.isV4(JOB_ID) >> { throw e }
        1 * this.response.sendError(404, e.getMessage())
    }

    def "ServeResource -- invalid URI"() {
        // TODO -- how to cause URI syntax exception?
    }

    def "ServeResource -- Active V4 job, manifest not found"() {
        when:
        this.service.serveResource(JOB_ID, BASE_URL, REL_PATH, this.request, this.response)

        then:
        1 * this.jobPersistenceService.getJobStatus(JOB_ID) >> JobStatus.RUNNING
        1 * this.jobPersistenceService.isV4(JOB_ID) >> true
        1 * this.agentFileStreamService.getManifest(JOB_ID) >> Optional.empty()
        1 * this.response.sendError(503, _ as String)
    }

    def "ServeResource -- Active V4 job, manifest entry not found"() {
        setup:

        when:
        service.serveResource(JOB_ID, BASE_URL, REL_PATH, this.request, this.response)

        then:
        1 * this.jobPersistenceService.getJobStatus(JOB_ID) >> JobStatus.RUNNING
        1 * this.jobPersistenceService.isV4(JOB_ID) >> true
        1 * this.agentFileStreamService.getManifest(JOB_ID) >> Optional.of(this.manifest)
        1 * this.manifest.getEntry(REL_PATH) >> Optional.empty()
        1 * this.response.sendError(404, _ as String)
    }

    def "ServeResource -- Active V4 job, return resource"() {
        setup:

        when:
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

    def "Archived job exceptions respond with the expected error codes"() {
        def jobNotFoundErrorMessage = "No job with id " + JOB_ID + " exists"
        def notArchivedErrorMessage = "Job " + JOB_ID + " wasn't archived"
        def manifestNotFoundErrorMessage = "No directory manifest for job " + JOB_ID + " exists"
        def runtimeMessage = "Something went wrong"

        when:
        this.service.serveResource(JOB_ID, BASE_URL, REL_PATH, this.request, this.response)

        then:
        1 * this.jobPersistenceService.getJobStatus(JOB_ID) >> JobStatus.SUCCEEDED
        1 * this.jobPersistenceService.isV4(JOB_ID) >> false
        1 * this.archivedJobService.getArchivedJobMetadata(JOB_ID) >> {
            throw new JobNotFoundException(jobNotFoundErrorMessage)
        }
        1 * this.response.sendError(HttpStatus.NOT_FOUND.value(), jobNotFoundErrorMessage)

        when:
        this.service.serveResource(JOB_ID, BASE_URL, REL_PATH, this.request, this.response)

        then:
        1 * this.jobPersistenceService.getJobStatus(JOB_ID) >> JobStatus.FAILED
        1 * this.jobPersistenceService.isV4(JOB_ID) >> true
        1 * this.archivedJobService.getArchivedJobMetadata(JOB_ID) >> {
            throw new JobNotArchivedException(notArchivedErrorMessage)
        }
        1 * this.response.sendError(HttpStatus.PRECONDITION_FAILED.value(), notArchivedErrorMessage)

        when:
        this.service.serveResource(JOB_ID, BASE_URL, REL_PATH, this.request, this.response)

        then:
        1 * this.jobPersistenceService.getJobStatus(JOB_ID) >> JobStatus.KILLED
        1 * this.jobPersistenceService.isV4(JOB_ID) >> true
        1 * this.archivedJobService.getArchivedJobMetadata(JOB_ID) >> {
            throw new JobDirectoryManifestNotFoundException(manifestNotFoundErrorMessage)
        }
        1 * this.response.sendError(HttpStatus.NOT_FOUND.value(), manifestNotFoundErrorMessage)

        when:
        this.service.serveResource(JOB_ID, BASE_URL, REL_PATH, this.request, this.response)

        then:
        1 * this.jobPersistenceService.getJobStatus(JOB_ID) >> JobStatus.INVALID
        1 * this.jobPersistenceService.isV4(JOB_ID) >> false
        1 * this.archivedJobService.getArchivedJobMetadata(JOB_ID) >> {
            throw new GenieRuntimeException(runtimeMessage)
        }
        1 * this.response.sendError(HttpStatus.INTERNAL_SERVER_ERROR.value(), runtimeMessage)
    }
}
