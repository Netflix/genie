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
import com.netflix.genie.common.internal.services.JobDirectoryManifestService
import com.netflix.genie.web.data.services.JobPersistenceService
import com.netflix.genie.web.resources.agent.AgentFileProtocolResolver
import com.netflix.genie.web.services.AgentFileStreamService
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

class JobDirectoryServerServiceImplSpec extends Specification {
    static final String JOB_ID = "123456"
    static final String relPath = "bar/foo.txt"
    static final URL BASE_URL = new URL("https", "genie.netflix.net", 8080, "/jobs/" + JOB_ID + "/output/" + relPath)
    static final URI EXPECTED_V4_FILE_URI = new URI(AgentFileProtocolResolver.URI_SCHEME, JOB_ID, "/" + relPath, null);

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
    JobDirectoryManifestService jobDirectoryManifestService

    void setup() {
        this.resourceLoader = Mock(ResourceLoader)
        this.jobPersistenceService = Mock(JobPersistenceService)
        this.jobFileService = Mock(JobFileService)
        this.agentFileStreamService = Mock(AgentFileStreamService)
        this.meterRegistry = Mock(MeterRegistry)
        this.handlerFactory = Mock(JobDirectoryServerServiceImpl.GenieResourceHandler.Factory)
        this.handler = Mock(JobDirectoryServerServiceImpl.GenieResourceHandler)
        this.jobDirectoryManifestService = Mock(JobDirectoryManifestService)
        this.service = new JobDirectoryServerServiceImpl(resourceLoader, jobPersistenceService, jobFileService, agentFileStreamService, meterRegistry, handlerFactory, jobDirectoryManifestService)

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
        service.serveResource(JOB_ID, BASE_URL, relPath, request, response)

        then:
        1 * jobPersistenceService.getJobStatus(JOB_ID) >> { throw e }
        1 * response.sendError(404, e.getMessage())
    }

    def "ServeResource -- job not found (v4)"() {
        setup:
        Exception e = new GenieJobNotFoundException("...")

        when:
        service.serveResource(JOB_ID, BASE_URL, relPath, request, response)

        then:
        1 * jobPersistenceService.getJobStatus(JOB_ID) >> JobStatus.RUNNING
        1 * jobPersistenceService.isV4(JOB_ID) >> { throw e }
        1 * response.sendError(404, e.getMessage())
    }

    def "ServeResource -- invalid URI"() {
        // TODO -- how to cause URI syntax exception?
    }

    def "ServeResource -- Active V4 job, manifest not found"() {
        when:
        service.serveResource(JOB_ID, BASE_URL, relPath, request, response)

        then:
        1 * jobPersistenceService.getJobStatus(JOB_ID) >> JobStatus.RUNNING
        1 * jobPersistenceService.isV4(JOB_ID) >> true
        1 * agentFileStreamService.getManifest(JOB_ID) >> Optional.empty()
        1 * response.sendError(503, _ as String)
    }

    def "ServeResource -- Active V4 job, manifest entry not found"() {
        setup:

        when:
        service.serveResource(JOB_ID, BASE_URL, relPath, request, response)

        then:
        1 * jobPersistenceService.getJobStatus(JOB_ID) >> JobStatus.RUNNING
        1 * jobPersistenceService.isV4(JOB_ID) >> true
        1 * agentFileStreamService.getManifest(JOB_ID) >> Optional.of(manifest)
        1 * manifest.getEntry(relPath) >> Optional.empty()
        1 * response.sendError(404, _ as String)
    }

    def "ServeResource -- Active V4 job, return resource"() {
        setup:

        when:
        service.serveResource(JOB_ID, BASE_URL, relPath, request, response)

        then:
        1 * jobPersistenceService.getJobStatus(JOB_ID) >> JobStatus.RUNNING
        1 * jobPersistenceService.isV4(JOB_ID) >> true
        1 * agentFileStreamService.getManifest(JOB_ID) >> Optional.of(manifest)
        1 * manifest.getEntry(relPath) >> Optional.of(manifestEntry)
        1 * manifestEntry.isDirectory() >> false
        1 * manifestEntry.getPath() >> relPath
        1 * resourceLoader.getResource(EXPECTED_V4_FILE_URI.toString()) >> resource
        1 * manifestEntry.getMimeType() >> Optional.of(MediaType.TEXT_PLAIN_VALUE)
        1 * handlerFactory.get(MediaType.TEXT_PLAIN_VALUE, resource) >> handler
        1 * handler.handleRequest(request, response)
    }

    def "Job done but not archived returns 404"() {
        when:
        this.service.serveResource(JOB_ID, BASE_URL, relPath, request, response)

        then:
        1 * this.jobPersistenceService.getJobStatus(JOB_ID) >> JobStatus.SUCCEEDED
        1 * this.jobPersistenceService.isV4(JOB_ID) >> false
        1 * this.jobPersistenceService.getJobArchiveLocation(JOB_ID) >> Optional.empty()
        1 * response.sendError(HttpStatus.NOT_FOUND.value(), "Job " + JOB_ID + " wasn't archived")
    }
}
