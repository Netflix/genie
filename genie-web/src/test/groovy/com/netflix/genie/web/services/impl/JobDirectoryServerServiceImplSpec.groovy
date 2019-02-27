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
import com.netflix.genie.common.internal.dto.JobDirectoryManifest
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobNotFoundException
import com.netflix.genie.test.categories.UnitTest
import com.netflix.genie.web.resources.agent.AgentFileProtocolResolver
import com.netflix.genie.web.services.AgentFileManifestService
import com.netflix.genie.web.services.JobDirectoryServerService
import com.netflix.genie.web.services.JobFileService
import com.netflix.genie.web.services.JobPersistenceService
import io.micrometer.core.instrument.MeterRegistry
import org.junit.experimental.categories.Category
import org.springframework.core.io.Resource
import org.springframework.core.io.ResourceLoader
import org.springframework.http.MediaType
import spock.lang.Specification

import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

@Category(UnitTest)
class JobDirectoryServerServiceImplSpec extends Specification {
    static final String JOB_ID = "123456"
    static final String relPath = "bar/foo.txt"
    static final URL BASE_URL = new URL("https", "genie.netflix.net", 8080, "/jobs/" + JOB_ID + "/output/" + relPath)
    static final URI EXPECTED_V4_FILE_URI = new URI(AgentFileProtocolResolver.URI_SCHEME, JOB_ID, "/" + relPath, null);

    ResourceLoader resourceLoader
    JobPersistenceService jobPersistenceService
    JobFileService jobFileService
    AgentFileManifestService agentFileManifestService
    MeterRegistry meterRegistry
    JobDirectoryServerService service
    HttpServletRequest request
    HttpServletResponse response
    JobDirectoryManifest manifest
    JobDirectoryManifest.ManifestEntry manifestEntry
    Resource resource
    JobDirectoryServerServiceImpl.GenieResourceHandler.Factory handlerFactory
    JobDirectoryServerServiceImpl.GenieResourceHandler handler

    void setup() {
        this.resourceLoader = Mock(ResourceLoader)
        this.jobPersistenceService = Mock(JobPersistenceService)
        this.jobFileService = Mock(JobFileService)
        this.agentFileManifestService = Mock(AgentFileManifestService)
        this.meterRegistry = Mock(MeterRegistry)
        this.handlerFactory = Mock(JobDirectoryServerServiceImpl.GenieResourceHandler.Factory)
        this.handler = Mock(JobDirectoryServerServiceImpl.GenieResourceHandler)
        this.service = new JobDirectoryServerServiceImpl(resourceLoader, jobPersistenceService, jobFileService, agentFileManifestService, meterRegistry, handlerFactory)

        this.request = Mock(HttpServletRequest)
        this.response = Mock(HttpServletResponse)
        this.manifest = Mock(JobDirectoryManifest)
        this.manifestEntry = Mock(JobDirectoryManifest.ManifestEntry)
        this.resource = Mock(Resource)
    }

    def "ServeResource -- job not found (status)"() {
        setup:
        Exception e = new GenieNotFoundException("...")

        when:
        service.serveResource(JOB_ID, BASE_URL, relPath, request, response)

        then:
        1 * jobPersistenceService.getJobStatus(JOB_ID) >> {throw e}
        1 * response.sendError(404, e.getMessage())
    }

    def "ServeResource -- job not found (v4)"() {
        setup:
        Exception e = new GenieJobNotFoundException("...")

        when:
        service.serveResource(JOB_ID, BASE_URL, relPath, request, response)

        then:
        1 * jobPersistenceService.getJobStatus(JOB_ID) >> JobStatus.RUNNING
        1 * jobPersistenceService.isV4(JOB_ID) >> {throw e}
        1 * response.sendError(404, e.getMessage())
    }

    def "ServeResource -- invalid URI"() {
        // TODO -- how to cause URI syntax exception?
    }

    def "ServeResource -- Active V4 job, manifest not found"() {
        setup:

        when:
        service.serveResource(JOB_ID, BASE_URL, relPath, request, response)

        then:
        1 * jobPersistenceService.getJobStatus(JOB_ID) >> JobStatus.RUNNING
        1 * jobPersistenceService.isV4(JOB_ID) >> true
        1 * agentFileManifestService.getManifest(JOB_ID) >> Optional.empty()
        1 * response.sendError(503, _ as String)
    }

    def "ServeResource -- Active V4 job, manifest entry not found"() {
        setup:

        when:
        service.serveResource(JOB_ID, BASE_URL, relPath, request, response)

        then:
        1 * jobPersistenceService.getJobStatus(JOB_ID) >> JobStatus.RUNNING
        1 * jobPersistenceService.isV4(JOB_ID) >> true
        1 * agentFileManifestService.getManifest(JOB_ID) >> Optional.of(manifest)
        1 * manifest.getEntry(relPath) >> Optional.empty()
        1 * response.sendError(404, _ as String)
    }

    def "ServeResource -- Active V4 job, ..."() {
        setup:

        when:
        service.serveResource(JOB_ID, BASE_URL, relPath, request, response)

        then:
        1 * jobPersistenceService.getJobStatus(JOB_ID) >> JobStatus.RUNNING
        1 * jobPersistenceService.isV4(JOB_ID) >> true
        1 * agentFileManifestService.getManifest(JOB_ID) >> Optional.of(manifest)
        1 * manifest.getEntry(relPath) >> Optional.of(manifestEntry)
        1 * manifestEntry.isDirectory() >> false
        1 * manifestEntry.getPath() >> relPath
        1 * resourceLoader.getResource(EXPECTED_V4_FILE_URI.toString()) >> resource
        1 * manifestEntry.getMimeType() >> Optional.of(MediaType.TEXT_PLAIN_VALUE)
        1 * handlerFactory.get(MediaType.TEXT_PLAIN_VALUE, resource) >> handler
        1 * handler.handleRequest(request, response)
    }
}
