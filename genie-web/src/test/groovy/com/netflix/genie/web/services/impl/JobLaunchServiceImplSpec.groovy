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

import com.fasterxml.jackson.databind.JsonNode
import com.netflix.genie.common.external.dtos.v4.ArchiveStatus
import com.netflix.genie.common.external.dtos.v4.JobRequest
import com.netflix.genie.common.external.dtos.v4.JobRequestMetadata
import com.netflix.genie.common.external.dtos.v4.JobStatus
import com.netflix.genie.common.internal.exceptions.checked.GenieJobResolutionException
import com.netflix.genie.web.agent.launchers.AgentLauncher
import com.netflix.genie.web.data.services.DataServices
import com.netflix.genie.web.data.services.PersistenceService
import com.netflix.genie.web.dtos.JobSubmission
import com.netflix.genie.web.dtos.ResolvedJob
import com.netflix.genie.web.dtos.ResourceSelectionResult
import com.netflix.genie.web.exceptions.checked.AgentLaunchException
import com.netflix.genie.web.exceptions.checked.IdAlreadyExistsException
import com.netflix.genie.web.exceptions.checked.NotFoundException
import com.netflix.genie.web.exceptions.checked.ResourceSelectionException
import com.netflix.genie.web.selectors.AgentLauncherSelectionContext
import com.netflix.genie.web.selectors.AgentLauncherSelector
import com.netflix.genie.web.services.JobResolverService
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Specifications for {@link JobLaunchServiceImpl}.
 *
 * @author tgianos
 */
class JobLaunchServiceImplSpec extends Specification {


    @Unroll
    def "Successful launch (requestedLauncherExt: #requestedLauncherExt launcherExt: #launcherExt)"() {
        def persistenceService = Mock(PersistenceService)
        def jobResolverService = Mock(JobResolverService)
        def agentLauncherSelector = Mock(AgentLauncherSelector)
        def agentLauncher = Mock(AgentLauncher)
        def registry = new SimpleMeterRegistry()
        def dataServices = Mock(DataServices) {
            getPersistenceService() >> persistenceService
        }
        def service = new JobLaunchServiceImpl(dataServices, jobResolverService, agentLauncherSelector, registry)

        def jobId = UUID.randomUUID().toString()
        def resolvedJob = Mock(ResolvedJob)
        def jobSubmission = Mock(JobSubmission)
        def jobRequest = Mock(JobRequest)
        def jobRequestMetadata = Mock(JobRequestMetadata)
        def selectionResult = Mock(ResourceSelectionResult)

        when:
        def savedJobId = service.launchJob(jobSubmission)

        then:
        1 * persistenceService.saveJobSubmission(jobSubmission) >> jobId
        1 * jobResolverService.resolveJob(jobId) >> resolvedJob
        1 * persistenceService.updateJobStatus(jobId, JobStatus.RESOLVED, JobStatus.ACCEPTED, _ as String)
        1 * agentLauncherSelector.getAgentLaunchers() >> [agentLauncher]
        1 * jobSubmission.getJobRequest() >> jobRequest
        1 * jobSubmission.getJobRequestMetadata() >> jobRequestMetadata
        1 * persistenceService.getRequestedLauncherExt(jobId) >> requestedLauncherExt
        1 * agentLauncherSelector.select(_ as AgentLauncherSelectionContext) >> selectionResult
        1 * selectionResult.getSelectedResource() >> Optional.of(agentLauncher)
        1 * agentLauncher.launchAgent(resolvedJob, requestedLauncherExt) >> Optional.ofNullable(launcherExt)
        if (launcherExt != null) {
            1 * persistenceService.updateLauncherExt(jobId, launcherExt)
        } else {
            0 * persistenceService.updateLauncherExt(_, _)
        }
        savedJobId == jobId

        where:
        requestedLauncherExt | launcherExt
        null                 | null
        Mock(JsonNode)       | Mock(JsonNode)
        null                 | Mock(JsonNode)
        Mock(JsonNode)       | null
    }

    def "error cases throw expected exceptions"() {
        def persistenceService = Mock(PersistenceService)
        def jobResolverService = Mock(JobResolverService)
        def agentLauncher = Mock(AgentLauncher)
        def agentLauncherSelector = Mock(AgentLauncherSelector) {
            getAgentLaunchers() >> [agentLauncher]
        }
        def registry = new SimpleMeterRegistry()
        def dataServices = Mock(DataServices) {
            getPersistenceService() >> persistenceService
        }
        def service = new JobLaunchServiceImpl(dataServices, jobResolverService, agentLauncherSelector, registry)

        def jobId = UUID.randomUUID().toString()
        def resolvedJob = Mock(ResolvedJob)
        def jobRequest = Mock(JobRequest) {}
        def jobRequestMetadata = Mock(JobRequestMetadata)
        def selectionResult = Mock(ResourceSelectionResult)
        def jobSubmission = Mock(JobSubmission) {
            getJobRequest() >> jobRequest
            getJobRequestMetadata() >> jobRequestMetadata
        }
        def requestedLauncherExt = Mock(JsonNode)
        def launcherExt = Mock(JsonNode)

        when:
        service.launchJob(jobSubmission)

        then:
        1 * persistenceService.saveJobSubmission(jobSubmission) >> {
            throw new IllegalStateException("fail")
        }
        0 * jobResolverService.resolveJob(_ as String)
        0 * persistenceService.updateJobStatus(jobId, JobStatus.RESOLVED, JobStatus.ACCEPTED, _ as String)
        0 * persistenceService.updateJobArchiveStatus(_, _)
        thrown(IllegalStateException)

        when:
        service.launchJob(jobSubmission)

        then:
        1 * persistenceService.saveJobSubmission(jobSubmission) >> {
            throw new IdAlreadyExistsException("try again")
        }
        0 * jobResolverService.resolveJob(_ as String)
        0 * persistenceService.updateJobStatus(jobId, JobStatus.RESOLVED, JobStatus.ACCEPTED, _ as String)
        0 * persistenceService.updateJobArchiveStatus(_, _)
        0 * agentLauncher.launchAgent(_ as ResolvedJob, requestedLauncherExt)
        thrown(IdAlreadyExistsException)

        when:
        service.launchJob(jobSubmission)

        then:
        1 * persistenceService.saveJobSubmission(jobSubmission) >> jobId
        1 * jobResolverService.resolveJob(jobId) >> {
            throw new GenieJobResolutionException("fail")
        }
        1 * persistenceService.updateJobStatus(jobId, JobStatus.RESERVED, JobStatus.FAILED, _ as String)
        1 * persistenceService.updateJobArchiveStatus(jobId, ArchiveStatus.NO_FILES)
        0 * persistenceService.updateJobStatus(jobId, JobStatus.RESOLVED, JobStatus.ACCEPTED, _ as String)
        0 * agentLauncher.launchAgent(_ as ResolvedJob, requestedLauncherExt)
        thrown(GenieJobResolutionException)

        when:
        service.launchJob(jobSubmission)

        then:
        1 * persistenceService.saveJobSubmission(jobSubmission) >> jobId
        1 * jobResolverService.resolveJob(jobId) >> resolvedJob
        1 * persistenceService.updateJobStatus(jobId, JobStatus.RESOLVED, JobStatus.ACCEPTED, _ as String) >> {
            throw new RuntimeException("fail")
        }
        0 * persistenceService.updateJobArchiveStatus(_, _)
        0 * agentLauncher.launchAgent(_ as ResolvedJob, requestedLauncherExt)
        thrown(AgentLaunchException)

        when:
        service.launchJob(jobSubmission)

        then:
        1 * persistenceService.saveJobSubmission(jobSubmission) >> jobId
        1 * jobResolverService.resolveJob(jobId) >> resolvedJob
        1 * persistenceService.updateJobStatus(jobId, JobStatus.RESOLVED, JobStatus.ACCEPTED, _ as String) >> { throw new NotFoundException() }
        0 * persistenceService.updateJobArchiveStatus(_, _)
        0 * agentLauncher.launchAgent(_ as ResolvedJob, requestedLauncherExt)
        0 * persistenceService.updateJobStatus(jobId, JobStatus.ACCEPTED, JobStatus.FAILED, _ as String)
        thrown(AgentLaunchException)

        when:
        service.launchJob(jobSubmission)

        then:
        1 * persistenceService.saveJobSubmission(jobSubmission) >> jobId
        1 * jobResolverService.resolveJob(jobId) >> resolvedJob
        1 * persistenceService.updateJobStatus(jobId, JobStatus.RESOLVED, JobStatus.ACCEPTED, _ as String)
        1 * persistenceService.getRequestedLauncherExt(jobId) >> requestedLauncherExt
        1 * agentLauncherSelector.select(_ as AgentLauncherSelectionContext) >> { throw new ResourceSelectionException("...") }
        0 * agentLauncher.launchAgent(resolvedJob, requestedLauncherExt)
        thrown(AgentLaunchException)

        when:
        service.launchJob(jobSubmission)

        then:
        1 * persistenceService.saveJobSubmission(jobSubmission) >> jobId
        1 * jobResolverService.resolveJob(jobId) >> resolvedJob
        1 * persistenceService.updateJobStatus(jobId, JobStatus.RESOLVED, JobStatus.ACCEPTED, _ as String)
        1 * persistenceService.getRequestedLauncherExt(jobId) >> requestedLauncherExt
        1 * agentLauncherSelector.select(_ as AgentLauncherSelectionContext) >> selectionResult
        1 * selectionResult.getSelectedResource() >> Optional.empty()
        1 * selectionResult.getSelectionRationale() >> Optional.empty()
        0 * agentLauncher.launchAgent(resolvedJob, requestedLauncherExt)
        thrown(AgentLaunchException)

        when:
        service.launchJob(jobSubmission)

        then:
        1 * persistenceService.saveJobSubmission(jobSubmission) >> jobId
        1 * jobResolverService.resolveJob(jobId) >> resolvedJob
        1 * persistenceService.updateJobStatus(jobId, JobStatus.RESOLVED, JobStatus.ACCEPTED, _ as String)
        1 * persistenceService.getRequestedLauncherExt(jobId) >> requestedLauncherExt
        1 * agentLauncherSelector.select(_ as AgentLauncherSelectionContext) >> selectionResult
        1 * selectionResult.getSelectedResource() >> Optional.of(agentLauncher)
        1 * agentLauncher.launchAgent(resolvedJob, requestedLauncherExt) >> {
            throw new AgentLaunchException("that didn't work")
        }
        1 * persistenceService.updateJobStatus(jobId, JobStatus.ACCEPTED, JobStatus.FAILED, _ as String)
        1 * persistenceService.updateJobArchiveStatus(jobId, ArchiveStatus.NO_FILES)
        thrown(AgentLaunchException)

        when:
        service.launchJob(jobSubmission)

        then:
        1 * persistenceService.saveJobSubmission(jobSubmission) >> jobId
        1 * jobResolverService.resolveJob(jobId) >> resolvedJob
        1 * persistenceService.updateJobStatus(jobId, JobStatus.RESOLVED, JobStatus.ACCEPTED, _ as String)
        1 * persistenceService.getRequestedLauncherExt(jobId) >> requestedLauncherExt
        1 * agentLauncherSelector.select(_ as AgentLauncherSelectionContext) >> selectionResult
        1 * selectionResult.getSelectedResource() >> Optional.of(agentLauncher)
        1 * agentLauncher.launchAgent(resolvedJob, requestedLauncherExt) >> Optional.of(launcherExt)
        1 * persistenceService.updateLauncherExt(jobId, launcherExt) >> { throw new NotFoundException("...") }
        0 * persistenceService.updateJobStatus(jobId, JobStatus.ACCEPTED, JobStatus.FAILED, _ as String)
        0 * persistenceService.updateJobArchiveStatus(jobId, ArchiveStatus.NO_FILES)
        thrown(NotFoundException)
    }
}
