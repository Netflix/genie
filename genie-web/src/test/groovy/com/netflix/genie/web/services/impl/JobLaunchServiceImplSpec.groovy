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
import com.netflix.genie.common.dto.JobStatusMessages
import com.netflix.genie.common.external.dtos.v4.ArchiveStatus
import com.netflix.genie.common.external.dtos.v4.JobRequest
import com.netflix.genie.common.external.dtos.v4.JobRequestMetadata
import com.netflix.genie.common.external.dtos.v4.JobStatus
import com.netflix.genie.common.internal.exceptions.checked.GenieJobResolutionException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieInvalidStatusException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobResolutionRuntimeException
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
@SuppressWarnings("GroovyAccessibility")
class JobLaunchServiceImplSpec extends Specification {

    PersistenceService persistenceService
    JobResolverService jobResolverService
    AgentLauncherSelector agentLauncherSelector
    JobLaunchServiceImpl service

    def setup() {
        this.persistenceService = Mock(PersistenceService)
        this.jobResolverService = Mock(JobResolverService)
        this.agentLauncherSelector = Mock(AgentLauncherSelector)
        def dataServices = Mock(DataServices) {
            getPersistenceService() >> this.persistenceService
        }
        this.service = new JobLaunchServiceImpl(
            dataServices,
            this.jobResolverService,
            this.agentLauncherSelector,
            new SimpleMeterRegistry()
        )
    }

    @Unroll
    def "Successful launch (requestedLauncherExt: #requestedLauncherExt launcherExt: #launcherExt)"() {
        def agentLauncher = Mock(AgentLauncher)
        def jobId = UUID.randomUUID().toString()
        def resolvedJob = Mock(ResolvedJob)
        def jobSubmission = Mock(JobSubmission)
        def jobRequest = Mock(JobRequest)
        def jobRequestMetadata = Mock(JobRequestMetadata)
        def selectionResult = Mock(ResourceSelectionResult)

        when:
        def savedJobId = this.service.launchJob(jobSubmission)

        then:
        1 * this.persistenceService.saveJobSubmission(jobSubmission) >> jobId
        1 * this.jobResolverService.resolveJob(jobId) >> resolvedJob
        1 * this.persistenceService.getJobStatus(jobId) >> JobStatus.RESOLVED
        1 * this.persistenceService.updateJobStatus(jobId, JobStatus.RESOLVED, JobStatus.ACCEPTED, _ as String)
        1 * this.agentLauncherSelector.getAgentLaunchers() >> [agentLauncher]
        1 * jobSubmission.getJobRequest() >> jobRequest
        1 * jobSubmission.getJobRequestMetadata() >> jobRequestMetadata
        1 * this.persistenceService.getRequestedLauncherExt(jobId) >> requestedLauncherExt
        1 * this.agentLauncherSelector.select(_ as AgentLauncherSelectionContext) >> selectionResult
        1 * selectionResult.getSelectedResource() >> Optional.of(agentLauncher)
        1 * agentLauncher.launchAgent(resolvedJob, requestedLauncherExt) >> Optional.ofNullable(launcherExt)
        if (launcherExt != null) {
            1 * this.persistenceService.updateLauncherExt(jobId, launcherExt)
        } else {
            0 * this.persistenceService.updateLauncherExt(_, _)
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
        def agentLauncher = Mock(AgentLauncher)
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
        this.service.launchJob(jobSubmission)

        then:
        1 * this.persistenceService.saveJobSubmission(jobSubmission) >> {
            throw new IllegalStateException("fail")
        }
        0 * this.jobResolverService.resolveJob(_ as String)
        0 * this.persistenceService.updateJobStatus(jobId, JobStatus.RESOLVED, JobStatus.ACCEPTED, _ as String)
        0 * this.persistenceService.updateJobArchiveStatus(_, _)
        thrown(IllegalStateException)

        when:
        this.service.launchJob(jobSubmission)

        then:
        1 * this.persistenceService.saveJobSubmission(jobSubmission) >> {
            throw new IdAlreadyExistsException("try again")
        }
        0 * this.jobResolverService.resolveJob(_ as String)
        0 * this.persistenceService.updateJobStatus(jobId, JobStatus.RESOLVED, JobStatus.ACCEPTED, _ as String)
        0 * this.persistenceService.updateJobArchiveStatus(_, _)
        0 * agentLauncher.launchAgent(_ as ResolvedJob, requestedLauncherExt)
        thrown(IdAlreadyExistsException)

        when:
        this.service.launchJob(jobSubmission)

        then:
        1 * this.persistenceService.saveJobSubmission(jobSubmission) >> jobId
        1 * this.jobResolverService.resolveJob(jobId) >> {
            throw new GenieJobResolutionException("fail")
        }
        1 * this.persistenceService.getJobStatus(jobId) >> JobStatus.RESERVED
        1 * this.persistenceService.updateJobStatus(
            jobId,
            JobStatus.RESERVED,
            JobStatus.FAILED,
            JobStatusMessages.FAILED_TO_RESOLVE_JOB
        )
        1 * this.persistenceService.updateJobArchiveStatus(jobId, ArchiveStatus.NO_FILES)
        0 * this.persistenceService.updateJobStatus(jobId, JobStatus.RESOLVED, JobStatus.ACCEPTED, _ as String)
        0 * agentLauncher.launchAgent(_ as ResolvedJob, requestedLauncherExt)
        thrown(GenieJobResolutionException)

        when:
        this.service.launchJob(jobSubmission)

        then:
        1 * this.persistenceService.saveJobSubmission(jobSubmission) >> jobId
        1 * this.jobResolverService.resolveJob(jobId) >> {
            throw new GenieJobResolutionRuntimeException("fail")
        }
        1 * this.persistenceService.getJobStatus(jobId) >> JobStatus.RESERVED
        1 * this.persistenceService.updateJobStatus(
            jobId,
            JobStatus.RESERVED,
            JobStatus.FAILED,
            JobStatusMessages.RESOLUTION_RUNTIME_ERROR
        )
        1 * this.persistenceService.updateJobArchiveStatus(jobId, ArchiveStatus.NO_FILES)
        0 * this.persistenceService.updateJobStatus(jobId, JobStatus.RESOLVED, JobStatus.ACCEPTED, _ as String)
        0 * agentLauncher.launchAgent(_ as ResolvedJob, requestedLauncherExt)
        thrown(GenieJobResolutionRuntimeException)

        when: "Job was killed during submission"
        this.service.launchJob(jobSubmission)

        then:
        1 * this.persistenceService.saveJobSubmission(jobSubmission) >> jobId
        1 * this.jobResolverService.resolveJob(jobId) >> resolvedJob
        1 * this.persistenceService.getJobStatus(jobId) >> JobStatus.KILLED
        0 * this.persistenceService.updateJobStatus(jobId, JobStatus.RESOLVED, JobStatus.ACCEPTED, _ as String)
        1 * this.persistenceService.updateJobArchiveStatus(jobId, ArchiveStatus.NO_FILES)
        0 * agentLauncher.launchAgent(_ as ResolvedJob, requestedLauncherExt)
        thrown(AgentLaunchException)

        when:
        this.service.launchJob(jobSubmission)

        then:
        1 * this.persistenceService.saveJobSubmission(jobSubmission) >> jobId
        1 * this.jobResolverService.resolveJob(jobId) >> resolvedJob
        JobLaunchServiceImpl.MAX_STATUS_UPDATE_ATTEMPTS * this.persistenceService.getJobStatus(jobId) >> JobStatus.RESOLVED
        JobLaunchServiceImpl.MAX_STATUS_UPDATE_ATTEMPTS * this.persistenceService.updateJobStatus(
            jobId,
            JobStatus.RESOLVED,
            JobStatus.ACCEPTED,
            _ as String
        ) >> { throw new GenieInvalidStatusException() }
        1 * this.persistenceService.updateJobArchiveStatus(jobId, ArchiveStatus.NO_FILES)
        0 * agentLauncher.launchAgent(_ as ResolvedJob, requestedLauncherExt)
        0 * this.persistenceService.updateJobStatus(jobId, JobStatus.ACCEPTED, JobStatus.FAILED, _ as String)
        thrown(AgentLaunchException)

        when:
        this.service.launchJob(jobSubmission)

        then:
        1 * this.persistenceService.saveJobSubmission(jobSubmission) >> jobId
        1 * this.jobResolverService.resolveJob(jobId) >> resolvedJob
        2 * this.persistenceService.getJobStatus(jobId) >>> [JobStatus.RESOLVED, JobStatus.ACCEPTED]
        1 * this.persistenceService.updateJobStatus(jobId, JobStatus.RESOLVED, JobStatus.ACCEPTED, _ as String)
        1 * this.persistenceService.getRequestedLauncherExt(jobId) >> requestedLauncherExt
        1 * this.agentLauncherSelector.getAgentLaunchers() >> [agentLauncher]
        1 * this.agentLauncherSelector.select(_ as AgentLauncherSelectionContext) >> {
            throw new ResourceSelectionException("...")
        }
        1 * this.persistenceService.updateJobStatus(jobId, JobStatus.ACCEPTED, JobStatus.FAILED, _ as String)
        0 * agentLauncher.launchAgent(resolvedJob, requestedLauncherExt)
        thrown(AgentLaunchException)

        when:
        this.service.launchJob(jobSubmission)

        then:
        1 * this.persistenceService.saveJobSubmission(jobSubmission) >> jobId
        1 * this.jobResolverService.resolveJob(jobId) >> resolvedJob
        2 * this.persistenceService.getJobStatus(jobId) >>> [JobStatus.RESOLVED, JobStatus.ACCEPTED]
        1 * this.persistenceService.updateJobStatus(jobId, JobStatus.RESOLVED, JobStatus.ACCEPTED, _ as String)
        1 * this.persistenceService.getRequestedLauncherExt(jobId) >> requestedLauncherExt
        1 * this.agentLauncherSelector.getAgentLaunchers() >> [agentLauncher]
        1 * this.agentLauncherSelector.select(_ as AgentLauncherSelectionContext) >> selectionResult
        1 * selectionResult.getSelectedResource() >> Optional.empty()
        1 * selectionResult.getSelectionRationale() >> Optional.empty()
        1 * this.persistenceService.updateJobStatus(jobId, JobStatus.ACCEPTED, JobStatus.FAILED, _ as String)
        1 * this.persistenceService.updateJobArchiveStatus(jobId, ArchiveStatus.NO_FILES)
        0 * agentLauncher.launchAgent(resolvedJob, requestedLauncherExt)
        thrown(AgentLaunchException)

        when:
        this.service.launchJob(jobSubmission)

        then:
        1 * this.persistenceService.saveJobSubmission(jobSubmission) >> jobId
        1 * this.jobResolverService.resolveJob(jobId) >> resolvedJob
        2 * this.persistenceService.getJobStatus(jobId) >>> [JobStatus.RESOLVED, JobStatus.ACCEPTED]
        1 * this.persistenceService.updateJobStatus(jobId, JobStatus.RESOLVED, JobStatus.ACCEPTED, _ as String)
        1 * this.persistenceService.getRequestedLauncherExt(jobId) >> requestedLauncherExt
        1 * this.agentLauncherSelector.getAgentLaunchers() >> [agentLauncher]
        1 * this.agentLauncherSelector.select(_ as AgentLauncherSelectionContext) >> selectionResult
        1 * selectionResult.getSelectedResource() >> Optional.of(agentLauncher)
        1 * agentLauncher.launchAgent(resolvedJob, requestedLauncherExt) >> {
            throw new AgentLaunchException("that didn't work")
        }
        1 * this.persistenceService.updateJobStatus(jobId, JobStatus.ACCEPTED, JobStatus.FAILED, _ as String)
        1 * this.persistenceService.updateJobArchiveStatus(jobId, ArchiveStatus.NO_FILES)
        thrown(AgentLaunchException)

        when:
        this.service.launchJob(jobSubmission)

        then:
        1 * this.persistenceService.saveJobSubmission(jobSubmission) >> jobId
        1 * this.jobResolverService.resolveJob(jobId) >> resolvedJob
        1 * this.persistenceService.getJobStatus(jobId) >> JobStatus.RESOLVED
        1 * this.persistenceService.updateJobStatus(jobId, JobStatus.RESOLVED, JobStatus.ACCEPTED, _ as String)
        1 * this.persistenceService.getRequestedLauncherExt(jobId) >> requestedLauncherExt
        1 * this.agentLauncherSelector.getAgentLaunchers() >> [agentLauncher]
        1 * this.agentLauncherSelector.select(_ as AgentLauncherSelectionContext) >> selectionResult
        1 * selectionResult.getSelectedResource() >> Optional.of(agentLauncher)
        1 * agentLauncher.launchAgent(resolvedJob, requestedLauncherExt) >> Optional.of(launcherExt)
        1 * this.persistenceService.updateLauncherExt(jobId, launcherExt) >> { throw new NotFoundException("...") }
        0 * this.persistenceService.updateJobStatus(jobId, JobStatus.ACCEPTED, JobStatus.FAILED, _ as String)
        0 * this.persistenceService.updateJobArchiveStatus(jobId, ArchiveStatus.NO_FILES)
        noExceptionThrown()
    }

    def "update job status works as expected"() {
        def jobId = UUID.randomUUID().toString()
        def desiredStatus = JobStatus.ACCEPTED
        def desiredMessage = UUID.randomUUID().toString()
        def attemptNumber = 0

        when: "The current status is already finished"
        def jobStatus = this.service.updateJobStatus(jobId, desiredStatus, desiredMessage, attemptNumber)

        then: "Nothing happens"
        1 * this.persistenceService.getJobStatus(jobId) >> JobStatus.KILLED
        0 * this.persistenceService.updateJobStatus(_ as String, _ as JobStatus, _ as JobStatus, _ as String)
        jobStatus == JobStatus.KILLED
        noExceptionThrown()

        when: "The current status isn't finished but changes when updated is attempted"
        jobStatus = this.service.updateJobStatus(jobId, desiredStatus, desiredMessage, attemptNumber)

        then: "Method is retried but finished status is respected"
        2 * this.persistenceService.getJobStatus(jobId) >>> [JobStatus.RESERVED, JobStatus.KILLED]
        1 * this.persistenceService.updateJobStatus(jobId, JobStatus.RESERVED, desiredStatus, desiredMessage) >> {
            throw new GenieInvalidStatusException("killed")
        }
        jobStatus == JobStatus.KILLED
        noExceptionThrown()

        when: "The current status isn't finished but changes when updated is attempted"
        jobStatus = this.service.updateJobStatus(jobId, desiredStatus, desiredMessage, attemptNumber)

        then: "Method is retried and succeeds"
        2 * this.persistenceService.getJobStatus(jobId) >>> [JobStatus.RESERVED, JobStatus.RESOLVED]
        1 * this.persistenceService.updateJobStatus(jobId, JobStatus.RESERVED, desiredStatus, desiredMessage) >> {
            throw new GenieInvalidStatusException("resolved")
        }
        1 * this.persistenceService.updateJobStatus(jobId, JobStatus.RESOLVED, desiredStatus, desiredMessage)
        jobStatus == desiredStatus
        noExceptionThrown()

        when: "Max retries are exceeded"
        jobStatus = this.service.updateJobStatus(jobId, desiredStatus, desiredMessage, attemptNumber)

        then: "Exception is swallowed and failure returned"
        JobLaunchServiceImpl.MAX_STATUS_UPDATE_ATTEMPTS * this.persistenceService.getJobStatus(jobId) >>
            JobStatus.RESERVED
        JobLaunchServiceImpl.MAX_STATUS_UPDATE_ATTEMPTS * this.persistenceService.updateJobStatus(
            jobId,
            JobStatus.RESERVED,
            desiredStatus,
            desiredMessage
        ) >> {
            throw new GenieInvalidStatusException("resolved")
        }
        noExceptionThrown()
        jobStatus == JobStatus.RESERVED
    }
}
