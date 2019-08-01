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
import com.netflix.genie.web.agent.launchers.AgentLauncher
import com.netflix.genie.web.data.services.JobPersistenceService
import com.netflix.genie.web.dtos.JobSubmission
import com.netflix.genie.web.dtos.ResolvedJob
import com.netflix.genie.web.exceptions.checked.AgentLaunchException
import com.netflix.genie.web.exceptions.checked.IdAlreadyExistsException
import com.netflix.genie.web.exceptions.checked.SaveAttachmentException
import com.netflix.genie.web.services.JobResolverService
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import spock.lang.Specification

/**
 * Specifications for {@link JobLaunchServiceImpl}.
 *
 * @author tgianos
 */
class JobLaunchServiceImplSpec extends Specification {

    def "successful launch returns the job id"() {
        def jobPersistenceService = Mock(JobPersistenceService)
        def jobResolverService = Mock(JobResolverService)
        def agentLauncher = Mock(AgentLauncher)
        def registry = new SimpleMeterRegistry()
        def service = new JobLaunchServiceImpl(jobPersistenceService, jobResolverService, agentLauncher, registry)

        def jobId = UUID.randomUUID().toString()
        def resolvedJob = Mock(ResolvedJob)
        def jobSubmission = Mock(JobSubmission)

        when:
        def savedJobId = service.launchJob(jobSubmission)

        then:
        1 * jobPersistenceService.saveJobSubmission(jobSubmission) >> jobId
        1 * jobResolverService.resolveJob(jobId) >> resolvedJob
        1 * jobPersistenceService.updateJobStatus(jobId, JobStatus.RESOLVED, JobStatus.ACCEPTED, _ as String)
        1 * agentLauncher.launchAgent(resolvedJob)
        savedJobId == jobId
    }

    def "error cases throw expected exceptions"() {
        def jobPersistenceService = Mock(JobPersistenceService)
        def jobResolverService = Mock(JobResolverService)
        def agentLauncher = Mock(AgentLauncher)
        def registry = new SimpleMeterRegistry()
        def service = new JobLaunchServiceImpl(jobPersistenceService, jobResolverService, agentLauncher, registry)

        def jobId = UUID.randomUUID().toString()
        def resolvedJob = Mock(ResolvedJob)
        def jobSubmission = Mock(JobSubmission)

        when:
        service.launchJob(jobSubmission)

        then:
        1 * jobPersistenceService.saveJobSubmission(jobSubmission) >> {
            throw new IllegalStateException("fail")
        }
        0 * jobResolverService.resolveJob(_ as String)
        0 * jobPersistenceService.updateJobStatus(jobId, JobStatus.RESOLVED, JobStatus.ACCEPTED, _ as String)
        0 * agentLauncher.launchAgent(_ as ResolvedJob)
        thrown(IllegalStateException)

        when:
        service.launchJob(jobSubmission)

        then:
        1 * jobPersistenceService.saveJobSubmission(jobSubmission) >> {
            throw new IdAlreadyExistsException("try again")
        }
        0 * jobResolverService.resolveJob(_ as String)
        0 * jobPersistenceService.updateJobStatus(jobId, JobStatus.RESOLVED, JobStatus.ACCEPTED, _ as String)
        0 * agentLauncher.launchAgent(_ as ResolvedJob)
        thrown(IdAlreadyExistsException)

        when:
        service.launchJob(jobSubmission)

        then:
        1 * jobPersistenceService.saveJobSubmission(jobSubmission) >> {
            throw new SaveAttachmentException("hmm that's not good")
        }
        0 * jobResolverService.resolveJob(_ as String)
        0 * jobPersistenceService.updateJobStatus(jobId, JobStatus.RESOLVED, JobStatus.ACCEPTED, _ as String)
        0 * agentLauncher.launchAgent(_ as ResolvedJob)
        thrown(SaveAttachmentException)

        when:
        service.launchJob(jobSubmission)

        then:
        1 * jobPersistenceService.saveJobSubmission(jobSubmission) >> jobId
        1 * jobResolverService.resolveJob(jobId) >> {
            throw new IllegalArgumentException("fail")
        }
        0 * jobPersistenceService.updateJobStatus(jobId, JobStatus.RESOLVED, JobStatus.ACCEPTED, _ as String)
        0 * agentLauncher.launchAgent(_ as ResolvedJob)
        thrown(AgentLaunchException)

        when:
        service.launchJob(jobSubmission)

        then:
        1 * jobPersistenceService.saveJobSubmission(jobSubmission) >> jobId
        1 * jobResolverService.resolveJob(jobId) >> resolvedJob
        1 * jobPersistenceService.updateJobStatus(jobId, JobStatus.RESOLVED, JobStatus.ACCEPTED, _ as String) >> {
            throw new RuntimeException("fail")
        }
        0 * agentLauncher.launchAgent(_ as ResolvedJob)
        thrown(AgentLaunchException)

        when:
        service.launchJob(jobSubmission)

        then:
        1 * jobPersistenceService.saveJobSubmission(jobSubmission) >> jobId
        1 * jobResolverService.resolveJob(jobId) >> resolvedJob
        1 * jobPersistenceService.updateJobStatus(jobId, JobStatus.RESOLVED, JobStatus.ACCEPTED, _ as String)
        1 * agentLauncher.launchAgent(resolvedJob) >> {
            throw new AgentLaunchException("that didn't work")
        }
        1 * jobPersistenceService.updateJobStatus(jobId, JobStatus.ACCEPTED, JobStatus.FAILED, _ as String)
        thrown(AgentLaunchException)
    }
}
