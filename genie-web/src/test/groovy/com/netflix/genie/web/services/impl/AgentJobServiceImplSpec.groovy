/*
 *
 *  Copyright 2018 Netflix, Inc.
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
import com.netflix.genie.common.dto.JobStatus
import com.netflix.genie.common.internal.dto.v4.AgentClientMetadata
import com.netflix.genie.common.internal.dto.v4.JobRequest
import com.netflix.genie.common.internal.dto.v4.JobRequestMetadata
import com.netflix.genie.common.internal.dto.v4.JobSpecification
import com.netflix.genie.common.internal.exceptions.unchecked.GenieAgentRejectedException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobNotFoundException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobSpecificationNotFoundException
import com.netflix.genie.web.services.AgentFilterService
import com.netflix.genie.web.services.JobPersistenceService
import com.netflix.genie.web.services.JobSpecificationService
import com.netflix.genie.web.util.InspectionReport
import com.netflix.genie.web.util.InspectionReport.Decision
import com.netflix.genie.web.util.MetricsConstants
import com.netflix.genie.web.util.MetricsUtils
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import spock.lang.Specification

/**
 * Specifications for the {@link AgentJobServiceImpl} class.
 *
 * @author tgianos
 */
class AgentJobServiceImplSpec extends Specification {

    public static final String version = "1.2.3"
    public static final String hostname = "127.0.0.1"


    JobPersistenceService jobPersistenceService
    JobSpecificationService jobSpecificationService
    AgentFilterService agentFilterService
    MeterRegistry meterRegistry
    AgentJobServiceImpl service
    Counter counter

    def setup() {
        this.jobPersistenceService = Mock(JobPersistenceService)
        this.jobSpecificationService = Mock(JobSpecificationService)
        this.agentFilterService = Mock(AgentFilterService)
        this.meterRegistry = Mock(MeterRegistry)
        this.counter = Mock(Counter)
        this.service = new AgentJobServiceImpl(
            jobPersistenceService,
            jobSpecificationService,
            agentFilterService,
            meterRegistry
        )
    }

    def "Can handshake successfully"() {
        def agentClientMetadata = Mock(AgentClientMetadata)
        def inspectionReport = Mock(InspectionReport)
        Set<Tag> expectedTags = Sets.newHashSet(
            Tag.of(AgentJobServiceImpl.HANDSHAKE_DECISION_METRIC_TAG_NAME, Decision.ACCEPT.name()),
            Tag.of(AgentJobServiceImpl.AGENT_VERSION_METRIC_TAG_NAME, version),
            Tag.of(AgentJobServiceImpl.AGENT_HOST_METRIC_TAG_NAME, hostname),
            MetricsUtils.SUCCESS_STATUS_TAG
        )

        when:
        service.handshake(agentClientMetadata)

        then:
        1 * agentClientMetadata.getVersion() >> Optional.of(version)
        1 * agentClientMetadata.getHostname() >> Optional.of(hostname)
        1 * agentFilterService.inspectAgentMetadata(agentClientMetadata) >> inspectionReport
        2 * inspectionReport.getDecision() >> Decision.ACCEPT
        0 * inspectionReport.getMessage()
        1 * meterRegistry.counter("genie.services.agentJob.handshake.counter", _ as Set<Tag>) >> {
            args ->
                assert args[1] as Set<Tag> == expectedTags
                return counter
        }
        1 * counter.increment()
    }

    def "Can handshake rejection"() {
        def agentClientMetadata = Mock(AgentClientMetadata)
        def inspectionReport = Mock(InspectionReport)
        String message = "Agent version is deprecated"
        Set<Tag> expectedTags = Sets.newHashSet(
            Tag.of(AgentJobServiceImpl.HANDSHAKE_DECISION_METRIC_TAG_NAME, Decision.REJECT.name()),
            Tag.of(AgentJobServiceImpl.AGENT_VERSION_METRIC_TAG_NAME, version),
            Tag.of(AgentJobServiceImpl.AGENT_HOST_METRIC_TAG_NAME, hostname),
            MetricsUtils.SUCCESS_STATUS_TAG
        )

        when:
        service.handshake(agentClientMetadata)

        then:
        1 * agentClientMetadata.getVersion() >> Optional.of(version)
        1 * agentClientMetadata.getHostname() >> Optional.of(hostname)
        1 * agentFilterService.inspectAgentMetadata(agentClientMetadata) >> inspectionReport
        2 * inspectionReport.getDecision() >> Decision.REJECT
        1 * inspectionReport.getMessage() >> message
        1 * meterRegistry.counter("genie.services.agentJob.handshake.counter", _ as Set<Tag>) >> {
            args ->
                assert args[1] as Set<Tag> == expectedTags
                return counter
        }
        1 * counter.increment()
        def e = thrown(GenieAgentRejectedException)
        e.getMessage().contains(message)
    }

    def "Can handle handshake exception"() {
        def agentClientMetadata = Mock(AgentClientMetadata)
        def exception = new RuntimeException("...")
        Set<Tag> expectedTags = Sets.newHashSet(
            Tag.of(AgentJobServiceImpl.AGENT_VERSION_METRIC_TAG_NAME, version),
            Tag.of(AgentJobServiceImpl.AGENT_HOST_METRIC_TAG_NAME, hostname),
            MetricsUtils.FAILURE_STATUS_TAG,
            Tag.of(MetricsConstants.TagKeys.EXCEPTION_CLASS, exception.getClass().getCanonicalName())
        )

        when:
        service.handshake(agentClientMetadata)

        then:
        thrown(exception.class)
        1 * agentClientMetadata.getVersion() >> Optional.of(version)
        1 * agentClientMetadata.getHostname() >> Optional.of(hostname)
        1 * agentFilterService.inspectAgentMetadata(agentClientMetadata) >> { throw exception }
        1 * meterRegistry.counter("genie.services.agentJob.handshake.counter", _ as Set<Tag>) >> {
            args ->
                assert args[1] as Set<Tag> == expectedTags
                return counter
        }
        1 * counter.increment()
    }


    def "Can reserve job id"() {
        def jobRequest = Mock(JobRequest)
        def agentClientMetadata = Mock(AgentClientMetadata)
        def reservedId = UUID.randomUUID().toString()

        when:
        def id = service.reserveJobId(jobRequest, agentClientMetadata)

        then:
        1 * jobPersistenceService.saveJobRequest(jobRequest, _ as JobRequestMetadata) >> reservedId
        id == reservedId
    }

    def "Can Resolve Job Specification"() {
        def jobId = UUID.randomUUID().toString()
        def jobRequest = Mock(JobRequest)
        def jobSpecificationMock = Mock(JobSpecification)

        when:
        service.resolveJobSpecification(jobId)

        then:
        1 * jobPersistenceService.getJobRequest(jobId) >> Optional.empty()
        thrown(GenieJobNotFoundException)

        when:
        def jobSpecification = service.resolveJobSpecification(jobId)

        then:
        1 * jobPersistenceService.getJobRequest(jobId) >> Optional.of(jobRequest)
        1 * this.jobSpecificationService.resolveJobSpecification(jobId, jobRequest) >> jobSpecificationMock
        1 * jobPersistenceService.saveJobSpecification(jobId, jobSpecificationMock)
        jobSpecification == jobSpecificationMock
    }

    def "Can retrieve Job Specification"() {
        def jobId = UUID.randomUUID().toString()

        def jobSpecificationMock = Mock(JobSpecification)
        JobSpecification jobSpecification

        when:
        jobSpecification = service.getJobSpecification(jobId)

        then:
        1 * jobPersistenceService.getJobSpecification(jobId) >> Optional.of(jobSpecificationMock)
        jobSpecification == jobSpecificationMock

        when:
        service.getJobSpecification(jobId)

        then:
        1 * jobPersistenceService.getJobSpecification(jobId) >> Optional.empty()
        thrown(GenieJobSpecificationNotFoundException)
    }

    def "Can dry run job specification resolution"() {
        def jobRequest = Mock(JobRequest)

        def jobSpecificationMock = Mock(JobSpecification)
        def id = UUID.randomUUID().toString()

        when:
        def jobSpecification = service.dryRunJobSpecificationResolution(jobRequest)

        then:
        1 * jobRequest.getRequestedId() >> Optional.empty()
        1 * jobSpecificationService.resolveJobSpecification(_ as String, jobRequest) >> jobSpecificationMock
        jobSpecification == jobSpecificationMock

        when:
        jobSpecification = service.dryRunJobSpecificationResolution(jobRequest)

        then:
        1 * jobRequest.getRequestedId() >> Optional.of(id)
        1 * jobSpecificationService.resolveJobSpecification(id, jobRequest) >> jobSpecificationMock
        jobSpecification == jobSpecificationMock
    }

    def "Can claim job"() {
        def agentClientMetadata = Mock(AgentClientMetadata)
        def id = UUID.randomUUID().toString()

        when:
        service.claimJob(id, agentClientMetadata)

        then:
        1 * jobPersistenceService.claimJob(id, agentClientMetadata)
    }

    def "Can update job status"() {
        def id = UUID.randomUUID().toString()

        when:
        service.updateJobStatus(id, JobStatus.CLAIMED, JobStatus.INIT, UUID.randomUUID().toString())

        then:
        1 * jobPersistenceService.updateJobStatus(id, JobStatus.CLAIMED, JobStatus.INIT, _ as String)
    }
}
