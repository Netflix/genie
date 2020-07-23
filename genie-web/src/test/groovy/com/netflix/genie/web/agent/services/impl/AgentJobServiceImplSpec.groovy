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
package com.netflix.genie.web.agent.services.impl

import com.google.common.collect.Sets
import com.netflix.genie.common.external.dtos.v4.AgentClientMetadata
import com.netflix.genie.common.external.dtos.v4.ArchiveStatus
import com.netflix.genie.common.external.dtos.v4.JobRequest
import com.netflix.genie.common.external.dtos.v4.JobSpecification
import com.netflix.genie.common.external.dtos.v4.JobStatus
import com.netflix.genie.common.internal.exceptions.checked.GenieJobResolutionException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieAgentRejectedException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobNotFoundException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobSpecificationNotFoundException
import com.netflix.genie.web.agent.inspectors.InspectionReport
import com.netflix.genie.web.agent.services.AgentConfigurationService
import com.netflix.genie.web.agent.services.AgentFilterService
import com.netflix.genie.web.data.services.DataServices
import com.netflix.genie.web.data.services.PersistenceService
import com.netflix.genie.web.dtos.JobSubmission
import com.netflix.genie.web.dtos.ResolvedJob
import com.netflix.genie.web.exceptions.checked.NotFoundException
import com.netflix.genie.web.services.JobResolverService
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
@SuppressWarnings("GroovyAccessibility")
class AgentJobServiceImplSpec extends Specification {

    public static final String version = "1.2.3"
    public static final String hostname = "127.0.0.1"

    PersistenceService persistenceService
    JobResolverService jobSpecificationService
    AgentFilterService agentFilterService
    AgentConfigurationService agentConfigurationService;
    MeterRegistry meterRegistry
    AgentJobServiceImpl service
    Counter counter

    def setup() {
        this.persistenceService = Mock(PersistenceService)
        this.jobSpecificationService = Mock(JobResolverService)
        this.agentFilterService = Mock(AgentFilterService)
        this.agentConfigurationService = Mock(AgentConfigurationService)
        this.meterRegistry = Mock(MeterRegistry)
        this.counter = Mock(Counter)
        def dataServices = Mock(DataServices) {
            getPersistenceService() >> this.persistenceService
        }
        this.service = new AgentJobServiceImpl(
            dataServices,
            this.jobSpecificationService,
            this.agentFilterService,
            this.agentConfigurationService,
            this.meterRegistry
        )
    }

    def "Can handshake successfully"() {
        def agentClientMetadata = Mock(AgentClientMetadata)
        def inspectionReport = Mock(InspectionReport)
        Set<Tag> expectedTags = Sets.newHashSet(
            Tag.of(AgentJobServiceImpl.HANDSHAKE_DECISION_METRIC_TAG_NAME, InspectionReport.Decision.ACCEPT.name()),
            Tag.of(AgentJobServiceImpl.AGENT_VERSION_METRIC_TAG_NAME, version),
            MetricsUtils.SUCCESS_STATUS_TAG
        )

        when:
        service.handshake(agentClientMetadata)

        then:
        1 * agentClientMetadata.getVersion() >> Optional.of(version)
        1 * agentFilterService.inspectAgentMetadata(agentClientMetadata) >> inspectionReport
        2 * inspectionReport.getDecision() >> InspectionReport.Decision.ACCEPT
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
            Tag.of(AgentJobServiceImpl.HANDSHAKE_DECISION_METRIC_TAG_NAME, InspectionReport.Decision.REJECT.name()),
            Tag.of(AgentJobServiceImpl.AGENT_VERSION_METRIC_TAG_NAME, version),
            MetricsUtils.SUCCESS_STATUS_TAG
        )

        when:
        service.handshake(agentClientMetadata)

        then:
        1 * agentClientMetadata.getVersion() >> Optional.of(version)
        1 * agentFilterService.inspectAgentMetadata(agentClientMetadata) >> inspectionReport
        2 * inspectionReport.getDecision() >> InspectionReport.Decision.REJECT
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
            MetricsUtils.FAILURE_STATUS_TAG,
            Tag.of(MetricsConstants.TagKeys.EXCEPTION_CLASS, exception.getClass().getCanonicalName())
        )

        when:
        service.handshake(agentClientMetadata)

        then:
        thrown(exception.class)
        1 * agentClientMetadata.getVersion() >> Optional.of(version)
        1 * agentFilterService.inspectAgentMetadata(agentClientMetadata) >> { throw exception }
        1 * meterRegistry.counter("genie.services.agentJob.handshake.counter", _ as Set<Tag>) >> {
            args ->
                assert args[1] as Set<Tag> == expectedTags
                return counter
        }
        1 * counter.increment()
    }

    def "Can get agent properties"() {
        def agentClientMetadata = Mock(AgentClientMetadata)
        Set<Tag> expectedTags = Sets.newHashSet(
            Tag.of(AgentJobServiceImpl.AGENT_VERSION_METRIC_TAG_NAME, version),
            MetricsUtils.SUCCESS_STATUS_TAG
        )

        when:
        Map<String, String> propertiesMap = service.getAgentProperties(agentClientMetadata)

        then:
        1 * agentConfigurationService.getAgentProperties() >> [:]
        1 * meterRegistry.counter("genie.services.agentJob.getAgentProperties.counter", _ as Set<Tag>) >> {
            args ->
                assert args[1] as Set<Tag> == expectedTags
                return counter
        }
        1 * agentClientMetadata.getVersion() >> Optional.of(version)
        1 * counter.increment()
        propertiesMap != null
    }


    def "Can handle get agent properties exception"() {
        def agentClientMetadata = Mock(AgentClientMetadata)
        def exception = new RuntimeException("...")
        Set<Tag> expectedTags = Sets.newHashSet(
            Tag.of(AgentJobServiceImpl.AGENT_VERSION_METRIC_TAG_NAME, version),
        )
        MetricsUtils.addFailureTagsWithException(expectedTags, exception)

        when:
        service.getAgentProperties(agentClientMetadata)

        then:
        1 * agentConfigurationService.getAgentProperties() >> { throw exception }
        1 * meterRegistry.counter("genie.services.agentJob.getAgentProperties.counter", _ as Set<Tag>) >> {
            args ->
                assert args[1] as Set<Tag> == expectedTags
                return counter
        }
        1 * agentClientMetadata.getVersion() >> Optional.of(version)
        1 * counter.increment()
        thrown(RuntimeException)
    }

    def "Can reserve job id"() {
        def jobRequest = Mock(JobRequest)
        def agentClientMetadata = Mock(AgentClientMetadata)
        def reservedId = UUID.randomUUID().toString()

        when:
        def id = service.reserveJobId(jobRequest, agentClientMetadata)

        then:
        1 * persistenceService.saveJobSubmission(_ as JobSubmission) >> reservedId
        id == reservedId
    }

    def "Can Resolve Job Specification"() {
        def jobId = UUID.randomUUID().toString()
        def jobRequest = Mock(JobRequest)
        def resolvedJobMock = Mock(ResolvedJob)
        def jobSpecificationMock = Mock(JobSpecification)

        when:
        service.resolveJobSpecification(jobId)

        then:
        1 * persistenceService.getJobRequest(jobId) >> { throw new NotFoundException("Not found") }
        thrown(GenieJobResolutionException)

        when:
        def jobSpecification = service.resolveJobSpecification(jobId)

        then:
        1 * persistenceService.getJobRequest(jobId) >> jobRequest
        1 * this.jobSpecificationService.resolveJob(jobId, jobRequest, false) >> resolvedJobMock
        1 * resolvedJobMock.getJobSpecification() >> jobSpecificationMock
        1 * persistenceService.saveResolvedJob(jobId, resolvedJobMock)
        jobSpecification == jobSpecificationMock
    }

    def "Can retrieve Job Specification"() {
        def jobId = UUID.randomUUID().toString()

        def jobSpecificationMock = Mock(JobSpecification)
        JobSpecification jobSpecification

        when:
        jobSpecification = service.getJobSpecification(jobId)

        then:
        1 * persistenceService.getJobSpecification(jobId) >> Optional.of(jobSpecificationMock)
        jobSpecification == jobSpecificationMock

        when:
        service.getJobSpecification(jobId)

        then:
        1 * persistenceService.getJobSpecification(jobId) >> Optional.empty()
        thrown(GenieJobSpecificationNotFoundException)
    }

    def "Can dry run job specification resolution"() {
        def jobRequest = Mock(JobRequest)

        def jobSpecificationMock = Mock(JobSpecification)
        def resolvedJobMock = Mock(ResolvedJob)
        def id = UUID.randomUUID().toString()

        when:
        def jobSpecification = service.dryRunJobSpecificationResolution(jobRequest)

        then:
        1 * jobRequest.getRequestedId() >> Optional.empty()
        1 * jobSpecificationService.resolveJob(_ as String, jobRequest, false) >> resolvedJobMock
        1 * resolvedJobMock.getJobSpecification() >> jobSpecificationMock
        jobSpecification == jobSpecificationMock

        when:
        jobSpecification = service.dryRunJobSpecificationResolution(jobRequest)

        then:
        1 * jobRequest.getRequestedId() >> Optional.of(id)
        1 * jobSpecificationService.resolveJob(id, jobRequest, false) >> resolvedJobMock
        1 * resolvedJobMock.getJobSpecification() >> jobSpecificationMock
        jobSpecification == jobSpecificationMock
    }

    def "Can claim job"() {
        def agentClientMetadata = Mock(AgentClientMetadata)
        def id = UUID.randomUUID().toString()

        when:
        service.claimJob(id, agentClientMetadata)

        then:
        1 * persistenceService.claimJob(id, agentClientMetadata)
    }

    def "Can update job status"() {
        def id = UUID.randomUUID().toString()

        when:
        service.updateJobStatus(id, JobStatus.CLAIMED, JobStatus.INIT, UUID.randomUUID().toString())

        then:
        1 * persistenceService.updateJobStatus(id, JobStatus.CLAIMED, JobStatus.INIT, _ as String)
    }

    def "Can get job status"() {
        def id = UUID.randomUUID().toString()
        def status = JobStatus.KILLED

        when:
        def s = service.getJobStatus(id)

        then:
        1 * persistenceService.getJobStatus(id) >> status
        s == status
    }

    def "Can handle get job status not found"() {
        def id = UUID.randomUUID().toString()

        when:
        service.getJobStatus(id)

        then:
        1 * persistenceService.getJobStatus(id) >> { throw new NotFoundException("...") }
        thrown(GenieJobNotFoundException)
    }

    def "Can update job archive status"() {
        def id = UUID.randomUUID().toString()

        when:
        service.updateJobArchiveStatus(id, status)

        then:
        1 * persistenceService.updateJobArchiveStatus(id, status)

        when:
        service.updateJobArchiveStatus(id, status)

        then:
        1 * persistenceService.updateJobArchiveStatus(id, status) >> { throw new NotFoundException("...") }
        thrown(GenieJobNotFoundException)

        where:
        status                 | _
        ArchiveStatus.ARCHIVED | _
        ArchiveStatus.FAILED   | _
    }
}
