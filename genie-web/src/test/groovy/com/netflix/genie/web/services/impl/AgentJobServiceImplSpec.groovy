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

import com.netflix.genie.common.dto.JobStatus
import com.netflix.genie.common.internal.dto.v4.AgentClientMetadata
import com.netflix.genie.common.internal.dto.v4.JobRequest
import com.netflix.genie.common.internal.dto.v4.JobRequestMetadata
import com.netflix.genie.common.internal.dto.v4.JobSpecification
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobNotFoundException
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobSpecificationNotFoundException
import com.netflix.genie.test.categories.UnitTest
import com.netflix.genie.web.services.JobPersistenceService
import com.netflix.genie.web.services.JobSpecificationService
import io.micrometer.core.instrument.MeterRegistry
import org.junit.experimental.categories.Category
import spock.lang.Specification

/**
 * Specifications for the {@link AgentJobServiceImpl} class.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Category(UnitTest.class)
class AgentJobServiceImplSpec extends Specification {

    def "Can reserve job id"() {
        def jobPersistenceService = Mock(JobPersistenceService)
        def jobSpecificationService = Mock(JobSpecificationService)
        def meterRegistry = Mock(MeterRegistry)

        def jobRequest = Mock(JobRequest)
        def agentClientMetadata = Mock(AgentClientMetadata)
        def reservedId = UUID.randomUUID().toString()

        AgentJobServiceImpl service = new AgentJobServiceImpl(
                jobPersistenceService,
                jobSpecificationService,
                meterRegistry
        )

        when:
        def id = service.reserveJobId(jobRequest, agentClientMetadata)

        then:
        1 * jobPersistenceService.saveJobRequest(jobRequest, _ as JobRequestMetadata) >> reservedId
        id == reservedId
    }

    def "Can Resolve Job Specification"() {
        def jobPersistenceService = Mock(JobPersistenceService)
        def jobSpecificationService = Mock(JobSpecificationService)
        def meterRegistry = Mock(MeterRegistry)
        def jobId = UUID.randomUUID().toString()
        def jobRequest = Mock(JobRequest)
        def jobSpecificationMock = Mock(JobSpecification)

        AgentJobServiceImpl service = new AgentJobServiceImpl(
                jobPersistenceService,
                jobSpecificationService,
                meterRegistry
        )

        when:
        service.resolveJobSpecification(jobId)

        then:
        1 * jobPersistenceService.getJobRequest(jobId) >> Optional.empty()
        thrown(GenieJobNotFoundException)

        when:
        def jobSpecification = service.resolveJobSpecification(jobId)

        then:
        1 * jobPersistenceService.getJobRequest(jobId) >> Optional.of(jobRequest)
        1 * jobSpecificationService.resolveJobSpecification(jobId, jobRequest) >> jobSpecificationMock
        1 * jobPersistenceService.saveJobSpecification(jobId, jobSpecificationMock)
        jobSpecification == jobSpecificationMock
    }

    def "Can retrieve Job Specification"() {
        def jobPersistenceService = Mock(JobPersistenceService)
        def jobSpecificationService = Mock(JobSpecificationService)
        def meterRegistry = Mock(MeterRegistry)
        def jobId = UUID.randomUUID().toString()

        AgentJobServiceImpl service = new AgentJobServiceImpl(
                jobPersistenceService,
                jobSpecificationService,
                meterRegistry
        )
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
        def jobPersistenceService = Mock(JobPersistenceService)
        def jobSpecificationService = Mock(JobSpecificationService)
        def meterRegistry = Mock(MeterRegistry)
        def jobRequest = Mock(JobRequest)

        AgentJobServiceImpl service = new AgentJobServiceImpl(
                jobPersistenceService,
                jobSpecificationService,
                meterRegistry
        )

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
        def jobPersistenceService = Mock(JobPersistenceService)
        def jobSpecificationService = Mock(JobSpecificationService)
        def meterRegistry = Mock(MeterRegistry)
        def agentClientMetadata = Mock(AgentClientMetadata)
        def id = UUID.randomUUID().toString()

        AgentJobServiceImpl service = new AgentJobServiceImpl(
                jobPersistenceService,
                jobSpecificationService,
                meterRegistry
        )

        when:
        service.claimJob(id, agentClientMetadata)

        then:
        1 * jobPersistenceService.claimJob(id, agentClientMetadata)
    }

    def "Can update job status"() {
        def jobPersistenceService = Mock(JobPersistenceService)
        def jobSpecificationService = Mock(JobSpecificationService)
        def meterRegistry = Mock(MeterRegistry)
        def id = UUID.randomUUID().toString()

        AgentJobServiceImpl service = new AgentJobServiceImpl(
                jobPersistenceService,
                jobSpecificationService,
                meterRegistry
        )

        when:
        service.updateJobStatus(id, JobStatus.CLAIMED, JobStatus.INIT, UUID.randomUUID().toString())

        then:
        1 * jobPersistenceService.updateJobStatus(id, JobStatus.CLAIMED, JobStatus.INIT, _ as String)
    }
}
