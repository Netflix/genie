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

import com.netflix.genie.common.internal.dto.v4.AgentClientMetadata
import com.netflix.genie.common.internal.dto.v4.JobRequest
import com.netflix.genie.common.internal.dto.v4.JobRequestMetadata
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

        AgentJobServiceImpl service = new AgentJobServiceImpl(
                jobPersistenceService,
                jobSpecificationService,
                meterRegistry
        )

        when:
        def jobSpecification = service.resolveJobSpecification(jobId)

        then:
        jobSpecification == null
    }
}
