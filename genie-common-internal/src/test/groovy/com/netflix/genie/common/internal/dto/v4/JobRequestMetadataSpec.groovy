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
package com.netflix.genie.common.internal.dto.v4

import com.netflix.genie.test.categories.UnitTest
import com.netflix.genie.test.suppliers.RandomSuppliers
import org.junit.experimental.categories.Category
import spock.lang.Specification

/**
 * Specifications for the {@link JobRequestMetadata} class.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Category(UnitTest.class)
class JobRequestMetadataSpec extends Specification {

    def "Can create JobRequestMetadata instance"() {
        def numAttachments = 10
        def totalSizeOfAttachments = 28_001L
        def apiClientMetadata = Mock(ApiClientMetadata)
        def agentClientMetadata = Mock(AgentClientMetadata)
        JobRequestMetadata jobRequestMetadata

        when:
        jobRequestMetadata = new JobRequestMetadata(null, null, -1, -5L)

        then:
        !jobRequestMetadata.getApiClientMetadata().isPresent()
        !jobRequestMetadata.getAgentClientMetadata().isPresent()
        jobRequestMetadata.getNumAttachments() == 0
        jobRequestMetadata.getTotalSizeOfAttachments() == 0

        when:
        jobRequestMetadata = new JobRequestMetadata(
                apiClientMetadata,
                agentClientMetadata,
                numAttachments,
                totalSizeOfAttachments
        )

        then:
        jobRequestMetadata.getApiClientMetadata().orElse(null) == apiClientMetadata
        jobRequestMetadata.getAgentClientMetadata().orElse(null) == agentClientMetadata
        jobRequestMetadata.getNumAttachments() == numAttachments
        jobRequestMetadata.getTotalSizeOfAttachments() == totalSizeOfAttachments
    }

    def "Test equals"() {
        def base = createJobRequestMetadata()
        Object comparable

        when:
        comparable = base

        then:
        base == comparable

        when:
        comparable = null

        then:
        base != comparable

        when:
        comparable = createJobRequestMetadata()

        then:
        base != comparable

        when:
        comparable = "I'm definitely not the right type of object"

        then:
        base != comparable

        when:
        def apiClientMetadata = Mock(ApiClientMetadata)
        def agentClientMetadata = Mock(AgentClientMetadata)
        def numAttachments = RandomSuppliers.INT.get()
        def sizeAttachments = RandomSuppliers.LONG.get()
        base = new JobRequestMetadata(apiClientMetadata, agentClientMetadata, numAttachments, sizeAttachments)
        comparable = new JobRequestMetadata(apiClientMetadata, agentClientMetadata, numAttachments, sizeAttachments)

        then:
        base == comparable
    }

    def "Test hashCode"() {
        JobRequestMetadata one
        JobRequestMetadata two

        when:
        one = createJobRequestMetadata()
        two = one

        then:
        one.hashCode() == two.hashCode()

        when:
        one = createJobRequestMetadata()
        two = createJobRequestMetadata()

        then:
        one.hashCode() != two.hashCode()

        when:
        def apiClientMetadata = Mock(ApiClientMetadata)
        def agentClientMetadata = Mock(AgentClientMetadata)
        def numAttachments = RandomSuppliers.INT.get()
        def sizeAttachments = RandomSuppliers.LONG.get()
        one = new JobRequestMetadata(apiClientMetadata, agentClientMetadata, numAttachments, sizeAttachments)
        two = new JobRequestMetadata(apiClientMetadata, agentClientMetadata, numAttachments, sizeAttachments)

        then:
        one.hashCode() == two.hashCode()
    }

    def "test toString"() {
        JobRequestMetadata one
        JobRequestMetadata two

        when:
        one = createJobRequestMetadata()
        two = one

        then:
        one.toString() == two.toString()

        when:
        one = createJobRequestMetadata()
        two = createJobRequestMetadata()

        then:
        one.toString() != two.toString()

        when:
        def apiClientMetadata = Mock(ApiClientMetadata)
        def agentClientMetadata = Mock(AgentClientMetadata)
        def numAttachments = RandomSuppliers.INT.get()
        def sizeAttachments = RandomSuppliers.LONG.get()
        one = new JobRequestMetadata(apiClientMetadata, agentClientMetadata, numAttachments, sizeAttachments)
        two = new JobRequestMetadata(apiClientMetadata, agentClientMetadata, numAttachments, sizeAttachments)

        then:
        one.toString() == two.toString()
    }

    JobRequestMetadata createJobRequestMetadata() {
        return new JobRequestMetadata(
                Mock(ApiClientMetadata),
                Mock(AgentClientMetadata),
                RandomSuppliers.INT.get(),
                RandomSuppliers.LONG.get()
        )
    }
}
