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
package com.netflix.genie.common.external.dtos.v4

import com.netflix.genie.test.suppliers.RandomSuppliers
import spock.lang.Specification

/**
 * Specifications for the {@link JobRequestMetadata} class.
 *
 * @author tgianos
 */
class JobRequestMetadataSpec extends Specification {

    def "Can create JobRequestMetadata instance"() {
        def numAttachments = 10
        def totalSizeOfAttachments = 28_001L
        def apiClientMetadata = Mock(ApiClientMetadata)
        def agentClientMetadata = Mock(AgentClientMetadata)
        JobRequestMetadata jobRequestMetadata

        when:
        new JobRequestMetadata(null, null, -1, -5L)

        then:
        thrown(IllegalArgumentException)

        when:
        new JobRequestMetadata(apiClientMetadata, agentClientMetadata, -1, -5L)

        then:
        thrown(IllegalArgumentException)

        when:
        jobRequestMetadata = new JobRequestMetadata(apiClientMetadata, null, -1, -5L)

        then:
        jobRequestMetadata.isApi()
        jobRequestMetadata.getApiClientMetadata().orElse(null) == apiClientMetadata
        !jobRequestMetadata.getAgentClientMetadata().isPresent()
        jobRequestMetadata.getNumAttachments() == 0
        jobRequestMetadata.getTotalSizeOfAttachments() == 0

        when:
        jobRequestMetadata = new JobRequestMetadata(
            null,
            agentClientMetadata,
            numAttachments,
            totalSizeOfAttachments
        )

        then:
        !jobRequestMetadata.isApi()
        !jobRequestMetadata.getApiClientMetadata().isPresent()
        jobRequestMetadata.getAgentClientMetadata().orElse(null) == agentClientMetadata
        jobRequestMetadata.getNumAttachments() == numAttachments
        jobRequestMetadata.getTotalSizeOfAttachments() == totalSizeOfAttachments
    }

    def "Test equals"() {
        def base = createJobRequestMetadata(true)
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
        comparable = createJobRequestMetadata(false)

        then:
        base != comparable

        when:
        comparable = "I'm definitely not the right type of object"

        then:
        base != comparable

        when:
        def agentClientMetadata = Mock(AgentClientMetadata)
        def numAttachments = RandomSuppliers.INT.get()
        def sizeAttachments = RandomSuppliers.LONG.get()
        base = new JobRequestMetadata(null, agentClientMetadata, numAttachments, sizeAttachments)
        comparable = new JobRequestMetadata(null, agentClientMetadata, numAttachments, sizeAttachments)

        then:
        base == comparable
    }

    def "Test hashCode"() {
        JobRequestMetadata one
        JobRequestMetadata two

        when:
        one = createJobRequestMetadata(true)
        two = one

        then:
        one.hashCode() == two.hashCode()

        when:
        one = createJobRequestMetadata(true)
        two = createJobRequestMetadata(false)

        then:
        one.hashCode() != two.hashCode()

        when:
        def apiClientMetadata = Mock(ApiClientMetadata)
        def numAttachments = RandomSuppliers.INT.get()
        def sizeAttachments = RandomSuppliers.LONG.get()
        one = new JobRequestMetadata(apiClientMetadata, null, numAttachments, sizeAttachments)
        two = new JobRequestMetadata(apiClientMetadata, null, numAttachments, sizeAttachments)

        then:
        one.hashCode() == two.hashCode()
    }

    def "test toString"() {
        JobRequestMetadata one
        JobRequestMetadata two

        when:
        one = createJobRequestMetadata(true)
        two = one

        then:
        one.toString() == two.toString()

        when:
        one = createJobRequestMetadata(true)
        two = createJobRequestMetadata(false)

        then:
        one.toString() != two.toString()

        when:
        def apiClientMetadata = Mock(ApiClientMetadata)
        def numAttachments = RandomSuppliers.INT.get()
        def sizeAttachments = RandomSuppliers.LONG.get()
        one = new JobRequestMetadata(apiClientMetadata, null, numAttachments, sizeAttachments)
        two = new JobRequestMetadata(apiClientMetadata, null, numAttachments, sizeAttachments)

        then:
        one.toString() == two.toString()
    }

    JobRequestMetadata createJobRequestMetadata(boolean api) {
        if (api) {
            return new JobRequestMetadata(
                Mock(ApiClientMetadata),
                null,
                RandomSuppliers.INT.get(),
                RandomSuppliers.LONG.get()
            )
        } else {
            return new JobRequestMetadata(
                null,
                Mock(AgentClientMetadata),
                RandomSuppliers.INT.get(),
                RandomSuppliers.LONG.get()
            )
        }
    }
}
