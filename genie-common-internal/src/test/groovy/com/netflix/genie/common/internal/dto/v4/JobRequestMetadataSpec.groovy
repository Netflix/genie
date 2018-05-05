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
}
