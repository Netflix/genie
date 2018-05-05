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

import com.netflix.genie.common.dto.ClusterStatus
import com.netflix.genie.test.categories.UnitTest
import org.junit.experimental.categories.Category
import spock.lang.Specification

/**
 * Specifications for the {@link ClusterRequest} class.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Category(UnitTest.class)
class ClusterRequestSpec extends Specification {

    def "Can build immutable cluster request"() {
        def metadata = new ClusterMetadata.Builder(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                ClusterStatus.OUT_OF_SERVICE
        ).build()
        def requestedId = UUID.randomUUID().toString()
        def resources = new ExecutionEnvironment(null, null, UUID.randomUUID().toString())
        ClusterRequest request

        when:
        request = new ClusterRequest.Builder(metadata)
                .withRequestedId(requestedId)
                .withResources(resources)
                .build()

        then:
        request.getMetadata() == metadata
        request.getRequestedId().orElse(UUID.randomUUID().toString()) == requestedId
        request.getResources() == resources

        when:
        request = new ClusterRequest.Builder(metadata).build()

        then:
        request.getMetadata() == metadata
        !request.getRequestedId().isPresent()
        request.getResources() != null

        when: "Requested id is blank it's ignored"
        request = new ClusterRequest.Builder(metadata).withRequestedId("   ").build()

        then:
        request.getMetadata() == metadata
        !request.getRequestedId().isPresent()
        request.getResources() != null
    }
}
