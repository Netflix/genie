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

    def "Test equals"() {
        def base = createClusterRequest()
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
        comparable = new ClusterRequest.Builder(Mock(ClusterMetadata))
                .withRequestedId(UUID.randomUUID().toString())
                .toString()

        then:
        base != comparable

        when:
        comparable = "I'm definitely not the right type of object"

        then:
        base != comparable

        when:
        def name = UUID.randomUUID().toString()
        def user = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def status = ClusterStatus.OUT_OF_SERVICE
        def baseMetadata = new ClusterMetadata.Builder(name, user, version, status).build()
        def comparableMetadata = new ClusterMetadata.Builder(name, user, version, status).build()
        base = new ClusterRequest.Builder(baseMetadata).build()
        comparable = new ClusterRequest.Builder(comparableMetadata).build()

        then:
        base == comparable
    }

    def "Test hashCode"() {
        ClusterRequest one
        ClusterRequest two

        when:
        one = createClusterRequest()
        two = one

        then:
        one.hashCode() == two.hashCode()

        when:
        one = createClusterRequest()
        two = createClusterRequest()

        then:
        one.hashCode() != two.hashCode()

        when:
        def name = UUID.randomUUID().toString()
        def user = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def status = ClusterStatus.OUT_OF_SERVICE
        def baseMetadata = new ClusterMetadata.Builder(name, user, version, status).build()
        def comparableMetadata = new ClusterMetadata.Builder(name, user, version, status).build()
        one = new ClusterRequest.Builder(baseMetadata).build()
        two = new ClusterRequest.Builder(comparableMetadata).build()

        then:
        one.hashCode() == two.hashCode()
    }

    def "toString is consistent"() {
        ClusterRequest one
        ClusterRequest two

        when:
        one = createClusterRequest()
        two = one

        then:
        one.toString() == two.toString()

        when:
        one = createClusterRequest()
        two = createClusterRequest()

        then:
        one.toString() != two.toString()

        when:
        def name = UUID.randomUUID().toString()
        def user = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def status = ClusterStatus.OUT_OF_SERVICE
        def baseMetadata = new ClusterMetadata.Builder(name, user, version, status).build()
        def comparableMetadata = new ClusterMetadata.Builder(name, user, version, status).build()
        one = new ClusterRequest.Builder(baseMetadata).build()
        two = new ClusterRequest.Builder(comparableMetadata).build()

        then:
        one.toString() == two.toString()
    }

    ClusterRequest createClusterRequest() {
        def metadata = new ClusterMetadata.Builder(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                ClusterStatus.UP
        ).build()
        def requestedId = UUID.randomUUID().toString()
        def resources = new ExecutionEnvironment(null, null, UUID.randomUUID().toString())
        return new ClusterRequest.Builder(metadata).withRequestedId(requestedId).withResources(resources).build()
    }
}
