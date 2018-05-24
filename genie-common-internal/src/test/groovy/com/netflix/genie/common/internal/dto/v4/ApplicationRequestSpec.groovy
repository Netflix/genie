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

import com.netflix.genie.common.dto.ApplicationStatus
import com.netflix.genie.test.categories.UnitTest
import org.junit.experimental.categories.Category
import spock.lang.Specification

/**
 * Specifications for the {@link ApplicationRequest} class.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Category(UnitTest.class)
class ApplicationRequestSpec extends Specification {

    def "Can build immutable application request"() {
        def metadata = new ApplicationMetadata.Builder(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                ApplicationStatus.DEPRECATED
        ).build()
        def requestedId = UUID.randomUUID().toString()
        def resources = new ExecutionEnvironment(null, null, UUID.randomUUID().toString())
        ApplicationRequest request

        when:
        request = new ApplicationRequest.Builder(metadata)
                .withRequestedId(requestedId)
                .withResources(resources)
                .build()

        then:
        request.getMetadata() == metadata
        request.getRequestedId().orElse(UUID.randomUUID().toString()) == requestedId
        request.getResources() == resources

        when:
        request = new ApplicationRequest.Builder(metadata).build()

        then:
        request.getMetadata() == metadata
        !request.getRequestedId().isPresent()
        request.getResources() != null

        when: "Requested id is blank it's ignored"
        request = new ApplicationRequest.Builder(metadata).withRequestedId("   ").build()

        then:
        request.getMetadata() == metadata
        !request.getRequestedId().isPresent()
        request.getResources() != null
    }

    def "Test equals"() {
        def base = createApplicationRequest()
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
        comparable = new ApplicationRequest.Builder(Mock(ApplicationMetadata))
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
        def status = ApplicationStatus.INACTIVE
        def baseMetadata = new ApplicationMetadata.Builder(name, user, version, status).build()
        def comparableMetadata = new ApplicationMetadata.Builder(name, user, version, status).build()
        base = new ApplicationRequest.Builder(baseMetadata).build()
        comparable = new ApplicationRequest.Builder(comparableMetadata).build()

        then:
        base == comparable
    }

    def "Test hashCode"() {
        ApplicationRequest one
        ApplicationRequest two

        when:
        one = createApplicationRequest()
        two = one

        then:
        one.hashCode() == two.hashCode()

        when:
        one = createApplicationRequest()
        two = createApplicationRequest()

        then:
        one.hashCode() != two.hashCode()

        when:
        def name = UUID.randomUUID().toString()
        def user = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def status = ApplicationStatus.INACTIVE
        def baseMetadata = new ApplicationMetadata.Builder(name, user, version, status).build()
        def comparableMetadata = new ApplicationMetadata.Builder(name, user, version, status).build()
        one = new ApplicationRequest.Builder(baseMetadata).build()
        two = new ApplicationRequest.Builder(comparableMetadata).build()

        then:
        one.hashCode() == two.hashCode()
    }

    def "toString is consistent"() {
        ApplicationRequest one
        ApplicationRequest two

        when:
        one = createApplicationRequest()
        two = one

        then:
        one.toString() == two.toString()

        when:
        one = createApplicationRequest()
        two = createApplicationRequest()

        then:
        one.toString() != two.toString()

        when:
        def name = UUID.randomUUID().toString()
        def user = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def status = ApplicationStatus.DEPRECATED
        def baseMetadata = new ApplicationMetadata.Builder(name, user, version, status).build()
        def comparableMetadata = new ApplicationMetadata.Builder(name, user, version, status).build()
        one = new ApplicationRequest.Builder(baseMetadata).build()
        two = new ApplicationRequest.Builder(comparableMetadata).build()

        then:
        one.toString() == two.toString()
    }

    ApplicationRequest createApplicationRequest() {
        def metadata = new ApplicationMetadata.Builder(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                ApplicationStatus.INACTIVE
        )
                .withType(UUID.randomUUID().toString())
                .build()
        def requestedId = UUID.randomUUID().toString()
        def resources = new ExecutionEnvironment(null, null, UUID.randomUUID().toString())
        return new ApplicationRequest.Builder(metadata).withRequestedId(requestedId).withResources(resources).build()
    }
}
