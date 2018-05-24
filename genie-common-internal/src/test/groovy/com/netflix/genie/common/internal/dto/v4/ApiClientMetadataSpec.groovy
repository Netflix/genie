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
 * Specifications for the {@link ApiClientMetadata} class.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Category(UnitTest.class)
class ApiClientMetadataSpec extends Specification {

    def "Can build instance"() {
        def hostname = UUID.randomUUID().toString()
        def userAgent = UUID.randomUUID().toString()
        ApiClientMetadata clientMetadata

        when:
        clientMetadata = new ApiClientMetadata(null, null)

        then:
        !clientMetadata.getHostname().isPresent()
        !clientMetadata.getUserAgent().isPresent()

        when:
        clientMetadata = new ApiClientMetadata(hostname, userAgent)

        then:
        clientMetadata.getHostname().orElse(UUID.randomUUID().toString()) == hostname
        clientMetadata.getUserAgent().orElse(UUID.randomUUID().toString()) == userAgent
    }

    def "Test equals"() {
        def base = createApiClientMetadata()
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
        comparable = new ApiClientMetadata(UUID.randomUUID().toString(), null)

        then:
        base != comparable

        when:
        comparable = "I'm definitely not the right type of object"

        then:
        base != comparable

        when:
        def host = UUID.randomUUID().toString()
        def userAgent = UUID.randomUUID().toString()
        base = new ApiClientMetadata(host, userAgent)
        comparable = new ApiClientMetadata(host, userAgent)

        then:
        base == comparable
    }

    def "Test hashCode"() {
        ApiClientMetadata one
        ApiClientMetadata two

        when:
        one = createApiClientMetadata()
        two = one

        then:
        one.hashCode() == two.hashCode()

        when:
        one = createApiClientMetadata()
        two = createApiClientMetadata()

        then:
        one.hashCode() != two.hashCode()

        when:
        def host = UUID.randomUUID().toString()
        def userAgent = UUID.randomUUID().toString()
        one = new ApiClientMetadata(host, userAgent)
        two = new ApiClientMetadata(host, userAgent)

        then:
        one.hashCode() == two.hashCode()
    }

    def "toString is consistent"() {
        ApiClientMetadata one
        ApiClientMetadata two

        when:
        one = createApiClientMetadata()
        two = one

        then:
        one.toString() == two.toString()

        when:
        one = createApiClientMetadata()
        two = createApiClientMetadata()

        then:
        one.toString() != two.toString()

        when:
        def host = UUID.randomUUID().toString()
        def userAgent = UUID.randomUUID().toString()
        one = new ApiClientMetadata(host, userAgent)
        two = new ApiClientMetadata(host, userAgent)

        then:
        one.toString() == two.toString()
    }

    ApiClientMetadata createApiClientMetadata() {
        return new ApiClientMetadata(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()
        )
    }
}
