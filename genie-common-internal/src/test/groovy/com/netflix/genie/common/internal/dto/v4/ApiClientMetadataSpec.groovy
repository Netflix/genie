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
}
