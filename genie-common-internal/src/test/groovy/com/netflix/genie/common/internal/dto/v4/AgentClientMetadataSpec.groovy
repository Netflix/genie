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
 * Specifications for the {@link ApiClientMetadata} class.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Category(UnitTest.class)
class AgentClientMetadataSpec extends Specification {

    def "Can build instance"() {
        def hostname = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def pid = 32_001
        AgentClientMetadata clientMetadata

        when:
        clientMetadata = new AgentClientMetadata(null, null, null)

        then:
        !clientMetadata.getHostname().isPresent()
        !clientMetadata.getVersion().isPresent()
        !clientMetadata.getPid().isPresent()

        when:
        clientMetadata = new AgentClientMetadata(hostname, version, pid)

        then:
        clientMetadata.getHostname().orElse(UUID.randomUUID().toString()) == hostname
        clientMetadata.getVersion().orElse(UUID.randomUUID().toString()) == version
        clientMetadata.getPid().orElse(pid - 1) == pid
    }

    def "Test equals"() {
        def base = createAgentClientMetadata()
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
        comparable = new AgentClientMetadata(UUID.randomUUID().toString(), null, RandomSuppliers.INT.get())

        then:
        base != comparable

        when:
        comparable = "I'm definitely not the right type of object"

        then:
        base != comparable

        when:
        def host = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def port = RandomSuppliers.INT.get()
        base = new AgentClientMetadata(host, version, port)
        comparable = new AgentClientMetadata(host, version, port)

        then:
        base == comparable
    }

    def "Test hashCode"() {
        AgentClientMetadata one
        AgentClientMetadata two

        when:
        one = createAgentClientMetadata()
        two = one

        then:
        one.hashCode() == two.hashCode()

        when:
        one = createAgentClientMetadata()
        two = createAgentClientMetadata()

        then:
        one.hashCode() != two.hashCode()

        when:
        def host = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def port = RandomSuppliers.INT.get()
        one = new AgentClientMetadata(host, version, port)
        two = new AgentClientMetadata(host, version, port)

        then:
        one.hashCode() == two.hashCode()
    }

    def "toString is consistent"() {
        AgentClientMetadata one
        AgentClientMetadata two

        when:
        one = createAgentClientMetadata()
        two = one

        then:
        one.toString() == two.toString()

        when:
        one = createAgentClientMetadata()
        two = createAgentClientMetadata()

        then:
        one.toString() != two.toString()

        when:
        def host = UUID.randomUUID().toString()
        def version = UUID.randomUUID().toString()
        def port = RandomSuppliers.INT.get()
        one = new AgentClientMetadata(host, version, port)
        two = new AgentClientMetadata(host, version, port)

        then:
        one.toString() == two.toString()
    }

    AgentClientMetadata createAgentClientMetadata() {
        return new AgentClientMetadata(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                RandomSuppliers.INT.get()
        )
    }
}
