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

import com.netflix.genie.common.internal.util.GenieHostInfo
import com.netflix.genie.test.categories.UnitTest
import com.netflix.genie.web.services.AgentConnectionPersistenceService
import com.netflix.genie.web.services.AgentRoutingService
import org.junit.experimental.categories.Category
import spock.lang.Specification

@Category(UnitTest.class)
class AgentRoutingServiceImplSpec extends Specification {
    private static final String HOSTNAME = "10.1.2.3"

    AgentConnectionPersistenceService persistenceService = Mock()
    GenieHostInfo genieHostInfo = Mock()
    AgentRoutingService service
    String jobId

    void setup() {
        service = new AgentRoutingServiceImpl(persistenceService, genieHostInfo);
        jobId = UUID.randomUUID().toString()
    }

    def "getHostnameForAgentConnection"() {
        boolean isLocalConnection

        when:
        Optional<String> optionalHostname = service.getHostnameForAgentConnection(jobId)

        then:
        1 * persistenceService.lookupAgentConnectionServer(jobId) >> Optional.of(HOSTNAME)

        expect:
        optionalHostname.isPresent()
        HOSTNAME == optionalHostname.get()

        when:
        isLocalConnection = service.isAgentConnectionLocal(jobId)

        then:
        1 * persistenceService.lookupAgentConnectionServer(jobId) >> Optional.of(HOSTNAME)
        1 * genieHostInfo.getHostname() >> HOSTNAME

        expect:
        isLocalConnection

        when:
        isLocalConnection = service.isAgentConnectionLocal(jobId)

        then:
        1 * persistenceService.lookupAgentConnectionServer(jobId) >> Optional.of("another.hostname")
        1 * genieHostInfo.getHostname() >> HOSTNAME

        expect:
        !isLocalConnection
    }

    def "isAgentConnectionLocal"() {
        boolean isLocalConnection

        when:
        isLocalConnection = service.isAgentConnectionLocal(jobId)

        then:
        1 * persistenceService.lookupAgentConnectionServer(jobId) >> Optional.of(HOSTNAME)
        1 * genieHostInfo.getHostname() >> HOSTNAME

        expect:
        isLocalConnection

        when:
        isLocalConnection = service.isAgentConnectionLocal(jobId)

        then:
        1 * persistenceService.lookupAgentConnectionServer(jobId) >> Optional.of("another.hostname")
        1 * genieHostInfo.getHostname() >> HOSTNAME

        expect:
        !isLocalConnection

        when:
        isLocalConnection = service.isAgentConnectionLocal(jobId)

        then:
        1 * persistenceService.lookupAgentConnectionServer(jobId) >> Optional.empty()
        0 * genieHostInfo.getHostname() >> HOSTNAME

        expect:
        !isLocalConnection
    }

    def "Reacting to connection and disconnection"() {

        when:
        service.handleClientConnected(jobId)

        then:
        1 * genieHostInfo.getHostname() >> HOSTNAME
        1 * persistenceService.saveAgentConnection(jobId, HOSTNAME)

        when:
        service.handleClientDisconnected(jobId)

        then:
        1 * genieHostInfo.getHostname() >> HOSTNAME
        1 * persistenceService.removeAgentConnection(jobId, HOSTNAME)
    }
}
