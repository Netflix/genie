/*
 *
 *  Copyright 2020 Netflix, Inc.
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
package com.netflix.genie.web.agent.services.impl

import com.netflix.genie.common.internal.util.GenieHostInfo
import com.netflix.genie.web.introspection.GenieWebHostInfo
import spock.lang.Specification

class AgentRoutingServiceSingleNodeImplSpec extends Specification {

    GenieHostInfo genieHostInfo
    AgentRoutingServiceSingleNodeImpl service

    def setup() {
        this.genieHostInfo = new GenieWebHostInfo("host-xyz")
        this.service = new AgentRoutingServiceSingleNodeImpl(genieHostInfo)
    }

    def "Test agents connecting and disconnecting"() {
        final String job1Id = "j1"
        final String job2Id = "j2"

        expect:
        !service.isAgentConnected(job1Id)
        !service.getHostnameForAgentConnection(job1Id).isPresent()
        !service.isAgentConnectionLocal(job1Id)

        when:
        service.handleClientConnected(job1Id)

        then:
        service.isAgentConnected(job1Id)
        service.getHostnameForAgentConnection(job1Id).isPresent()
        service.isAgentConnectionLocal(job1Id)
        !service.isAgentConnected(job2Id)
        !service.getHostnameForAgentConnection(job2Id).isPresent()
        !service.isAgentConnectionLocal(job2Id)

        when:
        service.handleClientConnected(job2Id)
        service.handleClientDisconnected(job1Id)

        then:
        !service.isAgentConnected(job1Id)
        !service.getHostnameForAgentConnection(job1Id).isPresent()
        !service.isAgentConnectionLocal(job1Id)
        service.isAgentConnected(job2Id)
        service.getHostnameForAgentConnection(job2Id).isPresent()
        service.isAgentConnectionLocal(job2Id)
    }
}
