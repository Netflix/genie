/*
 *
 *  Copyright 2019 Netflix, Inc.
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
import com.netflix.genie.web.services.AgentConnectionPersistenceService
import io.micrometer.core.instrument.MeterRegistry
import spock.lang.Specification

import java.util.function.ToDoubleFunction

/**
 * Specifications for {@link AgentMetricsServiceImpl}.
 *
 * @author tgianos
 */
class AgentMetricsServiceImplSpec extends Specification {

    def "Can get the number of connected agents on the server"() {
        def hostInfo = Mock(GenieHostInfo)
        def agentConnectionPersistenceService = Mock(AgentConnectionPersistenceService)
        def meterRegistry = Mock(MeterRegistry)
        def host = "netflix.github.io"

        when:
        def service = new AgentMetricsServiceImpl(hostInfo, agentConnectionPersistenceService, meterRegistry)

        then:
        1 * meterRegistry.gauge(
            AgentMetricsServiceImpl.CONNECTED_GUAGE_METRIC_NAME,
            _ as AgentMetricsServiceImpl,
            _ as ToDoubleFunction
        )

        when:
        long num = service.getNumConnectedAgents()

        then:
        1 * hostInfo.getHostname() >> host
        1 * agentConnectionPersistenceService.getNumAgentConnectionsOnServer(host) >> 3452L
        num == 3452L
    }
}
