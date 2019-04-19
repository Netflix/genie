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
package com.netflix.genie.web.health

import com.netflix.genie.web.services.AgentMetricsService
import org.springframework.boot.actuate.health.Status
import spock.lang.Specification

/**
 * Specifications for {@link GenieAgentHealthIndicator}.
 *
 * @author tgianos
 */
class GenieAgentHealthIndicatorSpec extends Specification {

    def "Can get health"() {
        def agentMetricsService = Mock(AgentMetricsService)
        def healthIndicator = new GenieAgentHealthIndicator(agentMetricsService)
        def connectedAgentCount = 53234L

        when:
        def health = healthIndicator.health()

        then:
        1 * agentMetricsService.getNumConnectedAgents() >> connectedAgentCount
        health.getStatus() == Status.UP
        health.getDetails().containsKey(GenieAgentHealthIndicator.NUM_CONNECTED_AGENTS)
        health.getDetails().get(GenieAgentHealthIndicator.NUM_CONNECTED_AGENTS) == connectedAgentCount
    }
}
