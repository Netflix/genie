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
package com.netflix.genie.web.agent.inspectors.impl

import com.netflix.genie.common.internal.dto.v4.AgentClientMetadata
import com.netflix.genie.web.agent.inspectors.AgentMetadataInspector
import com.netflix.genie.web.agent.inspectors.InspectionReport
import com.netflix.genie.web.properties.AgentFilterProperties
import spock.lang.Specification
import spock.lang.Unroll

class WhitelistedVersionAgentMetadataInspectorSpec extends Specification {
    AgentFilterProperties versionFilterProperties
    AgentClientMetadata agentClientMetadata
    AgentMetadataInspector inspector

    void setup() {
        versionFilterProperties = Mock(AgentFilterProperties)
        agentClientMetadata = Mock(AgentClientMetadata)
        inspector = new WhitelistedVersionAgentMetadataInspector(versionFilterProperties)
    }

    @Unroll
    def "Match #agentVersion against #blacklistExpression and expect #expectedDecision"() {

        InspectionReport decision

        when:
        decision = inspector.inspect(agentClientMetadata)

        then:
        1 * agentClientMetadata.getVersion() >> Optional.ofNullable(agentVersion)
        1 * versionFilterProperties.getWhitelistedVersions() >> whitelistExpression
        decision.getDecision() == expectedDecision
        !decision.getMessage().isEmpty()

        where:
        agentVersion | whitelistExpression        | expectedDecision
        "1.2.3"      | "^(1\\.2\\.3|1\\.2\\.4)\$" | InspectionReport.Decision.ACCEPT
        "1.2.3-RC.2" | "^(1\\.2\\.3|1\\.2\\.4)\$" | InspectionReport.Decision.REJECT
        "1.2.0"      | "^(1\\.2\\.3|1\\.2\\.4)\$" | InspectionReport.Decision.REJECT
        "1.2.0"      | "^(1\\.2\\.3|1\\.2\\.4)\$" | InspectionReport.Decision.REJECT
        "1.2.0"      | "1\\.2\\..*"               | InspectionReport.Decision.ACCEPT
        "1.2.0-RC.2" | "1\\.2\\..*"               | InspectionReport.Decision.ACCEPT
    }
}
