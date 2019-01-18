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

package com.netflix.genie.web.util

import com.netflix.genie.common.internal.dto.v4.AgentClientMetadata
import com.netflix.genie.web.properties.AgentFilterProperties
import com.netflix.genie.web.util.InspectionReport.Decision
import spock.lang.Specification
import spock.lang.Unroll

class BlacklistedVersionAgentMetadataInspectorSpec extends Specification {
    AgentFilterProperties versionFilterProperties
    AgentClientMetadata agentClientMetadata
    AgentMetadataInspector inspector

    void setup() {
        versionFilterProperties = Mock(AgentFilterProperties)
        agentClientMetadata = Mock(AgentClientMetadata)
        inspector = new BlacklistedVersionAgentMetadataInspector(versionFilterProperties)
    }

    @Unroll
    def "Match #agentVersion against #blacklistExpression and expect #expectedDecision"() {

        InspectionReport decision

        when:
        decision = inspector.inspect(agentClientMetadata)

        then:
        1 * agentClientMetadata.getVersion() >> Optional.ofNullable(agentVersion)
        1 * versionFilterProperties.getBlacklistedVersions() >> blacklistExpression
        decision.getDecision() == expectedDecision
        !decision.getMessage().isEmpty()

        where:
        agentVersion | blacklistExpression | expectedDecision
        "1.2.3"      | "^(1\\.2\\.3|1\\.2\\.4)\$"  | Decision.REJECT
        "1.2.3-RC.2" | "^(1\\.2\\.3|1\\.2\\.4)\$"  | Decision.ACCEPT
        "1.2.0"      | "^(1\\.2\\.3|1\\.2\\.4)\$"  | Decision.ACCEPT
        "1.2.0"      | "^(1\\.2\\.3|1\\.2\\.4)\$"  | Decision.ACCEPT
        "1.2.0"      | "1\\.2\\..*"                | Decision.REJECT
        "1.2.0-RC.2" | "1\\.2\\..*"                | Decision.REJECT
    }
}
