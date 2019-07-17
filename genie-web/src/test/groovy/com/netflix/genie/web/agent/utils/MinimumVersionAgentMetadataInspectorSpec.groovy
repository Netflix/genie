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
package com.netflix.genie.web.agent.utils

import com.netflix.genie.common.internal.dto.v4.AgentClientMetadata
import com.netflix.genie.web.agent.utils.InspectionReport.Decision
import com.netflix.genie.web.properties.AgentFilterProperties
import spock.lang.Specification
import spock.lang.Unroll

class MinimumVersionAgentMetadataInspectorSpec extends Specification {
    AgentClientMetadata agentClientMetadata
    AgentFilterProperties versionFilterProperties
    AgentMetadataInspector inspector

    void setup() {
        this.versionFilterProperties = Mock(AgentFilterProperties)
        this.agentClientMetadata = Mock(AgentClientMetadata)
        this.inspector = new MinimumVersionAgentMetadataInspector(versionFilterProperties)
    }

    @Unroll
    def "No minimum set inspection returns ACCEPT"() {
        when:
        InspectionReport report = inspector.inspect(agentClientMetadata)

        then:
        1 * versionFilterProperties.getMinimumVersion() >> propertyValue
        1 * agentClientMetadata.getVersion() >> Optional.of("1.2.3")
        report.getDecision() == Decision.ACCEPT
        !report.getMessage().isEmpty()

        where:
        propertyValue | _
        null          | _
        ""            | _

    }

    @Unroll
    def "No agent version set inspection returns REJECT"() {
        when:
        InspectionReport report = inspector.inspect(agentClientMetadata)

        then:
        1 * versionFilterProperties.getMinimumVersion() >> "1.2.3"
        1 * agentClientMetadata.getVersion() >> agentVersionValue
        report.getDecision() == Decision.REJECT
        !report.getMessage().isEmpty()

        where:
        agentVersionValue | _
        Optional.of("")   | _
        Optional.empty()  | _
    }

    @Unroll
    def "No agent version and no minimum set inspection returns ACCEPT"() {
        when:
        InspectionReport report = inspector.inspect(agentClientMetadata)

        then:
        1 * versionFilterProperties.getMinimumVersion() >> propertyValue
        1 * agentClientMetadata.getVersion() >> agentVersionValue
        report.getDecision() == Decision.ACCEPT
        !report.getMessage().isEmpty()

        where:
        agentVersionValue | propertyValue
        Optional.of("")   | ""
        Optional.empty()  | null
    }

    @Unroll
    def "Version #agentVersion returns #decision if minimum is #minimumVersion"() {
        when:
        InspectionReport report = inspector.inspect(agentClientMetadata)

        then:
        1 * versionFilterProperties.getMinimumVersion() >> minimumVersion
        1 * agentClientMetadata.getVersion() >> Optional.of(agentVersion)
        report.getDecision() == decision
        !report.getMessage().isEmpty()

        where:
        agentVersion     | minimumVersion   | decision
        "1.0.0"          | "1.0.0"          | Decision.ACCEPT
        "1.0.0"          | "1.0.1"          | Decision.REJECT
        "1.0.1"          | "1.0.0"          | Decision.ACCEPT
        "1.0.0-SNAPSHOT" | "1.0.0-SNAPSHOT" | Decision.ACCEPT
        "1.0.0-SNAPSHOT" | "1.0.1-SNAPSHOT" | Decision.REJECT
        "1.0.1-SNAPSHOT" | "1.0.0-SNAPSHOT" | Decision.ACCEPT
        "1.0.0"          | "1.0.0-SNAPSHOT" | Decision.ACCEPT
        "1.0.0-SNAPSHOT" | "1.0.0"          | Decision.REJECT
        "1.0.0-RC.1"     | "1.0.0"          | Decision.REJECT
        "1.0.0"          | "1.0.0-RC.1"     | Decision.ACCEPT
        "1.0.0-RC.1"     | "1.0.0-SNAPSHOT" | Decision.REJECT //N.B.
        "1.0.0-SNAPSHOT" | "1.0.0-RC.1"     | Decision.ACCEPT //N.B.
    }
}
