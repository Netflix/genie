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
import com.netflix.genie.web.services.AgentFilterService
import com.netflix.genie.web.services.AgentFilterService.InspectionReport.InspectionDecision
import spock.lang.Specification
import spock.lang.Unroll

class MinimumVersionAgentMetadataInspectorSpec extends Specification {
    AgentClientMetadata agentClientMetadata
    AgentFilterProperties versionFilterProperties
    AgentFilterService.AgentMetadataInspector inspector

    void setup() {
        this.versionFilterProperties = Mock(AgentFilterProperties)
        this.agentClientMetadata = Mock(AgentClientMetadata)
        this.inspector = new MinimumVersionAgentMetadataInspector(versionFilterProperties)
    }

    @Unroll
    def "No minimum set inspection returns CONTINUE"() {
        when:
        AgentFilterService.InspectionReport report = inspector.inspect(agentClientMetadata)

        then:
        1 * versionFilterProperties.getMinimumVersion() >> propertyValue
        !report.getMessage().isEmpty()
        report.getDecision() == InspectionDecision.CONTINUE

        where:
        propertyValue | _
        null          | _
        ""            | _

    }

    @Unroll
    def "No agent version set inspection returns REJECT"() {
        when:
        AgentFilterService.InspectionReport report = inspector.inspect(agentClientMetadata)

        then:
        1 * versionFilterProperties.getMinimumVersion() >> "1.2.3"
        1 * agentClientMetadata.getVersion() >> agentVersionValue
        !report.getMessage().isEmpty()
        report.getDecision() == InspectionDecision.REJECT

        where:
        agentVersionValue | _
        Optional.of("")   | _
        Optional.empty()  | _
    }

    @Unroll
    def "Version #agentVersion returns #decision if minimum is #minimumVersion"() {
        when:
        AgentFilterService.InspectionReport report = inspector.inspect(agentClientMetadata)

        then:
        1 * versionFilterProperties.getMinimumVersion() >> minimumVersion
        1 * agentClientMetadata.getVersion() >> Optional.of(agentVersion)
        !report.getMessage().isEmpty()
        report.getDecision() == decision

        where:
        agentVersion     | minimumVersion   | decision
        "1.0.0"          | "1.0.0"          | InspectionDecision.CONTINUE
        "1.0.0"          | "1.0.1"          | InspectionDecision.REJECT
        "1.0.1"          | "1.0.0"          | InspectionDecision.CONTINUE
        "1.0.0-SNAPSHOT" | "1.0.0-SNAPSHOT" | InspectionDecision.CONTINUE
        "1.0.0-SNAPSHOT" | "1.0.1-SNAPSHOT" | InspectionDecision.REJECT
        "1.0.1-SNAPSHOT" | "1.0.0-SNAPSHOT" | InspectionDecision.CONTINUE
        "1.0.0"          | "1.0.0-SNAPSHOT" | InspectionDecision.CONTINUE
        "1.0.0-SNAPSHOT" | "1.0.0"          | InspectionDecision.REJECT
        "1.0.0-RC.1"     | "1.0.0"          | InspectionDecision.REJECT
        "1.0.0"          | "1.0.0-RC.1"     | InspectionDecision.CONTINUE
        "1.0.0-RC.1"     | "1.0.0-SNAPSHOT" | InspectionDecision.REJECT //*
        "1.0.0-SNAPSHOT" | "1.0.0-RC.1"     | InspectionDecision.CONTINUE //*
    }
}
