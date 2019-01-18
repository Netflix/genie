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

import com.netflix.genie.common.internal.dto.v4.AgentClientMetadata
import com.netflix.genie.common.internal.exceptions.unchecked.GenieAgentRejectedException
import com.netflix.genie.web.util.AgentMetadataInspector
import com.netflix.genie.web.util.InspectionReport
import spock.lang.Specification

class AgentFilterServiceImplSpec extends Specification {

    List<AgentMetadataInspector> inspectors
    AgentFilterServiceImpl service
    AgentClientMetadata agentClientMetadata

    void setup() {
        this.inspectors = [
            Mock(AgentMetadataInspector),
            Mock(AgentMetadataInspector),
        ]
        this.agentClientMetadata = Mock(AgentClientMetadata)
        this.service = new AgentFilterServiceImpl(inspectors)
    }

    def "Default accept"() {
        InspectionReport finalDecision
        this.service = new AgentFilterServiceImpl([])

        when:
        finalDecision = service.inspectAgentMetadata(agentClientMetadata)

        then:
        finalDecision.getDecision() == InspectionReport.Decision.ACCEPT
        !finalDecision.getMessage().isEmpty()
    }

    def "Inspect and accept"() {
        InspectionReport finalDecision

        when:
        finalDecision = service.inspectAgentMetadata(agentClientMetadata)

        then:
        1 * inspectors[0].inspect(agentClientMetadata) >> InspectionReport.newAcceptance("Welcome")
        1 * inspectors[1].inspect(agentClientMetadata) >> InspectionReport.newAcceptance("Welcome")
        finalDecision.getDecision() == InspectionReport.Decision.ACCEPT
        !finalDecision.getMessage().isEmpty()
    }

    def "Inspect and reject"() {
        InspectionReport finalDecision

        when:
        finalDecision = service.inspectAgentMetadata(agentClientMetadata)

        then:
        1 * inspectors[0].inspect(agentClientMetadata) >> InspectionReport.newRejection("Thou shall not pass")
        0 * inspectors[1].inspect(agentClientMetadata)
        finalDecision.getDecision() == InspectionReport.Decision.REJECT
        !finalDecision.getMessage().isEmpty()
    }
}
