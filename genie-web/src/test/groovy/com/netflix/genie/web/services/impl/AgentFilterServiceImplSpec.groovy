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
import com.netflix.genie.web.services.AgentFilterService
import spock.lang.Specification

class AgentFilterServiceImplSpec extends Specification {

    List<AgentFilterService.AgentMetadataInspector> inspectors
    AgentFilterServiceImpl service
    AgentClientMetadata agentClientMetadata

    def final static CONTINUE_DECISION = new AgentFilterService.InspectionReport(
        AgentFilterService.InspectionReport.InspectionDecision.CONTINUE,
        "Meh. Go ahead."
    )

    def final static REJECT_DECISION = new AgentFilterService.InspectionReport(
        AgentFilterService.InspectionReport.InspectionDecision.REJECT,
        "Thou shall not pass."
    )

    def final static ACCEPT_DECISION = new AgentFilterService.InspectionReport(
        AgentFilterService.InspectionReport.InspectionDecision.ACCEPT,
        "Welcome."
    )

    void setup() {
        this.inspectors = [
            Mock(AgentFilterService.AgentMetadataInspector),
            Mock(AgentFilterService.AgentMetadataInspector),
        ]
        this.agentClientMetadata = Mock(AgentClientMetadata)
        this.service = new AgentFilterServiceImpl(inspectors)
    }

    def "AcceptOrThrow -- default accept"() {

        when:
        service.acceptOrThrow(agentClientMetadata)

        then:
        1 * inspectors[0].inspect(agentClientMetadata) >> CONTINUE_DECISION
        1 * inspectors[1].inspect(agentClientMetadata) >> CONTINUE_DECISION
        noExceptionThrown()
    }

    def "AcceptOrThrow -- reject"() {
        when:
        service.acceptOrThrow(agentClientMetadata)

        then:
        1 * inspectors[0].inspect(agentClientMetadata) >> REJECT_DECISION
        0 * inspectors[1].inspect(agentClientMetadata)
        thrown(GenieAgentRejectedException)
    }

    def "AcceptOrThrow -- accept"() {
        when:
        service.acceptOrThrow(agentClientMetadata)

        then:
        1 * inspectors[0].inspect(agentClientMetadata) >> ACCEPT_DECISION
        0 * inspectors[1].inspect(agentClientMetadata)
        noExceptionThrown()
    }
}
