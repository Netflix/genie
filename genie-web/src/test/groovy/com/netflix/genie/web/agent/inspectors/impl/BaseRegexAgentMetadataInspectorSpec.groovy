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

import com.netflix.genie.common.external.dtos.v4.AgentClientMetadata
import com.netflix.genie.web.agent.inspectors.InspectionReport
import org.apache.commons.lang3.NotImplementedException
import spock.lang.Specification
import spock.lang.Unroll

class BaseRegexAgentMetadataInspectorSpec extends Specification {

    void setup() {
    }

    @Unroll
    def "Decide #decisionIfMatch.name() if there is a match, handle corner cases and errors"() {

        setup:
        TestRegexInspector inspector = new TestRegexInspector(decisionIfMatch)
        InspectionReport decision

        when:
        decision = inspector.inspectWithPattern("^foo\$", "")

        then:
        decision.getDecision() == InspectionReport.Decision.REJECT
        !decision.getMessage().isEmpty()

        when:
        decision = inspector.inspectWithPattern("^foo\$", "foo")

        then:
        decision.getDecision() == decisionIfMatch
        !decision.getMessage().isEmpty()

        when:
        decision = inspector.inspectWithPattern("(", "foo")

        then:
        decision.getDecision() == InspectionReport.Decision.ACCEPT
        !decision.getMessage().isEmpty()

        when:
        decision = inspector.inspectWithPattern(null, "foo")

        then:
        decision.getDecision() == InspectionReport.Decision.ACCEPT
        !decision.getMessage().isEmpty()

        when:
        decision = inspector.inspectWithPattern(null, null)

        then:
        decision.getDecision() == InspectionReport.Decision.ACCEPT
        !decision.getMessage().isEmpty()

        when:
        decision = inspector.inspectWithPattern("^bar.*", "foo")

        then:
        decision.getDecision() != decisionIfMatch
        !decision.getMessage().isEmpty()

        where:
        decisionIfMatch                  | _
        InspectionReport.Decision.ACCEPT | _
        InspectionReport.Decision.REJECT | _
    }

    class TestRegexInspector extends BaseRegexAgentMetadataInspector {

        TestRegexInspector(
            final InspectionReport.Decision decisionIfMatch
        ) {
            super(decisionIfMatch)
        }

        @Override
        InspectionReport inspect(final AgentClientMetadata agentClientMetadata) {
            throw new NotImplementedException("Not implemented")
        }
    }
}
