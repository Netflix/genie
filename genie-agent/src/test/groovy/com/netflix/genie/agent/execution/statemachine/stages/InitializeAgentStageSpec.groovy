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
package com.netflix.genie.agent.execution.statemachine.stages

import com.netflix.genie.agent.AgentMetadata
import com.netflix.genie.agent.execution.statemachine.ExecutionContext
import com.netflix.genie.agent.execution.statemachine.ExecutionStage
import com.netflix.genie.common.external.dtos.v4.AgentClientMetadata
import spock.lang.Specification

class InitializeAgentStageSpec extends Specification {
    ExecutionStage stage
    ExecutionContext executionContext
    AgentMetadata agentMetadata

    void setup() {
        this.agentMetadata = Mock(AgentMetadata)
        this.executionContext = Mock(ExecutionContext)
        this.stage = new InitializeAgentStage(agentMetadata)
    }

    def "AttemptTransition"() {
        setup:
        String host = UUID.randomUUID().toString()
        String version = "1.2.3"
        String pid = "12345"

        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * agentMetadata.getAgentHostName() >> host
        1 * agentMetadata.getAgentVersion() >> version
        1 * agentMetadata.getAgentPid() >> pid
        1 * executionContext.setAgentClientMetadata(_ as AgentClientMetadata)
    }
}
