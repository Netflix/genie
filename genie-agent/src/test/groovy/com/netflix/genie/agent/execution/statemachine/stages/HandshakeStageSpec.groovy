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

import com.netflix.genie.agent.execution.exceptions.HandshakeException
import com.netflix.genie.agent.execution.services.AgentJobService
import com.netflix.genie.agent.execution.statemachine.ExecutionContext
import com.netflix.genie.agent.execution.statemachine.ExecutionStage
import com.netflix.genie.agent.execution.statemachine.FatalJobExecutionException
import com.netflix.genie.agent.execution.statemachine.RetryableJobExecutionException
import com.netflix.genie.common.external.dtos.v4.AgentClientMetadata
import com.netflix.genie.common.internal.exceptions.unchecked.GenieRuntimeException
import spock.lang.Specification

class HandshakeStageSpec extends Specification {
    ExecutionStage stage
    ExecutionContext executionContext
    AgentJobService agentJobService
    AgentClientMetadata agentClientMetadata

    void setup() {
        this.agentJobService = Mock(AgentJobService)
        this.agentClientMetadata = Mock(AgentClientMetadata)
        this.executionContext = Mock(ExecutionContext)
        this.stage = new HandshakeStage(agentJobService)
    }

    def "AttemptTransition -- success"() {
        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.getAgentClientMetadata() >> agentClientMetadata
        1 * agentJobService.handshake(agentClientMetadata)
    }

    def "AttemptTransition -- fatal error"() {
        setup:
        def handshakeException = Mock(HandshakeException)

        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.getAgentClientMetadata() >> agentClientMetadata
        1 * agentJobService.handshake(agentClientMetadata) >> { throw handshakeException }
        def e = thrown(FatalJobExecutionException)
        e.getCause() == handshakeException
    }

    def "AttemptTransition -- retryable error"() {
        setup:
        def handshakeException = Mock(GenieRuntimeException)

        when:
        stage.attemptStageAction(executionContext)

        then:
        1 * executionContext.getAgentClientMetadata() >> agentClientMetadata
        1 * agentJobService.handshake(agentClientMetadata) >> { throw handshakeException }
        def e = thrown(RetryableJobExecutionException)
        e.getCause() == handshakeException
    }
}
