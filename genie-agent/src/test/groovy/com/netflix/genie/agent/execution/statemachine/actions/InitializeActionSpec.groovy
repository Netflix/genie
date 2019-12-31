/*
 *
 *  Copyright 2018 Netflix, Inc.
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
package com.netflix.genie.agent.execution.statemachine.actions

import com.netflix.genie.agent.AgentMetadata
import com.netflix.genie.agent.execution.ExecutionContext
import com.netflix.genie.agent.execution.exceptions.HandshakeException
import com.netflix.genie.agent.execution.services.AgentJobService
import com.netflix.genie.agent.execution.statemachine.Events
import com.netflix.genie.common.internal.dtos.v4.AgentClientMetadata
import spock.lang.Specification

class InitializeActionSpec extends Specification {
    ExecutionContext executionContext
    AgentJobService agentJobService
    AgentMetadata agentMetadata
    String agentId
    String hostname = "123.123.123.123"
    String version = "1.2.3"
    int pid = 12345

    void setup() {
        this.executionContext = Mock(ExecutionContext)
        this.agentJobService = Mock(AgentJobService)
        this.agentMetadata = Mock(AgentMetadata)
        this.agentId = UUID.randomUUID().toString()
        this.hostname = "123.123.123.123"
        this.version = "1.2.3"
        this.pid = 12345
    }

    void cleanup() {
    }

    def "Initialize"() {
        setup:
        InitializeAction action = new InitializeAction(executionContext, agentJobService, agentMetadata)
        AgentClientMetadata agentClientMetadataCapture

        when:
        def event = action.executeStateAction(executionContext)

        then:
        1 * agentMetadata.getAgentHostName() >> hostname
        1 * agentMetadata.getAgentVersion() >> version
        1 * agentMetadata.getAgentPid() >> String.valueOf(pid)
        1 * agentJobService.handshake(_ as AgentClientMetadata) >> {
            args -> agentClientMetadataCapture = args[0]
        }
        agentClientMetadataCapture.getHostname().get() == hostname
        agentClientMetadataCapture.getVersion().get() == version
        agentClientMetadataCapture.getPid().get() == pid

        event == Events.INITIALIZE_COMPLETE
    }

    def "Rejected via handshake"() {
        setup:
        InitializeAction action = new InitializeAction(executionContext, agentJobService, agentMetadata)
        Exception exception = new HandshakeException("Client rejected")

        when:
        def e = action.executeStateAction(executionContext)

        then:
        1 * agentMetadata.getAgentHostName() >> hostname
        1 * agentMetadata.getAgentVersion() >> version
        1 * agentMetadata.getAgentPid() >> String.valueOf(pid)
        1 * agentJobService.handshake(_ as AgentClientMetadata) >> {
            throw exception
        }
        e = thrown(RuntimeException)
        e.getCause() == exception
    }

}
