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

package com.netflix.genie.web.rpc.grpc.services.impl.v4

import com.netflix.genie.proto.AgentRegistrationRequest
import com.netflix.genie.proto.AgentRegistrationResponse
import io.grpc.stub.StreamObserver
import org.eclipse.jetty.util.StringUtil
import spock.lang.Specification

class GRpcAgentRegistrationServiceImplSpec extends Specification {
    final String hostName = "server.com"

    GRpcAgentRegistrationServiceImpl agentRegistrationService
    StreamObserver<AgentRegistrationResponse> responseObserver

    void setup() {
        agentRegistrationService = new GRpcAgentRegistrationServiceImpl(hostName)
        responseObserver = Mock(StreamObserver)
    }

    void cleanup() {
    }

    def "RegisterAgent"() {
        setup:
        AgentRegistrationRequest request = AgentRegistrationRequest.newBuilder()
                .setAgentHostname("agent-host")
                .setAgentVersion("1.2.3")
                .setAgentPid("12345")
                .build()
        AgentRegistrationResponse capturedResponse

        when:
        agentRegistrationService.registerAgent(request, responseObserver)

        then:
        1 * responseObserver.onNext(_ as AgentRegistrationResponse) >> {args -> capturedResponse = args[0]}
        1 * responseObserver.onCompleted()

        expect:
        capturedResponse != null
        hostName == capturedResponse.getServerHostname()
        !StringUtil.isBlank(capturedResponse.getServerMessage())
        !StringUtil.isBlank(capturedResponse.getAgentId())
        capturedResponse.getAgentAccepted()
    }

    def "Reject agent empty hostname"() {
        setup:
        AgentRegistrationRequest request = AgentRegistrationRequest.newBuilder()
                .setAgentHostname("")
                .setAgentVersion("1.2.3")
                .setAgentPid("12345")
                .build()
        AgentRegistrationResponse capturedResponse

        when:
        agentRegistrationService.registerAgent(request, responseObserver)

        then:
        1 * responseObserver.onNext(_ as AgentRegistrationResponse) >> {args -> capturedResponse = args[0]}
        1 * responseObserver.onCompleted()

        expect:
        capturedResponse != null
        hostName == capturedResponse.getServerHostname()
        !StringUtil.isBlank(capturedResponse.getServerMessage())
        StringUtil.isBlank(capturedResponse.getAgentId())
        !capturedResponse.getAgentAccepted()
    }

    def "Reject agent empty version"() {
        setup:
        AgentRegistrationRequest request = AgentRegistrationRequest.newBuilder()
                .setAgentHostname("hostname")
                .setAgentVersion("")
                .setAgentPid("12345")
                .build()
        AgentRegistrationResponse capturedResponse

        when:
        agentRegistrationService.registerAgent(request, responseObserver)

        then:
        1 * responseObserver.onNext(_ as AgentRegistrationResponse) >> {args -> capturedResponse = args[0]}
        1 * responseObserver.onCompleted()

        expect:
        capturedResponse != null
        hostName == capturedResponse.getServerHostname()
        !StringUtil.isBlank(capturedResponse.getServerMessage())
        StringUtil.isBlank(capturedResponse.getAgentId())
        !capturedResponse.getAgentAccepted()
    }
}
