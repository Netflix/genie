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

package com.netflix.genie.agent.execution.services

import com.netflix.genie.agent.AgentMetadata
import com.netflix.genie.agent.cli.ArgumentDelegates
import com.netflix.genie.agent.execution.exceptions.AgentRegistrationException

import com.netflix.genie.proto.AgentRegistrationRequest
import com.netflix.genie.proto.AgentRegistrationResponse
import com.netflix.genie.proto.AgentRegistrationServiceGrpc
import io.grpc.stub.StreamObserver
import io.grpc.testing.GrpcServerRule
import org.junit.Rule
import spock.lang.Ignore
import spock.lang.Specification


class GrpcAgentRegistrationServiceImplSpec extends Specification {

    final String agentHostName = "agent.com"
    final String agentVersion = "1.2.3"
    final String agentPid = "54321"
    final String serverHost = "genie.com"
    final long rpcTimeout = 2

    @Rule
    GrpcServerRule grpcServerRule = new GrpcServerRule().directExecutor()
    ArgumentDelegates.ServerArguments serverArgs
    AgentRegistrationServiceGrpc.AgentRegistrationServiceFutureStub client
    AgentRegistrationRequest capturedRequest
    Exception serverException
    Throwable serverError
    AgentRegistrationResponse serverResponse
    AgentMetadata agentMetadata

    void setup() {
        this.grpcServerRule.getServiceRegistry().addService(new AgentRegistrationServiceGrpc.AgentRegistrationServiceImplBase() {
            @Override
            void registerAgent(AgentRegistrationRequest request, StreamObserver<AgentRegistrationResponse> responseObserver) {
                capturedRequest = request
                if (serverException != null) {
                    throw serverException
                } else if (serverError != null) {
                    responseObserver.onError(serverError)
                } else if (serverResponse != null) {
                    responseObserver.onNext(serverResponse)
                    responseObserver.onCompleted()
                }
            }
        })
        this.serverArgs = Mock(ArgumentDelegates.ServerArguments)
        this.client = AgentRegistrationServiceGrpc.newFutureStub(grpcServerRule.getChannel())
        this.agentMetadata = Mock(AgentMetadata)
        this.capturedRequest = null
        this.serverException = null
        this.serverError = null
        this.serverResponse = null
    }

    void cleanup() {
    }

    def "RegisterAgent"() {
        setup:
        String agentId = UUID.randomUUID().toString()

        serverResponse = AgentRegistrationResponse.newBuilder()
                .setServerHostname(serverHost)
                .setServerMessage("Welcome agent")
                .setAgentId(agentId)
                .setAgentAccepted(true)
                .build()

        when:
        String agentIdInResponse = new GrpcAgentRegistrationServiceImpl(
                serverArgs,
                client,
                agentMetadata
        ).registerAgent()

        then:
        1 * agentMetadata.getAgentVersion() >> agentVersion
        1 * agentMetadata.getAgentHostName() >> agentHostName
        1 * agentMetadata.getAgentPid() >> agentPid
        1 * serverArgs.getRpcTimeout() >> rpcTimeout

        expect:
        agentId == agentIdInResponse
        agentVersion == capturedRequest.getAgentVersion()
        agentHostName == capturedRequest.getAgentHostname()
        agentPid == capturedRequest.getAgentPid()
    }

    def "RegisterAgent invalid id"() {
        setup:
        serverResponse = AgentRegistrationResponse.newBuilder()
                .setServerHostname(serverHost)
                .setServerMessage("Welcome agent")
                .setAgentId(" ")
                .setAgentAccepted(true)
                .build()

        when:
        new GrpcAgentRegistrationServiceImpl(
                serverArgs,
                client,
                agentMetadata
        ).registerAgent()

        then:
        1 * agentMetadata.getAgentVersion() >> agentVersion
        1 * agentMetadata.getAgentHostName() >> agentHostName
        1 * agentMetadata.getAgentPid() >> agentPid
        1 * serverArgs.getRpcTimeout() >> rpcTimeout
        thrown(AgentRegistrationException)

        expect:
        agentVersion == capturedRequest.getAgentVersion()
        agentHostName == capturedRequest.getAgentHostname()
        agentPid == capturedRequest.getAgentPid()
    }

    def "RegisterAgent rejected"() {
        setup:
        serverResponse = AgentRegistrationResponse.newBuilder()
                .setServerHostname(serverHost)
                .setServerMessage("Rejecting agent")
                .setAgentAccepted(false)
                .build()

        when:
        new GrpcAgentRegistrationServiceImpl(
                serverArgs,
                client,
                agentMetadata
        ).registerAgent()

        then:
        1 * agentMetadata.getAgentVersion() >> agentVersion
        1 * agentMetadata.getAgentHostName() >> agentHostName
        1 * agentMetadata.getAgentPid() >> agentPid
        1 * serverArgs.getRpcTimeout() >> rpcTimeout
        thrown(AgentRegistrationException)

        expect:
        agentVersion == capturedRequest.getAgentVersion()
        agentHostName == capturedRequest.getAgentHostname()
        agentPid == capturedRequest.getAgentPid()
    }

    def "RegisterAgent server exception"() {
        setup:
        serverException = new RuntimeException("error")

        when:
        new GrpcAgentRegistrationServiceImpl(
                serverArgs,
                client,
                agentMetadata
        ).registerAgent()

        then:
        1 * agentMetadata.getAgentVersion() >> agentVersion
        1 * agentMetadata.getAgentHostName() >> agentHostName
        1 * agentMetadata.getAgentPid() >> agentPid
        1 * serverArgs.getRpcTimeout() >> rpcTimeout
        thrown(AgentRegistrationException)

        expect:
        agentVersion == capturedRequest.getAgentVersion()
        agentHostName == capturedRequest.getAgentHostname()
        agentPid == capturedRequest.getAgentPid()
    }

    def "RegisterAgent server error"() {
        setup:
        serverError = new RuntimeException("error")

        when:
        new GrpcAgentRegistrationServiceImpl(
                serverArgs,
                client,
                agentMetadata
        ).registerAgent()

        then:
        1 * agentMetadata.getAgentVersion() >> agentVersion
        1 * agentMetadata.getAgentHostName() >> agentHostName
        1 * agentMetadata.getAgentPid() >> agentPid
        1 * serverArgs.getRpcTimeout() >> rpcTimeout
        thrown(AgentRegistrationException)

        expect:
        agentVersion == capturedRequest.getAgentVersion()
        agentHostName == capturedRequest.getAgentHostname()
        agentPid == capturedRequest.getAgentPid()
    }

    //TODO
    @Ignore(value = "Future timeout seems to be ignored and always wait for 2+ minutes")
    def "RegisterAgent server timeout"() {
        setup:

        when:
        service = new GrpcAgentRegistrationServiceImpl(
                serverArgs,
                client,
                agentMetadata
        ).registerAgent()

        then:
        1 * agentMetadata.getAgentVersion() >> agentVersion
        1 * agentMetadata.getAgentHostName() >> agentHostName
        1 * agentMetadata.getAgentPid() >> agentPid
        1 * serverArgs.getRpcTimeout() >> rpcTimeout
        thrown(AgentRegistrationException)

        expect:
        agentVersion == capturedRequest.getAgentVersion()
        agentHostName == capturedRequest.getAgentHostname()
        agentPid == capturedRequest.getAgentPid()
    }
}
