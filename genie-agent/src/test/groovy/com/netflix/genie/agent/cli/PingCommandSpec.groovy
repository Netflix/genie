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

package com.netflix.genie.agent.cli

import com.google.protobuf.util.Timestamps
import com.netflix.genie.agent.AgentMetadata
import com.netflix.genie.proto.PingRequest
import com.netflix.genie.proto.PingServiceGrpc
import com.netflix.genie.proto.PongResponse
import com.netflix.genie.test.categories.UnitTest
import io.grpc.stub.StreamObserver
import io.grpc.testing.GrpcServerRule
import org.apache.commons.lang3.StringUtils
import org.junit.Rule
import org.junit.experimental.categories.Category
import spock.lang.Specification

@Category(UnitTest.class)
class PingCommandSpec extends Specification {

    final String agentHostName = "agent.com"
    final String agentVersion = "1.2.3"
    final String agentPid = "54321"
    final String source = agentPid + "@" + agentHostName

    @Rule
    public final GrpcServerRule grpcServerRule = new GrpcServerRule().directExecutor()
    PingServiceGrpc.PingServiceFutureStub pingServiceClient
    PingCommand.PingCommandArguments pingCommandArgs
    PingRequest capturedRequest
    AgentMetadata agentMetadata

    void setup() {
        this.pingServiceClient = PingServiceGrpc.newFutureStub(grpcServerRule.getChannel())
        this.pingCommandArgs = Mock(PingCommand.PingCommandArguments)
        this.agentMetadata = Mock(AgentMetadata)
    }

    void cleanup() {
    }

    def "Run"() {
        setup:
        grpcServerRule.getServiceRegistry().addService(new PingServiceGrpc.PingServiceImplBase() {
            @Override
            void ping(PingRequest request, StreamObserver<PongResponse> responseObserver) {
                capturedRequest = request
                final PongResponse response = PongResponse.newBuilder()
                    .setRequestId(request.getRequestId())
                    .setTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
                    .putServerMetadata("Foo", "bar")
                    .build()
                responseObserver.onNext(response)
                responseObserver.onCompleted()
            }
        })

        def pingCommand = new PingCommand(pingCommandArgs, pingServiceClient, agentMetadata)

        when:
        pingCommand.run()

        then:
        2 * agentMetadata.getAgentHostName() >> agentHostName
        1 * agentMetadata.getAgentPid() >> agentPid
        1 * pingCommandArgs.getRequestId()
        1 * pingCommandArgs.getRpcTimeout() >> 3L

        expect:
        source == capturedRequest.getSourceName()
        !StringUtils.isBlank(capturedRequest.getRequestId())
        capturedRequest.getTimestamp() != null
        agentHostName == capturedRequest.getClientMetadataOrThrow(PingCommand.CLIENT_HOST_NAME_METADATA_KEY)
    }

    def "RunWithError"() {
        setup:
        def requestId = UUID.randomUUID().toString()
        grpcServerRule.getServiceRegistry().addService(new PingServiceGrpc.PingServiceImplBase() {
            @Override
            void ping(PingRequest request, StreamObserver<PongResponse> responseObserver) {
                capturedRequest = request
                responseObserver.onError(new IOException())
            }
        })

        def pingCommand = new PingCommand(pingCommandArgs, pingServiceClient, agentMetadata)

        when:
        pingCommand.run()

        then:
        2 * agentMetadata.getAgentHostName() >> agentHostName
        1 * agentMetadata.getAgentPid() >> agentPid
        2 * pingCommandArgs.getRequestId() >> requestId
        1 * pingCommandArgs.getRpcTimeout() >> 3L
        thrown(RuntimeException)

        expect:
        source == capturedRequest.getSourceName()
        !StringUtils.isBlank(capturedRequest.getRequestId())
        capturedRequest.getTimestamp() != null
        agentHostName == capturedRequest.getClientMetadataOrThrow(PingCommand.CLIENT_HOST_NAME_METADATA_KEY)
    }
}
