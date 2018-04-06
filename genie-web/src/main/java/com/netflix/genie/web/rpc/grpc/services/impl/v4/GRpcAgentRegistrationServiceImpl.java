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

package com.netflix.genie.web.rpc.grpc.services.impl.v4;

import com.netflix.genie.proto.AgentRegistrationRequest;
import com.netflix.genie.proto.AgentRegistrationResponse;
import com.netflix.genie.proto.AgentRegistrationServiceGrpc;
import com.netflix.genie.web.rpc.interceptors.SimpleLoggingInterceptor;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.springboot.autoconfigure.grpc.server.GrpcService;
import org.apache.commons.lang3.StringUtils;

import java.util.UUID;

/**
 * Implementation of server-side AgentRegistrationService.
 *
 * @author mprimi
 * @since 4.0.0
 */
@GrpcService(
    value = AgentRegistrationServiceGrpc.class,
    interceptors = {
        SimpleLoggingInterceptor.class,
    }
)
@Slf4j
class GRpcAgentRegistrationServiceImpl extends AgentRegistrationServiceGrpc.AgentRegistrationServiceImplBase {

    private final String hostName;

    GRpcAgentRegistrationServiceImpl(
        final String hostName
    ) {
        this.hostName = hostName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void registerAgent(
        final AgentRegistrationRequest request,
        final StreamObserver<AgentRegistrationResponse> responseObserver
    ) {
        log.info(
            "Received agent registration request from: {}@{} (v{})",
            request.getAgentPid(),
            request.getAgentHostname(),
            request.getAgentVersion()
        );

        final AgentRegistrationResponse.Builder responseBuilder = AgentRegistrationResponse.newBuilder()
            .setServerHostname(hostName);

        if (!acceptAgentVersion(request.getAgentVersion())) {
            responseBuilder
                .setAgentAccepted(false)
                .setServerMessage("Agent version rejected: \"" + request.getAgentVersion() + "\"");
        } else if (!acceptedAgentHost(request.getAgentHostname())) {
            responseBuilder
                .setAgentAccepted(false)
                .setServerMessage("Agent host rejected: \"" + request.getAgentHostname() + "\"");
        } else {
            final String agentId = assignAgentId();
            responseBuilder
                .setAgentId(agentId)
                .setAgentAccepted(true)
                .setServerMessage("Welcome agent " + agentId);
        }

        final AgentRegistrationResponse response = responseBuilder.build();

        log.info(
            "Server {} agent with message: {}",
            response.getAgentAccepted() ? "accepted" : "rejected",
            response.getServerMessage()
        );

        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    private String assignAgentId() {
        // TODO placeholder for more advanced logic
        return UUID.randomUUID().toString();
    }

    private boolean acceptedAgentHost(final String agentHostname) {
        // TODO placeholder for more advanced logic
        return !StringUtils.isBlank(agentHostname);
    }

    private boolean acceptAgentVersion(final String agentVersion) {
        // TODO placeholder for more advanced logic
        return !StringUtils.isEmpty(agentVersion);
    }
}
