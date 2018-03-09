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

package com.netflix.genie.agent.execution.services.impl;

import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.genie.agent.AgentMetadata;
import com.netflix.genie.agent.cli.ArgumentDelegates;
import com.netflix.genie.agent.execution.exceptions.AgentRegistrationException;
import com.netflix.genie.agent.execution.services.AgentRegistrationService;
import com.netflix.genie.proto.AgentRegistrationRequest;
import com.netflix.genie.proto.AgentRegistrationResponse;
import com.netflix.genie.proto.AgentRegistrationServiceGrpc;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * gRPC implementation of AgentRegistrationService.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Service
@Lazy
@Slf4j
class GrpcAgentRegistrationServiceImpl implements AgentRegistrationService {
    private final ArgumentDelegates.ServerArguments serverArguments;
    private final AgentRegistrationServiceGrpc.AgentRegistrationServiceFutureStub client;
    private final AgentMetadata agentMetadata;

    GrpcAgentRegistrationServiceImpl(
        final ArgumentDelegates.ServerArguments serverArguments,
        final AgentRegistrationServiceGrpc.AgentRegistrationServiceFutureStub client,
        final AgentMetadata agentMetadata
    ) {
        this.serverArguments = serverArguments;
        this.client = client;
        this.agentMetadata = agentMetadata;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String registerAgent() throws AgentRegistrationException {
        final AgentRegistrationRequest request = AgentRegistrationRequest.newBuilder()
            .setAgentHostname(agentMetadata.getAgentHostName())
            .setAgentVersion(agentMetadata.getAgentVersion())
            .setAgentPid(agentMetadata.getAgentPid())
            .build();

        final AgentRegistrationResponse response;
        try {
            final ListenableFuture<AgentRegistrationResponse> responseFuture = client.registerAgent(request);
            response = responseFuture.get(serverArguments.getRpcTimeout(), TimeUnit.SECONDS);
        } catch (final InterruptedException | ExecutionException e) {
            throw new AgentRegistrationException("Failed to register agent", e);
        } catch (TimeoutException e) {
            throw new AgentRegistrationException("Timed out waiting to register agent", e);
        }

        log.info(
            "Received agent registration response from server {}",
            response.getServerHostname()
        );

        if (!response.getAgentAccepted()) {
            throw new AgentRegistrationException("Server rejected agent registration: " + response.getServerMessage());
        }

        final String agentId = response.getAgentId();

        if (StringUtils.isBlank(agentId)) {
            throw new AgentRegistrationException("Server returned an empty agent ID");
        }

        return agentId;
    }
}
