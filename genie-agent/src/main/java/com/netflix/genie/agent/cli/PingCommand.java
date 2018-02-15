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

package com.netflix.genie.agent.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.google.protobuf.util.Timestamps;
import com.netflix.genie.agent.AgentMetadata;
import com.netflix.genie.proto.PingRequest;
import com.netflix.genie.proto.PingServiceGrpc;
import com.netflix.genie.proto.PongResponse;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Command to ping the server and test for connectivity and latency.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
@Component
@Lazy
class PingCommand implements AgentCommand {

    static final String CLIENT_HOST_NAME_METADATA_KEY = "clientHostName";
    private final PingCommandArguments pingCommandArguments;
    private final PingServiceGrpc.PingServiceFutureStub pingServiceClient;
    private final AgentMetadata agentMetadata;

    PingCommand(
        final PingCommandArguments pingCommandArguments,
        final PingServiceGrpc.PingServiceFutureStub pingServiceClient,
        final AgentMetadata agentMetadata
    ) {
        this.pingCommandArguments = pingCommandArguments;
        this.pingServiceClient = pingServiceClient;
        this.agentMetadata = agentMetadata;
    }

    @Override
    public void run() {

        final String requestID =
            pingCommandArguments.getRequestId() == null
                ? UUID.randomUUID().toString() : pingCommandArguments.getRequestId();

        final String source = agentMetadata.getAgentPid() + "@" + agentMetadata.getAgentHostName();
        final PingRequest request = PingRequest.newBuilder()
            .setRequestId(requestID)
            .setSourceName(source)
            .setTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
            .putClientMetadata(CLIENT_HOST_NAME_METADATA_KEY, agentMetadata.getAgentHostName())
            .build();

        log.info(
            "Sending Ping with id: {}, timestamp: {}",
            requestID,
            Timestamps.toString(request.getTimestamp())
        );

        final long start = System.nanoTime();
        final PongResponse response;
        try {
            response = pingServiceClient.ping(request).get(
                pingCommandArguments.getRpcTimeout(),
                TimeUnit.SECONDS
            );
        } catch (final InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException("Failed to ping server", e);
        }
        final long end = System.nanoTime();

        final long elapsedMillis = TimeUnit.MILLISECONDS.convert(end - start, TimeUnit.NANOSECONDS);

        log.info("Server responded to ping in {} ms. Server metadata:", elapsedMillis);

        for (Map.Entry<String, String> serverMetadataEntry : response.getServerMetadataMap().entrySet()) {
            log.info(
                " - {} = {}",
                serverMetadataEntry.getKey(),
                serverMetadataEntry.getValue());
        }
    }

    @Component
    @Parameters(commandNames = CommandNames.PING, commandDescription = "Test connectivity with the server")
    static class PingCommandArguments implements AgentCommandArguments {

        @ParametersDelegate
        private final ArgumentDelegates.ServerArguments serverArguments;

        @Parameter(
            names = {"--requestId"},
            description = "Request id (defaults to UUID)",
            validateWith = ArgumentValidators.StringValidator.class
        )
        @Getter
        private String requestId;

        PingCommandArguments(final ArgumentDelegates.ServerArguments serverArguments) {
            this.serverArguments = serverArguments;
        }

        @Override
        public Class<? extends AgentCommand> getConsumerClass() {
            return PingCommand.class;
        }

        String getServerHost() {
            return serverArguments.getServerHost();
        }

        int getServerPort() {
            return serverArguments.getServerPort();
        }

        long getRpcTimeout() {
            return serverArguments.getRpcTimeout();
        }
    }
}
