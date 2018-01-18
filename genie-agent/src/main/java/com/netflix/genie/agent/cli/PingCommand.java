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
import com.google.protobuf.util.Timestamps;
import com.netflix.genie.agent.rpc.ClientFactory;
import com.netflix.genie.proto.PingRequest;
import com.netflix.genie.proto.PingServiceGrpc;
import com.netflix.genie.proto.PongResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

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

    private static final String CLIENT_HOST_NAME_METADATA_KEY = "clientHostName";
    private final PingCommandArguments pingCommandArguments;
    private final ClientFactory clientFactory;

    PingCommand(
        final PingCommandArguments pingCommandArguments,
        final ClientFactory clientFactory
    ) {
        this.pingCommandArguments = pingCommandArguments;
        this.clientFactory = clientFactory;
    }

    @Override
    public void run() {

        final PingServiceGrpc.PingServiceBlockingStub client =
            clientFactory.getBlockingPingClient(pingCommandArguments.serverHost, pingCommandArguments.serverPort);

        String localHostName = "unknown";
        try {
             localHostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            log.warn("Failed to get local host name", e);
        }

        final String requestID =
            pingCommandArguments.requestId == null
                ? UUID.randomUUID().toString() : pingCommandArguments.requestId;

        final PingRequest request = PingRequest.newBuilder()
            .setRequestId(requestID)
            .setSourceName(PingCommand.class.getCanonicalName())
            .setTimestamp(Timestamps.fromMillis(System.currentTimeMillis()))
            .putClientMetadata(CLIENT_HOST_NAME_METADATA_KEY, localHostName)
            .build();

        log.info(
            "Sending Ping with id: {}, timestamp: {}",
            requestID,
            Timestamps.toString(request.getTimestamp())
        );

        final long start = System.nanoTime();
        final PongResponse response = client.ping(request);
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
    @Parameters(commandNames = "ping", commandDescription = "Test connectivity with the server")
    static class PingCommandArguments implements AgentCommandArguments {

        @Parameter(
            names = {"--serverHost"},
            description = "Server hostname or address",
            validateWith = ArgumentValidators.StringValidator.class
        )
        private String serverHost = "genie.prod.netflix.net";

        @Parameter(
            names = {"--serverPort"},
            description = "Server port",
            validateWith = ArgumentValidators.PortValidator.class
        )
        private int serverPort = 7979;

        @Parameter(
            names = {"--requestId"},
            description = "Request id (defaults to UUID)"
        )
        private String requestId;

        @Override
        public Class<? extends AgentCommand> getConsumerClass() {
            return PingCommand.class;
        }
    }
}
