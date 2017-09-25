/*
 *
 *  Copyright 2015 Netflix, Inc.
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
package com.netflix.genie.core.grpc.poc;

import com.netflix.genie.protogen.GreeterServiceGrpc;
import com.netflix.genie.protogen.HelloReply;
import com.netflix.genie.protogen.HelloRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.concurrent.TimeUnit;

/**
 * Client for the gRPC example GreeterService.
 *
 * @author mprimi
 * @since 4.0.0
 */
public class GreeterClient {

    private final ManagedChannel channel;
    private final GreeterServiceGrpc.GreeterServiceBlockingStub blockingStub;

    /**
     * Constructor.
     * @param host server hostname
     * @param port server port
     */
    public GreeterClient(final String host, final int port) {
        this(ManagedChannelBuilder
            .forAddress(host, port)
            .usePlaintext(true) // Disable TLS
            .build());
    }

    GreeterClient(final ManagedChannel channel) {
        this.channel = channel;
        this.blockingStub = GreeterServiceGrpc.newBlockingStub(channel);
    }

    /**
     * Shutdown.
     * @throws InterruptedException if shoutdown is interrupted or times out.
     */
    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    /**
     * Seend a greeting to the server.
     * @param name the name in the request
     * @return the server greeting message
     */
    public String greet(final String name) {
        final HelloRequest request = HelloRequest.newBuilder().setName(name).build();
        final HelloReply response = blockingStub.sayHello(request);
        return response.getMessage();
    }

    /**
     * Create a client and receive a greeting from the server.
     * @param args CLI arguments: host name, port, user name
     * @throws Exception in case of error
     */
    public static void main(final String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Required arguments: host port username");
            System.exit(1);
        }
        final String host = args[0];
        final int port = Integer.parseInt(args[1]);
        final String name = args[2];

        System.out.println("Requesting greeting from: " + host + ":" + port);

        final GreeterClient client = new GreeterClient(host, port);

        try {
            final String serverGreetingMessage = client.greet(name);
            System.out.println("Server says: \"" + serverGreetingMessage + "\"");
        } catch (Throwable t) {
            System.err.println("Failed to greet: " + t.getMessage());
            throw t;
        } finally {
            client.shutdown();
        }
    }
}
