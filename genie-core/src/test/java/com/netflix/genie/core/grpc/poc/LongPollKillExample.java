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

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.genie.protogen.KillRequest;
import com.netflix.genie.protogen.KillResponse;
import com.netflix.genie.protogen.KillServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Long poll protocol example.
 * Server parks requests until it decides to respond (and kill the corresponding client), also randomly shuts down.
 * Client waits for kill signal from server using long-polling and handles disconnections transparently.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public final class LongPollKillExample {
    private static final int NEVER = -1;
    private static final int DEFAULT_NUM_CLIENTS = 3;
    private static final int DEFAULT_MAX_TIME_BETWEEN_OUTAGES = 300;
    private static final int DEFAULT_MAX_DOWNTIME = 5;
    private static final int DEFAULT_MAX_TIME_BETWEEN_KILL = 60;

    /**
     * Utility class private constructor.
     */
    private LongPollKillExample() { }

    /**
     * Server.
     */
    private static class LongPollKillServerImpl {

        private final ServerBuilder<?> serverBuilder;
        private final ScheduledExecutorService executorService;
        private final Random random;
        private final int maxTimeBetweenKill;
        private final int maxTimeBetweenOutages;
        private final int maxDowntimeSeconds;
        private final CountDownLatch shutdownLatch;
        private final Map<String, ParkedKillRequest> parkedRequests;
        private Server server;

        /**
         * Stores a server-side parked request context until the server decides to respond
         * (thus killing the issuing client).
         */
        private static final class ParkedKillRequest {
            private final KillRequest request;
            private final StreamObserver<KillResponse> responseObserver;

            ParkedKillRequest(
                final KillRequest request,
                final StreamObserver<KillResponse> responseObserver
            ) {
                this.request = request;
                this.responseObserver = responseObserver;
            }
        }

        LongPollKillServerImpl(
            final int port,
            final int maxTimeBetweenKillSeconds,
            final int maxTimeBetweeOutagesSeconds,
            final int maxDowntimeSeconds
        ) {
            this.maxTimeBetweenKill = maxTimeBetweenKillSeconds;
            this.maxTimeBetweenOutages = maxTimeBetweeOutagesSeconds;
            this.maxDowntimeSeconds = maxDowntimeSeconds;
            this.executorService = Executors.newSingleThreadScheduledExecutor();
            this.random = new Random();
            this.shutdownLatch = new CountDownLatch(1);
            this.parkedRequests = Maps.newHashMap();
            this.serverBuilder = ServerBuilder.forPort(port)
                .intercept(new SimpleLoggingInterceptor())
                .addService(new KillServiceImpl());
        }

        void start() throws IOException {
            final long latchValue = shutdownLatch.getCount();
            if (latchValue != 1) {
                throw new RuntimeException("Unexpected shutdown latch value: " + latchValue);
            }
            server = serverBuilder.build();
            server.start();

            if (maxTimeBetweenOutages != NEVER) {
                scheduleOutage();
            }

            if (maxTimeBetweenKill != NEVER) {
                scheduleKillClient();
            }
        }

        private void scheduleOutage() {
            final int nextDisconnectionDelay = random.nextInt(maxTimeBetweenOutages);
            log.info("Scheduling next server outage in {} seconds", nextDisconnectionDelay);
            executorService.schedule(this::serverOutage, nextDisconnectionDelay, TimeUnit.SECONDS);
        }

        private void scheduleKillClient() {
            final int nextKillDelay = random.nextInt(maxTimeBetweenKill);
            log.info("Scheduling next client kill in {} seconds", nextKillDelay);
            executorService.schedule(this::killRandomClient, nextKillDelay, TimeUnit.SECONDS);
        }

        private void killRandomClient() {
            ParkedKillRequest parkedRequest = null;

            // Choose a random parked client request
            synchronized (parkedRequests) {
                if (parkedRequests.size() > 0) {
                    final int requestIndex = random.nextInt(parkedRequests.size());

                    int i = 0;

                    for (Map.Entry<String, ParkedKillRequest> parkedKillRequestEntry : parkedRequests.entrySet()) {
                        if (i == requestIndex) {
                            parkedRequest = parkedRequests.remove(parkedKillRequestEntry.getKey());
                            break;
                        }
                        ++i;
                    }
                }
            }

            // Respond to the request, thus telling the client to terminate
            if (parkedRequest != null) {
                log.info("Sending kill signal to client: {}", parkedRequest.request.getClientId());

                final KillResponse response = KillResponse.newBuilder()
                    .setClientId(parkedRequest.request.getClientId())
                    .build();

                parkedRequest.responseObserver.onNext(response);
                parkedRequest.responseObserver.onCompleted();
            }

            // Schedule next kill
            scheduleKillClient();
        }

        void shutdown() {
            executorService.shutdown();
            server.shutdown();
            shutdownLatch.countDown();
        }

        void awaitShutdown() throws InterruptedException {
            shutdownLatch.await();
        }

        private class KillServiceImpl extends KillServiceGrpc.KillServiceImplBase {

            @Override
            public void waitKill(
                final KillRequest request,
                final StreamObserver<KillResponse> responseObserver
            ) {
                final ParkedKillRequest parkedRequest = new ParkedKillRequest(request, responseObserver);

                synchronized (parkedRequests) {

                    // Rather than responding, park the request
                    final ParkedKillRequest oldParkedRequest = parkedRequests.put(request.getClientId(), parkedRequest);

                    if (oldParkedRequest != null) {
                        // TODO: how and what to do about this?
                        log.warn("Found a previous parked request for client {}", request.getClientId());
                    }
                }
            }
        }

        void serverOutage() {
            final int downtime = random.nextInt(maxDowntimeSeconds);

            log.info("Server shutting down");
            server.shutdown();
            assert server.isTerminated();

            // Discard all parked requests (without answering them)
            synchronized (parkedRequests) {
                parkedRequests.clear();
            }

            log.info("Server down for {} seconds", downtime);
            try {
                Thread.sleep(downtime * 1000);
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
                Thread.currentThread().interrupt();
                shutdown();
                return;
            }

            final Server newServer = serverBuilder.build();
            log.info("Server restarting");
            try {
                newServer.start();
            } catch (Throwable t) {
                log.error("Unexpected error", t);
                shutdown();
            }
            server = newServer;

            scheduleOutage();
        }
    }

    /**
     * Keeps a fixed size pool of clients running, replacing the ones that terminate.
     */
    private static class ClientsManager {
        private final int numClients;
        private final String host;
        private final int port;
        private final Object clientTerminatedCondition = new Object();
        private final LongPollKillClientImpl[] clients;
        private final ScheduledExecutorService executor;

        ClientsManager(
            final int numClients,
            final String host,
            final int port
        ) {
            this.numClients = numClients;
            this.host = host;
            this.port = port;
            this.clients = new LongPollKillClientImpl[numClients];
            this.executor = Executors.newScheduledThreadPool(3);
        }

        void run() {

            // Create and start the initial set of clients
            for (int i = 0; i < numClients; i++) {
                clients[i] = new LongPollKillClientImpl(host, port, clientTerminatedCondition, executor);
                clients[i].start();
            }

            while (true) {
                // Wait for one or more to terminate
                synchronized (clientTerminatedCondition) {
                    try {
                        clientTerminatedCondition.wait(10000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }

                // Replace terminated clients with new ones
                for (int i = 0; i < numClients; i++) {
                    if (clients[i].isTerminated()) {
                        clients[i] = new LongPollKillClientImpl(host, port, clientTerminatedCondition, executor);
                        clients[i].start();
                    }
                }
            }
        }
    }

    /**
     * Client.
     */
    private static class LongPollKillClientImpl {
        private final Object clientTerminatedCondition;
        private final ScheduledExecutorService scheduledExecutorService;
        private final KillServiceGrpc.KillServiceFutureStub futureStub;
        private final String clientId;
        private final Object futureLock = new Object();
        private final AtomicBoolean terminated = new AtomicBoolean(false);
        private final KillRequest killRequest;
        private ListenableFuture<KillResponse> responseFuture;

        LongPollKillClientImpl(
            final String host,
            final int port,
            final Object clientTerminatedCondition,
            final ScheduledExecutorService scheduledExecutorService
        ) {
            this.clientTerminatedCondition = clientTerminatedCondition;
            this.scheduledExecutorService = scheduledExecutorService;
            final ManagedChannel channel = ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext(true) // Disable TLS
                .build();
            this.futureStub = KillServiceGrpc.newFutureStub(channel);
            this.clientId = UUID.randomUUID().toString();
            this.killRequest = KillRequest.newBuilder()
                .setClientId(clientId)
                .build();
        }

        void start() {
            makeAsyncKillRequest();
        }

        private void makeAsyncKillRequest() {
            responseFuture = futureStub.waitKill(killRequest);
            responseFuture.addListener(this::handleResponse, scheduledExecutorService);
            log.info("Client {} waiting for kill signal", clientId);
        }

        private void handleResponse() {
            try {
                log.info("Client {} handling response", clientId);

                KillResponse response = null;

                synchronized (futureLock) {
                    try {
                        response = responseFuture.get();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        log.warn("Client {} interrupted while getting response", clientId, e);
                    } catch (ExecutionException e) {
                        if (e.getCause() != null
                            &&
                            e.getCause() instanceof StatusRuntimeException
                            &&
                            ((StatusRuntimeException) e.getCause()).getStatus().getCode() == Status.Code.UNAVAILABLE) {
                            log.warn("Client {} could not reach the server", clientId);
                        } else {
                            log.warn("Client {} error while getting response", clientId, e);
                        }
                    }

                    if (response != null) {
                        assert response.getClientId().equals(clientId);
                        log.info("Client {} received kill signal, terminating", clientId);
                        terminated.set(true);
                        synchronized (clientTerminatedCondition) {
                            clientTerminatedCondition.notify();
                        }
                    } else {
                        log.info("Client {} request failed, retrying shortly", clientId);
                        scheduledExecutorService.schedule(this::makeAsyncKillRequest, 3, TimeUnit.SECONDS);
                    }
                }

            } catch (Throwable t) { // Catch-all and swallow to avoid killing executor threads
                log.error("Unexpected exception while handling response", t);
            }
        }

        boolean isTerminated() {
            return terminated.get();
        }
    }

    /**
     * Start either a set of clients or a server.
     * @param args CLI arguments: (client|server) port [optional args]
     * @throws Exception in case of error
     */
    public static void main(final String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Invalid arguments");
            printUsage();
            System.exit(1);
        }
        final String role = args[0];
        final int port = Integer.parseInt(args[1]);

        if ("client".equals(role)) {

            final String host;

            if (args.length >= 3) {
                host = args[2];
            } else {
                host = "localhost";
            }

            final int numClients;

            if (args.length >= 4) {
                numClients = Integer.parseInt(args[3]);
            } else {
                numClients = DEFAULT_NUM_CLIENTS;
            }

            new ClientsManager(numClients, "localhost", port).run();

        } else if ("server".equals(role)) {

            final int maxTimeBetweenKill;

            if (args.length >= 3) {
                maxTimeBetweenKill = Integer.parseInt(args[2]);
            } else {
                maxTimeBetweenKill = DEFAULT_MAX_TIME_BETWEEN_KILL;
            }

            final int maxTimeBetweenOutages;

            if (args.length >= 4) {
                maxTimeBetweenOutages = Integer.parseInt(args[3]);
            } else {
                maxTimeBetweenOutages = DEFAULT_MAX_TIME_BETWEEN_OUTAGES;
            }

            final int maxDowntime;
            if (args.length >= 5) {
                maxDowntime = Integer.parseInt(args[4]);
            } else {
                maxDowntime = DEFAULT_MAX_DOWNTIME;
            }

            final LongPollKillServerImpl server = new LongPollKillServerImpl(
                port,
                maxTimeBetweenKill,
                maxTimeBetweenOutages,
                maxDowntime
            );
            server.start();
            server.awaitShutdown();

        } else {
            printUsage();
            System.exit(1);
        }
    }

    private static void printUsage() {
        final String usage = "Usage:\n"
            + "  " + LongPollKillExample.class.getSimpleName()
            + " server <port> [timeBetweenKill] [timeBetweenOutages] [outageDuration]\n"
            + "  " + LongPollKillExample.class.getSimpleName()
            + " client <port> [host] [numClients]" + "\n";

        System.out.println(usage);
    }

}
