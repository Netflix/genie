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

import com.google.protobuf.Timestamp;
import com.netflix.genie.protogen.HeartbeatRequest;
import com.netflix.genie.protogen.HeartbeatResponse;
import com.netflix.genie.protogen.HeartbeatServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Queue;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Heartbeat protocol example.
 * Server randomly shuts down.
 * Client enqueues heartbeats at fixed rate and handles disconnections/retries.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public final class HeartbeatExample {

    private static final int DEFAULT_MAX_TIME_BETWEEN_OUTAGES = 10;
    private static final int DEFAULT_MAX_DOWNTIME = 5;
    private static final int DEFAULT_HEARTBEAT_PERIOD = 5;

    /**
     * Utility class private constructor.
     */
    private HeartbeatExample() { }

    /**
     * Server.
     */
    private static class HeartbeatServerImpl {

        private final ServerBuilder<?> serverBuilder;
        private final ScheduledExecutorService executorService;
        private final Random random;
        private final int maxTimeBetweenOutages;
        private final int maxDowntimeSeconds;
        private final CountDownLatch shutdownLatch;
        private Server server;

        HeartbeatServerImpl(
            final int port,
            final int maxTimeBetweeOutagesSeconds,
            final int maxDowntimeSeconds
        ) {
            this.maxTimeBetweenOutages = maxTimeBetweeOutagesSeconds;
            this.maxDowntimeSeconds = maxDowntimeSeconds;
            this.executorService = new ScheduledThreadPoolExecutor(1);
            this.random = new Random();
            this.shutdownLatch = new CountDownLatch(1);
            this.serverBuilder = ServerBuilder.forPort(port)
                .intercept(new SimpleLoggingInterceptor())
                .addService(new HeartbeatServiceImpl());
        }

        void start() throws IOException {
            final long latchValue = shutdownLatch.getCount();
            if (latchValue != 1) {
                throw new RuntimeException("Unexpected shutdown latch value: " + latchValue);
            }
            server = serverBuilder.build();
            server.start();
            scheduleOutage();
        }

        private void scheduleOutage() {
            final int nextOutageDelay = random.nextInt(maxTimeBetweenOutages);
            log.info("Scheduling next outage in {} seconds", nextOutageDelay);
            executorService.schedule(this::serverOutage, nextOutageDelay, TimeUnit.SECONDS);
        }

        private void serverOutage() {
            final int downtime = random.nextInt(maxDowntimeSeconds);

            log.info("Server shutting down");
            server.shutdown();
            assert server.isTerminated();

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

            this.server = newServer;

            scheduleOutage();
        }

        void shutdown() {
            executorService.shutdown();
            server.shutdown();
            shutdownLatch.countDown();
        }

        void awaitShutdown() throws InterruptedException {
            shutdownLatch.await();
        }

        private class HeartbeatServiceImpl extends HeartbeatServiceGrpc.HeartbeatServiceImplBase {

            @Override
            public void heartbeat(
                final HeartbeatRequest request,
                final StreamObserver<HeartbeatResponse> responseObserver
            ) {
                if (StringUtils.isEmpty(request.getHeartbeatUuid())) {
                    responseObserver.onError(Status.INVALID_ARGUMENT.asException());
                } else {
                    final HeartbeatResponse response = HeartbeatResponse.newBuilder()
                        .setHeartbeatUuid(request.getHeartbeatUuid())
                        .setRequestTimestamp(request.getRequestTimestamp())
                        .setResponseTimestamp(Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000))
                        .build();
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                }
            }
        }
    }

    /**
     * Client.
     */
    private static final class HeartbeatClientImpl {
        private final ManagedChannel channel;
        private final HeartbeatServiceGrpc.HeartbeatServiceBlockingStub blockingStub;
        private final ScheduledExecutorService executorService;
        private final int heartbeatInterval;
        private final Queue<HeartbeatRequest> heartbeatRequestsQueue;
        private final Object queueNotEmpty;

        private HeartbeatClientImpl(
            final String host,
            final int port,
            final int heartbeatIntervalSeconds
        ) {
            this.executorService = new ScheduledThreadPoolExecutor(1);
            this.heartbeatInterval = heartbeatIntervalSeconds;
            this.heartbeatRequestsQueue = new ConcurrentLinkedQueue<>();
            this.queueNotEmpty = new Object();

            this.channel = ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext(true) // Disable TLS
                .build();
            this.blockingStub = HeartbeatServiceGrpc.newBlockingStub(channel);
        }

        void run() {
            // Enqueue heartbeats at fixed rate
            executorService.scheduleWithFixedDelay(
                this::enqueueHeartbeatRequest,
                heartbeatInterval,
                heartbeatInterval,
                TimeUnit.SECONDS);

            while (true) {
                // Wait until at least one heartbeat is in the queue
                waitForRequestEnqueue();
                //Deliver all queued heartbeats to the server
                drainQueue();
            }
        }

        private void enqueueHeartbeatRequest() {
            // Enqueue a new message
            heartbeatRequestsQueue.add(
                HeartbeatRequest.newBuilder()
                    .setHeartbeatUuid(UUID.randomUUID().toString())
                    .setRequestTimestamp(Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000))
                    .build());
            // Wake up delivery thread which may be waiting
            synchronized (queueNotEmpty) {
                queueNotEmpty.notify();
            }
        }

        private void waitForRequestEnqueue() {
            while (heartbeatRequestsQueue.isEmpty()) {
                try {
                    synchronized (queueNotEmpty) {
                        queueNotEmpty.wait(1000);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        private void drainQueue() {
            while (!heartbeatRequestsQueue.isEmpty()) {
                final HeartbeatRequest request = heartbeatRequestsQueue.peek();

                log.info("Attempting delivery of heartbeat: {}", request.getHeartbeatUuid());

                HeartbeatResponse response = null;
                try {
                    response = blockingStub.heartbeat(request);
                } catch (StatusRuntimeException e) {
                    if (e.getStatus().getCode() == Status.UNAVAILABLE.getCode()) {
                        log.warn("Server is down");
                    } else {
                        log.error("Failed to send request " + request.getHeartbeatUuid(), e);
                    }
                } catch (Throwable t) {
                    log.error("Failed to send request " + request.getHeartbeatUuid(), t);
                }

                if (response != null) {
                    assert request.getHeartbeatUuid().equals(response.getHeartbeatUuid());
                    heartbeatRequestsQueue.poll();
                    log.info("Request {} turnaround time: {}",
                        request.getHeartbeatUuid(),
                        response.getResponseTimestamp().getSeconds() - response.getRequestTimestamp().getSeconds());
                } else {
                    // Pause before retrying
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }

    /**
     * Start either a client or a server.
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

            final int heartbeatPeriod;

            if (args.length >= 4) {
                heartbeatPeriod = Integer.parseInt(args[3]);
            } else {
                heartbeatPeriod = DEFAULT_HEARTBEAT_PERIOD;
            }

            new HeartbeatClientImpl(host, port, heartbeatPeriod).run();

        } else if ("server".equals(role)) {

            final int maxTimeBetweenOutages;

            if (args.length >= 3) {
                maxTimeBetweenOutages = Integer.parseInt(args[2]);
            } else {
                maxTimeBetweenOutages = DEFAULT_MAX_TIME_BETWEEN_OUTAGES;
            }

            final int maxDowntime;
            if (args.length >= 4) {
                maxDowntime = Integer.parseInt(args[3]);
            } else {
                maxDowntime = DEFAULT_MAX_DOWNTIME;
            }

            final HeartbeatServerImpl server = new HeartbeatServerImpl(port, maxTimeBetweenOutages, maxDowntime);
            server.start();
            server.awaitShutdown();

        } else {
            printUsage();
            System.exit(1);
        }
    }

    private static void printUsage() {
        final String usage = "Usage:\n"
            + "  " + HeartbeatExample.class.getSimpleName() + " server <port> [timeBetweenOutages] [outageDuration]\n"
            + "  " + HeartbeatExample.class.getSimpleName() + " client <port> [host] [heartbeatInterval]\n";

        System.out.println(usage);
    }
}
