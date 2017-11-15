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

import com.netflix.genie.protogen.FileChunkRequest;
import com.netflix.genie.protogen.FileChunkResponse;
import com.netflix.genie.protogen.FileStreamServiceGrpc;
import com.netflix.genie.protogen.HelloRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Streaming backpressure ("Reactive stream") example.
 * Client produces messages faster than the server can consume.
 * Server consumes one messages serially, taking a fixed amount of time on each.
 * Take advantage of underlying gRPC/http2 flow control to avoid excessive buffering client-side.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public final class StreamBackpressureExample {

    private static final int DEFAULT_NUM_REQUESTS = 100;
    private static final int DEFAULT_MAX_PROCESSING_TIME = 10000;
    private static final int DEFAULT_MIN_PROCESSING_TIME = 1000;
    private static final int CLIENT_TRANSMISSION_WINDOW = 10;
    private static final int CLIENT_CHUNKS_QUEUE_SIZE = 5;

    /**
     * Utility class private constructor.
     */
    private StreamBackpressureExample() { }

    /**
     * Server.
     */
    private static class HelloStreamServerImpl {

        private final ServerBuilder<?> serverBuilder;
        private final CountDownLatch shutdownLatch;
        private final int minProcessingTime;
        private final int maxProcessingTime;
        private final Random random = new Random();
        private Server server;



        HelloStreamServerImpl(
            final int port,
            int minProcessingTime, int maxProcessingTime) {
            this.minProcessingTime = minProcessingTime;
            this.maxProcessingTime = maxProcessingTime;
            this.shutdownLatch = new CountDownLatch(1);
            this.serverBuilder = ServerBuilder.forPort(port)
                .intercept(new SimpleLoggingInterceptor())
                .addService(new FileChunkServiceImpl(this));
        }

        void start() throws IOException {
            final long latchValue = shutdownLatch.getCount();
            if (latchValue != 1) {
                throw new RuntimeException("Unexpected shutdown latch value: " + latchValue);
            }
            server = serverBuilder.build();
            server.start();
        }

        private void processChunk(FileChunkRequest fileChunkRequest) {
            log.info("Processing chunk");
            try {
                Thread.sleep(random.nextInt(maxProcessingTime - minProcessingTime) + minProcessingTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted during request processing");
            }
            log.info("Done processing chunk");
        }

        void shutdown() {
            server.shutdown();
            shutdownLatch.countDown();
        }

        void awaitShutdown() throws InterruptedException {
            shutdownLatch.await();
        }

        private class FileChunkServiceImpl extends FileStreamServiceGrpc.FileStreamServiceImplBase {
            private final HelloStreamServerImpl helloStreamServer;

            FileChunkServiceImpl(HelloStreamServerImpl helloStreamServer) {
                super();
                this.helloStreamServer = helloStreamServer;
            }

            @Override
            public StreamObserver<FileChunkRequest> streamFileChunks(StreamObserver<FileChunkResponse> responseObserver) {
                log.info("Stream begins");
                return new ServerRequestObserver(helloStreamServer, responseObserver);
            }

            private class ServerRequestObserver implements StreamObserver<FileChunkRequest> {
                private final ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(1);
                private final AtomicInteger processedFileChunksCount = new AtomicInteger(0);
                private final HelloStreamServerImpl helloStreamServer;
                private final StreamObserver<FileChunkResponse> responseObserver;
                private int watermark = 0;
                private AtomicBoolean streamDone = new AtomicBoolean(false);

                private ServerRequestObserver(HelloStreamServerImpl helloStreamServer, StreamObserver<FileChunkResponse> responseObserver) {
                    this.helloStreamServer = helloStreamServer;
                    this.responseObserver = responseObserver;
                    scheduledExecutorService.scheduleWithFixedDelay(this::sendChunkResponse, 1, 1, TimeUnit.SECONDS);
                    scheduledExecutorService.scheduleWithFixedDelay(this::closeStream, 1, 1, TimeUnit.SECONDS);
                }

                private void sendChunkResponse() {
                    final int currentWatermark = processedFileChunksCount.get();
                    if (currentWatermark > watermark) {
                        log.info("Acknowledging {} chunks", currentWatermark);
                        responseObserver.onNext(FileChunkResponse.newBuilder().setWatermark(currentWatermark).build());
                        watermark = currentWatermark;
                    }
                }

                private void closeStream() {
                    if (streamDone.get()) {
                        log.info("Shutting down stream");
                        responseObserver.onCompleted();
                        scheduledExecutorService.shutdown();
                    }
                }

                @Override
                public void onNext(FileChunkRequest fileChunkRequest) {
                    helloStreamServer.processChunk(fileChunkRequest);
                    processedFileChunksCount.incrementAndGet();
                }

                @Override
                public void onError(Throwable t) {
                    log.error("Request stream interrupted", t);
                }

                @Override
                public void onCompleted() {
                    log.info("Stream end");
                    streamDone.set(true);
                }
            }
        }
    }

    /**
     * Client.
     */
    private static class HelloStreamClientImpl {
        private final FileStreamServiceGrpc.FileStreamServiceStub serviceStub;
        private final StreamObserver<FileChunkResponse> responseObserver;
        private final CountDownLatch finishLatch;
        private final AtomicInteger watermark;
        private final FileChunkProducer producer;
        private Throwable streamingException;

        HelloStreamClientImpl(
            final String host,
            final int port,
            final FileChunkProducer producer
        ) {
            this.producer = producer;
            final ManagedChannel channel = ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext(true) // Disable TLS
                .build();
            this.serviceStub = FileStreamServiceGrpc.newStub(channel);
            this.finishLatch = new CountDownLatch(1);
            this.watermark = new AtomicInteger(0);
            this.responseObserver = new ClientResponseObserver();
        }

        void run() throws InterruptedException {

            StreamObserver<FileChunkRequest> requestObserver = serviceStub
                //.withWaitForReady() // Waits for connections indefinitely
                .streamFileChunks(responseObserver);

            try {
                int chunksSent = 0;
                while (true) {
                    log.info("Chunks sent so far: {}", chunksSent);

                    // Send as many chunks as we can now
                    while (chunksSent - watermark.get() < CLIENT_TRANSMISSION_WINDOW) {

                        // All chunks produced and consumed
                        if (!producer.hasNext()) {
                            break;
                        }

                        requestObserver.onNext(producer.getNextChunk());
                        ++chunksSent;
                    }

                    // Break out if all chunks were consumed
                    if (!producer.hasNext()) {
                        log.info("All chunks consumed");
                        break;
                    }

                    // Otherwise pause for a moment, since transmission window is full
                    synchronized (watermark) {
                        watermark.wait(1000);
                    }
                }

                while (chunksSent > watermark.get()) {
                    log.info(
                        "Waiting acknowledgement for last {} chunks",
                        chunksSent - watermark.get()
                    );
                    synchronized (watermark) {
                        watermark.wait(1000);
                    }
                }
            } catch (RuntimeException e) {
                // Cancel RPC
                requestObserver.onError(e);
                throw e;
            }
            // Mark the end of requests
            requestObserver.onCompleted();

            finishLatch.await();

            if (streamingException != null) {
                throw new RuntimeException("Failed to stream", streamingException);
            }
        }

        private class ClientResponseObserver implements StreamObserver<FileChunkResponse> {
            @Override
            public void onNext(FileChunkResponse fileChunkResponse) {
                final int currentWatermark = watermark.get();
                final int newWatermark = fileChunkResponse.getWatermark();
                log.info("Updating watermark {} -> {}", currentWatermark, newWatermark);
                if (newWatermark <= currentWatermark) {
                    log.error("Unexpected watermark {} below existing watermark {}",
                        newWatermark,
                        currentWatermark);
                    throw new RuntimeException("Unexpected watermark");
                }
                watermark.set(newWatermark);
                synchronized (watermark) {
                    watermark.notify();
                }
            }

            @Override
            public void onError(Throwable t) {
                log.error("Streaming failed", t);
                streamingException = t;
                finishLatch.countDown();
            }

            @Override
            public void onCompleted() {
                log.info("Received completed");
                finishLatch.countDown();
            }
        }
    }

    private static class FileChunkProducer {
        private final AtomicBoolean allChunksEnqueued;
        private final LinkedBlockingQueue<FileChunkRequest> chunksQueue;

        FileChunkProducer(final int clientChunksQueueSize) {
            this.chunksQueue = new LinkedBlockingQueue<>(clientChunksQueueSize);
            this.allChunksEnqueued = new AtomicBoolean(false);
        }

        void produceChunks(final int numChunks) {

            log.info("Starting to produce chunks ({})", numChunks);

            new Thread(() -> {
                for (int i = 0; i < numChunks; i++) {
                    try {
                        chunksQueue.put(
                            FileChunkRequest.newBuilder()
                                .setPath("/tmp/" + UUID.randomUUID().toString())
                                .build()
                        );
                        log.info("Enqeued chunk {}/{}", i+1, numChunks);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        log.error("Interrupted while enqueuing chunk");
                    }
                }
                log.info("All chunks enqueued");
                allChunksEnqueued.set(true);
            }).start();
        }

        boolean hasNext() {
            return !allChunksEnqueued.get() || !chunksQueue.isEmpty();
        }

        FileChunkRequest getNextChunk() throws InterruptedException {
            return chunksQueue.take();
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

            final int numRequests;

            if (args.length >= 4) {
                numRequests = Integer.parseInt(args[3]);
            } else {
                numRequests = DEFAULT_NUM_REQUESTS;
            }

            final FileChunkProducer producer = new FileChunkProducer(CLIENT_CHUNKS_QUEUE_SIZE);

            producer.produceChunks(numRequests);

            new HelloStreamClientImpl(host, port, producer).run();

        } else if ("server".equals(role)) {

            final int minProcessingTime;
            if (args.length >= 3) {
                minProcessingTime = Integer.parseInt(args[2]);
            } else {
                minProcessingTime = DEFAULT_MIN_PROCESSING_TIME;
            }

            final int maxProcessingTime;

            if (args.length >= 4) {
                maxProcessingTime = Integer.parseInt(args[3]);
            } else {
                maxProcessingTime = DEFAULT_MAX_PROCESSING_TIME;
            }

            final HelloStreamServerImpl server = new HelloStreamServerImpl(
                port,
                minProcessingTime,
                maxProcessingTime
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
            + "  " + StreamBackpressureExample.class.getSimpleName()
            + " server <port> [minProcessingTime] [maxProcessingTime]\n"
            + "  " + StreamBackpressureExample.class.getSimpleName()
            + " client <port> [host] [numRequests]" + "\n";

        System.out.println(usage);
    }
}
