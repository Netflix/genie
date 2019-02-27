/*
 *
 *  Copyright 2019 Netflix, Inc.
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

package com.netflix.genie.agent.execution.services.impl.grpc;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.netflix.genie.agent.execution.services.AgentFileStreamingService;
import com.netflix.genie.common.exceptions.GenieBadRequestException;
import com.netflix.genie.proto.AgentJobFileMessage;
import com.netflix.genie.proto.FileStreamAck;
import com.netflix.genie.proto.FileStreamChunk;
import com.netflix.genie.proto.FileStreamRegistration;
import com.netflix.genie.proto.FileStreamRequest;
import com.netflix.genie.proto.JobFileServiceGrpc;
import com.netflix.genie.proto.ServerJobFileMessage;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.TaskScheduler;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;

/**
 * Implementation of {@link AgentFileStreamingService} based on gRPC.
 * This service starts by opening one or more bi-directional streaming channels. With the first message the agent
 * identifies itself and the job being executed.
 * The server parks this channels for later use.
 * When a request comes in, the server takes an open channel and requests a file.
 * The agent then starts sending file chunks and waits for server acknowledgements of the same.
 * Once the entire file is transmitted, the channel is shut down.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class GRpcAgentFileStreamingServiceImpl implements AgentFileStreamingService {
    private static final long MAX_CHUNK_SIZE = 1024 * 1024; //TODO externalize
    private static final int MAX_STREAMS = 5; //TODO externalize
    private static final int READY_STREAMS_POOL = 3; //TODO externalize
    private static final long GRPC_RETRY_DELAY_MILLIS = 3000; //TODO externalize
    private static final boolean ENABLE_COMPRESSION = true; //TODO externalize

    private final JobFileServiceGrpc.JobFileServiceStub jobFileServiceStub;
    private final TaskScheduler taskScheduler;
    private final Map<String, ReadyStream> readyStreamsMap = Maps.newHashMap();
    private final Map<String, ActiveStream> activeStreamsMap = Maps.newHashMap();

    private boolean started;
    private AgentJobFileMessage registrationMessage;
    private Path jobDirectoryPath;
    private ScheduledFuture<?> delayedRetryTask;

    GRpcAgentFileStreamingServiceImpl(
        final JobFileServiceGrpc.JobFileServiceStub jobFileServiceStub,
        final TaskScheduler taskScheduler
    ) {
        this.jobFileServiceStub = jobFileServiceStub;
        this.taskScheduler = taskScheduler;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void start(final String claimedJobId, final Path jobDirectoryRoot) {
        //Service can be started only once
        if (this.started) {
            throw new IllegalStateException("Service can be started only once");
        }

        this.started = true;

        this.jobDirectoryPath = jobDirectoryRoot;
        this.registrationMessage = AgentJobFileMessage.newBuilder()
            .setFileStreamRegistration(
                FileStreamRegistration.newBuilder()
                    .setJobId(claimedJobId)
                    .build()
            )
            .build();

        this.initializeStreams();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void stop() {
        this.started = false;
        for (final ReadyStream readyStream : this.readyStreamsMap.values()) {
            readyStream.requestStreamObserver.onCompleted();
        }
        this.readyStreamsMap.clear();

        //TODO close active streams, maybe waiting for them to complete.
    }

    private synchronized void initializeStreams() {

        while (this.started
            && readyStreamsMap.size() < READY_STREAMS_POOL
            && readyStreamsMap.size() + activeStreamsMap.size() < MAX_STREAMS) {

            final String streamId = UUID.randomUUID().toString();
            log.info("New stream stream: {}", streamId);
            final ResponseStreamObserver responseStreamObserver = new ResponseStreamObserver(this, streamId);
            final StreamObserver<AgentJobFileMessage> requestStreamObserver =
                this.jobFileServiceStub.streamFile(responseStreamObserver);
            if (requestStreamObserver instanceof ClientCallStreamObserver) {
                ((ClientCallStreamObserver) requestStreamObserver).setMessageCompression(ENABLE_COMPRESSION);
            }

            requestStreamObserver.onNext(registrationMessage);

            this.readyStreamsMap.put(
                streamId,
                new ReadyStream(requestStreamObserver, responseStreamObserver)
            );
        }
    }

    private synchronized void scheduleInitializeStreams() {
        // Schedule only if not already scheduled
        if (this.started && (this.delayedRetryTask == null || this.delayedRetryTask.isDone())) {
            this.delayedRetryTask = this.taskScheduler.schedule(
                this::initializeStreams,
                Instant.now().plusMillis(GRPC_RETRY_DELAY_MILLIS)
            );
            log.debug("Scheduled stream initialization");
        }
    }

    private synchronized void handleFileRequest(final String streamId, final FileStreamRequest fileStreamRequest) {

        final String requestedPath = fileStreamRequest.getRelativePath();
        final Path filePath = this.jobDirectoryPath.resolve(fileStreamRequest.getRelativePath());
        final ReadyStream readyStream = this.readyStreamsMap.get(streamId);

        try {
            if (readyStream == null) {
                log.warn("Server requesting file: {}, but stream is not in 'ready' state", requestedPath);
                throw new IllegalArgumentException("Stream is not in 'ready' state: " + streamId);
            } else if (!Files.exists(filePath)) {
                log.warn("Server requesting file: {}, but it doesn't exist", requestedPath);
                throw new GenieBadRequestException("No such file: " + filePath);
            } else if (!Files.isRegularFile(filePath)) {
                log.warn("Server requesting file: {}, but it's not a file", requestedPath);
                throw new GenieBadRequestException("Not a file file: " + filePath);
            }

            log.info("Activating stream: {} to transmit file: {}", streamId, filePath.toAbsolutePath().toString());

            final ActiveStream activeStream = readyStream.beginFileStreaming(
                filePath,
                fileStreamRequest.getStartOffset(),
                fileStreamRequest.getEndOffset()
            );

            this.readyStreamsMap.remove(streamId);
            this.activeStreamsMap.put(streamId, activeStream);

        } catch (final Throwable e) {
            handleStreamError(streamId, e, true);
        }

        this.initializeStreams();
    }

    private synchronized void handleAck(final String streamId, final FileStreamAck fileStreamAck) {
        final ActiveStream activeStream = this.activeStreamsMap.get(streamId);

        log.info("Received ACK for stream: {}", streamId);

        try {
            if (activeStream == null) {
                log.warn("Received ACK for unknown stream: {}", streamId);
                throw new IllegalStateException("Received ACK for unknown stream: " + streamId);
            }

            if (activeStream.allDataSent()) {
                log.info("Transfer complete for stream: {}", streamId);
                this.activeStreamsMap.remove(streamId);
                activeStream.sendCompletion();

            } else {
                log.info("Continuing transmission stream: {}", streamId);
                activeStream.sendFileChunk();
            }

        } catch (final Throwable t) {
            handleStreamError(streamId, t, true);
        }

        this.initializeStreams();
    }

    private synchronized void handleStreamCompletion(final String streamId) {
        if (this.readyStreamsMap.containsKey(streamId)) {
            log.info("Completing stream: {} (ready)", streamId);
            this.readyStreamsMap.remove(streamId).sendCompletion();
        } else if (this.activeStreamsMap.containsKey(streamId)) {
            log.info("Completing stream: {} (active)", streamId);
            this.activeStreamsMap.remove(streamId).sendCompletion();
        } else {
            log.info("Completing stream: {} (unknown)", streamId);
        }
        this.initializeStreams();
    }

    private synchronized void handleStreamError(
        final String streamId,
        final Throwable throwable,
        final boolean sendError
    ) {
        log.warn(
            "Closing stream: {} due to error: {}: {}",
            streamId,
            throwable.getClass().getSimpleName(),
            throwable.getMessage()
        );
        if (this.readyStreamsMap.containsKey(streamId)) {
            final ReadyStream readyStream = this.readyStreamsMap.remove(streamId);
            if (readyStream != null && sendError) {
                readyStream.sendError(throwable);
            }
        } else if (this.activeStreamsMap.containsKey(streamId)) {
            final ActiveStream activeStream = this.activeStreamsMap.remove(streamId);
            if (activeStream != null && sendError) {
                activeStream.sendError(throwable);
            }
        }

        if (throwable instanceof StatusRuntimeException
            && ((StatusRuntimeException) throwable).getStatus().getCode() == Status.UNAVAILABLE.getCode()) {
            // Special case, gRPC channel is unavailable (i.e. cannot connect to server).
            // If new streams are opened, they will immediately fail with the same error, leading to thousands of
            // attempts and errors per second.
            // Instead, "slow" down and retry in a little bit.
            log.warn("gRPC unavailable, delay attempt to re-open streams");
            this.scheduleInitializeStreams();
        } else {
            this.initializeStreams();
        }
    }

    private static class ResponseStreamObserver implements StreamObserver<ServerJobFileMessage> {
        private final GRpcAgentFileStreamingServiceImpl gRpcAgentFileStreamingService;
        private final String streamId;

        ResponseStreamObserver(
            final GRpcAgentFileStreamingServiceImpl gRpcAgentFileStreamingService,
            final String streamId
        ) {
            this.gRpcAgentFileStreamingService = gRpcAgentFileStreamingService;
            this.streamId = streamId;
        }

        @Override
        public void onNext(final ServerJobFileMessage value) {
            switch (value.getMessageCase()) {
                case FILE_STREAM_REQUEST:
                    this.gRpcAgentFileStreamingService.handleFileRequest(streamId, value.getFileStreamRequest());
                    break;
                case FILE_STREAM_ACK:
                    this.gRpcAgentFileStreamingService.handleAck(streamId, value.getFileStreamAck());
                    break;
                default:
                    this.gRpcAgentFileStreamingService.handleStreamError(
                        streamId,
                        new IllegalArgumentException("Unknown message type: " + value.getMessageCase().name()),
                        true
                    );
            }
        }

        @Override
        public void onError(final Throwable t) {
            this.gRpcAgentFileStreamingService.handleStreamError(streamId, t, false);
        }

        @Override
        public void onCompleted() {
            this.gRpcAgentFileStreamingService.handleStreamCompletion(streamId);
        }
    }

    private static class ReadyStream {
        private final StreamObserver<AgentJobFileMessage> requestStreamObserver;
        private final StreamObserver<ServerJobFileMessage> responseStreamObserver;

        ReadyStream(
            final StreamObserver<AgentJobFileMessage> requestStreamObserver,
            final StreamObserver<ServerJobFileMessage> responseStreamObserver
        ) {
            this.responseStreamObserver = responseStreamObserver;
            this.requestStreamObserver = requestStreamObserver;
        }

        ActiveStream beginFileStreaming(
            final Path filePath,
            final long startOffset,
            final long endOffset
        ) throws IOException {
            return new ActiveStream(
                this.requestStreamObserver,
                this.responseStreamObserver,
                filePath,
                startOffset,
                endOffset);
        }

        void sendCompletion() {
            this.requestStreamObserver.onCompleted();
        }

        void sendError(final Throwable throwable) {
            this.requestStreamObserver.onError(throwable);
        }
    }

    private static class ActiveStream {
        private final StreamObserver<AgentJobFileMessage> requestStreamObserver;
        private final StreamObserver<ServerJobFileMessage> responseStreamObserver;
        private final Path filePath;
        private final long startOffset;
        private final long endOffset;
        private long watermark;
        private final byte[] chunkBuffer = new byte[Math.toIntExact(MAX_CHUNK_SIZE)];

        ActiveStream(
            final StreamObserver<AgentJobFileMessage> requestStreamObserver,
            final StreamObserver<ServerJobFileMessage> responseStreamObserver,
            final Path filePath,
            final long startOffset,
            final long endOffset
        ) throws IOException {
            this.requestStreamObserver = requestStreamObserver;
            this.responseStreamObserver = responseStreamObserver;
            this.filePath = filePath;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.watermark = startOffset;

            if (startOffset > endOffset) {
                throw new IllegalArgumentException("End offset is greater than start offset");
            }

            this.sendFileChunk();
        }

        private void sendFileChunk() throws IOException {
            final long bytesLeftToSend = this.endOffset - this.watermark;
            log.info("{} bytes left to send for file: {}", bytesLeftToSend, filePath.toAbsolutePath());

            final long chunkSize = Math.min(GRpcAgentFileStreamingServiceImpl.MAX_CHUNK_SIZE, bytesLeftToSend);

            final ByteString chunk;
            try (InputStream fileInputStream = Files.newInputStream(this.filePath)) {
                final long skipped = fileInputStream.skip(this.watermark);
                if (skipped < this.watermark) {
                    throw new IllegalStateException("Failed to skip ahead in file, probably truncated");
                }
                final int readSize = fileInputStream.read(this.chunkBuffer, 0, Math.toIntExact(chunkSize));

                if (readSize != chunkSize) {
                    throw new IOException(
                        "Could only read " + readSize + "/" + chunkSize + " "
                            + "bytes from " + this.filePath.toAbsolutePath()
                    );
                }

                chunk = ByteString.copyFrom(chunkBuffer, 0, Math.toIntExact(chunkSize));
            }
            log.info(
                "Sending chunk of {} bytes (offset: {}) for file: {}",
                chunk.size(),
                this.watermark,
                this.filePath.toAbsolutePath()
            );

            this.watermark += chunkSize;

            final AgentJobFileMessage chunkMessage = AgentJobFileMessage.newBuilder()
                .setFileStreamChunk(
                    FileStreamChunk.newBuilder()
                        .setFileChunk(chunk)
                        .build()
                )
                .build();

            this.requestStreamObserver.onNext(chunkMessage);
        }

        boolean allDataSent() {
            return this.watermark == this.endOffset;
        }

        void sendCompletion() {
            this.requestStreamObserver.onCompleted();
        }

        void sendError(final Throwable throwable) {
            this.requestStreamObserver.onError(throwable);
        }
    }
}
