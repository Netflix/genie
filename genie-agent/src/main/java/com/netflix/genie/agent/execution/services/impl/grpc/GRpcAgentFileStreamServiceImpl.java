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

import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.netflix.genie.agent.execution.services.AgentFileStreamService;
import com.netflix.genie.common.internal.dto.v4.converters.JobDirectoryManifestProtoConverter;
import com.netflix.genie.common.internal.exceptions.GenieConversionException;
import com.netflix.genie.common.internal.services.JobDirectoryManifestService;
import com.netflix.genie.common.internal.util.ExponentialBackOffTrigger;
import com.netflix.genie.proto.AgentFileMessage;
import com.netflix.genie.proto.AgentManifestMessage;
import com.netflix.genie.proto.FileStreamServiceGrpc;
import com.netflix.genie.proto.ServerAckMessage;
import com.netflix.genie.proto.ServerControlMessage;
import com.netflix.genie.proto.ServerFileRequestMessage;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.TaskScheduler;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ConcurrentModificationException;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implementation of {@link AgentFileStreamService} over gRPC.
 * Sets up a persistent 2-way stream ('sync') to push manifest updates and receive file requests.
 * When a file request is received, a creates a new 2 way stream ('transmit') and pushes file chunks, waits for ACK,
 * sends the next chunk, ... until the file range requested is transmitted. Then the stream is shut down.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class GRpcAgentFileStreamServiceImpl implements AgentFileStreamService {
    private static final boolean ENABLE_COMPRESSION = true; //TODO make configurable
    private static final int MAX_DATA_CHUNK_SIZE = 1024 * 1024; //TODO make configurable
    private static final int MAX_CONCURRENT_TRANSMIT_STREAMS = 5;  //TODO make configurable

    private final FileStreamServiceGrpc.FileStreamServiceStub fileStreamServiceStub;
    private final TaskScheduler taskScheduler;
    private final ExponentialBackOffTrigger trigger;
    private final JobDirectoryManifestProtoConverter manifestProtoConverter;
    private final StreamObserver<ServerControlMessage> responseObserver;
    private final Semaphore concurrentTransfersSemaphore;
    private final Set<FileTransfer> activeFileTransfers;
    private final JobDirectoryManifestService jobDirectoryManifestService;

    private StreamObserver<AgentManifestMessage> controlStreamObserver;
    private String jobId;
    private Path jobDirectoryPath;
    private AtomicBoolean started = new AtomicBoolean();
    private ScheduledFuture<?> scheduledTask;


    GRpcAgentFileStreamServiceImpl(
        final FileStreamServiceGrpc.FileStreamServiceStub fileStreamServiceStub,
        final TaskScheduler taskScheduler,
        final JobDirectoryManifestProtoConverter manifestProtoConverter,
        final JobDirectoryManifestService jobDirectoryManifestService
    ) {
        this.fileStreamServiceStub = fileStreamServiceStub;
        this.taskScheduler = taskScheduler;
        this.manifestProtoConverter = manifestProtoConverter;
        this.jobDirectoryManifestService = jobDirectoryManifestService;
        this.trigger = new ExponentialBackOffTrigger(
            ExponentialBackOffTrigger.DelayType.FROM_PREVIOUS_EXECUTION_BEGIN,
            1000, //TODO make configurable
            10000, //TODO make configurable
            1.1f //TODO make configurable
        );
        this.responseObserver = new ServerControlStreamObserver(this);
        this.concurrentTransfersSemaphore = new Semaphore(MAX_CONCURRENT_TRANSMIT_STREAMS);
        this.activeFileTransfers = Sets.newConcurrentHashSet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void start(final String claimedJobId, final Path jobDirectoryRoot) {
        //Service can be started only once
        if (!this.started.compareAndSet(false, true)) {
            throw new IllegalStateException("Service can be started only once");
        }
        this.jobId = claimedJobId;
        this.jobDirectoryPath = jobDirectoryRoot;
        this.scheduledTask = this.taskScheduler.schedule(this::pushManifest, trigger);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void stop() {

        if (this.started.compareAndSet(true, false)) {
            this.scheduledTask.cancel(false);
            this.scheduledTask = null;
            this.discardCurrentStream(true);
            while (!this.activeFileTransfers.isEmpty()) {
                try {
                    final FileTransfer fileTransfer = this.activeFileTransfers.iterator().next();
                    if (this.activeFileTransfers.remove(fileTransfer)) {
                        fileTransfer.completeTransfer(true, new InterruptedException("Shutting down"));
                    }
                } catch (NoSuchElementException | ConcurrentModificationException e) {
                    // Swallow. Not unexpected, collection and state may change.
                }
            }
        }
    }

    private synchronized void pushManifest() {
        if (started.get()) {
            final AgentManifestMessage jobFileManifest;
            try {
                jobFileManifest = manifestProtoConverter.manifestToProtoMessage(
                    this.jobId,
                    this.jobDirectoryManifestService.getDirectoryManifest(this.jobDirectoryPath)
                );
            } catch (final IOException e) {
                log.error("Failed to construct manifest", e);
                return;
            } catch (GenieConversionException e) {
                log.error("Failed to serialize manifest", e);
                return;
            }

            if (this.controlStreamObserver == null) {
                this.controlStreamObserver = fileStreamServiceStub.sync(this.responseObserver);
                if (this.controlStreamObserver instanceof ClientCallStreamObserver) {
                    ((ClientCallStreamObserver) this.controlStreamObserver).setMessageCompression(ENABLE_COMPRESSION);
                }
            }

            this.controlStreamObserver.onNext(jobFileManifest);
        }
    }

    private void handleControlStreamError(final Throwable t) {
        log.warn("Manifest stream error: {}", t.getMessage(), t);
        this.trigger.reset();
        this.discardCurrentStream(false);
    }

    private void handleControlStreamCompletion() {
        log.debug("Manifest stream completed");
        this.discardCurrentStream(false);
    }

    private synchronized void discardCurrentStream(final boolean sendStreamCompletion) {
        if (this.controlStreamObserver != null) {
            if (sendStreamCompletion) {
                this.controlStreamObserver.onCompleted();
            }
            this.controlStreamObserver = null;
        }
    }

    private synchronized void handleFileRequest(
        final String streamId,
        final String relativePath,
        final int startOffset,
        final int endOffset
    ) {
        log.info(
            "Server is requesting file {} (range: [{}, {}), streamId: {})",
            relativePath,
            startOffset,
            endOffset,
            streamId
        );

        if (!this.started.get()) {
            log.warn("Ignoring file request, service shutting down");
            return;
        }

        final Path absolutePath = this.jobDirectoryPath.resolve(relativePath);

        if (!Files.exists(absolutePath)) {
            log.warn("Ignoring request for a file that does not exist: {}", absolutePath);
            return;
        }

        final boolean permitAcquired = this.concurrentTransfersSemaphore.tryAcquire();

        if (!permitAcquired) {
            log.warn("Ignoring file request, too many transfers already in progress");
            return;
        }

        final FileTransfer fileTransfer = new FileTransfer(this, streamId, absolutePath, startOffset, endOffset);
        this.activeFileTransfers.add(fileTransfer);
        fileTransfer.start();
    }

    private void handleTransferComplete(final FileTransfer fileTransfer) {
        this.activeFileTransfers.remove(fileTransfer);
        this.concurrentTransfersSemaphore.release();
    }

    private static class ServerControlStreamObserver implements StreamObserver<ServerControlMessage> {
        private final GRpcAgentFileStreamServiceImpl gRpcAgentFileManifestService;

        ServerControlStreamObserver(final GRpcAgentFileStreamServiceImpl gRpcAgentFileManifestService) {
            this.gRpcAgentFileManifestService = gRpcAgentFileManifestService;
        }

        @Override
        public void onNext(final ServerControlMessage value) {
            if (value.getMessageCase() == ServerControlMessage.MessageCase.SERVER_FILE_REQUEST) {
                final ServerFileRequestMessage fileRequest = value.getServerFileRequest();
                this.gRpcAgentFileManifestService.handleFileRequest(
                    fileRequest.getStreamId(),
                    fileRequest.getRelativePath(),
                    fileRequest.getStartOffset(),
                    fileRequest.getEndOffset()
                );
            } else {
                log.warn("Unknown message type: " + value.getMessageCase().name());
            }
        }

        @Override
        public void onError(final Throwable t) {
            this.gRpcAgentFileManifestService.handleControlStreamError(t);
        }

        @Override
        public void onCompleted() {
            this.gRpcAgentFileManifestService.handleControlStreamCompletion();
        }
    }

    private static class FileTransfer implements StreamObserver<ServerAckMessage> {
        private final GRpcAgentFileStreamServiceImpl gRpcAgentFileStreamService;
        private final String streamId;
        private final Path absolutePath;
        private final int startOffset;
        private final int endOffset;
        private final StreamObserver<AgentFileMessage> outboundStreamObserver;
        private final ByteBuffer readBuffer;
        private final AtomicBoolean completed = new AtomicBoolean();
        private int watermark;

        FileTransfer(
            final GRpcAgentFileStreamServiceImpl gRpcAgentFileStreamService,
            final String streamId,
            final Path absolutePath,
            final int startOffset,
            final int endOffset
        ) {
            this.gRpcAgentFileStreamService = gRpcAgentFileStreamService;
            this.streamId = streamId;
            this.absolutePath = absolutePath;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.outboundStreamObserver = this.gRpcAgentFileStreamService.fileStreamServiceStub.transmit(this);
            this.watermark = startOffset;
            this.readBuffer = ByteBuffer.allocate(GRpcAgentFileStreamServiceImpl.MAX_DATA_CHUNK_SIZE);
        }

        void start() {
            try {
                this.sendChunk();
            } catch (IOException e) {
                log.warn("Failed to send first chunk");
                this.completeTransfer(true, e);
            }
        }

        private void completeTransfer(final boolean shutdownStream, @Nullable final Exception error) {
            if (this.completed.compareAndSet(false, true)) {
                if (shutdownStream) {
                    if (error != null) {
                        this.outboundStreamObserver.onError(error);
                    } else {
                        this.outboundStreamObserver.onCompleted();
                    }
                }

                this.gRpcAgentFileStreamService.handleTransferComplete(this);
            }
        }

        private void sendChunk() throws IOException {

            if (this.watermark < this.endOffset - 1) {
                final int bytesRead;
                try (FileChannel channel = FileChannel.open(this.absolutePath, StandardOpenOption.READ)) {
                    channel.position(this.watermark);
                    bytesRead = channel.read(readBuffer);
                }

                // Reset mark before reading!
                readBuffer.rewind();

                final AgentFileMessage chunkMessage = AgentFileMessage.newBuilder()
                    .setStreamId(this.streamId)
                    .setData(ByteString.copyFrom(readBuffer, bytesRead))
                    .build();

                this.outboundStreamObserver.onNext(chunkMessage);

                this.watermark += bytesRead;
            } else {
                this.completeTransfer(true, null);
            }
        }

        @Override
        public void onNext(final ServerAckMessage value) {
            try {
                sendChunk();
            } catch (IOException e) {
                log.warn("Failed to send chunk");
                this.completeTransfer(true, e);
            }
        }

        @Override
        public void onError(final Throwable t) {
            log.warn("Stream error: {} : {}", t.getClass().getSimpleName(), t.getMessage());
            this.completeTransfer(false, null);
        }

        @Override
        public void onCompleted() {
            log.debug("Stream completed");
            this.completeTransfer(false, null);
        }
    }
}
