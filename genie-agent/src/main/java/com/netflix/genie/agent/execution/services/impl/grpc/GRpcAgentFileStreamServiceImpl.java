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
import com.netflix.genie.agent.properties.FileStreamServiceProperties;
import com.netflix.genie.common.internal.dtos.converters.JobDirectoryManifestProtoConverter;
import com.netflix.genie.common.internal.exceptions.checked.GenieConversionException;
import com.netflix.genie.common.internal.services.JobDirectoryManifestCreatorService;
import com.netflix.genie.common.internal.util.ExponentialBackOffTrigger;
import com.netflix.genie.proto.AgentFileMessage;
import com.netflix.genie.proto.AgentManifestMessage;
import com.netflix.genie.proto.FileStreamServiceGrpc;
import com.netflix.genie.proto.ServerAckMessage;
import com.netflix.genie.proto.ServerControlMessage;
import com.netflix.genie.proto.ServerFileRequestMessage;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.grpc.Context;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.TaskScheduler;

import jakarta.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ConcurrentModificationException;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * Implementation of {@link AgentFileStreamService} over gRPC.
 * Sets up a persistent 2-way stream ('sync') to push manifest updates and receive file requests.
 * When a file request is received, a creates a new 2 way stream ('transmit') and pushes file chunks, waits for ACK,
 * sends the next chunk, ... until the file range requested is transmitted. Then the stream is shut down.
 * This implementation periodically pushes manifests at a fixed interval, and also checks for directory changes
 * every 5 seconds, pushing immediately if changes are detected or if 30 seconds have passed since the last push.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class GRpcAgentFileStreamServiceImpl implements AgentFileStreamService {

    // Manifest push interval in seconds
    private static final int MANIFEST_PUSH_INTERVAL_SECONDS = 30;

    // Directory check interval in seconds
    private static final int DIRECTORY_CHECK_INTERVAL_SECONDS = 5;

    // Service dependencies
    private final FileStreamServiceGrpc.FileStreamServiceStub fileStreamServiceStub;
    private final TaskScheduler taskScheduler;
    private final ExponentialBackOffTrigger trigger;
    private final FileStreamServiceProperties properties;
    private final JobDirectoryManifestProtoConverter manifestProtoConverter;
    private final StreamObserver<ServerControlMessage> responseObserver;
    private final Semaphore concurrentTransfersSemaphore;
    private final Set<FileTransfer> activeFileTransfers;
    private final JobDirectoryManifestCreatorService jobDirectoryManifestCreatorService;
    private final int maxStreams;

    // Service state
    private StreamObserver<AgentManifestMessage> controlStreamObserver;
    private String jobId;
    private Path jobDirectoryPath;
    private final AtomicBoolean started = new AtomicBoolean();
    private ScheduledFuture<?> scheduledCheckTask;

    // Directory state tracking
    private Set<String> directoryEntries = Sets.newHashSet();
    private final AtomicLong lastManifestPushTime = new AtomicLong(0);

    // Monitoring counters
    private final AtomicInteger manifestPushCount = new AtomicInteger(0);

    /**
     * Constructor.
     *
     * @param fileStreamServiceStub              The gRPC stub for file streaming
     * @param taskScheduler                      The task scheduler for periodic operations
     * @param manifestProtoConverter             Converter for manifest proto messages
     * @param jobDirectoryManifestCreatorService Service to create directory manifests
     * @param properties                         Configuration properties
     */
    GRpcAgentFileStreamServiceImpl(
        final FileStreamServiceGrpc.FileStreamServiceStub fileStreamServiceStub,
        final TaskScheduler taskScheduler,
        final JobDirectoryManifestProtoConverter manifestProtoConverter,
        final JobDirectoryManifestCreatorService jobDirectoryManifestCreatorService,
        final FileStreamServiceProperties properties
    ) {
        this.fileStreamServiceStub = fileStreamServiceStub;
        this.taskScheduler = taskScheduler;
        this.manifestProtoConverter = manifestProtoConverter;
        this.jobDirectoryManifestCreatorService = jobDirectoryManifestCreatorService;
        this.properties = properties;

        this.trigger = new ExponentialBackOffTrigger(properties.getErrorBackOff());
        this.responseObserver = new ServerControlStreamObserver(this);
        this.maxStreams = properties.getMaxConcurrentStreams();
        this.concurrentTransfersSemaphore = new Semaphore(this.maxStreams);
        this.activeFileTransfers = Sets.newConcurrentHashSet();
    }

    /**
     * {@inheritDoc}
     * Start the file stream service for a specific job.
     */
    @Override
    public synchronized void start(final String claimedJobId, final Path jobDirectoryRoot) {
        // Service can be started only once
        if (!this.started.compareAndSet(false, true)) {
            throw new IllegalStateException("Service can be started only once");
        }
        this.jobId = claimedJobId;
        this.jobDirectoryPath = jobDirectoryRoot;

        // Schedule initial push immediately
        this.taskScheduler.schedule(this::pushManifest, Instant.now());
        this.lastManifestPushTime.set(System.currentTimeMillis());

        // Schedule periodic directory checks
        this.scheduledCheckTask = this.taskScheduler.scheduleAtFixedRate(
            this::checkDirectoryAndPushIfNeeded,
            Duration.ofSeconds(DIRECTORY_CHECK_INTERVAL_SECONDS)
        );

        log.debug("Started with periodic manifest push every {} seconds and directory checks every {} seconds",
            MANIFEST_PUSH_INTERVAL_SECONDS, DIRECTORY_CHECK_INTERVAL_SECONDS);
    }

    /**
     * {@inheritDoc}
     * Stop the file stream service and clean up resources.
     */
    @Override
    public synchronized void stop() {
        log.debug("Stopping");

        if (this.started.compareAndSet(true, false)) {
            if (!this.activeFileTransfers.isEmpty()) {
                this.drain();
            }
            if (this.scheduledCheckTask != null) {
                this.scheduledCheckTask.cancel(false);
                this.scheduledCheckTask = null;
            }
            this.discardCurrentStream(true);
            while (!this.activeFileTransfers.isEmpty()) {
                log.debug("{} transfers are still active after waiting", this.activeFileTransfers.size());
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
        log.debug("Stopped");
    }

    /**
     * {@inheritDoc}
     * Force an immediate manifest push to the server.
     */
    @Override
    public synchronized Optional<ScheduledFuture<?>> forceServerSync() {
        if (started.get()) {
            try {
                log.debug("Forcing a manifest refresh");
                this.jobDirectoryManifestCreatorService.invalidateCachedDirectoryManifest(jobDirectoryPath);

                // Schedule immediate push
                final ScheduledFuture<?> future = this.taskScheduler.schedule(
                    this::pushManifest,
                    Instant.now()
                );

                return Optional.of(future);
            } catch (Exception e) {
                log.error("Failed to force push a fresh manifest", e);
            }
        }
        return Optional.empty();
    }

    /**
     * Wait for all active transfers to complete or timeout.
     */
    private void drain() {
        final AtomicInteger acquiredPermitsCount = new AtomicInteger();
        // Task that attempts to acquire all available transfer permits.
        // This ensures no new transfers will be started.
        // If the task completes successfully, it also guarantees there are no in-progress transfers.
        final Runnable drainTask = () -> {
            while (acquiredPermitsCount.get() < this.maxStreams) {
                try {
                    if (this.concurrentTransfersSemaphore.tryAcquire(1, TimeUnit.SECONDS)) {
                        acquiredPermitsCount.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    log.warn("Interrupted while waiting for a permit");
                    break;
                }
            }
        };

        // Submit the task for immediate execution, but do not wait more than a fixed amount of time
        final ScheduledFuture<?> drainTaskFuture = taskScheduler.schedule(drainTask, Instant.now());
        try {
            drainTaskFuture.get(this.properties.getDrainTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.warn("Failed to wait for in-flight transfers to complete", e);
        }

        final int ongoingTransfers = this.maxStreams - acquiredPermitsCount.get();
        if (ongoingTransfers > 0) {
            log.warn("{} file transfers are still active after waiting for completion", ongoingTransfers);
        }
    }

    /**
     * Check for directory changes and push manifest if needed.
     */
    private synchronized void checkDirectoryAndPushIfNeeded() {
        if (!started.get()) {
            return;
        }

        try {
            final boolean controlStreamDisconnected = (this.controlStreamObserver == null);
            final boolean hasChanges = this.checkForDirectoryChanges();
            final long timeSinceLastPush = System.currentTimeMillis() - this.lastManifestPushTime.get();
            final boolean timeThresholdExceeded = timeSinceLastPush >= TimeUnit.SECONDS.toMillis(MANIFEST_PUSH_INTERVAL_SECONDS);

            if (controlStreamDisconnected) {
                log.warn("Control stream disconnected, immediately trying to reconnect and push manifest");
                pushManifest();
            } else if (hasChanges) {
                log.debug("Directory changes detected, pushing manifest");
                pushManifest();
            } else if (timeThresholdExceeded) {
                log.debug("Time threshold exceeded ({} seconds), pushing manifest", MANIFEST_PUSH_INTERVAL_SECONDS);
                pushManifest();
            }
        } catch (Exception e) {
            log.error("Error during directory check", e);
        }
    }

    /**
     * Check for changes in the directory structure.
     *
     * @return true if changes were detected, false otherwise
     * @throws IOException if there's an error accessing the file system
     */
    private boolean checkForDirectoryChanges() throws IOException {
        final Set<String> currentEntries = Sets.newHashSet();

        try (Stream<Path> pathStream = Files.walk(jobDirectoryPath)) {
            pathStream
                .filter(Files::exists)
                .forEach(path -> currentEntries.add(jobDirectoryPath.relativize(path).toString()));
        }

        // Check if files/directories were added or removed
        if (!currentEntries.equals(directoryEntries)) {
            directoryEntries = currentEntries;
            return true;
        }

        return false;
    }

    /**
     * Push the current job directory manifest to the server.
     */
    private synchronized void pushManifest() {
        if (started.get()) {
            final int pushAttempt = manifestPushCount.incrementAndGet();
            log.debug("Pushing manifest (attempt #{})", pushAttempt);

            final AgentManifestMessage jobFileManifest;
            try {
                jobFileManifest = manifestProtoConverter.manifestToProtoMessage(
                    this.jobId,
                    this.jobDirectoryManifestCreatorService.getDirectoryManifest(this.jobDirectoryPath)
                );
            } catch (final IOException e) {
                log.error("Failed to construct manifest", e);
                return;
            } catch (GenieConversionException e) {
                log.error("Failed to serialize manifest", e);
                return;
            }

            if (this.controlStreamObserver == null) {
                log.debug("Creating new control stream");
                this.controlStreamObserver = fileStreamServiceStub.sync(this.responseObserver);
                if (this.controlStreamObserver instanceof ClientCallStreamObserver) {
                    ((ClientCallStreamObserver<?>) this.controlStreamObserver)
                        .setMessageCompression(this.properties.isEnableCompression());
                }
            }

            log.debug("Sending manifest via control stream");
            this.controlStreamObserver.onNext(jobFileManifest);

            // Update last push time
            this.lastManifestPushTime.set(System.currentTimeMillis());
        }
    }

    /**
     * Handle errors on the control stream.
     *
     * @param t The error that occurred
     */
    private void handleControlStreamError(final Throwable t) {
        log.warn("Control stream error: {}", t.getMessage(), t);
        this.trigger.reset();
        this.discardCurrentStream(false);
    }

    /**
     * Handle normal completion of the control stream.
     */
    private void handleControlStreamCompletion() {
        log.debug("Control stream completed");
        this.discardCurrentStream(false);
    }

    /**
     * Discard the current control stream.
     *
     * @param sendStreamCompletion Whether to send a completion message before discarding
     */
    private synchronized void discardCurrentStream(final boolean sendStreamCompletion) {
        if (this.controlStreamObserver != null) {
            log.debug("Discarding current control stream");
            if (sendStreamCompletion) {
                log.debug("Sending control stream completion");
                this.controlStreamObserver.onCompleted();
            }
            this.controlStreamObserver = null;
        }
    }

    /**
     * Handle a file request from the server.
     *
     * @param streamId     The ID of the stream for this file transfer
     * @param relativePath The path of the requested file relative to job directory
     * @param startOffset  The starting byte offset
     * @param endOffset    The ending byte offset
     */
    private synchronized void handleFileRequest(
        final String streamId,
        final String relativePath,
        final long startOffset,
        final long endOffset
    ) {
        log.debug(
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

        // Decouple outgoing file transfer from incoming file request
        Context.current().run(
            () -> {
                final FileTransfer fileTransfer = new FileTransfer(
                    this,
                    streamId,
                    absolutePath,
                    startOffset,
                    endOffset,
                    properties.getDataChunkMaxSize().toBytes()
                );
                this.activeFileTransfers.add(fileTransfer);
                fileTransfer.start();
                log.debug("Created and started new file transfer: {}", fileTransfer.streamId);
            }
        );
    }

    /**
     * Handle completion of a file transfer.
     *
     * @param fileTransfer The completed file transfer
     */
    private void handleTransferComplete(final FileTransfer fileTransfer) {
        this.activeFileTransfers.remove(fileTransfer);
        this.concurrentTransfersSemaphore.release();
        log.debug("File transfer completed: {}", fileTransfer.streamId);
    }

    /**
     * Observer for server control messages.
     */
    private static class ServerControlStreamObserver implements StreamObserver<ServerControlMessage> {
        private final GRpcAgentFileStreamServiceImpl gRpcAgentFileManifestService;

        /**
         * Constructor.
         *
         * @param gRpcAgentFileManifestService The file stream service
         */
        ServerControlStreamObserver(final GRpcAgentFileStreamServiceImpl gRpcAgentFileManifestService) {
            this.gRpcAgentFileManifestService = gRpcAgentFileManifestService;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onNext(final ServerControlMessage value) {
            if (value.getMessageCase() == ServerControlMessage.MessageCase.SERVER_FILE_REQUEST) {
                log.debug("Received control stream file request");
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

        /**
         * {@inheritDoc}
         */
        @Override
        public void onError(final Throwable t) {
            log.debug("Received control stream error: {}", t.getClass().getSimpleName());
            this.gRpcAgentFileManifestService.handleControlStreamError(t);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onCompleted() {
            log.debug("Received control stream completion");
            this.gRpcAgentFileManifestService.handleControlStreamCompletion();
        }
    }

    /**
     * Represents an active file transfer.
     */
    private static class FileTransfer implements StreamObserver<ServerAckMessage> {
        private final GRpcAgentFileStreamServiceImpl gRpcAgentFileStreamService;
        private final String streamId;
        private final Path absolutePath;
        private final long startOffset;
        private final long endOffset;
        private final StreamObserver<AgentFileMessage> outboundStreamObserver;
        private final ByteBuffer readBuffer;
        private final AtomicBoolean completed = new AtomicBoolean();
        private long watermark;

        /**
         * Constructor.
         *
         * @param gRpcAgentFileStreamService The file stream service
         * @param streamId                   The ID for this transfer stream
         * @param absolutePath               The absolute path to the file
         * @param startOffset                The starting byte offset
         * @param endOffset                  The ending byte offset
         * @param maxChunkSize               The maximum size of each chunk
         */
        FileTransfer(
            final GRpcAgentFileStreamServiceImpl gRpcAgentFileStreamService,
            final String streamId,
            final Path absolutePath,
            final long startOffset,
            final long endOffset,
            final long maxChunkSize
        ) {
            this.gRpcAgentFileStreamService = gRpcAgentFileStreamService;
            this.streamId = streamId;
            this.absolutePath = absolutePath;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.outboundStreamObserver = this.gRpcAgentFileStreamService.fileStreamServiceStub.transmit(this);
            this.watermark = startOffset;
            this.readBuffer = ByteBuffer.allocate(Math.toIntExact(maxChunkSize));
            log.debug(
                "Created new FileTransfer: {} (path: {} range: {}-{})",
                streamId,
                absolutePath,
                startOffset,
                endOffset
            );
        }

        /**
         * Start the file transfer.
         */
        void start() {
            log.debug("Starting file transfer: {}", streamId);
            try {
                this.sendChunk();
            } catch (IOException e) {
                log.warn("Failed to send first chunk");
                this.completeTransfer(true, e);
            }
        }

        /**
         * Complete the file transfer.
         *
         * @param shutdownStream Whether to shut down the stream
         * @param error          The error that occurred, if any
         */
        private void completeTransfer(final boolean shutdownStream, @Nullable final Exception error) {
            if (this.completed.compareAndSet(false, true)) {
                log.debug(
                    "Completing transfer: {} (shutdown: {}, error: {})",
                    streamId,
                    shutdownStream,
                    error != null
                );

                if (shutdownStream) {
                    if (error != null) {
                        log.debug("Terminating transfer stream {} with error", streamId);
                        this.outboundStreamObserver.onError(error);
                    } else {
                        log.debug("Terminating transfer stream {} without error", streamId);
                        this.outboundStreamObserver.onCompleted();
                    }
                }

                this.gRpcAgentFileStreamService.handleTransferComplete(this);
            }
        }

        /**
         * Send the next chunk of the file.
         *
         * @throws IOException If there's an error reading the file
         */
        @SuppressFBWarnings(
            value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE",
            justification = "https://github.com/spotbugs/spotbugs/issues/756"
        )
        private void sendChunk() throws IOException {
            if (this.watermark < this.endOffset - 1) {
                // Reset mark before reading into the buffer
                readBuffer.rewind();

                final int bytesRead;
                try (FileChannel channel = FileChannel.open(this.absolutePath, StandardOpenOption.READ)) {
                    channel.position(this.watermark);
                    bytesRead = channel.read(readBuffer);
                }

                // Reset mark again before copying data out
                readBuffer.rewind();

                final AgentFileMessage chunkMessage = AgentFileMessage.newBuilder()
                    .setStreamId(this.streamId)
                    .setData(ByteString.copyFrom(readBuffer, bytesRead))
                    .build();

                log.debug("Sending next chunk in stream {} ({} bytes)", streamId, bytesRead);

                this.outboundStreamObserver.onNext(chunkMessage);

                this.watermark += bytesRead;
            } else {
                log.debug("All data transmitted");
                this.completeTransfer(true, null);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onNext(final ServerAckMessage value) {
            log.debug("Received chunk acknowledgement");
            try {
                sendChunk();
            } catch (IOException e) {
                log.warn("Failed to send chunk");
                this.completeTransfer(true, e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onError(final Throwable t) {
            log.warn("Stream error: {} : {}", t.getClass().getSimpleName(), t.getMessage());
            this.completeTransfer(false, null);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onCompleted() {
            log.debug("Stream completed");
            this.completeTransfer(false, null);
        }
    }
}
