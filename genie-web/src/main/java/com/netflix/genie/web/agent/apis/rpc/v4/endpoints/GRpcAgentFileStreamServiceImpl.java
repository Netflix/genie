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
package com.netflix.genie.web.agent.apis.rpc.v4.endpoints;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.netflix.genie.common.internal.dtos.DirectoryManifest;
import com.netflix.genie.common.internal.dtos.v4.converters.JobDirectoryManifestProtoConverter;
import com.netflix.genie.common.internal.exceptions.checked.GenieConversionException;
import com.netflix.genie.proto.AgentFileMessage;
import com.netflix.genie.proto.AgentManifestMessage;
import com.netflix.genie.proto.FileStreamServiceGrpc;
import com.netflix.genie.proto.ServerAckMessage;
import com.netflix.genie.proto.ServerControlMessage;
import com.netflix.genie.proto.ServerFileRequestMessage;
import com.netflix.genie.web.agent.resources.AgentFileResourceImpl;
import com.netflix.genie.web.agent.services.AgentFileStreamService;
import com.netflix.genie.web.exceptions.checked.NotFoundException;
import com.netflix.genie.web.properties.AgentFileStreamProperties;
import com.netflix.genie.web.util.StreamBuffer;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpRange;
import org.springframework.scheduling.TaskScheduler;

import javax.annotation.Nullable;
import javax.naming.LimitExceededException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link AgentFileStreamService} gRPC implementation.
 * Receives and caches manifests from connected agents.
 * Allows requesting a file, which is returned in the form of a {@link AgentFileResource}.
 * <p>
 * Implementation overview:
 * Each agent maintains a single "control" bidirectional stream (through the 'sync' RPC method).
 * This stream is used by the agent to regularly push manifests.
 * And it is used by the server to request files.
 * <p>
 * When a file is requested, the agent opens a separate "transfer" bidirectional stream (through the 'transmit' RPC
 * method) for that file transfer and starts sending chunks (currently one at the time), the server sends
 * acknowledgements in the same stream.
 * <p>
 * This service returns a resource immediately, but maintains a handle on a buffer where data is written as it is
 * received.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class GRpcAgentFileStreamServiceImpl
    extends FileStreamServiceGrpc.FileStreamServiceImplBase
    implements AgentFileStreamService {

    private static final String METRICS_PREFIX = "genie.agents.fileTransfers";
    private static final String TRANSFER_COUNTER = METRICS_PREFIX + ".requested.counter";
    private static final String TRANSFER_LIMIT_EXCEEDED_COUNTER = METRICS_PREFIX + ".rejected.counter";
    private static final String MANIFEST_CACHE_SIZE_GAUGE = METRICS_PREFIX + ".manifestCache.size";
    private static final String CONTROL_STREAMS_GAUGE = METRICS_PREFIX + ".controlStreams.size";
    private static final String TRANSFER_TIMEOUT_COUNTER = METRICS_PREFIX + ".timeout.counter";
    private static final String TRANSFER_SIZE_DISTRIBUTION = METRICS_PREFIX + ".transferSize.summary";
    private static final String ACTIVE_TRANSFER_GAUGE = METRICS_PREFIX + ".activeTransfers.size";

    private final ControlStreamManager controlStreamsManager;
    private final TransferManager transferManager;
    private final Counter fileTransferLimitExceededCounter;

    /**
     * Constructor.
     *
     * @param converter     The {@link JobDirectoryManifestProtoConverter} instance to use
     * @param taskScheduler A {@link TaskScheduler} instance to use
     * @param properties    The service properties
     * @param registry      The meter registry
     */
    public GRpcAgentFileStreamServiceImpl(
        final JobDirectoryManifestProtoConverter converter,
        final TaskScheduler taskScheduler,
        final AgentFileStreamProperties properties,
        final MeterRegistry registry
    ) {
        this.fileTransferLimitExceededCounter = registry.counter(TRANSFER_LIMIT_EXCEEDED_COUNTER);
        this.controlStreamsManager = new ControlStreamManager(converter, properties, registry);
        this.transferManager = new TransferManager(controlStreamsManager, taskScheduler, properties, registry);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<AgentFileResource> getResource(
        final String jobId,
        final Path relativePath,
        final URI uri,
        @Nullable final HttpRange range
    ) {
        if (range == null) {
            log.warn("Attempting to stream file with no range: {} of job: {}", relativePath, jobId);
        }
        log.debug("Attempting to stream file: {} of job: {}", relativePath, jobId);
        final Optional<DirectoryManifest> optionalManifest = this.getManifest(jobId);

        if (!optionalManifest.isPresent()) {
            log.warn("No manifest found for job: {}" + jobId);
            return Optional.empty();
        }

        final DirectoryManifest manifest = optionalManifest.get();
        final DirectoryManifest.ManifestEntry manifestEntry =
            manifest.getEntry(relativePath.toString()).orElse(null);

        if (manifestEntry == null) {
            // File does not exist according to manifest
            log.warn(
                "Requesting a file ({}) that does not exist in the manifest for job id: {}: ",
                relativePath,
                jobId
            );
            return Optional.of(AgentFileResourceImpl.forNonExistingResource());
        }

        final FileTransfer fileTransfer;
        try {
            // Attempt to start a file transfer
            fileTransfer = this.transferManager.startFileTransfer(
                jobId,
                manifestEntry,
                relativePath,
                range
            );
        } catch (NotFoundException e) {
            log.warn("No available stream to request file {} from agent running job: {}", relativePath, jobId);
            return Optional.empty();
        } catch (LimitExceededException e) {
            log.warn("No available slots to request file {} from agent running job: {}", relativePath, jobId);
            this.fileTransferLimitExceededCounter.increment();
            return Optional.empty();
        } catch (IndexOutOfBoundsException e) {
            log.warn("Cannot serve large file {} from agent running job: {}", relativePath, jobId);
            return Optional.empty();
        }

        // Return the resource
        return Optional.of(
            AgentFileResourceImpl.forAgentFile(
                uri,
                manifestEntry.getSize(),
                manifestEntry.getLastModifiedTime(),
                relativePath,
                jobId,
                fileTransfer.getInputStream()
            )
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<DirectoryManifest> getManifest(final String jobId) {
        return Optional.ofNullable(this.controlStreamsManager.getManifest(jobId));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamObserver<AgentManifestMessage> sync(final StreamObserver<ServerControlMessage> responseObserver) {
        return this.controlStreamsManager.handleNewControlStream(responseObserver);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamObserver<AgentFileMessage> transmit(final StreamObserver<ServerAckMessage> responseObserver) {
        return this.transferManager.handleNewTransferStream(responseObserver);
    }

    // Manages control streams, in theory one for each agent connected to this node
    private static final class ControlStreamManager {
        private final Map<String, ControlStreamObserver> controlStreamMap = Maps.newHashMap();
        private final Cache<String, DirectoryManifest> manifestCache;
        private final JobDirectoryManifestProtoConverter converter;
        private final Counter fileTansferCounter;
        private final MeterRegistry registry;

        private ControlStreamManager(
            final JobDirectoryManifestProtoConverter converter,
            final AgentFileStreamProperties properties,
            final MeterRegistry registry
        ) {
            this.converter = converter;
            this.manifestCache = Caffeine.newBuilder()
                .expireAfterWrite(properties.getManifestCacheExpiration())
                .build();
            this.fileTansferCounter = registry.counter(TRANSFER_COUNTER);

            this.registry = registry;
        }

        private synchronized void requestFile(
            final String jobId,
            final String fileTransferId,
            final String relativePath,
            final long startOffset,
            final long endOffset
        ) throws NotFoundException, IndexOutOfBoundsException {

            final ControlStreamObserver controlStreamObserver = this.controlStreamMap.get(jobId);
            if (controlStreamObserver == null) {
                throw new NotFoundException("No active stream control stream for job: " + jobId);
            }

            this.fileTansferCounter.increment();


            if (!controlStreamObserver.allowLargeFiles.get()
                && (startOffset > Integer.MAX_VALUE || endOffset > Integer.MAX_VALUE)) {
                // Agents that are not marked with 'allowLargeFiles' use a version of the protocol that uses
                // int32. They cannot serve file if the range goes beyond the 2GB mark.
                // The two casts to int below can potentially overflow, but that is ok because such request will only
                // be sent to an agent that ignores those fields.
                throw new IndexOutOfBoundsException("Outdated agent does not support ranges beyond the 2GB mark");
            }

            // Send the file request
            controlStreamObserver.responseObserver.onNext(
                ServerControlMessage.newBuilder()
                    .setServerFileRequest(
                        ServerFileRequestMessage.newBuilder()
                            .setStreamId(fileTransferId)
                            .setRelativePath(relativePath)
                            .setDeprecatedStartOffset((int) startOffset) // Possible integer overflow
                            .setDeprecatedEndOffset((int) endOffset) // Possible integer overflow
                            .setStartOffset(startOffset)
                            .setEndOffset(endOffset)
                            .build()
                    )
                    .build()
            );
        }

        private StreamObserver<AgentManifestMessage> handleNewControlStream(
            final StreamObserver<ServerControlMessage> responseObserver
        ) {
            log.debug("New agent control stream established");
            return new ControlStreamObserver(this, responseObserver);
        }

        private DirectoryManifest getManifest(final String jobId) {
            return this.manifestCache.getIfPresent(jobId);
        }

        private synchronized void updateManifestAndStream(
            final ControlStreamObserver controlStreamObserver,
            final String jobId,
            final DirectoryManifest manifest
        ) {
            // Keep the most recent manifest for each job id
            this.manifestCache.put(jobId, manifest);

            // Keep the most recent control stream for each job id
            final ControlStreamObserver previousObserver = this.controlStreamMap.put(jobId, controlStreamObserver);
            if (previousObserver != null && previousObserver != controlStreamObserver) {
                // If the older one is still present, close it
                previousObserver.closeStreamWithError(
                    new IllegalStateException("A new stream was registered for the same job id: " + jobId)
                );
            }

            this.registry.gauge(MANIFEST_CACHE_SIZE_GAUGE, this.manifestCache.estimatedSize());
            this.registry.gauge(CONTROL_STREAMS_GAUGE, this.controlStreamMap.size());
        }

        private synchronized void removeControlStream(
            final ControlStreamObserver controlStreamObserver,
            @Nullable final Throwable t
        ) {
            log.debug("Control stream {}", t == null ? "completed" : "error: " + t.getMessage());

            final boolean foundAndRemoved = this.controlStreamMap
                .entrySet()
                .removeIf(entry -> entry.getValue() == controlStreamObserver);

            if (foundAndRemoved) {
                log.debug(
                    "Removed a control stream due to {}",
                    t == null ? "completion" : "error: " + t.getMessage()
                );
            }
        }
    }

    private static final class ControlStreamObserver implements StreamObserver<AgentManifestMessage> {
        private final ControlStreamManager controlStreamManager;
        private final StreamObserver<ServerControlMessage> responseObserver;
        private final AtomicBoolean allowLargeFiles = new AtomicBoolean(false);

        private ControlStreamObserver(
            final ControlStreamManager controlStreamManager,
            final StreamObserver<ServerControlMessage> responseObserver
        ) {
            this.controlStreamManager = controlStreamManager;
            this.responseObserver = responseObserver;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onNext(final AgentManifestMessage value) {
            final String jobId = value.getJobId();
            this.allowLargeFiles.set(value.getLargeFilesSupported());

            DirectoryManifest manifest = null;
            try {
                manifest = this.controlStreamManager.converter.toManifest(value);
            } catch (GenieConversionException e) {
                log.warn("Failed to parse manifest for job id: {}", jobId, e);
            }

            if (manifest != null) {
                this.controlStreamManager.updateManifestAndStream(this, jobId, manifest);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onError(final Throwable t) {
            // Drop the stream, no other actions necessary
            this.controlStreamManager.removeControlStream(this, t);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onCompleted() {
            // Drop the stream, no other actions necessary
            this.controlStreamManager.removeControlStream(this, null);
            this.responseObserver.onCompleted();
        }

        public void closeStreamWithError(final Throwable e) {
            this.responseObserver.onError(e);
        }
    }

    // Manages in-progress file transfers
    private static final class TransferManager {
        private final Map<String, FileTransfer> activeTransfers = Maps.newHashMap();
        private final Set<AgentFileChunkObserver> unclaimedTransferStreams = Sets.newHashSet();
        // Little hack to get private inner class
        private final Class<? extends HttpRange> suffixRangeClass = HttpRange.createSuffixRange(1).getClass();
        private final ControlStreamManager controlStreamsManager;
        private final TaskScheduler taskScheduler;
        private final AgentFileStreamProperties properties;
        private final Counter transferTimeOutCounter;
        private final DistributionSummary transferSizeDistribution;
        private final MeterRegistry registry;

        private TransferManager(
            final ControlStreamManager controlStreamsManager,
            final TaskScheduler taskScheduler,
            final AgentFileStreamProperties properties,
            final MeterRegistry registry
        ) {
            this.controlStreamsManager = controlStreamsManager;
            this.taskScheduler = taskScheduler;
            this.properties = properties;
            this.registry = registry;
            this.transferTimeOutCounter = registry.counter(TRANSFER_TIMEOUT_COUNTER);
            this.transferSizeDistribution = registry.summary(TRANSFER_SIZE_DISTRIBUTION);

            this.taskScheduler.scheduleAtFixedRate(
                this::reapStalledTransfers,
                this.properties.getStalledTransferCheckInterval()
            );
        }

        private synchronized void reapStalledTransfers() {
            final AtomicInteger stalledTrasfersCounter = new AtomicInteger();
            final Instant now = Instant.now();
            // Iterate active transfers, shut down and remove the ones that are not making progress
            this.activeTransfers.entrySet().removeIf(
                entry -> {
                    final String transferId = entry.getKey();
                    final FileTransfer transfer = entry.getValue();
                    final Instant deadline =
                        transfer.lastAckTimestamp.plus(this.properties.getStalledTransferTimeout());
                    if (now.isAfter(deadline)) {
                        stalledTrasfersCounter.incrementAndGet();
                        log.warn("Transfer {} is stalled of job {}, shutting it down", transferId,
                            entry.getValue().jobId);
                        final TimeoutException exception = new TimeoutException("Transfer not making progress");
                        // Shut down stream, if one was associated to this transfer
                        final AgentFileChunkObserver observer = transfer.getAgentFileChunkObserver();
                        if (observer != null) {
                            observer.getResponseObserver().onError(exception);
                        }
                        // Close the buffer
                        transfer.closeWithError(exception);
                        // Remove from active transfers
                        return true;
                    } else {
                        // Do not remove from active, made progress recently enough.
                        return false;
                    }
                }
            );

            this.transferTimeOutCounter.increment(stalledTrasfersCounter.get());
            this.registry.gauge(ACTIVE_TRANSFER_GAUGE, this.activeTransfers.size());
        }

        private synchronized FileTransfer startFileTransfer(
            final String jobId,
            final DirectoryManifest.ManifestEntry manifestEntry,
            final Path relativePath,
            @Nullable final HttpRange range
        ) throws NotFoundException, LimitExceededException {
            // Create a unique ID for this file transfer
            final String fileTransferId = UUID.randomUUID().toString();
            log.debug(
                "Initiating transfer {} for file: {} of job: {}",
                fileTransferId,
                relativePath,
                jobId
            );

            // No need to use a semaphore since class is synchronized
            if (this.activeTransfers.size() >= properties.getMaxConcurrentTransfers()) {
                log.warn("Rejecting request for {}:{}, too many active transfers", jobId, relativePath);
                throw new LimitExceededException("Too many concurrent downloads");
            }

            final long fileSize = manifestEntry.getSize();

            // Http range is inclusive, agent protocol is not.
            // Convert from one to the other.
            final long startOffset;
            final long endOffset;

            if (range == null) {
                startOffset = 0;
                endOffset = fileSize;
            } else if (range.getClass() == this.suffixRangeClass) {
                startOffset = range.getRangeStart(fileSize);
                endOffset = fileSize;
            } else {
                startOffset = Math.min(fileSize, range.getRangeStart(fileSize));
                endOffset = 1 + range.getRangeEnd(fileSize);
            }

            log.debug("Transfer {} effective range {}-{}: of job: {} ", fileTransferId, startOffset, endOffset, jobId);

            // Allocate and park the buffer that will store the data in transit.
            final StreamBuffer buffer = new StreamBuffer(startOffset);

            // Create a file transfer
            final FileTransfer fileTransfer = new FileTransfer(
                fileTransferId,
                jobId,
                relativePath,
                startOffset,
                endOffset,
                fileSize,
                buffer
            );

            this.transferSizeDistribution.record(endOffset - startOffset);

            if (endOffset - startOffset == 0) {
                log.debug("Transfer {} is empty, completing of job: {}", fileTransferId, jobId);
                // When requesting an empty file (or a range of 0 bytes), short-circuit and just return an empty
                // buffer, without tracking it as active transfer.
                buffer.closeForCompleted();
            } else {
                log.debug("Tracking new transfer {} of job: {}", fileTransferId, jobId);
                // Expecting some data. Track this stream and its buffer so incoming chunks can be appended.
                this.activeTransfers.put(fileTransferId, fileTransfer);

                log.debug("Requesting start of transfer {} of job: {}", fileTransferId, jobId);
                // Request file over control channel
                try {
                    this.controlStreamsManager.requestFile(
                        jobId,
                        fileTransferId,
                        relativePath.toString(),
                        startOffset,
                        endOffset
                    );
                } catch (IndexOutOfBoundsException | NotFoundException e) {
                    log.error(
                        "Failed to request file {}:{}, terminating transfer {}: {}",
                        jobId,
                        relativePath,
                        fileTransferId,
                        e.getMessage()
                    );
                    this.activeTransfers.remove(fileTransferId, fileTransfer);
                    buffer.closeForError(e);
                    throw e;
                }
            }

            return fileTransfer;
        }

        private synchronized StreamObserver<AgentFileMessage> handleNewTransferStream(
            final StreamObserver<ServerAckMessage> responseObserver
        ) {
            log.info("New file transfer stream established");
            final AgentFileChunkObserver agentFileChunkObserver =
                new AgentFileChunkObserver(this, responseObserver);

            // Observer are not associated to a specific transfer until the first message is received
            this.unclaimedTransferStreams.add(agentFileChunkObserver);

            // Schedule a timeout for this stream to get associated with a pending transfer
            taskScheduler.schedule(
                () -> this.handleUnclaimedStreamTimeout(agentFileChunkObserver),
                Instant.now().plus(properties.getUnclaimedStreamStartTimeout())
            );

            return agentFileChunkObserver;
        }

        private synchronized void handleUnclaimedStreamTimeout(final AgentFileChunkObserver agentFileChunkObserver) {
            final boolean streamUnclaimed = this.unclaimedTransferStreams.remove(agentFileChunkObserver);
            if (streamUnclaimed) {
                // If found in the unclaimed set, this stream did not send any message yet, shut down the stream
                log.warn("Shutting down unclaimed transfer stream");
                agentFileChunkObserver.getResponseObserver().onError(
                    new TimeoutException("No messages received in stream")
                );
                this.transferTimeOutCounter.increment();
            }
        }

        private synchronized void handleFileChunk(
            final String transferStreamId,
            final AgentFileChunkObserver agentFileChunkObserver,
            final ByteString data
        ) {
            final FileTransfer fileTransfer = this.activeTransfers.get(transferStreamId);
            final boolean unclaimedStream = this.unclaimedTransferStreams.remove(agentFileChunkObserver);

            if (fileTransfer != null) {
                if (unclaimedStream) {
                    // There is a transfer pending, and this stream just sent the first chunk of data
                    // Associate the stream to the file transfer
                    fileTransfer.claimStreamObserver(agentFileChunkObserver);
                }

                // Write and ack in a different thread, to avoid locking this during a potentially blocking operation
                this.taskScheduler.schedule(
                    () -> this.writeDataAndAck(fileTransfer, data),
                    new Date() // Ack: use date rather than instant to make the distinction easier in tests
                );

            } else {
                log.warn("Received a chunk for a transfer no longer in progress: {}", transferStreamId);
            }
        }

        private synchronized void removeTransferStream(
            final AgentFileChunkObserver agentFileChunkObserver,
            @Nullable final Throwable t
        ) {
            log.info("Removing file transfer: {}", t == null ? "completed" : t.getMessage());
            // Received error or completion on a transfer stream.
            final FileTransfer fileTransfer = this.findFileTransfer(agentFileChunkObserver);
            if (fileTransfer != null) {
                // Transfer is no longer active, remove it
                final boolean removed = this.activeTransfers.remove(fileTransfer.getTransferId(), fileTransfer);
                if (removed && t == null) {
                    fileTransfer.close();
                } else if (removed) {
                    fileTransfer.closeWithError(t);
                }
                // If not removed, another thread already got to it, for example due to timeout. Nothing to do
            } else {
                // Stream is not associated with a file transfer, may be unclaimed.
                // Remove it so the timeout task does not try to close it again.
                this.unclaimedTransferStreams.remove(agentFileChunkObserver);
            }
        }

        private synchronized FileTransfer findFileTransfer(final AgentFileChunkObserver agentFileChunkObserver) {
            // Find a transfer by iterating over the active ones.
            // Could keep a map indexed by observers, but this should be fine since it's only called when a stream
            // terminates.
            for (final FileTransfer fileTransfer : this.activeTransfers.values()) {
                if (fileTransfer.getAgentFileChunkObserver() == agentFileChunkObserver) {
                    return fileTransfer;
                }
            }
            return null;
        }

        // N.B. this should not synchronized to avoid locking up the transfer manager
        private void writeDataAndAck(final FileTransfer fileTransfer, final ByteString data) {
            final String fileTransferId = fileTransfer.getTransferId();
            try {
                // Try to write. May fail if buffer consumer is slow and buffer is not drained yet.
                if (fileTransfer.append(data)) {
                    log.debug("Wrote chunk of transfer {} to buffer. Sending ack", fileTransferId);
                    fileTransfer.sendAck();
                } else {
                    // Try again in a little bit
                    this.taskScheduler.schedule(
                        () -> this.writeDataAndAck(fileTransfer, data),
                        Instant.now().plus(this.properties.getWriteRetryDelay())
                    );
                }
            } catch (IllegalStateException e) {
                // Eventually retries will stop because the transfer times out due to lack of progress
                log.warn("Buffer of transfer {} of job {} is closed", fileTransferId, fileTransfer.jobId);
            }
        }
    }

    private static final class FileTransfer {
        private final String jobId;
        @Getter
        private final String transferId;
        private final StreamBuffer buffer;
        private final String description;
        @Getter
        private AgentFileChunkObserver agentFileChunkObserver;
        private State state = State.NEW;
        private Instant lastAckTimestamp;

        private FileTransfer(
            final String transferId,
            final String jobId,
            final Path relativePath,
            final long startOffset,
            final long endOffset,
            final long fileSize,
            final StreamBuffer buffer
        ) {
            this.jobId = jobId;
            this.transferId = transferId;
            this.buffer = buffer;
            this.lastAckTimestamp = Instant.now();
            this.description = "FileTransfer " + transferId
                + ", agent://" + jobId + "/" + relativePath + " "
                + "(range: (" + startOffset + "-" + endOffset + "] file.size: " + fileSize + ")";
        }

        @Override
        public String toString() {
            return "" + this.state + " " + this.description;
        }

        private void claimStreamObserver(final AgentFileChunkObserver observer) {
            this.state = State.IN_PROGRESS;
            this.agentFileChunkObserver = observer;
        }

        private InputStream getInputStream() {
            return this.buffer.getInputStream();
        }

        private boolean append(final ByteString data) {
            return buffer.tryWrite(data);
        }

        private void closeWithError(final Throwable t) {
            this.state = State.FAILED;
            this.buffer.closeForError(t);
        }

        private void close() {
            this.state = State.COMPLETED;
            this.buffer.closeForCompleted();
        }

        private void sendAck() {
            this.getAgentFileChunkObserver().getResponseObserver().onNext(
                ServerAckMessage.newBuilder().build()
            );
            this.lastAckTimestamp = Instant.now();
        }

        private enum State {
            NEW,
            IN_PROGRESS,
            COMPLETED,
            FAILED
        }
    }

    private static final class AgentFileChunkObserver implements StreamObserver<AgentFileMessage> {
        private final TransferManager transferManager;
        @Getter
        private final StreamObserver<ServerAckMessage> responseObserver;

        AgentFileChunkObserver(
            final TransferManager transferManager,
            final StreamObserver<ServerAckMessage> responseObserver
        ) {
            this.transferManager = transferManager;
            this.responseObserver = responseObserver;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onNext(final AgentFileMessage value) {
            final String transferStreamId = value.getStreamId();
            log.debug("Received file chunk of transfer: {}", transferStreamId);
            this.transferManager.handleFileChunk(transferStreamId, this, value.getData());
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onError(final Throwable t) {
            this.transferManager.removeTransferStream(this, t);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onCompleted() {
            this.transferManager.removeTransferStream(this, null);
            this.responseObserver.onCompleted();
        }
    }
}
