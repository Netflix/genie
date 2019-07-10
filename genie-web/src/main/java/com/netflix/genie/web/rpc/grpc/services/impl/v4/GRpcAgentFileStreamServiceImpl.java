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
package com.netflix.genie.web.rpc.grpc.services.impl.v4;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.netflix.genie.common.exceptions.GenieTimeoutException;
import com.netflix.genie.common.internal.dto.DirectoryManifest;
import com.netflix.genie.common.internal.dto.v4.converters.JobDirectoryManifestProtoConverter;
import com.netflix.genie.common.internal.exceptions.GenieConversionException;
import com.netflix.genie.proto.AgentFileMessage;
import com.netflix.genie.proto.AgentManifestMessage;
import com.netflix.genie.proto.FileStreamServiceGrpc;
import com.netflix.genie.proto.ServerAckMessage;
import com.netflix.genie.proto.ServerControlMessage;
import com.netflix.genie.proto.ServerFileRequestMessage;
import com.netflix.genie.web.properties.GRpcServerProperties;
import com.netflix.genie.web.resources.agent.AgentFileResourceImpl;
import com.netflix.genie.web.rpc.grpc.interceptors.SimpleLoggingInterceptor;
import com.netflix.genie.web.services.AgentFileStreamService;
import com.netflix.genie.web.util.StreamBuffer;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.springboot.autoconfigure.grpc.server.GrpcService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.TaskScheduler;

import javax.annotation.Nullable;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@link AgentFileStreamService} gRPC implementation.
 * Receives and caches manifests from connected agents.
 * Allows requesting a file, which is returned in the form of a {@link AgentFileStreamService.AgentFileResource}.
 * <p>
 * Implementation overview:
 * Each agent maintains a single 'sync' channel, through which manifests are pushed to the server.
 * On top of the same channel, the server can request a file.
 * When a file is requested, the agent opens a separate 'transmit' stream and sends the file in chunks.
 * The server acknowledges a chunk in order to request the next one.
 * <p>
 * This service returns a resource immediately, but maintains a handle on a buffer where data is written as it is
 * received.
 *
 * @author mprimi
 * @since 4.0.0
 */
@ConditionalOnProperty(value = GRpcServerProperties.ENABLED_PROPERTY, havingValue = "true")
@GrpcService(
    value = FileStreamServiceGrpc.class,
    interceptors = {
        SimpleLoggingInterceptor.class,
    }
)
@Slf4j
class GRpcAgentFileStreamServiceImpl
    extends FileStreamServiceGrpc.FileStreamServiceImplBase
    implements AgentFileStreamService {

    private static final long FILE_TRANSFER_BEGIN_TIMEOUT_MILLIS = 3000;

    private final Map<String, ControlStreamObserver> jobIdControlStreamMap = Maps.newConcurrentMap();
    private final Map<String, StreamBuffer> pendingTransferBuffersMap = Maps.newConcurrentMap();
    private final Set<FileTransferStreamObserver> pendingTransferObserversSet = Sets.newConcurrentHashSet();
    private final Map<String, StreamBuffer> inProgressTransferBuffersMap = Maps.newConcurrentMap();
    private final JobDirectoryManifestProtoConverter converter;
    private final TaskScheduler taskScheduler;

    GRpcAgentFileStreamServiceImpl(
        final JobDirectoryManifestProtoConverter converter,
        @Qualifier("genieTaskScheduler") final TaskScheduler taskScheduler
    ) {
        this.converter = converter;
        this.taskScheduler = taskScheduler;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<AgentFileResource> getResource(final String jobId, final Path relativePath, final URI uri) {

        final ControlStreamObserver streamObserver = this.jobIdControlStreamMap.get(jobId);
        if (streamObserver == null) {
            log.warn("Stream Record not found for job id: {}", jobId);
            return Optional.empty();
        }

        final DirectoryManifest manifest = streamObserver.manifestRef.get();
        if (manifest == null) {
            log.warn("Stream record for job id: {} does not have a manifest", jobId);
            return Optional.empty();
        }

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

        // A unique ID for this file transfer
        final String fileTransferId = UUID.randomUUID().toString();

        // TODO: code upstream of here assumes files is requested in its entirety.
        // But rest of the code downstream actually treats everything as a range request.
        final int startOffset = 0;
        final int endOffset = Math.toIntExact(manifestEntry.getSize());

        // Allocate and park the buffer that will store the data in transit.
        final StreamBuffer buffer = new StreamBuffer();

        if (endOffset - startOffset == 0) {
            // When requesting an empty file (or a range of 0 bytes), short-circuit and just return an empty resource.
            buffer.closeForCompleted();
        } else {
            // Expecting some data. Track this stream and its buffer so incoming chunks can be appended.
            this.pendingTransferBuffersMap.put(fileTransferId, buffer);

            // Request file over control channel
            streamObserver.responseObserver.onNext(
                ServerControlMessage.newBuilder()
                    .setServerFileRequest(
                        ServerFileRequestMessage.newBuilder()
                            .setStreamId(fileTransferId)
                            .setRelativePath(relativePath.toString())
                            .setStartOffset(startOffset)
                            .setEndOffset(endOffset)
                            .build()
                    )
                    .build()
            );

            // Schedule a timeout for this transfer to start (first chunk received)
            this.taskScheduler.schedule(
                () -> {
                    final StreamBuffer b = pendingTransferBuffersMap.remove(fileTransferId);
                    // Is this stream/buffer still in the 'pending' map?
                    if (b != null) {
                        b.closeForError(
                            new TimeoutException("Timeout waiting for transfer to start")
                        );
                    }
                },
                Instant.now().plusMillis(FILE_TRANSFER_BEGIN_TIMEOUT_MILLIS)
            );
        }

        final AgentFileResource resource = AgentFileResourceImpl.forAgentFile(
            uri,
            manifestEntry.getSize(),
            manifestEntry.getLastModifiedTime(),
            Paths.get(manifestEntry.getPath()),
            jobId,
            buffer.getInputStream()
        );

        return Optional.of(resource);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<DirectoryManifest> getManifest(final String jobId) {
        final ControlStreamObserver streamObserver = this.jobIdControlStreamMap.get(jobId);
        if (streamObserver != null) {
            return Optional.ofNullable(streamObserver.manifestRef.get());
        }
        log.warn("Stream Record not found for job id: {}", jobId);
        return Optional.empty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamObserver<AgentManifestMessage> sync(final StreamObserver<ServerControlMessage> responseObserver) {
        log.info("New agent control stream established");
        return new ControlStreamObserver(this, responseObserver);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamObserver<AgentFileMessage> transmit(final StreamObserver<ServerAckMessage> responseObserver) {
        log.info("New file transfer stream established");
        final FileTransferStreamObserver fileTransferStreamObserver =
            new FileTransferStreamObserver(this, responseObserver);

        this.pendingTransferObserversSet.add(fileTransferStreamObserver);

        // Schedule a timeout for this transfer to start (first chunk received)
        this.taskScheduler.schedule(
            () -> {
                final boolean removed = pendingTransferObserversSet.remove(fileTransferStreamObserver);
                if (removed) {
                    fileTransferStreamObserver.responseObserver.onError(
                        new GenieTimeoutException("Timeout waiting for transfer to begin")
                    );
                }
            },
            Instant.now().plusMillis(FILE_TRANSFER_BEGIN_TIMEOUT_MILLIS)
        );

        return fileTransferStreamObserver;
    }

    private void registerControlStream(final String jobId, final ControlStreamObserver controlStreamObserver) {
        final ControlStreamObserver previousObserver = this.jobIdControlStreamMap.put(jobId, controlStreamObserver);
        if (previousObserver != null) {
            // In theory, this cannot happen
            log.warn("Found an previous observer for job id: {}", jobId);
        }
    }

    private void unregisterControlStream(final String jobId, final ControlStreamObserver controlStreamObserver) {
        final boolean removed = this.jobIdControlStreamMap.remove(jobId, controlStreamObserver);
        if (!removed) {
            log.warn("Could not remove observer for job: {}, not found in map", jobId);
        }
    }

    private void handleFileTransferChunk(
        final FileTransferStreamObserver fileTransferStreamObserver,
        final String streamId,
        final ByteString data
    ) {

        // Remove observer from the set of transfers waiting to start.
        final boolean removed = this.pendingTransferObserversSet.remove(fileTransferStreamObserver);
        if (removed) {
            log.debug("Removed observer for file stream: {} from 'pending' set", streamId);
        }

        // Remove buffer from the set of transfers waiting to start and move it to the 'in progress' map.
        final StreamBuffer streamBufferFromPending = this.pendingTransferBuffersMap.remove(streamId);
        if (streamBufferFromPending != null) {
            log.debug("Moving buffer for file stream {} from 'pending' to 'in progress'", streamId);
            this.inProgressTransferBuffersMap.put(streamId, streamBufferFromPending);
        }

        // Look up the buffer where chunk data is written into
        final StreamBuffer streamBuffer = this.inProgressTransferBuffersMap.get(streamId);

        // Write into it, if the stream is still there
        if (streamBuffer != null) {
            streamBuffer.write(data);
        }
    }

    private void handleFileTransferError(
        final FileTransferStreamObserver fileTransferStreamObserver,
        @Nullable final String streamId,
        final Throwable t
    ) {
        log.error("Error in file transfer stream: {}: {}", streamId, t.getMessage(), t);

        this.pendingTransferObserversSet.remove(fileTransferStreamObserver);

        if (streamId != null) {
            final StreamBuffer pendingTransferBuffer = this.pendingTransferBuffersMap.remove(streamId);
            if (pendingTransferBuffer != null) {
                pendingTransferBuffer.closeForError(t);
            }

            final StreamBuffer inProgressTransferBuffer = this.inProgressTransferBuffersMap.remove(streamId);
            if (inProgressTransferBuffer != null) {
                inProgressTransferBuffer.closeForError(t);
            }
        }
    }

    private void handleFileTransferCompletion(
        final FileTransferStreamObserver fileTransferStreamObserver,
        @Nullable final String streamId
    ) {
        this.pendingTransferObserversSet.remove(fileTransferStreamObserver);

        final StreamBuffer pendingTransferBuffer = this.pendingTransferBuffersMap.remove(streamId);
        if (pendingTransferBuffer != null) {
            pendingTransferBuffer.closeForCompleted();
        }

        final StreamBuffer inProgressTransferBuffer = this.inProgressTransferBuffersMap.remove(streamId);
        if (inProgressTransferBuffer != null) {
            inProgressTransferBuffer.closeForCompleted();
        }
    }


    private static class ControlStreamObserver implements StreamObserver<AgentManifestMessage> {
        private final GRpcAgentFileStreamServiceImpl gRpcAgentFileStreamService;
        private final StreamObserver<ServerControlMessage> responseObserver;
        private final AtomicReference<DirectoryManifest> manifestRef = new AtomicReference<>();
        private final AtomicReference<String> jobIdRef = new AtomicReference<>();

        ControlStreamObserver(
            final GRpcAgentFileStreamServiceImpl gRpcAgentFileStreamService,
            final StreamObserver<ServerControlMessage> responseObserver
        ) {
            this.gRpcAgentFileStreamService = gRpcAgentFileStreamService;
            this.responseObserver = responseObserver;
        }

        @Override
        public void onNext(final AgentManifestMessage value) {
            log.debug("Received a manifest");

            final String jobId = value.getJobId();

            final boolean isFirstMessage = this.jobIdRef.compareAndSet(null, jobId);

            if (isFirstMessage) {
                this.gRpcAgentFileStreamService.registerControlStream(jobId, this);
            }

            // Save the manifest just received
            try {
                manifestRef.set(
                    this.gRpcAgentFileStreamService.converter.toManifest(value)
                );
            } catch (GenieConversionException e) {
                log.warn("Failed to parse manifest for job id: {}", jobId, e);
            }
        }

        @Override
        public void onError(final Throwable t) {
            log.warn("Manifest stream error", t);
            this.unregisterStream();
        }

        @Override
        public void onCompleted() {
            log.debug("Manifest stream completed");
            this.unregisterStream();
        }

        private void unregisterStream() {
            final String jobId = jobIdRef.get();
            if (jobId != null) {
                this.gRpcAgentFileStreamService.unregisterControlStream(jobId, this);
            }
        }

    }

    private static class FileTransferStreamObserver implements StreamObserver<AgentFileMessage> {
        private final GRpcAgentFileStreamServiceImpl gRpcAgentFileStreamService;
        private final StreamObserver<ServerAckMessage> responseObserver;
        private final AtomicReference<String> streamId = new AtomicReference<>();


        FileTransferStreamObserver(
            final GRpcAgentFileStreamServiceImpl gRpcAgentFileStreamService,
            final StreamObserver<ServerAckMessage> responseObserver
        ) {
            this.gRpcAgentFileStreamService = gRpcAgentFileStreamService;
            this.responseObserver = responseObserver;
        }

        @Override
        public void onNext(final AgentFileMessage value) {
            final String messageStreamId = value.getStreamId();

            if (StringUtils.isBlank(messageStreamId)) {
                log.warn("Received file chunk with empty stream identifier");
                return;
            }

            if (streamId.compareAndSet(null, messageStreamId)) {
                log.debug("Received first chunk for transfer: {}", messageStreamId);
            }

            if (!messageStreamId.equals(streamId.get())) {
                log.warn(
                    "Received chunk with id: {}, but this stream was previously used by stream: {}",
                    messageStreamId,
                    streamId.get()
                );
                return;
            }

            // May block if the queue of chunks is full
            this.gRpcAgentFileStreamService.handleFileTransferChunk(
                this,
                value.getStreamId(),
                value.getData()
            );

            // Send ACK after successfully enqueuing chunk for consumption
            this.responseObserver.onNext(
                ServerAckMessage.newBuilder().build()
            );
        }

        @Override
        public void onError(final Throwable t) {
            this.gRpcAgentFileStreamService.handleFileTransferError(this, streamId.get(), t);
        }

        @Override
        public void onCompleted() {
            this.gRpcAgentFileStreamService.handleFileTransferCompletion(this, streamId.get());
        }
    }

}
