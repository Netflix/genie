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

import com.google.common.collect.Sets;
import com.google.protobuf.Empty;
import com.netflix.genie.common.internal.dto.JobDirectoryManifest;
import com.netflix.genie.common.internal.dto.v4.converters.JobDirectoryManifestProtoConverter;
import com.netflix.genie.common.internal.exceptions.GenieConversionException;
import com.netflix.genie.common.internal.exceptions.StreamUnavailableException;
import com.netflix.genie.common.internal.util.FileBuffer;
import com.netflix.genie.common.internal.util.FileBufferFactory;
import com.netflix.genie.proto.AgentJobFileMessage;
import com.netflix.genie.proto.FileStreamAck;
import com.netflix.genie.proto.FileStreamChunk;
import com.netflix.genie.proto.FileStreamRegistration;
import com.netflix.genie.proto.FileStreamRequest;
import com.netflix.genie.proto.JobFileManifestMessage;
import com.netflix.genie.proto.JobFileServiceGrpc;
import com.netflix.genie.proto.ServerJobFileMessage;
import com.netflix.genie.web.properties.GRpcServerProperties;
import com.netflix.genie.web.rpc.grpc.interceptors.SimpleLoggingInterceptor;
import com.netflix.genie.web.services.AgentFileManifestService;
import com.netflix.genie.web.services.AgentFileStreamService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.springboot.autoconfigure.grpc.server.GrpcService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

import javax.annotation.Nullable;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Implementation of {@link JobFileServiceGrpc.JobFileServiceImplBase} to serve files via API for jobs being executed
 * by a remote agent connected through gRPC.
 *
 * Note: this implementation makes use of a DelegatingStreamObserver. This object is used so that:
 *  - One stream observer is created to handle inbound messages when the gRPC stream is intialized
 *  - The 'target' of the messages changes over time, as the stream goes through different stages:
 *    * Uninitialized: the stream exists but it's not known who the client is.
 *    * Ready: the agent makes itself known with a first message. The stream can now be used to request files
 *             for that particular job by the server
 *    * Active: the server requested a file. The client is sending it in chunks.
 *              After transfer is completed, the stream is closed and discarded.
 *
 * @author mprimi
 * @since 4.0.0
 */
@ConditionalOnProperty(value = GRpcServerProperties.ENABLED_PROPERTY, havingValue = "true")
@GrpcService(
    value = JobFileServiceGrpc.class,
    interceptors = {
        SimpleLoggingInterceptor.class,
    }
)
@Slf4j
public class GRpcJobFileServiceImpl extends JobFileServiceGrpc.JobFileServiceImplBase {

    private final JobDirectoryManifestProtoConverter converter;
    private final AgentFileManifestService agentFileManifestService;
    private final AgentFileStreamService agentFileStreamService;
    private final Set<UninitializedStream> uninitializedStreams = Sets.newHashSet();
    private final FileBufferFactory fileBufferFactory;

    /**
     * Constructor.
     *
     * @param converter                a manifest message converter
     * @param agentFileManifestService a manifest service
     * @param agentFileStreamService   a file streaming service
     * @param fileBufferFactory        a file buffer factory
     */
    public GRpcJobFileServiceImpl(
        final JobDirectoryManifestProtoConverter converter,
        final AgentFileManifestService agentFileManifestService,
        final AgentFileStreamService agentFileStreamService,
        final FileBufferFactory fileBufferFactory
    ) {
        this.converter = converter;
        this.agentFileManifestService = agentFileManifestService;
        this.agentFileStreamService = agentFileStreamService;
        this.fileBufferFactory = fileBufferFactory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamObserver<JobFileManifestMessage> pushManifest(final StreamObserver<Empty> responseObserver) {
        log.debug("Initiating new manifest stream");
        // TODO: worth tracking active streams by saving this observer in a set?
        // TODO: worth sending back Empty messages, to weed out broken streams?
        return new ManifestMessageObserver(this, responseObserver);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamObserver<AgentJobFileMessage> streamFile(final StreamObserver<ServerJobFileMessage> responseObserver) {

        log.debug("Initializing new file stream");

        final DelegatingStreamObserver requestObserver = new DelegatingStreamObserver(null);

        // A new stream is created but until the first message is received, it cannot be linked to a job.
        final UninitializedStream uninitializedStream =
            new UninitializedStream(this, requestObserver, responseObserver);
        requestObserver.setDelegate(uninitializedStream);

        uninitializedStreams.add(uninitializedStream);

        return requestObserver;
    }

    /**
     * File streams go through 3 phases: uninitialized, ready, active.
     * This interface allows the gRPC observer to be created early as {@link DelegatingStreamObserver} and the target
     * of messages received (the delegate) is updated as time passes and the stream changes state.
     */
    private interface StreamObserverDelegate<T> {
        void handleMessage(StreamObserver<T> delegatingStreamObserver, T value);

        void handleError(StreamObserver<T> delegatingStreamObserver, Throwable t);

        void handleCompleted(StreamObserver<T> delegatingStreamObserver);
    }

    /**
     * Observer handling manifest updates from the agent.
     */
    private static class ManifestMessageObserver implements StreamObserver<JobFileManifestMessage> {
        private final GRpcJobFileServiceImpl gRpcJobFileService;
        private final StreamObserver<Empty> responseObserver;
        private final AtomicReference<String> jobIdReference = new AtomicReference<>();

        ManifestMessageObserver(
            final GRpcJobFileServiceImpl gRpcJobFileService,
            final StreamObserver<Empty> responseObserver
        ) {
            this.gRpcJobFileService = gRpcJobFileService;
            this.responseObserver = responseObserver;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onNext(final JobFileManifestMessage message) {
            final String jobId = message.getJobId();
            log.debug("Received file manifest for job: " + jobId);

            // If this is the first manifest update, store the job id
            jobIdReference.compareAndSet(null, jobId);

            final JobDirectoryManifest manifest;
            try {
                manifest = this.gRpcJobFileService.converter.toManifest(message);
            } catch (GenieConversionException e) {
                log.warn("Failed to parse manifest from message", e);
                return;
            }
            this.gRpcJobFileService.agentFileManifestService.updateManifest(jobId, manifest);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onError(final Throwable t) {
            final String jobId = jobIdReference.get();
            if (jobId != null) {
                log.warn("Error in manifest stream for job: " + jobId);
                this.gRpcJobFileService.agentFileManifestService.deleteManifest(jobId);
            }
            this.responseObserver.onCompleted();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onCompleted() {
            final String jobId = jobIdReference.get();
            if (jobId != null) {
                log.debug("Completed manifest stream for job: " + jobId);
                this.gRpcJobFileService.agentFileManifestService.deleteManifest(jobId);
            }
            this.responseObserver.onCompleted();
        }
    }

    /**
     * A file stream is "uninitialized" if it was opened but the first message ({@link FileStreamRegistration}) has not
     * been received yet. As such, the stream cannot be linked to a given job.
     */
    private static class UninitializedStream implements Closeable, StreamObserverDelegate<AgentJobFileMessage> {
        private final GRpcJobFileServiceImpl gRpcJobFileService;
        private final DelegatingStreamObserver delegatingRequestObserver;
        private final StreamObserver<ServerJobFileMessage> responseObserver;

        UninitializedStream(
            final GRpcJobFileServiceImpl gRpcJobFileService,
            final DelegatingStreamObserver delegatingRequestObserver,
            final StreamObserver<ServerJobFileMessage> responseObserver
        ) {
            this.gRpcJobFileService = gRpcJobFileService;
            this.delegatingRequestObserver = delegatingRequestObserver;
            this.responseObserver = responseObserver;
        }

        @Override
        public void handleMessage(
            final StreamObserver<AgentJobFileMessage> delegatingStreamObserver,
            final AgentJobFileMessage value
        ) {
            // Remove from uninitialized
            this.gRpcJobFileService.uninitializedStreams.remove(this);

            if (value.getMessageCase() == AgentJobFileMessage.MessageCase.FILE_STREAM_REGISTRATION) {
                final FileStreamRegistration registration = value.getFileStreamRegistration();
                final String jobId = registration.getJobId();
                log.info("Stream registered as belonging to job: {}", jobId);
                // Stream transforms from Uninitialized to Ready
                final AgentFileStreamService.ReadyStream readyStream = new ReadyStreamImpl(
                    gRpcJobFileService,
                    jobId,
                    this.delegatingRequestObserver,
                    this.responseObserver
                );
                gRpcJobFileService.agentFileStreamService.registerReadyStream(readyStream);
            } else {
                log.error("Unexpected message type for uninitialized stream: {}", value.getMessageCase().name());
                this.close();
            }
        }

        @Override
        public void handleError(final StreamObserver<AgentJobFileMessage> delegatingStreamObserver, final Throwable t) {
            log.warn("Stream error while in uninitialized state", t);
            this.close();
        }

        @Override
        public void handleCompleted(final StreamObserver<AgentJobFileMessage> delegatingStreamObserver) {
            log.info("Stream closed while in uninitialized state");
            this.close();
        }

        @Override
        public void close() {
            this.responseObserver.onCompleted();
            // Remove from uninitialized
            this.gRpcJobFileService.uninitializedStreams.remove(this);
        }
    }

    /**
     * A file stream is "ready" if the first message ({@link FileStreamRegistration}) was received from the agent,
     * declaring what job the agent is executing.
     * This stream is parked and unused until the server activates in order to request and receive a file from that
     * agent.
     */
    private static class ReadyStreamImpl
        implements AgentFileStreamService.ReadyStream, StreamObserverDelegate<AgentJobFileMessage> {
        private final GRpcJobFileServiceImpl gRpcJobFileService;
        private final String jobId;
        private final GRpcJobFileServiceImpl.DelegatingStreamObserver delegatingStreamObserver;
        private final StreamObserver<ServerJobFileMessage> responseObserver;
        private final AtomicBoolean closed = new AtomicBoolean();

        ReadyStreamImpl(
            final GRpcJobFileServiceImpl gRpcJobFileService,
            final String jobId,
            final DelegatingStreamObserver delegatingStreamObserver,
            final StreamObserver<ServerJobFileMessage> responseObserver
        ) {
            this.gRpcJobFileService = gRpcJobFileService;
            this.jobId = jobId;
            this.delegatingStreamObserver = delegatingStreamObserver;
            this.responseObserver = responseObserver;
            this.delegatingStreamObserver.setDelegate(this);
        }

        @Override
        public void handleMessage(
            final StreamObserver<AgentJobFileMessage> streamObserver,
            final AgentJobFileMessage value
        ) {
            // No message is expected from agent when in 'ready' state
            final String messageCase = value.getMessageCase().name();
            log.warn("Unexpected message (type: {}) received by stream in ready state", messageCase);
            this.close();
        }

        @Override
        public void handleError(
            final StreamObserver<AgentJobFileMessage> streamObserver,
            final Throwable t
        ) {
            log.error("Stream error while in ready state", t);
            this.close();
        }

        @Override
        public void handleCompleted(
            final StreamObserver<AgentJobFileMessage> streamObserver
        ) {
            log.info("Stream closed while in ready state");
            this.close();
        }

        @Override
        public @NotBlank String getJobId() {
            return this.jobId;
        }

        @Override
        public AgentFileStreamService.ActiveStream activateStream(
            final Path relativePath,
            final long startOffset,
            final long endOffset
        ) throws StreamUnavailableException, IOException {
            log.info("Activating stream for job id {}", getJobId());
            if (this.closed.get()) {
                throw new StreamUnavailableException("Stream was closed and should not be used");
            }
            final FileBuffer fileBuffer =
                this.gRpcJobFileService.fileBufferFactory.get(Math.toIntExact(endOffset - startOffset));
            return new ActiveStreamImpl(
                jobId,
                relativePath,
                startOffset,
                endOffset,
                delegatingStreamObserver,
                responseObserver,
                fileBuffer
            );
        }

        @Override
        public void close() {
            if (!closed.getAndSet(true)) {
                this.delegatingStreamObserver.setDelegate(null);
                this.gRpcJobFileService.agentFileStreamService.unregisterReadyStream(this);
                this.responseObserver.onCompleted();
            }
        }
    }

    /**
     * A file stream is 'active' after the server uses it to request a file from the agent at the other end.
     * This implementation uses {@link FileBuffer} to temporary store the file received and at the same time make it
     * available as input stream.
     */
    private static class ActiveStreamImpl
        implements AgentFileStreamService.ActiveStream, StreamObserverDelegate<AgentJobFileMessage> {
        private final String jobId;
        private final Path relativePath;
        private final long expectedDataSize;
        private final GRpcJobFileServiceImpl.DelegatingStreamObserver delegatingStreamObserver;
        private final StreamObserver<ServerJobFileMessage> responseObserver;
        private final OutputStream outputStream;
        private final AtomicBoolean closed = new AtomicBoolean();
        private long bytesReceived;
        private final FileBuffer fileBuffer;

        ActiveStreamImpl(
            final String jobId,
            final Path relativePath,
            final long startOffset,
            final long endOffset,
            final DelegatingStreamObserver delegatingStreamObserver,
            final StreamObserver<ServerJobFileMessage> responseObserver,
            final FileBuffer fileBuffer
        ) {
            this.jobId = jobId;
            this.relativePath = relativePath;
            this.outputStream = fileBuffer.getOutputStream();
            this.expectedDataSize = endOffset - startOffset;
            this.delegatingStreamObserver = delegatingStreamObserver;
            this.responseObserver = responseObserver;
            this.fileBuffer = fileBuffer;

            this.delegatingStreamObserver.setDelegate(this);

            final ServerJobFileMessage request = ServerJobFileMessage.newBuilder()
                .setFileStreamRequest(
                    FileStreamRequest.newBuilder()
                        .setRelativePath(relativePath.toString())
                        .setStartOffset(startOffset)
                        .setEndOffset(endOffset)
                        .build()
                )
                .build();
            this.responseObserver.onNext(request);

            log.info(
                "Activated stream for job id: {} to request file: {} [{},{}]",
                getJobId(),
                relativePath,
                startOffset,
                endOffset
            );
        }

        @Override
        public void handleMessage(
            final StreamObserver<AgentJobFileMessage> streamObserver,
            final AgentJobFileMessage value
        ) {
            if (value.getMessageCase() != AgentJobFileMessage.MessageCase.FILE_STREAM_CHUNK) {
                log.warn("Received unexpected message type: {}", value.getMessageCase().name());
                this.close();
            } else {
                final FileStreamChunk chunkMessage = value.getFileStreamChunk();
                final byte[] chunkBytes = chunkMessage.getFileChunk().toByteArray();
                log.debug(
                    "Received chunk for job: {}, file: {} ({} bytes)",
                    this.jobId,
                    this.relativePath,
                    chunkBytes.length
                );
                try {
                    // Write bytes in output stream
                    this.outputStream.write(chunkBytes);
                    this.outputStream.flush();
                    this.bytesReceived += chunkBytes.length;
                } catch (IOException e) {
                    this.close();
                    return;
                }

                // Send ACK
                this.responseObserver.onNext(
                    ServerJobFileMessage.newBuilder()
                        .setFileStreamAck(FileStreamAck.getDefaultInstance())
                        .build()
                );
                log.debug("Total bytes received and acknowledged: {}/{}", this.bytesReceived, this.expectedDataSize);
            }
        }

        @Override
        public void handleError(final StreamObserver<AgentJobFileMessage> streamObserver, final Throwable t) {
            log.error("Stream error for job: {}, file: {}: {}", jobId, relativePath, t.getMessage(), t);
            this.close();
        }

        @Override
        public void handleCompleted(final StreamObserver<AgentJobFileMessage> streamObserver) {

            if (bytesReceived < expectedDataSize) {
                log.error(
                    "Stream completed for job: {}, file: {}: after only {}/{} bytes received",
                    jobId,
                    relativePath,
                    this.bytesReceived,
                    this.expectedDataSize
                );
            } else {
                log.info("Stream closed, file received successfully");
            }
            this.close();
        }

        @Override
        public @NotNull Path getRelativePath() {
            return this.relativePath;
        }

        @Override
        public @NotBlank String getJobId() {
            return this.jobId;
        }

        @Override
        public InputStream getInputStream() {
            return this.fileBuffer.getInputStream();
        }

        @Override
        public void close() {
            if (!this.closed.getAndSet(true)) {
                // Complete the stream
                this.responseObserver.onCompleted();
                // Remove self as delegate
                this.delegatingStreamObserver.setDelegate(null);
                // Close file buffer and streams
                try {
                    this.outputStream.close();
                } catch (IOException e) {
                    log.error(
                        "Failed to close output stream of job: {}, file: {}: {}",
                        jobId,
                        relativePath,
                        e.getMessage(),
                        e
                    );
                }
            }
        }
    }

    /**
     * A stream observer that delegates the stream events to a delegate object.
     * The latter can be updated over time.
     * In this specific case, the target is initially an uninitialized stream, then a ready one, then an active one.
     */
    private static class DelegatingStreamObserver implements StreamObserver<AgentJobFileMessage> {

        private final AtomicReference<StreamObserverDelegate<AgentJobFileMessage>> delegate = new AtomicReference<>();

        DelegatingStreamObserver(final StreamObserverDelegate<AgentJobFileMessage> delegate) {
            this.delegate.set(delegate);
        }

        StreamObserverDelegate<AgentJobFileMessage> setDelegate(
            @Nullable final StreamObserverDelegate<AgentJobFileMessage> newDelegate
        ) {
            return this.delegate.getAndSet(newDelegate);
        }

        @Override
        public void onNext(final AgentJobFileMessage value) {
            final StreamObserverDelegate<AgentJobFileMessage> currentDelegate = this.delegate.get();
            if (currentDelegate != null) {
                currentDelegate.handleMessage(this, value);
            }
        }

        @Override
        public void onError(final Throwable t) {
            final StreamObserverDelegate<AgentJobFileMessage> currentDelegate = this.delegate.get();
            if (currentDelegate != null) {
                currentDelegate.handleError(this, t);
            }
        }

        @Override
        public void onCompleted() {
            final StreamObserverDelegate<AgentJobFileMessage> currentDelegate = this.delegate.get();
            if (currentDelegate != null) {
                currentDelegate.handleCompleted(this);
            }
        }
    }
}
