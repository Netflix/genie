/*
 *
 *  Copyright 2018 Netflix, Inc.
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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.genie.common.internal.dto.v4.files.JobFileState;
import com.netflix.genie.proto.BeginAcknowledgement;
import com.netflix.genie.proto.BeginSync;
import com.netflix.genie.proto.DataUpload;
import com.netflix.genie.proto.DeleteFile;
import com.netflix.genie.proto.JobDirectoryState;
import com.netflix.genie.proto.JobFileSyncServiceGrpc;
import com.netflix.genie.proto.ResetSync;
import com.netflix.genie.proto.SyncAcknowledgement;
import com.netflix.genie.proto.SyncComplete;
import com.netflix.genie.proto.SyncRequest;
import com.netflix.genie.proto.SyncRequestResult;
import com.netflix.genie.proto.SyncResponse;
import com.netflix.genie.web.properties.GRpcServerProperties;
import com.netflix.genie.web.properties.JobFileSyncRpcProperties;
import com.netflix.genie.web.services.JobFileService;
import io.grpc.stub.StreamObserver;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.devh.springboot.autoconfigure.grpc.server.GrpcService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.TaskScheduler;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Implementation of the gRPC Job File Sync interface for syncing job files from agent to server via bi-directional
 * connection.
 *
 * @author tgianos
 * @since 4.0.0
 */
@ConditionalOnProperty(value = GRpcServerProperties.ENABLED_PROPERTY, havingValue = "true")
@GrpcService(value = JobFileSyncServiceGrpc.class)
@Slf4j
public class GRpcJobFileSyncServiceImpl extends JobFileSyncServiceGrpc.JobFileSyncServiceImplBase {

    private final JobFileSyncRpcProperties jobFileSyncRpcProperties;
    private final JobFileService jobFileService;
    private final ScheduledFuture<?> ackFuture;

    private final ConcurrentMap<String, JobFileSyncObserver> jobSyncRequestObservers = Maps.newConcurrentMap();

    /**
     * Constructor.
     *
     * @param jobFileSyncProperties The properties that configure how the sync server behaves
     * @param jobFileService        The log service to use to interact with the server side job directory
     * @param taskScheduler         The task scheduler to use
     */
    public GRpcJobFileSyncServiceImpl(
        final JobFileSyncRpcProperties jobFileSyncProperties,
        final JobFileService jobFileService,
        final TaskScheduler taskScheduler
    ) {
        this.jobFileSyncRpcProperties = jobFileSyncProperties;
        this.jobFileService = jobFileService;

        this.ackFuture = taskScheduler.scheduleWithFixedDelay(
            this::executeObserverAcknowledgements,
            this.jobFileSyncRpcProperties.getAckIntervalMilliseconds()
        );
    }

    /**
     * Sync job files from an agent to the server.
     *
     * @param responseObserver The observer to use to send responses to periodically
     * @return An observer implementation to handle messages from the agent
     */
    @Override
    public StreamObserver<SyncRequest> sync(final StreamObserver<SyncResponse> responseObserver) {
        return new JobFileSyncObserverImpl(
            this.jobFileSyncRpcProperties,
            responseObserver,
            this.jobFileService,
            this::addJobFileSyncObserver,
            this::removeJobFileSyncObserver
        );
    }

    /**
     * Perform any cleanup necessary at the end of this instances lifecycle.
     */
    @PreDestroy
    public void cleanup() {
        if (this.ackFuture != null && !this.ackFuture.isDone()) {
            // TODO: Perhaps we loop here until we're sure it's cancelled using isCancelled()?
            log.debug("Attempting to cancel the job file sync acknowledgement thread");
            if (!this.ackFuture.cancel(false)) {
                log.error("Unable to cancel the job file sync acknowledgement thread");
            } else {
                log.debug("Cancelled the job file sync acknowledgement thread");
            }
        }
    }

    /**
     * Activate any cleanup necessary when an agent detaches.
     *
     * @param jobId The id of the job the agent was responsible for
     */
    public void onAgentDetached(final String jobId /* TODO: This will be event type eventually likely */) {
        // TODO: Possible race condition here if this is invoked after a new response observer has already been added
        //       for the same job. Just putting this here as temporary logic till we figure that out
        final JobFileSyncObserver observer = this.jobSyncRequestObservers.get(jobId);
        if (observer != null) {
            // Cleanup will invoke the cleanup callback and remove it from the map
            observer.cleanup();
        }
    }

    /**
     * Used as a callback to add it to a map of job id to observers this class is tracking.
     *
     * @param observer The observer from which to get the job id
     * @throws IllegalArgumentException if the job id hasn't yet been populated in the observer before this is called
     */
    private void addJobFileSyncObserver(final JobFileSyncObserver observer) {
        // TODO: Related to the race condition described in on agent detached what happens when a new observer with the
        //       same job id is done. We might need to remove/cleanup the old one proactively? Messy. Could possibly
        //       use the creation time of a observer to always evict the oldest on the assumption if there is a newer
        //       one for the same job id it is the only active one from perspective of the agent?
        this.jobSyncRequestObservers.put(
            observer.getJobId().orElseThrow(() -> new IllegalArgumentException("Job id not yet set in observer")),
            observer
        );
    }

    /**
     * Used as a callback to remove it from the map of job id to observers this class is tracking on completion of
     * work by the observer.
     *
     * @param observer The observer to remove
     */
    private void removeJobFileSyncObserver(final JobFileSyncObserver observer) {
        observer.getJobId().ifPresent(
            jobId -> {
                if (this.jobSyncRequestObservers.remove(jobId, observer)) {
                    log.debug("Successfully removed observer with id {} for job {}", observer.getId(), jobId);
                } else {
                    log.debug("Failed to remove observer with id {} for job {}", observer.getId(), jobId);
                }
            }
        );
    }

    /**
     * Send acknowledgement messages from all the observers.
     */
    private void executeObserverAcknowledgements() {
        log.debug("Invoking job file sync request observers send acknowledgement methods");
        this.jobSyncRequestObservers
            .values()
            .parallelStream()
            .forEach(JobFileSyncObserver::sendSyncAckMessageIfNecessary);
    }

    /**
     * Interface to define a contract that a Genie Job Request Observer should adhere to in order to provide external
     * control and monitoring by services.
     *
     * @author tgianos
     * @since 4.0.0
     */
    interface JobFileSyncObserver {
        /**
         * Get the unique identifier of the observer.
         *
         * @return the unique identifier
         */
        String getId();

        /**
         * Get the ID of the job this observer is responsible for. May or may not be set yet.
         *
         * @return {@link Optional} of the job id if it has been set otherwise {@link Optional#empty()}
         */
        Optional<String> getJobId();

        /**
         * Perform any necessary cleanup of this observer. Should be able to be called multiple times.
         */
        void cleanup();

        /**
         * Force this observer to send an acknowledgement message if there is data to send.
         */
        void sendSyncAckMessageIfNecessary();
    }

    /**
     * Main logic for dealing with messages from the agent.
     * <p>
     * Expected healthy workflow
     * <p>
     * 1. {@link BeginSync} message is received and ownership of job is established
     * 2. One to N mix of {@link DataUpload} or {@link DeleteFile} messages are received and acknowledged either when
     * the number of received messages reaches a limit or some amount of time has passed and a separate thread
     * invokes acknowledgement
     * - Acknowledgements are sent as {@link SyncResponse} messages containing a {@link SyncAcknowledgement}
     * instances
     * - The messages are synced to the provided {@link JobFileService} instance for storage in final location
     * - Success and failure is captured based on results of calls to the {@link JobFileService} methods
     * 3. {@link SyncComplete} message is received when the client (agent) is done processing files in its local
     * environment. This sync complete will clean up everything locally and kick off side effect processes
     * <p>
     * Reconnect workflow
     * <p>
     * 1. Any message other than {@link BeginSync} is received by this instance first
     * 2. A {@link ResetSync} message is sent to the client (agent) and all messages until a {@link BeginSync} is
     * received are ignored
     * - Only one {@link ResetSync} message is sent no matter how many other messages that aren't {@link BeginSync}
     * are received before the client sends a {@link BeginSync}
     * 3. Once the {@link BeginSync} message is received processing continues as per the healthy workflow
     *
     * @author tgianos
     * @since 4.0.0
     */
    @Slf4j
    @EqualsAndHashCode(of = {"id"}, doNotUseGetters = true)
    @SuppressWarnings("checkstyle:finalclass")
    private static class JobFileSyncObserverImpl implements StreamObserver<SyncRequest>, JobFileSyncObserver {

        @Getter
        private final String id = UUID.randomUUID().toString();
        private final Object messagesLock = new Object();

        // Use lists to maintain order
        private final List<SyncRequestResult> requestResults = Lists.newArrayList();

        private final StreamObserver<SyncResponse> responseObserver;
        private final JobFileService jobFileService;
        private final Consumer<JobFileSyncObserver> jobIdPopulatedCallback;
        private final Consumer<JobFileSyncObserver> completionCallback;
        private final AtomicBoolean cleanedUp = new AtomicBoolean(false);
        private final int maxSyncMessages;
        private boolean waitingForBeginMessage = true;
        private boolean sentResetMessage; // default false
        private String jobId;

        private JobFileSyncObserverImpl(
            final JobFileSyncRpcProperties jobFileSyncRpcProperties,
            final StreamObserver<SyncResponse> responseObserver,
            final JobFileService jobFileService,
            final Consumer<JobFileSyncObserver> jobIdPopulatedCallback,
            final Consumer<JobFileSyncObserver> completionCallback
        ) {
            this.responseObserver = responseObserver;
            this.jobFileService = jobFileService;
            this.jobIdPopulatedCallback = jobIdPopulatedCallback;
            this.completionCallback = completionCallback;
            this.maxSyncMessages = jobFileSyncRpcProperties.getMaxSyncMessages();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onNext(final SyncRequest request) {
            try {
                if (request.hasBeginSync()) {
                    this.handleBeginSync(request.getBeginSync());
                } else if (request.hasDataUpload()) {
                    this.handleDataUpload(request.getDataUpload());
                } else if (request.hasDeleteFile()) {
                    this.handleDeleteFile(request.getDeleteFile());
                } else if (request.hasSyncComplete()) {
                    this.handleSyncComplete(request.getSyncComplete());
                } else {
                    // Unrecognized message type
                    log.error("Received unknown message type {}", request);
                }
            } catch (final IOException e) {
                // TODO: Should probably catch every kind of exception here but findbugs yelling at me late on a Friday
                //       stash for now
                log.error("Error for upload request {}", request, e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onError(final Throwable t) {
            this.cleanup();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onCompleted() {
            this.cleanup();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Optional<String> getJobId() {
            return Optional.ofNullable(this.jobId);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void cleanup() {
            // Cleanup should only be called once
            if (!this.cleanedUp.getAndSet(true)) {
                log.debug("Cleaning up");
                this.completionCallback.accept(this);
                log.debug("Cleaned up");
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void sendSyncAckMessageIfNecessary() {
            synchronized (this.messagesLock) {
                // Only send if we have something to send
                if (!this.requestResults.isEmpty()) {
                    log.debug("Sending sync acknowledgment for messages {}", this.requestResults);

                    this.responseObserver.onNext(
                        SyncResponse
                            .newBuilder()
                            .setSyncAck(
                                SyncAcknowledgement
                                    .newBuilder()
                                    .addAllResults(this.requestResults)
                                    .build()
                            )
                            .build()
                    );

                    this.requestResults.clear();
                }
            }
        }

        private void handleBeginSync(final BeginSync beginSync) throws IOException {
            // Handle the begin message provided we're still waiting for it
            if (this.waitingForBeginMessage) {
                this.jobId = beginSync.getJobId();

                if (StringUtils.isBlank(this.jobId)) {
                    // TODO: We probably should return an error to the client here but for now...
                    throw new IllegalArgumentException("No job id provided to sync service. Unable to continue");
                }

                log.debug("Beginning to sync job files for job {}", this.jobId);

                // Call back to the listening method when we know what job this listener is responsible for
                this.jobIdPopulatedCallback.accept(this);

                // TODO: Here we need to get the acknowledged agent side directory state and compare it to
                //       the server side state to know what we should "block" requests for if we want to

                final boolean includeMd5 = false;

                final Set<JobFileState> jobFileStates
                    = this.jobFileService.getJobDirectoryFileState(this.jobId, includeMd5);

                // Set the flag to indicate that we should stop ignoring data upload messages
                this.waitingForBeginMessage = false;

                // Tell the agent server is ready for it to send files and give it the state of the job directory
                // on the server so it can determine where to start sending files from to save on redundant
                // sending of data
                this.responseObserver.onNext(
                    SyncResponse
                        .newBuilder()
                        .setBeginAck(
                            BeginAcknowledgement
                                .newBuilder()
                                .setServerDirectoryState(
                                    JobDirectoryState
                                        .newBuilder()
                                        .setIncludesMd5(includeMd5)
                                        .addAllFiles(
                                            jobFileStates
                                                .stream()
                                                .map(
                                                    file ->
                                                        com.netflix.genie.proto.JobFileState
                                                            .newBuilder()
                                                            .setPath(file.getPath())
                                                            .setSize(file.getSize())
                                                            .build()
                                                )
                                                .collect(Collectors.toSet())
                                        )
                                        .build())
                                .build()
                        )
                        .build()
                );
            } else {
                log.warn(
                    "Received a {} message after one had already been received",
                    BeginSync.class.getCanonicalName()
                );
            }
        }

        private void handleDataUpload(final DataUpload dataUpload) {
            final String messageId = dataUpload.getId();
            log.debug("Received data upload message with id {} for job {}", messageId, this.jobId);

            // Write some data and save the message in the ack buffer
            if (this.waitingForBeginMessage) {
                log.debug(
                    "Haven't received a {} message. Ignoring data upload message {}.",
                    BeginSync.class.getCanonicalName(),
                    messageId
                );
                this.sendResetMessageIfNecessary();
            } else {
                try {
                    this.jobFileService.updateFile(
                        this.jobId,
                        dataUpload.getPath(),
                        dataUpload.getStartByte(),
                        // TODO: There are other methods here to go right to a ByteBuffer we might want to explore
                        dataUpload.getData().toByteArray()
                    );

                    synchronized (this.messagesLock) {
                        this.requestResults.add(this.createRequestResult(messageId, true));
                    }
                } catch (final Exception e) {
                    // For some reason saving the log failed. Mark this as a failed message
                    log.error(
                        "Unable to save data for job {} from message {} due to {}",
                        this.jobId,
                        messageId,
                        e.getMessage(),
                        e
                    );

                    synchronized (this.messagesLock) {
                        this.requestResults.add(this.createRequestResult(messageId, false));
                    }
                }

                this.checkIfShouldSendAck();
            }
        }

        private void handleDeleteFile(final DeleteFile deleteFile) {
            final String messageId = deleteFile.getId();
            log.debug("Received file delete message {} in job {}", messageId, this.jobId);

            if (this.waitingForBeginMessage) {
                log.debug(
                    "Haven't received a {} message. Ignoring file delete message {}.",
                    BeginSync.class.getCanonicalName(),
                    messageId
                );
                this.sendResetMessageIfNecessary();
            } else {
                try {
                    this.jobFileService.deleteJobFile(this.jobId, deleteFile.getPath());

                    synchronized (this.messagesLock) {
                        this.requestResults.add(this.createRequestResult(messageId, true));
                    }
                } catch (final Exception e) {
                    log.error(
                        "Deleting {} for job {} failed due to {}",
                        deleteFile.getPath(),
                        this.jobId,
                        e.getMessage(),
                        e
                    );

                    synchronized (this.messagesLock) {
                        this.requestResults.add(this.createRequestResult(messageId, false));
                    }
                }
            }

            this.checkIfShouldSendAck();
        }

        private void handleSyncComplete(final SyncComplete syncComplete) throws IOException {
            if (this.waitingForBeginMessage) {
                log.debug(
                    "Haven't received a {} message. Ignoring file sync complete message {}.",
                    BeginSync.class.getCanonicalName(),
                    syncComplete
                );
                this.sendResetMessageIfNecessary();
            } else {
                log.debug("Job file synchronization from agent for job {} is complete.", this.jobId);

                // TODO: We really might not need to do this, though what happened if there were failures
                //       At this point the agent thinks it's done syncing and can shut down. If we send back
                //       some failure messages there's not really much sense in it retrying. Perhaps we build in
                //       a different message for this end case to do one more set of exchanges.
                this.sendSyncAckMessageIfNecessary();

                // Stop the watcher thread
                this.cleanup();

                final JobDirectoryState agentJobDirectoryState = syncComplete.getFinalAgentDirectoryState();
                final boolean includeMd5 = agentJobDirectoryState.getIncludesMd5();
                final Set<JobFileState> agentJobFileStates = agentJobDirectoryState
                    .getFilesList()
                    .stream()
                    .map(
                        jobFileState ->
                            new JobFileState(
                                jobFileState.getPath(),
                                jobFileState.getSize(),
                                includeMd5 ? jobFileState.getMd5() : null
                            )
                    )
                    .collect(Collectors.toSet());

                final Set<JobFileState> serverJobFileStates
                    = this.jobFileService.getJobDirectoryFileState(this.jobId, includeMd5);

                if (!agentJobFileStates.equals(serverJobFileStates)) {
                    log.warn(
                        "After the agent finished syncing job files for job {} the state of the files on the "
                            + "server {} is different than the supplied state of the files on the agent {}",
                        this.jobId,
                        serverJobFileStates,
                        agentJobFileStates
                    );

                    // TODO: Probably invoke some service API which will in the background download from long term
                    //       storage location into a tmp location and then swap it into place for existing
                    //       job directory locally
                }

                // TODO: Should invoke some log upload service here to backup the completed logs somewhere OR
                //       if we've been pushing logs to long term archival in the background signal that these are done
                //       and can be closed
            }
        }

        private void sendResetMessageIfNecessary() {
            if (!this.sentResetMessage) {
                log.debug("Sending job file sync reset message to agent");
                this.responseObserver.onNext(
                    SyncResponse
                        .newBuilder()
                        .setReset(ResetSync.newBuilder().build())
                        .build()
                );
                this.sentResetMessage = true;
            }
        }

        private void checkIfShouldSendAck() {
            boolean sendAck = false;
            synchronized (this.messagesLock) {
                if (this.requestResults.size() == this.maxSyncMessages) {
                    sendAck = true;
                }
            }

            if (sendAck) {
                // Proactively send ACK don't wait for timed thread
                this.sendSyncAckMessageIfNecessary();
            }
        }

        private SyncRequestResult createRequestResult(final String messageId, final boolean successful) {
            return SyncRequestResult.newBuilder().setId(messageId).setSuccessful(successful).build();
        }
    }
}
