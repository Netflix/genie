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
package com.netflix.genie.agent.execution.services.impl.grpc;

import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.genie.agent.execution.services.AgentJobKillService;
import com.netflix.genie.agent.execution.services.KillService;
import com.netflix.genie.agent.properties.JobKillServiceProperties;
import com.netflix.genie.common.internal.util.ExponentialBackOffTrigger;
import com.netflix.genie.proto.JobKillRegistrationRequest;
import com.netflix.genie.proto.JobKillRegistrationResponse;
import com.netflix.genie.proto.JobKillServiceGrpc;
import jakarta.validation.constraints.NotBlank;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.TaskScheduler;
import java.util.concurrent.CancellationException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Implementation of the {@link AgentJobKillService} that listens for kill signals from the server
 * using event listeners.
 * <p>
 * This implementation works by sending a unary gRPC request to the server, which holds the response
 * until it has a kill signal to send or until the request times out. It attaches event listeners
 * to the response future, which automatically process the response when it arrives.
 * If a request fails, a periodic task with exponential backoff is used to retry creating a new request.
 * The implementation properly handles request cancellation during shutdown.
 * <p>
 * Note: this implementation still suffers from a limitation: because it uses unary gRPC calls,
 * the server will never realize the client is gone if the connection is broken. This can lead to an accumulation of
 * parked calls on the server if clients disconnect abnormally. A protocol based on bidirectional streaming
 * would be necessary to fully solve this problem.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class GRpcAgentJobKillServiceImpl implements AgentJobKillService {

    private final JobKillServiceGrpc.JobKillServiceFutureStub client;
    private final KillService killService;
    private final TaskScheduler taskScheduler;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final JobKillServiceProperties properties;
    private final ExponentialBackOffTrigger trigger;

    // Using volatile to ensure visibility across threads
    private volatile PeriodicTask periodicTask;
    private volatile ScheduledFuture<?> periodicTaskScheduledFuture;
    private String jobId;

    /**
     * Constructor.
     *
     * @param client        The gRPC client to use to call the server
     * @param killService   KillService for killing the agent
     * @param taskScheduler A task scheduler
     * @param properties    The service properties
     */
    public GRpcAgentJobKillServiceImpl(
        final JobKillServiceGrpc.JobKillServiceFutureStub client,
        final KillService killService,
        final TaskScheduler taskScheduler,
        final JobKillServiceProperties properties
    ) {
        this.client = client;
        this.killService = killService;
        this.taskScheduler = taskScheduler;
        this.properties = properties;
        this.trigger = new ExponentialBackOffTrigger(this.properties.getResponseCheckBackOff());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void start(@NotBlank(message = "Job id cannot be blank") final String requestedJobId) {
        this.jobId = requestedJobId;

        if (started.compareAndSet(false, true)) {
            log.info("Starting job kill service for job: {}", requestedJobId);

            // Create the periodic task
            this.periodicTask = new PeriodicTask(client, killService, requestedJobId, this.trigger);

            // Run once immediately
            this.periodicTask.run();

            // Then run on task scheduler periodically
            this.periodicTaskScheduledFuture = this.taskScheduler.schedule(this.periodicTask, this.trigger);
        } else {
            log.debug("Service already running for job: {}, ignoring start request", this.jobId);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void stop() {
        if (this.started.compareAndSet(true, false)) {
            log.info("Stopping job kill service for job: {}", this.jobId);

            // Cancel the scheduled periodic task
            final ScheduledFuture<?> scheduledFutureToCancel = this.periodicTaskScheduledFuture;
            if (scheduledFutureToCancel != null) {
                scheduledFutureToCancel.cancel(true);
                this.periodicTaskScheduledFuture = null;
            }

            final PeriodicTask taskToCancel = this.periodicTask;
            if (taskToCancel != null) {
                taskToCancel.cancelPendingRequest();
                this.periodicTask = null;
            }
        }
    }

    /**
     * Task that periodically checks for kill requests and handles responses.
     */
    private static final class PeriodicTask implements Runnable {
        private final JobKillServiceGrpc.JobKillServiceFutureStub client;
        private final KillService killService;
        private final String jobId;
        private final ExponentialBackOffTrigger trigger;

        // Using AtomicReference for thread-safe updates
        private final AtomicReference<ListenableFuture<JobKillRegistrationResponse>> pendingKillRequestFutureRef =
            new AtomicReference<>();

        /**
         * Constructor.
         *
         * @param client      The gRPC client
         * @param killService The kill service
         * @param jobId       The job ID
         * @param trigger     The exponential backoff trigger
         */
        PeriodicTask(
            final JobKillServiceGrpc.JobKillServiceFutureStub client,
            final KillService killService,
            final String jobId,
            final ExponentialBackOffTrigger trigger
        ) {
            this.client = client;
            this.killService = killService;
            this.jobId = jobId;
            this.trigger = trigger;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run() {
            try {
                this.periodicCheckForKill();
            } catch (final Throwable t) {
                log.error("Error in periodic kill check task: {}", t.getMessage(), t);
            }
        }

        /**
         * Check for kill requests and create new ones if needed.
         */
        private void periodicCheckForKill() {
            // Get the current future atomically
            final ListenableFuture<JobKillRegistrationResponse> currentFuture = pendingKillRequestFutureRef.get();

            // Create a new request if there isn't one already
            if (currentFuture == null) {
                createNewKillRequest();
            } else if (currentFuture.isDone()) {
                // Request is done, clear the reference and create a new request
                pendingKillRequestFutureRef.compareAndSet(currentFuture, null);
                createNewKillRequest();
            } else {
                // Request is still pending, nothing to do
                log.debug("Kill request still pending for job: {}", jobId);
            }
        }

        /**
         * Create a new kill notification request.
         */
        private void createNewKillRequest() {
            // Create the request
            final JobKillRegistrationRequest request = JobKillRegistrationRequest.newBuilder()
                .setJobId(jobId)
                .build();

            // Send request and store the future atomically
            final ListenableFuture<JobKillRegistrationResponse> future =
                this.client.registerForKillNotification(request);
            pendingKillRequestFutureRef.set(future);

            // Add listener to handle response
            future.addListener(() -> {
                try {
                    // Wait for the response
                    future.get();

                    // Process the kill signal
                    log.info("Received kill signal from server for job: {}", jobId);
                    killService.kill(KillService.KillSource.API_KILL_REQUEST);

                    // Reset trigger for next request
                    trigger.reset();
                } catch (CancellationException e) {
                    log.debug("Kill request was cancelled for job: {}", jobId);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.debug("Kill request interrupted for job: {}", jobId);
                } catch (final ExecutionException e) {
                    final Throwable cause = e.getCause() != null ? e.getCause() : e;
                    log.debug("Kill request failed for job {}: {}", jobId, cause.getMessage());

                    // Reset trigger to retry sooner
                    trigger.reset();
                } finally {
                    // Clear the reference if it still points to this future
                    pendingKillRequestFutureRef.compareAndSet(future, null);
                }
            }, Runnable::run);

            log.debug("Created new kill notification request for job: {}", jobId);
        }

        /**
         * Cancel any pending kill request.
         */
        void cancelPendingRequest() {
            // Atomically get and clear the reference
            final ListenableFuture<JobKillRegistrationResponse> futureToCancel =
                pendingKillRequestFutureRef.getAndSet(null);

            if (futureToCancel != null && !futureToCancel.isDone()) {
                final boolean cancelled = futureToCancel.cancel(true);
                log.debug("Cancelled pending kill request for job {}: {}", jobId,
                    cancelled ? "success" : "failed");
            }
        }
    }
}
