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
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.TaskScheduler;

import javax.validation.constraints.NotBlank;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implementation of the {@link AgentJobKillService}, listens for kill coming from server using long-polling.
 * <p>
 * Note: this implementation still suffers from a serious flaw: because it is implemented with a unary call,
 * the server will never realize the client is gone if the connection is broken. This can lead to an accumulation of
 * parked calls on the server. A new protocol (based on a bidirectional stream) is necessary to solve this problem.
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
    private ScheduledFuture<?> periodicTaskScheduledFuture;

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
    public synchronized void start(@NotBlank(message = "Job id cannot be blank") final String jobId) {
        //Service can be started only once
        if (started.compareAndSet(false, true)) {
            final PeriodicTask periodicTask = new PeriodicTask(client, killService, jobId, this.trigger);

            // Run once immediately
            periodicTask.run();

            // Then run on task scheduler periodically
            this.periodicTaskScheduledFuture = this.taskScheduler.schedule(periodicTask, this.trigger);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        if (this.started.compareAndSet(true, false)) {
            this.periodicTaskScheduledFuture.cancel(true);
            this.periodicTaskScheduledFuture = null;
        }
    }

    private static final class PeriodicTask implements Runnable {
        private final JobKillServiceGrpc.JobKillServiceFutureStub client;
        private final KillService killService;
        private final String jobId;
        private final ExponentialBackOffTrigger trigger;
        private ListenableFuture<JobKillRegistrationResponse> pendingKillRequestFuture;

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
            } catch (Throwable t) {
                log.error("Error in periodic kill check task: {}", t.getMessage(), t);
            }
        }

        private void periodicCheckForKill() throws InterruptedException {

            if (this.pendingKillRequestFuture == null) {
                // No pending kill request, create one
                this.pendingKillRequestFuture = this.client
                    .registerForKillNotification(
                        JobKillRegistrationRequest.newBuilder()
                            .setJobId(jobId)
                            .build()
                    );
            }

            if (!this.pendingKillRequestFuture.isDone()) {
                // Still waiting for a kill, nothing to do
                log.debug("Kill request still pending");

            } else {
                // Kill response received or error
                JobKillRegistrationResponse killResponse = null;
                Throwable exception = null;

                try {
                    killResponse = this.pendingKillRequestFuture.get();
                } catch (ExecutionException e) {
                    log.warn("Kill request failed");
                    exception = e.getCause() != null ? e.getCause() : e;
                }

                // Delete current pending so a new one will be re-created
                this.pendingKillRequestFuture = null;

                if (killResponse != null) {
                    log.info("Received kill signal from server");
                    this.killService.kill(KillService.KillSource.API_KILL_REQUEST);
                }

                if (exception != null) {
                    log.warn("Kill request produced an error", exception);
                    // Re-schedule this task soon
                    this.trigger.reset();
                }
            }
        }
    }
}
