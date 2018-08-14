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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.genie.agent.execution.services.AgentJobKillService;
import com.netflix.genie.agent.execution.services.KillService;
import com.netflix.genie.proto.JobKillRegistrationRequest;
import com.netflix.genie.proto.JobKillRegistrationResponse;
import com.netflix.genie.proto.JobKillServiceGrpc;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Service;

import javax.validation.constraints.NotBlank;

/**
 * Implementation of the {@link AgentJobKillService}.
 *
 * @author standon
 * @since 4.0.0
 */
@Lazy
@Service
@Slf4j
public class GRpcAgentJobKillServiceImpl implements AgentJobKillService {

    private final JobKillServiceGrpc.JobKillServiceFutureStub client;
    private final KillService killService;
    private ListenableFuture<JobKillRegistrationResponse> jobKillFuture;
    private boolean started;
    private final TaskExecutor killTaskExecutor;


    /**
     * Constructor.
     *
     * @param client           The gRPC client to use to call the server
     * @param killService      KillService for killing the agent
     * @param killTaskExecutor A task executor to execute killing the agent
     */
    public GRpcAgentJobKillServiceImpl(
        final JobKillServiceGrpc.JobKillServiceFutureStub client,
        final KillService killService,
        @Qualifier("sharedAgentTaskExecutor") final TaskExecutor killTaskExecutor
    ) {
        this.client = client;
        this.killService = killService;
        this.killTaskExecutor = killTaskExecutor;
    }

    @Override
    public synchronized void start(@NotBlank(message = "Job id cannot be blank") final String jobId) {
        //Service can be started only once
        if (started) {
           throw new IllegalStateException("Service can be started only once");
        }

        registerForRemoteKillNotification(jobId);
        started = true;
    }

    @Override
    public void stop() {
        cancelPreviousAndUpdateJobKillFuture(null);
    }

    private void registerForRemoteKillNotification(final String jobId) {

        final ListenableFuture<JobKillRegistrationResponse> future = this.client
            .registerForKillNotification(
                JobKillRegistrationRequest.newBuilder()
                    .setJobId(jobId)
                    .build()
            );

        cancelPreviousAndUpdateJobKillFuture(future);

        Futures.addCallback(
            future,
            new JobKillFutureCallback(jobId),
            this.killTaskExecutor
        );
    }

    /* Cancel the current future */
    private synchronized void cancelJobKillFuture() {
        if (this.jobKillFuture != null) {
            this.jobKillFuture.cancel(false);
        }
    }

    /* Cancel current future and store the new future */
    private synchronized void cancelPreviousAndUpdateJobKillFuture(
        final ListenableFuture<JobKillRegistrationResponse> killFuture
    ) {
        cancelJobKillFuture();
        this.jobKillFuture = killFuture;
    }

    private class JobKillFutureCallback implements FutureCallback<JobKillRegistrationResponse> {

        private final String jobId;

        JobKillFutureCallback(final String jobId) {
            this.jobId = jobId;
        }

        @Override
        public void onSuccess(final JobKillRegistrationResponse result) {
            log.info("Received kill signal from server");
            killService.kill(KillService.KillSource.API_KILL_REQUEST);
        }

        @Override
        public void onFailure(final Throwable t) {
            registerForRemoteKillNotification(this.jobId);
        }
    }
}
