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

import com.netflix.genie.agent.execution.services.AgentHeartBeatService;
import com.netflix.genie.proto.AgentHeartBeat;
import com.netflix.genie.proto.HeartBeatServiceGrpc;
import com.netflix.genie.proto.ServerHeartBeat;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotBlank;
import java.time.Instant;
import java.util.concurrent.ScheduledFuture;

/**
 * gRPC implementation of AgentHeartBeatService.
 * Sends heartbeats to the server.
 * Transparently handles disconnections and stream errors by establishing a new stream.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Component
@Lazy
@Slf4j
@Validated
class GrpcAgentHeartBeatServiceImpl implements AgentHeartBeatService {

    private static final long HEART_BEAT_PERIOD_MILLIS = 2_000L; //TODO make configurable
    private static final long STREAM_RESET_DELAY_MILLIS = 1_000L; //TODO make configurable

    private final HeartBeatServiceGrpc.HeartBeatServiceStub client;
    private final TaskScheduler taskScheduler;

    private boolean isConnected;
    private StreamObserver<AgentHeartBeat> requestObserver;
    private ScheduledFuture<?> heartbeatFuture;
    private String claimedJobId;
    private AgentHeartBeat heartBeatMessage;

    GrpcAgentHeartBeatServiceImpl(
        final HeartBeatServiceGrpc.HeartBeatServiceStub client,
        @Qualifier("heartBeatServiceTaskExecutor") final TaskScheduler taskScheduler
    ) {
        this.client = client;
        this.taskScheduler = taskScheduler;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void start(@NotBlank final String jobId) {
        if (StringUtils.isNotBlank(this.claimedJobId)) {
            throw new IllegalStateException("Previously started with a different job id");
        }

        this.claimedJobId = jobId;
        this.heartBeatMessage = AgentHeartBeat.newBuilder()
            .setClaimedJobId(claimedJobId)
            .build();

        this.heartbeatFuture = taskScheduler.scheduleAtFixedRate(
            this::sendHeartBeatTask,
            HEART_BEAT_PERIOD_MILLIS
        );

        this.requestObserver = client.heartbeat(new ResponseObserver(this));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void stop() {
        if (this.heartbeatFuture != null) {
            this.heartbeatFuture.cancel(false);
            this.heartbeatFuture = null;
        }

        if (this.requestObserver != null) {
            this.requestObserver.onCompleted();
            this.requestObserver = null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized boolean isConnected() {
        return isConnected;
    }

    private synchronized void setConnected() {
        this.isConnected = true;
    }

    private synchronized void setDisconnected() {
        this.isConnected = false;

        // Schedule a stream reset
        this.taskScheduler.schedule(
            this::resetStreamTask,
            Instant.ofEpochMilli(System.currentTimeMillis() + STREAM_RESET_DELAY_MILLIS)
        );
    }

    /**
     * Regularly scheduled to send heart beats.
     */
    private synchronized void sendHeartBeatTask() {
        if (requestObserver != null) {
            requestObserver.onNext(heartBeatMessage);
        }
    }

    /**
     * Scheduled once after a disconnection or error.
     */
    private synchronized void resetStreamTask() {
        if (!isConnected) {
            this.requestObserver = client.heartbeat(new ResponseObserver(this));
        }
    }

    private static class ResponseObserver implements StreamObserver<com.netflix.genie.proto.ServerHeartBeat> {
        private final GrpcAgentHeartBeatServiceImpl grpcAgentHeartBeatService;

        ResponseObserver(final GrpcAgentHeartBeatServiceImpl grpcAgentHeartBeatService) {
            this.grpcAgentHeartBeatService = grpcAgentHeartBeatService;
        }

        @Override
        public void onNext(final ServerHeartBeat value) {
            log.debug("Received server heartbeat");
            grpcAgentHeartBeatService.setConnected();
        }

        @Override
        public void onError(final Throwable t) {
            log.info("Stream error");
            grpcAgentHeartBeatService.setDisconnected();
        }

        @Override
        public void onCompleted() {
            log.info("Stream completed");
            grpcAgentHeartBeatService.setDisconnected();
        }
    }
}
