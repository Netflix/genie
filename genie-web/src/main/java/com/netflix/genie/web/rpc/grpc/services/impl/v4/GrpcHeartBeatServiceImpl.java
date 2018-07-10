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

import com.google.common.collect.Maps;
import com.netflix.genie.proto.AgentHeartBeat;
import com.netflix.genie.proto.HeartBeatServiceGrpc;
import com.netflix.genie.proto.ServerHeartBeat;
import com.netflix.genie.web.rpc.grpc.interceptors.SimpleLoggingInterceptor;
import com.netflix.genie.web.services.AgentRoutingService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.springboot.autoconfigure.grpc.server.GrpcService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.TaskScheduler;

import javax.annotation.PreDestroy;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;

/**
 * An edge gRPC service that uses bi-directional streaming.
 * This is useful to reliably track which connection is handled by which server and to detect disconnections on both
 * ends.
 *
 * @author mprimi
 * @since 4.0.0
 */
@GrpcService(
    value = HeartBeatServiceGrpc.class,
    interceptors = {
        SimpleLoggingInterceptor.class,
    }
)
@Slf4j
class GrpcHeartBeatServiceImpl extends HeartBeatServiceGrpc.HeartBeatServiceImplBase {

    private static final long HEART_BEAT_PERIOD_MILLIS = 5_000L; // TODO make configurable
    private final TaskScheduler taskScheduler;
    private final Map<String, AgentStreamRecord> activeStreamsMap = Maps.newHashMap();
    private final ScheduledFuture<?> sendHeartbeatsFuture;
    private final AgentRoutingService agentRoutingService;

    GrpcHeartBeatServiceImpl(
        final AgentRoutingService agentRoutingService,
        @Qualifier("heartBeatServiceTaskScheduler") final TaskScheduler taskScheduler
    ) {
        this.agentRoutingService = agentRoutingService;
        this.taskScheduler = taskScheduler;
        this.sendHeartbeatsFuture = this.taskScheduler.scheduleWithFixedDelay(
            this::sendHeartbeats,
            HEART_BEAT_PERIOD_MILLIS
        );
    }

    @PreDestroy
    public synchronized void shutdown() {
        if (sendHeartbeatsFuture != null) {
            sendHeartbeatsFuture.cancel(false);
        }

        synchronized (activeStreamsMap) {
            for (final AgentStreamRecord agentStreamRecord : activeStreamsMap.values()) {
                agentStreamRecord.responseObserver.onCompleted();
                if (agentStreamRecord.hasJobId()) {
                    notifyAgentDisconnected(agentStreamRecord.getJobId());
                }
            }
            activeStreamsMap.clear();
        }
    }

    /**
     * Regularly scheduled to send heartbeat to the client.
     * Using the connection ensures server-side eventually detects a broken connection.
     */
    private void sendHeartbeats() {
        synchronized (activeStreamsMap) {
            for (final AgentStreamRecord agentStreamRecord : activeStreamsMap.values()) {
                agentStreamRecord.responseObserver.onNext(ServerHeartBeat.getDefaultInstance());
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamObserver<AgentHeartBeat> heartbeat(
        final StreamObserver<ServerHeartBeat> responseObserver
    ) {
        // Handle new stream / client connection
        final String streamId = UUID.randomUUID().toString();
        final RequestObserver requestObserver = new RequestObserver(this, streamId);
        synchronized (activeStreamsMap) {

            // Create a record for this connection
            activeStreamsMap.put(streamId, new AgentStreamRecord(responseObserver));
        }
        return requestObserver;
    }

    private void handleAgentHeartBeat(
        final String streamId,
        final AgentHeartBeat agentHeartBeat
    ) {
        // Pull the record, if one exists
        final AgentStreamRecord agentStreamRecord;
        synchronized (activeStreamsMap) {
            agentStreamRecord = activeStreamsMap.get(streamId);
        }

        final String claimedJobId = agentHeartBeat.getClaimedJobId();
        if (agentStreamRecord == null) {
            log.warn("Received heartbeat from an unknown stream");
        } else if (StringUtils.isBlank(claimedJobId)) {
            log.warn("Ignoring heartbeat lacking job id");
        } else {
            log.info("Received heartbeat from agent that claimed job: {}", claimedJobId);
            final boolean isFirstHeartBeat = agentStreamRecord.updateRecord(claimedJobId);
            // On first heartbeat, notify listeners of a new agent connection
            if (isFirstHeartBeat) {
                notifyAgentConnected(agentStreamRecord.getJobId());
            }
        }
    }

    private void handleStreamCompletion(final String streamId) {
        // Pull the record, if one exists
        final AgentStreamRecord agentStreamRecord;
        synchronized (activeStreamsMap) {
            agentStreamRecord = activeStreamsMap.remove(streamId);
        }

        if (agentStreamRecord == null) {
            log.warn("Received completion from an unknown stream");
        } else {
            agentStreamRecord.responseObserver.onCompleted();
            if (agentStreamRecord.hasJobId()) {
                notifyAgentDisconnected(agentStreamRecord.getJobId());
            }
        }
    }

    private void handleStreamError(final String streamId, final Throwable t) {
        // Pull the record, if one exists
        final AgentStreamRecord agentStreamRecord;
        synchronized (activeStreamsMap) {
            agentStreamRecord = activeStreamsMap.remove(streamId);
        }

        if (agentStreamRecord == null) {
            log.warn("Received error from an unknown stream");
        } else {
            agentStreamRecord.responseObserver.onError(t);
            if (agentStreamRecord.hasJobId()) {
                notifyAgentDisconnected(agentStreamRecord.getJobId());
            }
        }
    }

    private void notifyAgentConnected(final String jobId) {
        agentRoutingService.handleClientConnected(jobId);
    }

    private void notifyAgentDisconnected(final String jobId) {
        agentRoutingService.handleClientDisconnected(jobId);
    }

    private static class AgentStreamRecord {
        private final StreamObserver<ServerHeartBeat> responseObserver;
        private String claimedJobId;

        AgentStreamRecord(
            final StreamObserver<ServerHeartBeat> responseObserver
        ) {
            this.responseObserver = responseObserver;
        }

        synchronized boolean updateRecord(final String jobId) {
            if (hasJobId() || StringUtils.isBlank(jobId)) {
                return false;
            } else {
                this.claimedJobId = jobId;
                return true;
            }
        }

        String getJobId() {
            return claimedJobId;
        }

        boolean hasJobId() {
            return !StringUtils.isBlank(claimedJobId);
        }
    }

    private static class RequestObserver implements StreamObserver<AgentHeartBeat> {
        private final GrpcHeartBeatServiceImpl grpcHeartBeatService;
        private final String streamId;

        RequestObserver(
            final GrpcHeartBeatServiceImpl grpcHeartBeatService,
            final String streamId
        ) {

            this.grpcHeartBeatService = grpcHeartBeatService;
            this.streamId = streamId;
        }

        @Override
        public void onNext(final AgentHeartBeat agentHeartBeat) {
            grpcHeartBeatService.handleAgentHeartBeat(streamId, agentHeartBeat);
        }

        @Override
        public void onError(final Throwable t) {
            grpcHeartBeatService.handleStreamError(streamId, t);
        }

        @Override
        public void onCompleted() {
            grpcHeartBeatService.handleStreamCompletion(streamId);
        }
    }
}
