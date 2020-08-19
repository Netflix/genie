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
package com.netflix.genie.web.agent.apis.rpc.v4.endpoints;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.genie.proto.AgentHeartBeat;
import com.netflix.genie.proto.HeartBeatServiceGrpc;
import com.netflix.genie.proto.ServerHeartBeat;
import com.netflix.genie.web.agent.services.AgentConnectionTrackingService;
import com.netflix.genie.web.agent.services.AgentRoutingService;
import com.netflix.genie.web.properties.HeartBeatProperties;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.scheduling.TaskScheduler;

import javax.annotation.PreDestroy;
import java.util.Map;
import java.util.Set;
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
@Slf4j
public class GRpcHeartBeatServiceImpl extends HeartBeatServiceGrpc.HeartBeatServiceImplBase {

    private static final String HEARTBEATING_GAUGE_NAME = "genie.agents.heartbeating.gauge";
    private final AgentConnectionTrackingService agentConnectionTrackingService;
    private final HeartBeatProperties properties;
    private final Map<String, AgentStreamRecord> activeStreamsMap = Maps.newHashMap();
    private final ScheduledFuture<?> sendHeartbeatsFuture;
    private final MeterRegistry registry;

    /**
     * Constructor.
     *
     * @param agentConnectionTrackingService The {@link AgentRoutingService} implementation to use
     * @param properties                     The service properties
     * @param taskScheduler                  The {@link TaskScheduler} instance to use
     * @param registry                       The meter registry
     */
    public GRpcHeartBeatServiceImpl(
        final AgentConnectionTrackingService agentConnectionTrackingService,
        final HeartBeatProperties properties,
        final TaskScheduler taskScheduler,
        final MeterRegistry registry
    ) {
        this.agentConnectionTrackingService = agentConnectionTrackingService;
        this.properties = properties;
        this.sendHeartbeatsFuture = taskScheduler.scheduleWithFixedDelay(
            this::sendHeartbeats,
            this.properties.getSendInterval()
        );
        this.registry = registry;
        this.registry.gaugeMapSize(HEARTBEATING_GAUGE_NAME, Sets.newHashSet(), activeStreamsMap);
    }

    /**
     * Shutdown this service.
     */
    @PreDestroy
    public synchronized void shutdown() {
        if (sendHeartbeatsFuture != null) {
            sendHeartbeatsFuture.cancel(false);
        }

        synchronized (activeStreamsMap) {
            for (final Map.Entry<String, AgentStreamRecord> agentStreamRecordEntry : activeStreamsMap.entrySet()) {
                final String streamId = agentStreamRecordEntry.getKey();
                final AgentStreamRecord agentStreamRecord = agentStreamRecordEntry.getValue();
                if (agentStreamRecord.hasJobId()) {
                    final String jobId = agentStreamRecord.getJobId();
                    log.debug("Unregistering stream of job: {} (stream id: {})", jobId, streamId);
                    this.agentConnectionTrackingService.notifyDisconnected(streamId, jobId);
                }
            }
            for (final AgentStreamRecord agentStreamRecord : activeStreamsMap.values()) {
                agentStreamRecord.responseObserver.onCompleted();
            }
            activeStreamsMap.clear();
        }
    }

    /**
     * Regularly scheduled to send heartbeat to the client.
     * Using the connection ensures server-side eventually detects a broken connection.
     */
    private void sendHeartbeats() {
        final Set<String> brokenStreams = Sets.newHashSet();
        synchronized (activeStreamsMap) {
            for (final Map.Entry<String, AgentStreamRecord> entry : this.activeStreamsMap.entrySet()) {
                final String streamId = entry.getKey();
                final AgentStreamRecord agentStreamRecord = entry.getValue();

                try {
                    agentStreamRecord.responseObserver.onNext(ServerHeartBeat.getDefaultInstance());
                } catch (StatusRuntimeException | IllegalStateException e) {
                    log.warn("Stream {} of job {} is broken", streamId, agentStreamRecord.getJobId());
                    log.debug("Error probing job {} stream {}", agentStreamRecord.getJobId(), streamId, e);
                    brokenStreams.add(streamId);
                }
            }
        }

        for (final String streamId : brokenStreams) {
            synchronized (activeStreamsMap) {
                final AgentStreamRecord agentStreamRecord = this.activeStreamsMap.remove(streamId);
                if (agentStreamRecord != null) {
                    log.debug("Removed broken stream {} of job {}", streamId, agentStreamRecord.getJobId());
                    if (agentStreamRecord.hasJobId()) {
                        this.agentConnectionTrackingService.notifyDisconnected(streamId, agentStreamRecord.getJobId());
                    }
                }
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
            log.debug("Received heartbeat from job: {} (stream id: {})", claimedJobId, streamId);
            final boolean isFirstHeartBeat = agentStreamRecord.updateRecord(claimedJobId);
            if (isFirstHeartBeat) {
                log.info("Received first heartbeat from job: {}", claimedJobId);
            }
            this.agentConnectionTrackingService.notifyHeartbeat(streamId, claimedJobId);
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
            log.debug("Received completion from stream {}", streamId);
            if (agentStreamRecord.hasJobId()) {
                this.agentConnectionTrackingService.notifyDisconnected(streamId, agentStreamRecord.getJobId());
            }
            agentStreamRecord.responseObserver.onCompleted();
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
            log.debug("Received error from stream {}", streamId);
            if (agentStreamRecord.hasJobId()) {
                this.agentConnectionTrackingService.notifyDisconnected(streamId, agentStreamRecord.getJobId());
            }
        }
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
        private final GRpcHeartBeatServiceImpl grpcHeartBeatService;
        private final String streamId;

        RequestObserver(
            final GRpcHeartBeatServiceImpl grpcHeartBeatService,
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
