/*
 *
 *  Copyright 2020 Netflix, Inc.
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
package com.netflix.genie.web.agent.services.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.genie.web.agent.services.AgentConnectionTrackingService;
import com.netflix.genie.web.agent.services.AgentRoutingService;
import com.netflix.genie.web.properties.AgentConnectionTrackingServiceProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.actuate.info.Info;
import org.springframework.boot.actuate.info.InfoContributor;
import org.springframework.scheduling.TaskScheduler;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * This service keeps track of agent connections and heartbeats. It notifies the downstream {@link AgentRoutingService}
 * of connected/disconnected agents while hiding details of connections, disconnections, missed heartbeats.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class AgentConnectionTrackingServiceImpl implements AgentConnectionTrackingService, InfoContributor {

    private final AgentRoutingService agentRoutingService;
    private final TaskScheduler taskScheduler;
    private final HashMap<String, JobStreamsRecord> jobStreamRecordsMap = Maps.newHashMap();
    private final AgentConnectionTrackingServiceProperties serviceProperties;
    private final Supplier<Instant> timeSupplier;

    /**
     * Constructor.
     *
     * @param agentRoutingService the agent routing service
     * @param taskScheduler       the task scheduler
     * @param serviceProperties   the service properties
     */
    public AgentConnectionTrackingServiceImpl(
        final AgentRoutingService agentRoutingService,
        final TaskScheduler taskScheduler,
        final AgentConnectionTrackingServiceProperties serviceProperties
    ) {
        this(agentRoutingService, taskScheduler, serviceProperties, Instant::now);
    }

    @VisibleForTesting
    AgentConnectionTrackingServiceImpl(
        final AgentRoutingService agentRoutingService,
        final TaskScheduler taskScheduler,
        final AgentConnectionTrackingServiceProperties serviceProperties,
        final Supplier<Instant> timeSupplier
    ) {
        this.agentRoutingService = agentRoutingService;
        this.taskScheduler = taskScheduler;
        this.serviceProperties = serviceProperties;
        this.timeSupplier = timeSupplier;

        this.taskScheduler.scheduleAtFixedRate(this::cleanupTask, this.serviceProperties.getCleanupInterval());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void notifyHeartbeat(final String streamId, final String claimedJobId) {

        boolean isNew = false;
        final JobStreamsRecord record;

        if (this.jobStreamRecordsMap.containsKey(claimedJobId)) {
            record = this.jobStreamRecordsMap.get(claimedJobId);
        } else {
            record = new JobStreamsRecord(claimedJobId);
            this.jobStreamRecordsMap.put(claimedJobId, record);
            isNew = true;
        }

        // Update TTL for this stream
        record.updateActiveStream(streamId, timeSupplier.get());

        log.debug(
            "Received heartbeat for {} job {} using stream {}",
            isNew ? "new" : "existing",
            claimedJobId,
            streamId
        );

        // If this job record is new, wake up observer
        if (isNew) {
            log.debug("Notify new agent connection for job {}", claimedJobId);
            this.agentRoutingService.handleClientConnected(claimedJobId);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void notifyDisconnected(final String streamId, final String claimedJobId) {
        // Retrieve entry
        final JobStreamsRecord jobStreamsRecord = this.jobStreamRecordsMap.get(claimedJobId);

        log.debug(
            "Received disconnection for {} job {} using stream {}",
            jobStreamsRecord == null ? "unknown" : "existing",
            claimedJobId,
            streamId
        );

        // If record exist, expunge the stream
        if (jobStreamsRecord != null) {
            jobStreamsRecord.removeActiveStream(streamId);

            if (!jobStreamsRecord.hasActiveStreams()) {
                log.debug("Job {} last stream disconnected, notifying routing service", claimedJobId);
                this.jobStreamRecordsMap.remove(claimedJobId);
                this.agentRoutingService.handleClientDisconnected(claimedJobId);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized long getConnectedAgentsCount() {
        return this.jobStreamRecordsMap.size();
    }

    private synchronized void cleanupTask() {
        final Instant cutoff = this.timeSupplier.get().minus(serviceProperties.getConnectionExpirationPeriod());

        // Drop all streams that didn't heartbeat recently
        this.jobStreamRecordsMap.forEach(
            (jobId, record) -> record.expungeExpiredStreams(cutoff)
        );

        // Remove all records that have no active streams
        final Set<String> removedJobIds = Sets.newHashSet();
        this.jobStreamRecordsMap.entrySet().removeIf(
            entry -> {
                if (!entry.getValue().hasActiveStreams()) {
                    removedJobIds.add(entry.getKey());
                    return true;
                }
                return false;
            }
        );

        // Notify routing service
        for (final String jobId : removedJobIds) {
            log.debug("Job {} last stream expired, notifying routing service", jobId);
            this.agentRoutingService.handleClientDisconnected(jobId);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void contribute(final Info.Builder builder) {
        final List<String> jobIds = this.getConnectedAgentsIds();
        builder.withDetail("connectedAgents", jobIds);
    }

    private synchronized List<String> getConnectedAgentsIds() {
        return ImmutableList.copyOf(this.jobStreamRecordsMap.keySet());
    }

    private static final class JobStreamsRecord {
        private final String jobId;
        private final Map<String, Instant> streamsLastHeartbeatMap = Maps.newHashMap();

        private JobStreamsRecord(final String jobId) {
            this.jobId = jobId;
        }

        private void updateActiveStream(final String streamId, final Instant currentTime) {
            final Instant previousHeartbeat = this.streamsLastHeartbeatMap.put(streamId, currentTime);

            log.debug(
                "{} heartbeat for job {} stream {}",
                previousHeartbeat == null ? "Created" : "Updated",
                this.jobId,
                streamId
            );
        }

        private void removeActiveStream(final String streamId) {
            final Instant previousHeartbeat = this.streamsLastHeartbeatMap.remove(streamId);

            if (previousHeartbeat != null) {
                log.debug("Removed job {} stream {}", this.jobId, streamId);
            }
        }

        private boolean hasActiveStreams() {
            return !this.streamsLastHeartbeatMap.isEmpty();
        }

        private void expungeExpiredStreams(final Instant cutoff) {
            final boolean removed = this.streamsLastHeartbeatMap.entrySet().removeIf(
                entry -> entry.getValue().isBefore(cutoff)
            );

            if (removed) {
                log.debug("Removed expired streams for job {}", this.jobId);
            }
        }
    }
}
