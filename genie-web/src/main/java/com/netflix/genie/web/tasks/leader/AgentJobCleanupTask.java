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
package com.netflix.genie.web.tasks.leader;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.genie.common.external.dtos.v4.ArchiveStatus;
import com.netflix.genie.common.external.dtos.v4.JobStatus;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieInvalidStatusException;
import com.netflix.genie.web.agent.services.AgentRoutingService;
import com.netflix.genie.web.data.services.DataServices;
import com.netflix.genie.web.data.services.PersistenceService;
import com.netflix.genie.web.exceptions.checked.NotFoundException;
import com.netflix.genie.web.properties.AgentCleanupProperties;
import com.netflix.genie.web.tasks.GenieTaskScheduleType;
import com.netflix.genie.web.util.MetricsUtils;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Leader task that cleans up jobs whose agent crashed or disconnected.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class AgentJobCleanupTask extends LeaderTask {
    private static final String AWOL_STATUS_MESSAGE = "Agent AWOL for too long";
    private static final String NEVER_CLAIMED_STATUS_MESSAGE = "No agent claimed the job for too long";
    private static final String TERMINATED_COUNTER_METRIC_NAME = "genie.jobs.agentDisconnected.terminated.counter";
    private static final String DISCONNECTED_GAUGE_METRIC_NAME = "genie.jobs.agentDisconnected.gauge";
    private final Map<String, Instant> awolJobsMap;
    private final PersistenceService persistenceService;
    private final AgentCleanupProperties properties;
    private final MeterRegistry registry;
    private final AgentRoutingService agentRoutingService;

    /**
     * Constructor.
     *
     * @param dataServices        The {@link DataServices} encapsulation instance to use
     * @param properties          the task properties
     * @param registry            the metrics registry
     * @param agentRoutingService the agent routing service
     */
    public AgentJobCleanupTask(
        final DataServices dataServices,
        final AgentCleanupProperties properties,
        final MeterRegistry registry,
        final AgentRoutingService agentRoutingService
    ) {
        this.persistenceService = dataServices.getPersistenceService();
        this.properties = properties;
        this.registry = registry;
        this.agentRoutingService = agentRoutingService;
        this.awolJobsMap = Maps.newConcurrentMap();

        // Auto-publish number of jobs tracked for shutdown due to agent not being connected.
        this.registry.gaugeMapSize(
            DISCONNECTED_GAUGE_METRIC_NAME,
            Sets.newHashSet(),
            this.awolJobsMap
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {
        // Get agent jobs that in active status
        final Set<String> activeAgentJobIds = this.persistenceService.getActiveJobs();

        // Get agent jobs that in ACCEPTED status (i.e. waiting for agent to start)
        final Set<String> acceptedAgentJobIds = this.persistenceService.getUnclaimedJobs();

        // Filter out jobs whose agent is connected
        final Set<String> currentlyAwolJobsIds = activeAgentJobIds
            .stream()
            .filter(jobId -> !this.agentRoutingService.isAgentConnected(jobId))
            .collect(Collectors.toSet());

        // Purge records if corresponding agent is now connected
        this.awolJobsMap.entrySet().removeIf(awolJobEntry -> !currentlyAwolJobsIds.contains(awolJobEntry.getKey()));

        final Instant now = Instant.now();

        // Add records for any agent that was not previously AWOL
        currentlyAwolJobsIds.forEach(jobId -> this.awolJobsMap.putIfAbsent(jobId, now));

        // Iterate over jobs whose agent is currently AWOL
        for (final Map.Entry<String, Instant> entry : this.awolJobsMap.entrySet()) {
            final String awolJobId = entry.getKey();
            final Instant awolJobFirstSeen = entry.getValue();

            final boolean jobWasClaimed = !acceptedAgentJobIds.contains(awolJobId);
            final Instant claimDeadline = awolJobFirstSeen.plus(this.properties.getLaunchTimeLimit());
            final Instant reconnectDeadline = awolJobFirstSeen.plus(this.properties.getReconnectTimeLimit());

            if (!jobWasClaimed && now.isBefore(claimDeadline)) {
                log.debug("Job {} agent still pending agent start/claim", awolJobId);
            } else if (jobWasClaimed && now.isBefore(reconnectDeadline)) {
                log.debug("Job {} agent still disconnected", awolJobId);
            } else {
                log.warn("Job {} agent AWOL for too long, marking failed", awolJobId);
                try {
                    final JobStatus currentStatus = this.persistenceService.getJobStatus(awolJobId);
                    final ArchiveStatus archiveStatus = this.persistenceService.getJobArchiveStatus(awolJobId);

                    // Update job archive status
                    if (archiveStatus == ArchiveStatus.PENDING) {
                        this.persistenceService.updateJobArchiveStatus(
                            awolJobId,
                            jobWasClaimed ? ArchiveStatus.UNKNOWN : ArchiveStatus.FAILED
                        );
                    }

                    // Mark the job as failed
                    this.persistenceService.updateJobStatus(
                        awolJobId,
                        currentStatus,
                        JobStatus.FAILED,
                        jobWasClaimed ? AWOL_STATUS_MESSAGE : NEVER_CLAIMED_STATUS_MESSAGE
                    );

                    // If marking as failed succeeded, remove it from the map
                    this.awolJobsMap.remove(awolJobId);

                    // Increment counter, tag as successful
                    this.registry.counter(
                        TERMINATED_COUNTER_METRIC_NAME,
                        MetricsUtils.newSuccessTagsSet()
                    ).increment();
                } catch (NotFoundException | GenieInvalidStatusException e) {
                    log.warn("Failed to mark AWOL job {} as failed: ", awolJobId, e);
                    // Increment counter, tag as failure
                    this.registry.counter(
                        TERMINATED_COUNTER_METRIC_NAME,
                        MetricsUtils.newFailureTagsSetForException(e)
                    ).increment();
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void cleanup() {
        // Throw away all deadlines
        this.awolJobsMap.clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GenieTaskScheduleType getScheduleType() {
        return GenieTaskScheduleType.FIXED_RATE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getFixedRate() {
        return this.properties.getRefreshInterval().toMillis();
    }
}
