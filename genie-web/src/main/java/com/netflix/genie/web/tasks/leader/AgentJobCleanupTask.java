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
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.web.agent.services.AgentRoutingService;
import com.netflix.genie.web.data.services.DataServices;
import com.netflix.genie.web.data.services.PersistenceService;
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
    private static final String STATUS_MESSAGE = "Agent AWOL for too long";
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
        final Set<String> activeAgentJobIds = this.persistenceService.getActiveAgentJobs();

        // Filter out jobs whose agent is connected
        final Set<String> currentlyAwolJobsIds = activeAgentJobIds.stream()
            .filter(jobId -> !this.agentRoutingService.isAgentConnected(jobId))
            .collect(Collectors.toSet());

        // If any previously AWOL job that does not appear in the "currently AWOL" list has either re-connected
        // or completed. Throw away their records.
        this.awolJobsMap.entrySet().removeIf(
            awolJobEntry -> !currentlyAwolJobsIds.contains(awolJobEntry.getKey())
        );

        final Instant now = Instant.now();

        // Iterate over job that currently look AWOL
        for (final String awolJobId : currentlyAwolJobsIds) {

            final Instant awolJobFirstSeen = this.awolJobsMap.get(awolJobId);

            if (awolJobFirstSeen == null) {
                // First time this job is noticed AWOL. Start tracking it.
                log.debug("Starting to track AWOL job {}", awolJobId);
                this.awolJobsMap.put(awolJobId, now);
            } else if (now.isAfter(awolJobFirstSeen.plusMillis(this.properties.getTimeLimit()))) {
                // Job has been AWOL past its deadline
                log.debug("Job {} no longer AWOL", awolJobId);
                try {
                    // Mark the job as failed
                    this.persistenceService.setJobCompletionInformation(
                        awolJobId,
                        -1,
                        JobStatus.FAILED,
                        STATUS_MESSAGE,
                        null,
                        null
                    );
                    // If marking as failed succeeded, remove it from the map
                    this.awolJobsMap.remove(awolJobId);
                    // Increment counter, tag as successful
                    this.registry.counter(
                        TERMINATED_COUNTER_METRIC_NAME,
                        MetricsUtils.newSuccessTagsSet()
                    ).increment();
                } catch (GenieException e) {
                    log.warn("Failed to mark AWOL job {} as failed: ", awolJobId, e);
                    // Increment counter, tag as failure
                    this.registry.counter(
                        TERMINATED_COUNTER_METRIC_NAME,
                        MetricsUtils.newFailureTagsSetForException(e)
                    ).increment();
                }
            } else {
                // Job is still AWOL, but not past its deadline.
                log.debug("Job {} is still AWOL", awolJobId);
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
        return properties.getRefreshInterval();
    }

}
