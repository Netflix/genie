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
package com.netflix.genie.web.tasks.leader;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.netflix.genie.common.external.dtos.v4.ArchiveStatus;
import com.netflix.genie.common.external.dtos.v4.JobStatus;
import com.netflix.genie.web.agent.services.AgentRoutingService;
import com.netflix.genie.web.data.services.DataServices;
import com.netflix.genie.web.data.services.PersistenceService;
import com.netflix.genie.web.exceptions.checked.NotFoundException;
import com.netflix.genie.web.properties.ArchiveStatusCleanupProperties;
import com.netflix.genie.web.tasks.GenieTaskScheduleType;
import com.netflix.genie.web.util.MetricsUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Leader task that find jobs whose archival status was left in 'PENDING' state.
 * This can for example happen if the agent fails to update the server after successfully archiving.
 * The logic is summarized as:
 * If a job finished running more than N minutes ago, and the agent is disconnected and the archive status is PENDING,
 * then set the archive status to UNKNOWN.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class ArchiveStatusCleanupTask extends LeaderTask {

    private static final String CLEAR_ARCHIVE_STATUS_COUNTER_NAME = "genie.jobs.archiveStatus.cleanup.counter";
    private static final String CLEAR_ARCHIVE_STATUS_TIMER_NAME = "genie.tasks.archiveStatusCleanup.timer";
    private static final Set<ArchiveStatus> PENDING_STATUS_SET = ImmutableSet.of(ArchiveStatus.PENDING);
    private final PersistenceService persistenceService;
    private final AgentRoutingService agentRoutingService;
    private final ArchiveStatusCleanupProperties properties;
    private final MeterRegistry registry;

    /**
     * Constructor.
     *
     * @param dataServices        data services
     * @param agentRoutingService agent routing service
     * @param properties          task properties
     * @param registry            metrics registry
     */
    public ArchiveStatusCleanupTask(
        final DataServices dataServices,
        final AgentRoutingService agentRoutingService,
        final ArchiveStatusCleanupProperties properties,
        final MeterRegistry registry
    ) {
        this.persistenceService = dataServices.getPersistenceService();
        this.agentRoutingService = agentRoutingService;
        this.properties = properties;
        this.registry = registry;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {
        final Set<Tag> tags = Sets.newHashSet();
        final long start = System.nanoTime();
        try {
            final Instant updatedThreshold = Instant.now().minus(this.properties.getGracePeriod());
            final Set<String> jobIds = this.persistenceService.getJobsWithStatusAndArchiveStatusUpdatedBefore(
                JobStatus.getFinishedStatuses(),
                PENDING_STATUS_SET,
                updatedThreshold
            );
            if (!jobIds.isEmpty()) {
                log.debug("Found {} finished jobs with PENDING archive status", jobIds.size());
                this.clearJobsArchiveStatus(jobIds);
            }
            MetricsUtils.addSuccessTags(tags);
        } catch (Exception e) {
            MetricsUtils.addFailureTagsWithException(tags, e);
            log.error("Archive status cleanup task failed with exception: {}", e.getMessage(), e);
        } finally {
            final long taskDuration = System.nanoTime() - start;
            registry.timer(CLEAR_ARCHIVE_STATUS_TIMER_NAME, tags).record(taskDuration, TimeUnit.NANOSECONDS);
        }
    }

    private void clearJobsArchiveStatus(final Set<String> jobIds) {

        for (final String jobId : jobIds) {
            if (this.agentRoutingService.isAgentConnected(jobId)) {
                log.debug("Agent for job {} is still connected and probably archiving", jobId);
            } else {
                log.warn("Marking job {} archive status to UNKNOWN", jobId);
                final Set<Tag> tags = Sets.newHashSet();

                try {
                    this.persistenceService.updateJobArchiveStatus(jobId, ArchiveStatus.UNKNOWN);
                    MetricsUtils.addSuccessTags(tags);
                } catch (NotFoundException e) {
                    log.error("Tried to update a job that does not exist: {}", jobId);
                    MetricsUtils.addFailureTagsWithException(tags, e);
                } finally {
                    registry.counter(CLEAR_ARCHIVE_STATUS_COUNTER_NAME, tags).increment();
                }
            }
        }
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
        return this.properties.getCheckInterval().toMillis();
    }
}
