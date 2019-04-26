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
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.web.properties.AgentCleanupProperties;
import com.netflix.genie.web.services.JobPersistenceService;
import com.netflix.genie.web.services.JobSearchService;
import com.netflix.genie.web.tasks.GenieTaskScheduleType;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.Map;
import java.util.Set;

/**
 * Leader task that cleans up jobs whose agent crashed or disconnected.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class AgentJobCleanupTask extends LeadershipTask {
    private static final String STATUS_MESSAGE = "Agent AWOL for too long";
    private final Map<String, Instant> awolJobDeadlines = Maps.newHashMap();
    private final JobSearchService jobSearchService;
    private final JobPersistenceService jobPersistenceService;
    private final AgentCleanupProperties properties;

    /**
     * Constructor.
     *
     * @param jobSearchService      the job search service
     * @param jobPersistenceService the job persistence service
     * @param properties            the task properties
     */
    public AgentJobCleanupTask(
        final JobSearchService jobSearchService,
        final JobPersistenceService jobPersistenceService,
        final AgentCleanupProperties properties
    ) {
        this.jobSearchService = jobSearchService;
        this.jobPersistenceService = jobPersistenceService;
        this.properties = properties;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {

        // Get agent jobs that in active status but but not connected to any node
        final Set<String> currentlyAwolJobsIds = this.jobSearchService.getActiveDisconnectedAgentJobs();

        // If any previously AWOL job that does not appear in the "currently AWOL" list has either re-connected
        // or completed. Throw away their records.
        awolJobDeadlines.entrySet().removeIf(
            awolJobEntry -> !currentlyAwolJobsIds.contains(awolJobEntry.getKey())
        );

        final Instant now = Instant.now();

        // Iterate over job that currently look AWOL
        for (final String awolJobId : currentlyAwolJobsIds) {

            final Instant awolJobDeadline = this.awolJobDeadlines.get(awolJobId);

            if (awolJobDeadline == null) {
                // First time this job is noticed AWOL. Start tracking it.
                log.debug("Starting to track AWOL job {}", awolJobId);
                this.awolJobDeadlines.put(
                    awolJobId,
                    now.plusMillis(this.properties.getTimeLimit())
                );
            } else if (now.isAfter(awolJobDeadline)) {
                // Job has been AWOL past its deadline
                log.debug("Job {} no longer AWOL", awolJobId);
                try {
                    // Mark the job as failed
                    this.jobPersistenceService.setJobCompletionInformation(
                        awolJobId,
                        -1,
                        JobStatus.FAILED,
                        STATUS_MESSAGE,
                        null,
                        null
                    );
                    // If marking as failed succeeded, remove it from the map
                    this.awolJobDeadlines.remove(awolJobId);
                } catch (GenieException e) {
                    log.warn("Failed to mark AWOL job {} as failed: ", awolJobId, e);
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
        this.awolJobDeadlines.clear();
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
