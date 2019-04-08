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

import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.UserJobCount;
import com.netflix.genie.common.dto.UserMemoryAmount;
import com.netflix.genie.core.services.JobSearchService;
import com.netflix.genie.core.util.MetricsConstants;
import com.netflix.genie.web.properties.UserMetricsProperties;
import com.netflix.genie.web.tasks.GenieTaskScheduleType;
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;

/**
 * Retrieves metrics about currently active jobs and publishes aggregate metrics by user.
 * These metrics can answer questions such as:
 * - Which users use the most memory?
 * - What is the highest number of concurrent job that any user executes?
 * - Etc.
 *
 * @author mprimi
 * @since 3.3.19
 */
@ConditionalOnProperty(value = "genie.tasks.userMetrics.enabled", havingValue = "true")
@Component
@Slf4j
public class UserMetricsTask extends LeadershipTask {

    private static final String USER_ACTIVE_JOBS_METRIC_NAME = "genie.user.active-jobs.gauge";
    private static final String USER_ACTIVE_MEMORY_METRIC_NAME = "genie.user.active-memory.gauge";
    private final Registry registry;
    private final JobSearchService jobSearchService;
    private final UserMetricsProperties userMetricsProperties;
    private Set<String> usersWithJobsToReset = Sets.newHashSet();
    private Set<String> usersWithMemoryToReset = Sets.newHashSet();

    /**
     * Constructor.
     *
     * @param registry              the metrics registry
     * @param jobSearchService      the job search service
     * @param userMetricsProperties the properties that configure this task
     */
    @Autowired
    public UserMetricsTask(
        final Registry registry,
        final JobSearchService jobSearchService,
        final UserMetricsProperties userMetricsProperties
    ) {
        this.registry = registry;
        this.jobSearchService = jobSearchService;
        this.userMetricsProperties = userMetricsProperties;
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
        return this.userMetricsProperties.getRate();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {

        final Set<String> usersWithActiveJobs = Sets.newHashSet();

        final List<UserJobCount> userActiveJobCounts = this.jobSearchService.getActiveJobCountsPerUser();
        for (final UserJobCount userActiveJobCount : userActiveJobCounts) {
            final String user = userActiveJobCount.getUser();
            registry.gauge(
                USER_ACTIVE_JOBS_METRIC_NAME,
                MetricsConstants.TagKeys.USER,
                user
            ).set(userActiveJobCount.getCount());
            usersWithActiveJobs.add(user);
        }

        final Set<String> usersWithActiveMemory = Sets.newHashSet();

        final List<UserMemoryAmount> userActiveMemoryAmounts = this.jobSearchService.getActiveJobMemoryPerUser();
        for (final UserMemoryAmount userActiveMemoryAmount : userActiveMemoryAmounts) {
            final String user = userActiveMemoryAmount.getUser();
            registry.gauge(
                USER_ACTIVE_MEMORY_METRIC_NAME,
                MetricsConstants.TagKeys.USER,
                user
            ).set(userActiveMemoryAmount.getAmount());
            usersWithActiveMemory.add(user);
        }

        // Gauges retain value. If a metric was published with value V last iteration and the same user
        // no longer has a value, we need to update the gauge value to 0.

        for (final String user : this.usersWithJobsToReset) {
            if (!usersWithActiveJobs.contains(user)) {
                registry.gauge(
                    USER_ACTIVE_JOBS_METRIC_NAME,
                    MetricsConstants.TagKeys.USER,
                    user
                ).set(0);
            }
        }

        for (final String user : this.usersWithMemoryToReset) {

            if (!usersWithActiveMemory.contains(user)) {
                registry.gauge(
                    USER_ACTIVE_MEMORY_METRIC_NAME,
                    MetricsConstants.TagKeys.USER,
                    user
                ).set(0.0);
            }
        }

        this.usersWithJobsToReset = usersWithActiveJobs;
        this.usersWithMemoryToReset = usersWithActiveMemory;
    }
}
