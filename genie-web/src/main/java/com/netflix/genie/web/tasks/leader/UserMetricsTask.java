/*
 *
 *  Copyright 2016 Netflix, Inc.
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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.UserResourcesSummary;
import com.netflix.genie.web.properties.UserMetricsProperties;
import com.netflix.genie.web.services.JobSearchService;
import com.netflix.genie.web.tasks.GenieTaskScheduleType;
import com.netflix.genie.web.util.MetricsConstants;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A task which publishes user metrics.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class UserMetricsTask extends LeadershipTask {

    private static final String USER_ACTIVE_JOBS_METRIC_NAME = "genie.user.active-jobs.gauge";
    private static final String USER_ACTIVE_MEMORY_METRIC_NAME = "genie.user.active-memory.gauge";
    private static final String USER_ACTIVE_USERS_METRIC_NAME = "genie.user.active-users.gauge";
    private final MeterRegistry registry;
    private final JobSearchService jobSearchService;
    private final UserMetricsProperties userMetricsProperties;
    private final Set<String> usersToReset = Sets.newHashSet();

    /**
     * Constructor.
     *
     * @param registry              the metrics registry
     * @param jobSearchService      the job search service
     * @param userMetricsProperties the properties that configure this task
     */
    public UserMetricsTask(
        final MeterRegistry registry,
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
        return this.userMetricsProperties.getRefreshInterval();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {

        final Map<String, UserResourcesSummary> summaries = this.jobSearchService.getUserResourcesSummaries();

        // Set gagues back to 0 for all users whose metric were published the previous iterations.
        for (final String user : this.usersToReset) {
            // Unless we're about to publish new values, in which case don't bother.
            if (!summaries.containsKey(user)) {
                this.publishUserMetrics(user, 0, 0);
            }
        }

        // Save the set of users to be reset next iteration.
        this.usersToReset.clear();
        this.usersToReset.addAll(summaries.keySet());

        // Publish metrics for all users.
        for (final UserResourcesSummary userResourcesSummary : summaries.values()) {
            this.publishUserMetrics(
                userResourcesSummary.getUser(),
                userResourcesSummary.getRunningJobsCount(),
                userResourcesSummary.getUsedMemory()
            );
        }

        // Publish metric with number of active users.
        registry.gauge(USER_ACTIVE_USERS_METRIC_NAME, summaries.size());
    }

    private void publishUserMetrics(@NotNull final String user, final long jobCount, final long memoryAmount) {
        final List<Tag> tags = Lists.newArrayList(Tag.of(MetricsConstants.TagKeys.USER, user));
        registry.gauge(USER_ACTIVE_JOBS_METRIC_NAME, tags, jobCount);
        registry.gauge(USER_ACTIVE_MEMORY_METRIC_NAME, tags, memoryAmount);
    }
}
