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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.UserResourcesSummary;
import com.netflix.genie.web.properties.UserMetricsProperties;
import com.netflix.genie.web.services.JobSearchService;
import com.netflix.genie.web.tasks.GenieTaskScheduleType;
import com.netflix.genie.web.util.MetricsConstants;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

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

    private final Map<String, UserResourcesRecord> userResourcesRecordMap = Maps.newHashMap();
    private final AtomicLong activeUsersCount = new AtomicLong(0);

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
        this.registry.gauge(USER_ACTIVE_USERS_METRIC_NAME, this.activeUsersCount);
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
        log.info("Publishing user metrics");

        final Map<String, UserResourcesSummary> summaries = this.jobSearchService.getUserResourcesSummaries();

        // Update number of active users
        log.info("Number of users with active jobs: {}", summaries.size());
        this.activeUsersCount.set(summaries.size());

        // Track users who previously had jobs but no longer do
        final Set<String> usersToReset = Sets.newHashSet(this.userResourcesRecordMap.keySet());
        usersToReset.removeAll(summaries.keySet());

        for (final String user : usersToReset) {
            // Remove from map, gauge will eventually stop publishing
            this.userResourcesRecordMap.remove(user).reset();
        }

        // Update existing user metrics
        for (final UserResourcesSummary userResourcesSummary : summaries.values()) {
            final String user = userResourcesSummary.getUser();
            final long jobs = userResourcesSummary.getRunningJobsCount();
            final long memory = userResourcesSummary.getUsedMemory();

            log.info("User {}: {} jobs running, using {}MB", user, jobs, memory);

            this.userResourcesRecordMap.computeIfAbsent(
                userResourcesSummary.getUser(),
                userName -> new UserResourcesRecord(userName, registry)
            ).update(
                userResourcesSummary.getRunningJobsCount(),
                userResourcesSummary.getUsedMemory()
            );
        }

        log.info("Done publishing user metrics");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void cleanup() {

        log.info("Cleaning up user metrics publishing");

        // Reset all users
        this.userResourcesRecordMap.forEach((s, userResourcesRecord) -> userResourcesRecord.reset());
        this.userResourcesRecordMap.clear();

        // Reset active jobs count
        this.activeUsersCount.set(0);
    }

    private static class UserResourcesRecord {
        private final AtomicLong jobCount = new AtomicLong(0);
        private final AtomicLong memoryAmount = new AtomicLong(0);

        UserResourcesRecord(final String user, final MeterRegistry registry) {
            final List<Tag> tags = Lists.newArrayList(Tag.of(MetricsConstants.TagKeys.USER, user));
            registry.gauge(USER_ACTIVE_JOBS_METRIC_NAME, tags, jobCount);
            registry.gauge(USER_ACTIVE_MEMORY_METRIC_NAME, tags, memoryAmount);
        }

        void update(final long runningJobsCount, final long usedMemory) {
            this.jobCount.set(runningJobsCount);
            this.memoryAmount.set(usedMemory);
        }

        void reset() {
            this.jobCount.set(0);
            this.memoryAmount.set(0);
        }
    }
}
