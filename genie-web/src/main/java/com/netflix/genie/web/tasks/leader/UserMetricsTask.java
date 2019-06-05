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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.UserResourcesSummary;
import com.netflix.genie.web.properties.UserMetricsProperties;
import com.netflix.genie.web.services.JobSearchService;
import com.netflix.genie.web.tasks.GenieTaskScheduleType;
import com.netflix.genie.web.util.MetricsConstants;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;

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
    private final AtomicLong activeUsersCount;

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
        this.activeUsersCount = new AtomicLong(0);

        // Register gauge for count of distinct users with active jobs.
        Gauge.builder(USER_ACTIVE_USERS_METRIC_NAME, () -> this.getUsersCount())
            .register(registry);
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
            this.userResourcesRecordMap.remove(user);
        }

        // Update existing user metrics
        for (final UserResourcesSummary userResourcesSummary : summaries.values()) {
            final String user = userResourcesSummary.getUser();
            final long jobs = userResourcesSummary.getRunningJobsCount();
            final long memory = userResourcesSummary.getUsedMemory();

            log.info("User {}: {} jobs running, using {}MB", user, jobs, memory);

            this.userResourcesRecordMap.computeIfAbsent(
                userResourcesSummary.getUser(),
                userName -> {
                    // Register gauges for a new or returning user.
                    // N.B. Lifecycle of gauges is controlled by Micrometer and unrelated to the lifecycle
                    // of the UserResourceRecord. If a gauge with the same ID was previously recorded, it may now get
                    // reused. Or not. Hence this lambda that will always return the up-to-date value.
                    Gauge.builder(USER_ACTIVE_JOBS_METRIC_NAME, () -> this.getUserJobCount(userName))
                        .tags(MetricsConstants.TagKeys.USER, userName)
                        .strongReference(false)
                        .register(registry);
                    Gauge.builder(USER_ACTIVE_MEMORY_METRIC_NAME, () -> this.getUserMemoryAmount(userName))
                        .tags(MetricsConstants.TagKeys.USER, userName)
                        .strongReference(false)
                        .register(registry);

                    return new UserResourcesRecord(userName, jobs, memory);
                }

            ).update(jobs, memory);
        }

        log.info("Done publishing user metrics");
    }

    private Number getUserJobCount(final String userName) {
        final UserResourcesRecord record = this.userResourcesRecordMap.get(userName);
        final long jobCount;
        if (record == null) {
            jobCount = 0;
        } else {
            jobCount = record.jobCount.get();
        }
        log.debug("Current jobs count for user '{}' is {}", userName, jobCount);
        return jobCount;
    }

    private long getUserMemoryAmount(final String userName) {
        final UserResourcesRecord record = this.userResourcesRecordMap.get(userName);
        final long memoryAmount;
        if (record == null) {
            memoryAmount = 0;
        } else {
            memoryAmount = record.memoryAmount.get();
        }
        log.debug("Current memory amount for user '{}' is {}MB", userName, memoryAmount);
        return memoryAmount;
    }

    private long getUsersCount() {
        return activeUsersCount.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void cleanup() {

        log.info("Cleaning up user metrics publishing");

        // Reset all users
        this.userResourcesRecordMap.clear();

        // Reset active jobs count
        this.activeUsersCount.set(0);
    }

    private static class UserResourcesRecord {
        private final String userName;
        private final AtomicLong jobCount;
        private final AtomicLong memoryAmount;

        UserResourcesRecord(
            final String userName,
            final long initialJobs,
            final long initialMemory
        ) {
            this.userName = userName;
            this.jobCount = new AtomicLong(initialJobs);
            this.memoryAmount = new AtomicLong(initialMemory);
        }

        void update(final long runningJobsCount, final long usedMemory) {
            log.debug(
                "Updating usage of user '{}': {} jobs totalling {}MB",
                this.userName,
                runningJobsCount,
                usedMemory
            );
            this.jobCount.set(runningJobsCount);
            this.memoryAmount.set(usedMemory);
        }
    }
}
