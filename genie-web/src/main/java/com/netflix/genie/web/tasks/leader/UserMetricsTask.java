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
import com.google.common.util.concurrent.AtomicDouble;
import com.netflix.genie.common.dto.UserResourcesSummary;
import com.netflix.genie.common.internal.dtos.JobStatus;
import com.netflix.genie.web.data.services.DataServices;
import com.netflix.genie.web.data.services.PersistenceService;
import com.netflix.genie.web.properties.UserMetricsProperties;
import com.netflix.genie.web.tasks.GenieTaskScheduleType;
import com.netflix.genie.web.util.MetricsConstants;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * A task which publishes user metrics.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class UserMetricsTask extends LeaderTask {

    private static final String USER_ACTIVE_JOBS_METRIC_NAME = "genie.user.active-jobs.gauge";
    private static final String USER_ACTIVE_MEMORY_METRIC_NAME = "genie.user.active-memory.gauge";
    private static final String USER_ACTIVE_USERS_METRIC_NAME = "genie.user.active-users.gauge";
    private static final String USER_ACTIVE_JOBS_BY_SUBMISSION_MODE_METRIC_NAME =
        "genie.user.active-jobs.by-submission-mode.gauge";
    private static final String USER_ACTIVE_MEMORY_BY_SUBMISSION_MODE_METRIC_NAME =
        "genie.user.active-memory.by-submission-mode.gauge";
    private static final UserResourcesRecord USER_RECORD_PLACEHOLDER = new UserResourcesRecord("nobody");
    private final MeterRegistry registry;
    private final PersistenceService persistenceService;
    private final UserMetricsProperties userMetricsProperties;

    private final Map<String, UserResourcesRecord> userResourcesRecordMap = Maps.newHashMap();
    // Records for the optional per-submission-mode resource breakdown: api (true/false) -> user -> record.
    private final Map<Boolean, Map<String, UserResourcesRecord>> userResourcesBySubmissionModeRecordMap =
        Maps.newHashMap();
    private final AtomicDouble activeUsersCount;

    /**
     * Constructor.
     *
     * @param registry              the metrics registry
     * @param dataServices          The {@link DataServices} instance to use
     * @param userMetricsProperties the properties that configure this task
     */
    public UserMetricsTask(
        final MeterRegistry registry,
        final DataServices dataServices,
        final UserMetricsProperties userMetricsProperties
    ) {
        this.registry = registry;
        this.persistenceService = dataServices.getPersistenceService();
        this.userMetricsProperties = userMetricsProperties;
        this.activeUsersCount = new AtomicDouble(Double.NaN);

        // Register gauge for count of distinct users with active jobs.
        Gauge.builder(USER_ACTIVE_USERS_METRIC_NAME, this::getUsersCount)
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
        log.debug("Publishing user metrics");

        // An opt-in per-submission-mode breakdown (API vs. agent/CLI) is published below when enabled.
        final Map<String, UserResourcesSummary> summaries = this.persistenceService.getUserResourcesSummaries(
            JobStatus.getActiveStatuses(),
            true
        );

        // Update number of active users
        log.debug("Number of users with active jobs: {}", summaries.size());
        this.activeUsersCount.set(summaries.size());

        // Track users who previously had jobs but no longer do
        final Set<String> usersToReset = Sets.newHashSet(this.userResourcesRecordMap.keySet());
        usersToReset.removeAll(summaries.keySet());

        for (final String user : usersToReset) {
            // Remove user. If gauge is polled, it'll return NaN
            this.userResourcesRecordMap.remove(user);
        }

        // Update existing user metrics
        for (final UserResourcesSummary userResourcesSummary : summaries.values()) {
            final String user = userResourcesSummary.getUser();
            final long jobs = userResourcesSummary.getRunningJobsCount();
            final long memory = userResourcesSummary.getUsedMemory();

            log.debug("User {}: {} jobs running, using {}MB", user, jobs, memory);

            this.userResourcesRecordMap.computeIfAbsent(
                userResourcesSummary.getUser(),
                userName -> {
                    // Register gauges for this user.
                    // Gauge creation is idempotent so it doesn't matter if the user is new or seen before.
                    // Registry holds a reference to the gauge so no need to save it.
                    Gauge.builder(
                            USER_ACTIVE_JOBS_METRIC_NAME,
                            () -> this.getUserJobCount(userName)
                        )
                        .tags(MetricsConstants.TagKeys.USER, userName)
                        .register(registry);
                    Gauge.builder(
                            USER_ACTIVE_MEMORY_METRIC_NAME,
                            () -> this.getUserMemoryAmount(userName)
                        )
                        .tags(MetricsConstants.TagKeys.USER, userName)
                        .register(registry);

                    return new UserResourcesRecord(userName);
                }

            ).update(jobs, memory);
        }

        // Optionally publish a finer-grained breakdown tagged by submission mode (mode=api for REST vs
        // mode=agent for agent/CLI). Emitted under separate metric names so the gauges above are unchanged.
        // Disabled by default.
        if (this.userMetricsProperties.isSplitBySubmissionMode()) {
            this.publishResourcesBySubmissionMode(summaries);
        }

        log.debug("Done publishing user metrics");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void cleanup() {

        log.debug("Cleaning up user metrics publishing");

        // Reset all users
        this.userResourcesRecordMap.clear();
        this.userResourcesBySubmissionModeRecordMap.clear();

        // Reset active users count
        this.activeUsersCount.set(Double.NaN);
    }

    private Number getUserJobCount(final String userName) {
        final UserResourcesRecord record = this.userResourcesRecordMap.getOrDefault(userName, USER_RECORD_PLACEHOLDER);
        final double jobCount = record.jobCount.get();
        log.debug("Current jobs count for user '{}' is {}", userName, (long) jobCount);
        return jobCount;
    }

    private Number getUserMemoryAmount(final String userName) {
        final UserResourcesRecord record = this.userResourcesRecordMap.getOrDefault(userName, USER_RECORD_PLACEHOLDER);
        final double memoryAmount = record.memoryAmount.get();
        log.debug("Current memory amount for user '{}' is {}MB", userName, (long) memoryAmount);
        return memoryAmount;
    }

    private Number getUsersCount() {
        return activeUsersCount.get();
    }

    private void publishResourcesBySubmissionMode(final Map<String, UserResourcesSummary> apiSummaries) {
        // Reuse the api=true summaries already fetched above; only the agent (api=false) slice is
        // an additional query.
        this.updateSubmissionModeRecords(true, apiSummaries);
        this.updateSubmissionModeRecords(
            false,
            this.persistenceService.getUserResourcesSummaries(JobStatus.getActiveStatuses(), false)
        );
    }

    private void updateSubmissionModeRecords(final boolean api, final Map<String, UserResourcesSummary> summaries) {
        final Map<String, UserResourcesRecord> recordsForMode =
            this.userResourcesBySubmissionModeRecordMap.computeIfAbsent(api, mode -> Maps.newHashMap());

        final String modeTagValue =
            api ? MetricsConstants.TagValues.MODE_API : MetricsConstants.TagValues.MODE_AGENT;

        // Drop users no longer active in this mode so their gauges fall back to NaN.
        recordsForMode.keySet().retainAll(summaries.keySet());

        for (final UserResourcesSummary summary : summaries.values()) {
            final String user = summary.getUser();
            recordsForMode.computeIfAbsent(
                user,
                userName -> {
                    // Gauge creation is idempotent so re-registration on reappearance is harmless.
                    Gauge.builder(
                            USER_ACTIVE_JOBS_BY_SUBMISSION_MODE_METRIC_NAME,
                            () -> this.getSubmissionModeJobCount(api, userName)
                        )
                        .tags(
                            MetricsConstants.TagKeys.USER, userName,
                            MetricsConstants.TagKeys.MODE, modeTagValue
                        )
                        .register(this.registry);
                    Gauge.builder(
                            USER_ACTIVE_MEMORY_BY_SUBMISSION_MODE_METRIC_NAME,
                            () -> this.getSubmissionModeMemoryAmount(api, userName)
                        )
                        .tags(
                            MetricsConstants.TagKeys.USER, userName,
                            MetricsConstants.TagKeys.MODE, modeTagValue
                        )
                        .register(this.registry);
                    return new UserResourcesRecord(userName);
                }
            ).update(summary.getRunningJobsCount(), summary.getUsedMemory());
        }
    }

    private Number getSubmissionModeJobCount(final boolean api, final String user) {
        return this.userResourcesBySubmissionModeRecordMap
            .getOrDefault(api, Collections.emptyMap())
            .getOrDefault(user, USER_RECORD_PLACEHOLDER)
            .jobCount.get();
    }

    private Number getSubmissionModeMemoryAmount(final boolean api, final String user) {
        return this.userResourcesBySubmissionModeRecordMap
            .getOrDefault(api, Collections.emptyMap())
            .getOrDefault(user, USER_RECORD_PLACEHOLDER)
            .memoryAmount.get();
    }

    private static class UserResourcesRecord {
        private final String userName;
        private final AtomicDouble jobCount = new AtomicDouble(Double.NaN);
        private final AtomicDouble memoryAmount = new AtomicDouble(Double.NaN);

        UserResourcesRecord(
            final String userName
        ) {
            this.userName = userName;
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
