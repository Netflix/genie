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

import com.netflix.genie.core.jobs.JobConstants;
import com.netflix.genie.core.services.JobPersistenceService;
import com.netflix.genie.core.util.MetricsUtils;
import com.netflix.genie.web.properties.DatabaseCleanupProperties;
import com.netflix.genie.web.tasks.GenieTaskScheduleType;
import com.netflix.genie.web.tasks.TaskUtils;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Component;

import javax.validation.constraints.NotNull;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A task which will clean up the database of old jobs if desired.
 *
 * @author tgianos
 * @since 3.0.0
 */
@ConditionalOnProperty("genie.tasks.databaseCleanup.enabled")
@Component
@Slf4j
public class DatabaseCleanupTask extends LeadershipTask {

    private final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    private final DatabaseCleanupProperties cleanupProperties;
    private final JobPersistenceService jobPersistenceService;

    private final Registry registry;
    private final AtomicLong numDeletedJobs;
    private final Id deletionTimerId;

    /**
     * Constructor.
     *
     * @param cleanupProperties     The properties to use to configure this task
     * @param jobPersistenceService The persistence service to use to cleanup the data store
     * @param registry              The metrics registry
     */
    @Autowired
    public DatabaseCleanupTask(
        @NotNull final DatabaseCleanupProperties cleanupProperties,
        @NotNull final JobPersistenceService jobPersistenceService,
        @NotNull final Registry registry
    ) {
        this.registry = registry;
        this.cleanupProperties = cleanupProperties;
        this.jobPersistenceService = jobPersistenceService;

        this.numDeletedJobs = registry.gauge("genie.tasks.databaseCleanup.numDeletedJobs.gauge", new AtomicLong());
        this.deletionTimerId = registry.createId("genie.tasks.databaseCleanup.duration.timer");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GenieTaskScheduleType getScheduleType() {
        return GenieTaskScheduleType.TRIGGER;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Trigger getTrigger() {
        return new CronTrigger(this.cleanupProperties.getExpression(), JobConstants.UTC);
    }

    /**
     * Clean out database based on date.
     */
    @Override
    public void run() {
        final long start = System.nanoTime();
        final Map<String, String> tags = MetricsUtils.newSuccessTagsMap();
        try {
            final Calendar cal = TaskUtils.getMidnightUTC();
            // Move the date back the number of days retention is set for
            TaskUtils.subtractDaysFromDate(cal, this.cleanupProperties.getRetention());
            final Date retentionLimit = cal.getTime();
            final String retentionLimitString = this.dateFormat.format(retentionLimit);
            final int batchSize = this.cleanupProperties.getBatchSize();

            log.info(
                "Attempting to delete jobs from before {} in batches of {} jobs per iteration",
                retentionLimitString,
                batchSize
            );
            final long numberDeletedJobs
                = this.jobPersistenceService.deleteAllJobsCreatedBeforeDate(retentionLimit, batchSize);
            log.info("Deleted {} jobs from before {}", numberDeletedJobs, retentionLimitString);
            this.numDeletedJobs.set(numberDeletedJobs);
        } catch (Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw t;
        } finally {
            final long finish = System.nanoTime();
            this.registry.timer(
                deletionTimerId.withTags(tags)
            ).record(finish - start, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void cleanup() {
        this.numDeletedJobs.set(0L);
    }
}
