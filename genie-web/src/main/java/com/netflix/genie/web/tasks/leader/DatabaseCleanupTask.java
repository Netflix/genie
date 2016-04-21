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
import com.netflix.genie.web.properties.DatabaseCleanupProperties;
import com.netflix.genie.web.tasks.GenieTaskScheduleType;
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

/**
 * A task which will clean up the database of old jobs if desired.
 *
 * @author tgianos
 * @since 3.0.0
 */
@ConditionalOnProperty("genie.leader.databaseCleanup.enabled")
@Component
@Slf4j
public class DatabaseCleanupTask extends LeadershipTask {

    private final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    private final DatabaseCleanupProperties cleanupProperties;
    private final JobPersistenceService jobPersistenceService;

    /**
     * Constructor.
     *
     * @param cleanupProperties     The properties to use to configure this task
     * @param jobPersistenceService The persistence service to use to cleanup the data store
     */
    @Autowired
    public DatabaseCleanupTask(
        @NotNull final DatabaseCleanupProperties cleanupProperties,
        @NotNull final JobPersistenceService jobPersistenceService
    ) {
        this.cleanupProperties = cleanupProperties;
        this.jobPersistenceService = jobPersistenceService;
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
        final Calendar cal = Calendar.getInstance(JobConstants.UTC);
        // Make sure everything but the year, month, day is 0
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        // Move the date back the number of days retention is set for
        final int retention = this.cleanupProperties.getRetention();
        cal.add(Calendar.DAY_OF_YEAR, retention < 0 ? retention : retention * -1);
        final Date retentionLimit = cal.getTime();

        final long numberDeletedJobs = this.jobPersistenceService.deleteAllJobsCreatedBeforeDate(retentionLimit);
        log.info("Deleted {} jobs from before {}", numberDeletedJobs, this.dateFormat.format(retentionLimit));
    }
}
