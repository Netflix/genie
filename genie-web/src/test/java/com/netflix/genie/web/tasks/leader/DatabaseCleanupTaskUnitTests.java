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
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.genie.web.properties.DatabaseCleanupProperties;
import com.netflix.genie.web.tasks.GenieTaskScheduleType;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.scheduling.support.CronTrigger;

import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Unit tests for DatabaseCleanupTask.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class DatabaseCleanupTaskUnitTests {

    private DatabaseCleanupProperties cleanupProperties;
    private JobPersistenceService jobPersistenceService;
    private DatabaseCleanupTask task;
    private AtomicLong numDeletedJobs;
    private Timer deletionTimer;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.cleanupProperties = Mockito.mock(DatabaseCleanupProperties.class);
        this.jobPersistenceService = Mockito.mock(JobPersistenceService.class);
        this.numDeletedJobs = new AtomicLong();
        this.deletionTimer = Mockito.mock(Timer.class);
        final Registry registry = Mockito.mock(Registry.class);
        Mockito
            .when(
                registry.gauge(
                    Mockito.eq("genie.tasks.databaseCleanup.numDeletedJobs.gauge"),
                    Mockito.any(AtomicLong.class)
                )
            ).thenReturn(this.numDeletedJobs);
        Mockito.when(registry.timer("genie.tasks.databaseCleanup.duration.timer")).thenReturn(this.deletionTimer);
        this.task = new DatabaseCleanupTask(this.cleanupProperties, this.jobPersistenceService, registry);
    }

    /**
     * Make sure the schedule type returns the correct thing.
     */
    @Test
    public void canGetScheduleType() {
        Assert.assertThat(this.task.getScheduleType(), Matchers.is(GenieTaskScheduleType.TRIGGER));
    }

    /**
     * Make sure the trigger returned is accurate.
     */
    @Test
    public void canGetTrigger() {
        Mockito.when(this.cleanupProperties.getExpression()).thenReturn("0 0 0 * * *");
        Assert.assertTrue(this.task.getTrigger() instanceof CronTrigger);
    }

    /**
     * Make sure the run method passes in the expected date.
     */
    @Test
    public void canRun() {
        final int days = 5;
        final int negativeDays = -1 * days;
        final int batchSize = 10;

        Mockito.when(this.cleanupProperties.getRetention()).thenReturn(days).thenReturn(negativeDays);
        Mockito.when(this.cleanupProperties.getBatchSize()).thenReturn(batchSize);
        final ArgumentCaptor<Date> argument = ArgumentCaptor.forClass(Date.class);

        final long deletedCount1 = 6L;
        final long deletedCount2 = 18L;
        Mockito
            .when(this.jobPersistenceService.deleteAllJobsCreatedBeforeDate(Mockito.any(Date.class), Mockito.anyInt()))
            .thenReturn(deletedCount1)
            .thenReturn(deletedCount2);

        // The multiple calendar instances are to protect against running this test when the day flips
        final Calendar before = Calendar.getInstance(JobConstants.UTC);
        this.task.run();
        Assert.assertThat(this.numDeletedJobs.get(), Matchers.is(deletedCount1));
        this.task.run();
        final Calendar after = Calendar.getInstance(JobConstants.UTC);
        Assert.assertThat(this.numDeletedJobs.get(), Matchers.is(deletedCount2));

        Mockito
            .verify(this.deletionTimer, Mockito.times(2))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));

        if (before.get(Calendar.DAY_OF_YEAR) == after.get(Calendar.DAY_OF_YEAR)) {
            Mockito
                .verify(this.jobPersistenceService, Mockito.times(2))
                .deleteAllJobsCreatedBeforeDate(argument.capture(), Mockito.eq(batchSize));
            final Calendar date = Calendar.getInstance(JobConstants.UTC);
            date.set(Calendar.HOUR_OF_DAY, 0);
            date.set(Calendar.MINUTE, 0);
            date.set(Calendar.SECOND, 0);
            date.set(Calendar.MILLISECOND, 0);
            date.add(Calendar.DAY_OF_YEAR, negativeDays);
            Assert.assertThat(argument.getAllValues().get(0), Matchers.is(date.getTime()));
            Assert.assertThat(argument.getAllValues().get(1), Matchers.is(date.getTime()));
        }
    }
}
