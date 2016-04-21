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

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.cleanupProperties = Mockito.mock(DatabaseCleanupProperties.class);
        this.jobPersistenceService = Mockito.mock(JobPersistenceService.class);
        this.task = new DatabaseCleanupTask(this.cleanupProperties, this.jobPersistenceService);
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

        Mockito.when(this.cleanupProperties.getRetention()).thenReturn(days).thenReturn(negativeDays);
        final ArgumentCaptor<Date> argument = ArgumentCaptor.forClass(Date.class);

        // The multiple calendar instances are to protect against running this test when the day flips
        final Calendar before = Calendar.getInstance(JobConstants.UTC);
        this.task.run();
        this.task.run();
        final Calendar after = Calendar.getInstance(JobConstants.UTC);

        if (before.get(Calendar.DAY_OF_YEAR) == after.get(Calendar.DAY_OF_YEAR)) {
            Mockito
                .verify(this.jobPersistenceService, Mockito.times(2))
                .deleteAllJobsCreatedBeforeDate(argument.capture());
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
