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

import com.netflix.genie.common.internal.jobs.JobConstants;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.genie.web.properties.DatabaseCleanupProperties;
import com.netflix.genie.web.services.ClusterPersistenceService;
import com.netflix.genie.web.services.FilePersistenceService;
import com.netflix.genie.web.services.JobPersistenceService;
import com.netflix.genie.web.services.TagPersistenceService;
import com.netflix.genie.web.tasks.GenieTaskScheduleType;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.scheduling.support.CronTrigger;

import java.time.Instant;
import java.util.Calendar;

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
    private ClusterPersistenceService clusterPersistenceService;
    private FilePersistenceService filePersistenceService;
    private TagPersistenceService tagPersistenceService;
    private DatabaseCleanupTask task;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.cleanupProperties = Mockito.mock(DatabaseCleanupProperties.class);
        this.jobPersistenceService = Mockito.mock(JobPersistenceService.class);
        this.clusterPersistenceService = Mockito.mock(ClusterPersistenceService.class);
        this.filePersistenceService = Mockito.mock(FilePersistenceService.class);
        this.tagPersistenceService = Mockito.mock(TagPersistenceService.class);
        this.task = new DatabaseCleanupTask(
            this.cleanupProperties,
            this.jobPersistenceService,
            this.clusterPersistenceService,
            this.filePersistenceService,
            this.tagPersistenceService,
            new SimpleMeterRegistry()
        );
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
        final int pageSize = 10;
        final int maxDeleted = 10_000;

        Mockito.when(this.cleanupProperties.getRetention()).thenReturn(days).thenReturn(negativeDays);
        Mockito.when(this.cleanupProperties.getPageSize()).thenReturn(pageSize);
        Mockito.when(this.cleanupProperties.getMaxDeletedPerTransaction()).thenReturn(maxDeleted);
        final ArgumentCaptor<Instant> argument = ArgumentCaptor.forClass(Instant.class);

        final long deletedCount1 = 6L;
        final long deletedCount2 = 18L;
        final long deletedCount3 = 2L;
        Mockito
            .when(
                this.jobPersistenceService.deleteBatchOfJobsCreatedBeforeDate(
                    Mockito.any(Instant.class),
                    Mockito.anyInt(),
                    Mockito.anyInt()
                )
            )
            .thenReturn(deletedCount1)
            .thenReturn(0L)
            .thenReturn(deletedCount2)
            .thenReturn(deletedCount3)
            .thenReturn(0L);

        Mockito.when(this.clusterPersistenceService.deleteTerminatedClusters()).thenReturn(1L, 2L);
        Mockito.when(this.filePersistenceService.deleteUnusedFiles(Mockito.any(Instant.class))).thenReturn(3L, 4L);
        Mockito.when(this.tagPersistenceService.deleteUnusedTags(Mockito.any(Instant.class))).thenReturn(5L, 6L);

        // The multiple calendar instances are to protect against running this test when the day flips
        final Calendar before = Calendar.getInstance(JobConstants.UTC);
        this.task.run();
        this.task.run();
        final Calendar after = Calendar.getInstance(JobConstants.UTC);

        if (before.get(Calendar.DAY_OF_YEAR) == after.get(Calendar.DAY_OF_YEAR)) {
            Mockito
                .verify(this.jobPersistenceService, Mockito.times(5))
                .deleteBatchOfJobsCreatedBeforeDate(argument.capture(), Mockito.eq(maxDeleted), Mockito.eq(pageSize));
            final Calendar date = Calendar.getInstance(JobConstants.UTC);
            date.set(Calendar.HOUR_OF_DAY, 0);
            date.set(Calendar.MINUTE, 0);
            date.set(Calendar.SECOND, 0);
            date.set(Calendar.MILLISECOND, 0);
            date.add(Calendar.DAY_OF_YEAR, negativeDays);
            Assert.assertThat(argument.getAllValues().get(0).toEpochMilli(), Matchers.is(date.getTime().getTime()));
            Assert.assertThat(argument.getAllValues().get(1).toEpochMilli(), Matchers.is(date.getTime().getTime()));
            Mockito.verify(this.clusterPersistenceService, Mockito.times(2)).deleteTerminatedClusters();
            Mockito
                .verify(this.filePersistenceService, Mockito.times(2))
                .deleteUnusedFiles(Mockito.any(Instant.class));
            Mockito
                .verify(this.tagPersistenceService, Mockito.times(2))
                .deleteUnusedTags(Mockito.any(Instant.class));
        }
    }

    /**
     * Make sure the run method throws when an error is encountered.
     */
    @Test(expected = RuntimeException.class)
    public void cantRun() {
        final int days = 5;
        final int negativeDays = -1 * days;
        final int pageSize = 10;
        final int maxDeleted = 10_000;

        Mockito.when(this.cleanupProperties.getRetention()).thenReturn(days).thenReturn(negativeDays);
        Mockito.when(this.cleanupProperties.getPageSize()).thenReturn(pageSize);
        Mockito.when(this.cleanupProperties.getMaxDeletedPerTransaction()).thenReturn(maxDeleted);

        Mockito
            .when(
                this.jobPersistenceService.deleteBatchOfJobsCreatedBeforeDate(
                    Mockito.any(Instant.class),
                    Mockito.anyInt(),
                    Mockito.anyInt()
                )
            )
            .thenThrow(new RuntimeException("test"));

        this.task.run();
    }

    /**
     * Make sure individual cleanup sub-tasks are skipped according to properties.
     */
    @Test
    public void skipAll() {

        Mockito.when(this.cleanupProperties.isSkipJobsCleanup()).thenReturn(true);
        Mockito.when(this.cleanupProperties.isSkipClustersCleanup()).thenReturn(true);
        Mockito.when(this.cleanupProperties.isSkipTagsCleanup()).thenReturn(true);
        Mockito.when(this.cleanupProperties.isSkipFilesCleanup()).thenReturn(true);

        this.task.run();

        Mockito
            .verify(this.jobPersistenceService, Mockito.never())
            .deleteBatchOfJobsCreatedBeforeDate(
                Mockito.any(Instant.class),
                Mockito.anyInt(),
                Mockito.anyInt()
            );
        Mockito
            .verify(this.clusterPersistenceService, Mockito.never())
            .deleteTerminatedClusters();
        Mockito
            .verify(this.filePersistenceService, Mockito.never())
            .deleteUnusedFiles(Mockito.any(Instant.class));
        Mockito
            .verify(this.tagPersistenceService, Mockito.never())
            .deleteUnusedTags(Mockito.any(Instant.class));
    }
}
