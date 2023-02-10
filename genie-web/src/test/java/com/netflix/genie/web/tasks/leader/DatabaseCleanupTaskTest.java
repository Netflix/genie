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

import com.netflix.genie.common.internal.dtos.ClusterStatus;
import com.netflix.genie.common.internal.dtos.CommandStatus;
import com.netflix.genie.common.internal.dtos.JobStatus;
import com.netflix.genie.common.internal.jobs.JobConstants;
import com.netflix.genie.web.data.services.DataServices;
import com.netflix.genie.web.data.services.PersistenceService;
import com.netflix.genie.web.properties.DatabaseCleanupProperties;
import com.netflix.genie.web.tasks.GenieTaskScheduleType;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.support.CronTrigger;

import java.time.Instant;
import java.util.Calendar;
import java.util.EnumSet;

/**
 * Unit tests for {@link DatabaseCleanupTask}.
 *
 * @author tgianos
 * @since 3.0.0
 */
class DatabaseCleanupTaskTest {

    private DatabaseCleanupProperties cleanupProperties;
    private DatabaseCleanupProperties.ApplicationDatabaseCleanupProperties applicationCleanupProperties;
    private DatabaseCleanupProperties.ClusterDatabaseCleanupProperties clusterCleanupProperties;
    private DatabaseCleanupProperties.CommandDatabaseCleanupProperties commandCleanupProperties;
    private DatabaseCleanupProperties.CommandDeactivationDatabaseCleanupProperties commandDeactivationProperties;
    private DatabaseCleanupProperties.FileDatabaseCleanupProperties fileCleanupProperties;
    private DatabaseCleanupProperties.JobDatabaseCleanupProperties jobCleanupProperties;
    private DatabaseCleanupProperties.TagDatabaseCleanupProperties tagCleanupProperties;
    private MockEnvironment environment;
    private PersistenceService persistenceService;
    private DatabaseCleanupTask task;

    /**
     * Setup for the tests.
     */
    @BeforeEach
    void setup() {
        this.cleanupProperties = Mockito.mock(DatabaseCleanupProperties.class);
        this.applicationCleanupProperties
            = Mockito.mock(DatabaseCleanupProperties.ApplicationDatabaseCleanupProperties.class);
        Mockito.when(this.cleanupProperties.getApplicationCleanup()).thenReturn(this.applicationCleanupProperties);
        this.clusterCleanupProperties = Mockito.mock(DatabaseCleanupProperties.ClusterDatabaseCleanupProperties.class);
        Mockito.when(this.cleanupProperties.getClusterCleanup()).thenReturn(this.clusterCleanupProperties);
        this.commandCleanupProperties = Mockito.mock(DatabaseCleanupProperties.CommandDatabaseCleanupProperties.class);
        Mockito.when(this.cleanupProperties.getCommandCleanup()).thenReturn(this.commandCleanupProperties);
        this.commandDeactivationProperties
            = Mockito.mock(DatabaseCleanupProperties.CommandDeactivationDatabaseCleanupProperties.class);
        Mockito.when(this.cleanupProperties.getCommandDeactivation()).thenReturn(this.commandDeactivationProperties);
        this.fileCleanupProperties = Mockito.mock(DatabaseCleanupProperties.FileDatabaseCleanupProperties.class);
        Mockito.when(this.cleanupProperties.getFileCleanup()).thenReturn(this.fileCleanupProperties);
        this.jobCleanupProperties = Mockito.mock(DatabaseCleanupProperties.JobDatabaseCleanupProperties.class);
        Mockito.when(this.cleanupProperties.getJobCleanup()).thenReturn(this.jobCleanupProperties);
        this.tagCleanupProperties = Mockito.mock(DatabaseCleanupProperties.TagDatabaseCleanupProperties.class);
        Mockito.when(this.cleanupProperties.getTagCleanup()).thenReturn(this.tagCleanupProperties);
        this.environment = new MockEnvironment();
        this.persistenceService = Mockito.mock(PersistenceService.class);
        final DataServices dataServices = Mockito.mock(DataServices.class);
        Mockito.when(dataServices.getPersistenceService()).thenReturn(this.persistenceService);
        this.task = new DatabaseCleanupTask(
            this.cleanupProperties,
            this.environment,
            dataServices,
            new SimpleMeterRegistry()
        );
    }

    /**
     * Make sure the schedule type returns the correct thing.
     */
    @Test
    void canGetScheduleType() {
        Assertions.assertThat(this.task.getScheduleType()).isEqualTo(GenieTaskScheduleType.TRIGGER);
    }

    /**
     * Make sure the trigger returned is accurate.
     */
    @Test
    void canGetTrigger() {
        final String expression = "0 0 1 * * *";
        this.environment.setProperty(DatabaseCleanupProperties.EXPRESSION_PROPERTY, expression);
        Mockito.when(this.cleanupProperties.getExpression()).thenReturn("0 0 0 * * *");
        final Trigger trigger = this.task.getTrigger();
        if (trigger instanceof CronTrigger) {
            final CronTrigger cronTrigger = (CronTrigger) trigger;
            Assertions.assertThat(cronTrigger.getExpression()).isEqualTo(expression);
        } else {
            Assertions.fail("Trigger was not of expected type: " + CronTrigger.class.getName());
        }
    }

    /**
     * Make sure the run method passes in the expected date.
     */
    @Test
    void canRun() {
        final int days = 5;
        final int negativeDays = -1 * days;
        final int pageSize = 10;
        final int batchSize = 100;
        final int rollingWindow = 12;
        final int batchDaysWithin = 3;

        Mockito.when(this.cleanupProperties.getBatchSize()).thenReturn(batchSize);
        Mockito.when(this.fileCleanupProperties.getBatchDaysWithin()).thenReturn(batchDaysWithin);
        Mockito.when(this.fileCleanupProperties.getRollingWindowHours()).thenReturn(rollingWindow);
        Mockito.when(this.jobCleanupProperties.getRetention()).thenReturn(days).thenReturn(negativeDays);
        Mockito.when(this.jobCleanupProperties.getPageSize()).thenReturn(pageSize);
        Mockito.when(this.commandDeactivationProperties.getCommandCreationThreshold()).thenReturn(60);

        final ArgumentCaptor<Instant> argument = ArgumentCaptor.forClass(Instant.class);

        final long deletedCount1 = 6L;
        final long deletedCount2 = 18L;
        final long deletedCount3 = 2L;
        Mockito
            .when(
                this.persistenceService.deleteJobsCreatedBefore(
                    Mockito.any(Instant.class),
                    Mockito.eq(JobStatus.getActiveStatuses()),
                    Mockito.eq(pageSize)
                )
            )
            .thenReturn(deletedCount1)
            .thenReturn(0L)
            .thenReturn(deletedCount2)
            .thenReturn(deletedCount3)
            .thenReturn(0L);

        Mockito
            .when(
                this.persistenceService.deleteUnusedClusters(
                    Mockito.eq(EnumSet.of(ClusterStatus.TERMINATED)),
                    Mockito.any(Instant.class),
                    Mockito.eq(batchSize)
                )
            ).thenReturn(1L, 0L, 2L, 0L);
        Mockito
            .when(this.persistenceService.deleteUnusedFiles(
                Mockito.any(Instant.class), Mockito.any(Instant.class), Mockito.eq(batchSize)))
            .thenReturn(3L, 0L, 4L, 0L);
        Mockito
            .when(this.persistenceService.deleteUnusedTags(Mockito.any(Instant.class), Mockito.eq(batchSize)))
            .thenReturn(5L, 0L, 6L, 0L);
        Mockito
            .when(this.persistenceService.deleteUnusedApplications(Mockito.any(Instant.class), Mockito.eq(batchSize)))
            .thenReturn(11L, 0L, 100L, 17L, 0L);
        Mockito
            .when(
                this.persistenceService.updateStatusForUnusedCommands(
                    Mockito.eq(CommandStatus.INACTIVE),
                    Mockito.any(Instant.class),
                    Mockito.eq(EnumSet.of(CommandStatus.DEPRECATED, CommandStatus.ACTIVE)),
                    Mockito.anyInt()
                )
            )
            .thenReturn(50, 0, 242, 0);
        Mockito
            .when(
                this.persistenceService.deleteUnusedCommands(
                    Mockito.eq(EnumSet.of(CommandStatus.INACTIVE)),
                    Mockito.any(Instant.class),
                    Mockito.eq(batchSize)
                )
            )
            .thenReturn(11L, 0L, 81L, 0L);

        // The multiple calendar instances are to protect against running this test when the day flips
        final Calendar before = Calendar.getInstance(JobConstants.UTC);
        this.task.run();
        this.task.run();
        final Calendar after = Calendar.getInstance(JobConstants.UTC);

        if (before.get(Calendar.DAY_OF_YEAR) == after.get(Calendar.DAY_OF_YEAR)) {
            Mockito
                .verify(this.persistenceService, Mockito.times(5))
                .deleteJobsCreatedBefore(
                    argument.capture(),
                    Mockito.eq(JobStatus.getActiveStatuses()),
                    Mockito.eq(pageSize)
                );
            final Calendar date = Calendar.getInstance(JobConstants.UTC);
            date.set(Calendar.HOUR_OF_DAY, 0);
            date.set(Calendar.MINUTE, 0);
            date.set(Calendar.SECOND, 0);
            date.set(Calendar.MILLISECOND, 0);
            date.add(Calendar.DAY_OF_YEAR, negativeDays);
            Assertions.assertThat(argument.getAllValues().get(0).toEpochMilli()).isEqualTo(date.getTime().getTime());
            Assertions.assertThat(argument.getAllValues().get(1).toEpochMilli()).isEqualTo(date.getTime().getTime());
            Mockito.verify(this.persistenceService, Mockito.times(4)).deleteUnusedClusters(
                Mockito.eq(EnumSet.of(ClusterStatus.TERMINATED)),
                Mockito.any(Instant.class),
                Mockito.eq(batchSize)
            );
            Mockito
                .verify(this.persistenceService, Mockito.times(16))
                .deleteUnusedFiles(Mockito.any(Instant.class), Mockito.any(Instant.class), Mockito.eq(batchSize));
            Mockito
                .verify(this.persistenceService, Mockito.times(4))
                .deleteUnusedTags(Mockito.any(Instant.class), Mockito.eq(batchSize));
            Mockito
                .verify(this.persistenceService, Mockito.times(5))
                .deleteUnusedApplications(Mockito.any(Instant.class), Mockito.eq(batchSize));
            Mockito
                .verify(this.persistenceService, Mockito.times(4))
                .deleteUnusedCommands(
                    Mockito.eq(EnumSet.of(CommandStatus.INACTIVE)),
                    Mockito.any(Instant.class),
                    Mockito.eq(batchSize)
                );
            Mockito
                .verify(this.persistenceService, Mockito.times(4))
                .updateStatusForUnusedCommands(
                    Mockito.eq(CommandStatus.INACTIVE),
                    Mockito.any(Instant.class),
                    Mockito.eq(EnumSet.of(CommandStatus.DEPRECATED, CommandStatus.ACTIVE)),
                    Mockito.anyInt()
                );
        }
    }

    /**
     * Make sure the run method throws when an error is encountered.
     */
    @Test
    void cantRun() {
        final int days = 5;
        final int negativeDays = -1 * days;
        final int pageSize = 10;
        final int batchSize = 100;

        Mockito.when(this.cleanupProperties.getBatchSize()).thenReturn(batchSize);
        Mockito.when(this.jobCleanupProperties.getRetention()).thenReturn(days).thenReturn(negativeDays);
        Mockito.when(this.jobCleanupProperties.getPageSize()).thenReturn(pageSize);

        Mockito
            .when(
                this.persistenceService.deleteJobsCreatedBefore(
                    Mockito.any(Instant.class),
                    Mockito.anySet(),
                    Mockito.anyInt()
                )
            )
            .thenThrow(new RuntimeException("test"));

        Assertions.assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> this.task.run());
    }

    /**
     * Make sure individual cleanup sub-tasks are skipped according to properties.
     */
    @Test
    void skipAll() {
        this.environment.setProperty(
            DatabaseCleanupProperties.ApplicationDatabaseCleanupProperties.SKIP_PROPERTY,
            "true"
        );
        this.environment.setProperty(DatabaseCleanupProperties.CommandDatabaseCleanupProperties.SKIP_PROPERTY, "true");
        this.environment.setProperty(
            DatabaseCleanupProperties.CommandDeactivationDatabaseCleanupProperties.SKIP_PROPERTY,
            "true"
        );
        this.environment.setProperty(DatabaseCleanupProperties.ClusterDatabaseCleanupProperties.SKIP_PROPERTY, "true");
        this.environment.setProperty(DatabaseCleanupProperties.FileDatabaseCleanupProperties.SKIP_PROPERTY, "true");
        this.environment.setProperty(DatabaseCleanupProperties.JobDatabaseCleanupProperties.SKIP_PROPERTY, "true");
        this.environment.setProperty(DatabaseCleanupProperties.TagDatabaseCleanupProperties.SKIP_PROPERTY, "true");
        Mockito.when(this.applicationCleanupProperties.isSkip()).thenReturn(false);
        Mockito.when(this.commandCleanupProperties.isSkip()).thenReturn(false);
        Mockito.when(this.commandDeactivationProperties.isSkip()).thenReturn(false);
        Mockito.when(this.clusterCleanupProperties.isSkip()).thenReturn(false);
        Mockito.when(this.fileCleanupProperties.isSkip()).thenReturn(false);
        Mockito.when(this.tagCleanupProperties.isSkip()).thenReturn(false);
        Mockito.when(this.jobCleanupProperties.isSkip()).thenReturn(false);

        this.task.run();

        Mockito
            .verify(this.persistenceService, Mockito.never())
            .deleteUnusedApplications(Mockito.any(Instant.class), Mockito.anyInt());
        Mockito
            .verify(this.persistenceService, Mockito.never())
            .deleteUnusedCommands(Mockito.anySet(), Mockito.any(Instant.class), Mockito.anyInt());
        Mockito
            .verify(this.persistenceService, Mockito.never())
            .updateStatusForUnusedCommands(
                Mockito.any(CommandStatus.class),
                Mockito.any(Instant.class),
                Mockito.anySet(),
                Mockito.anyInt()
            );
        Mockito
            .verify(this.persistenceService, Mockito.never())
            .deleteJobsCreatedBefore(
                Mockito.any(Instant.class),
                Mockito.anySet(),
                Mockito.anyInt()
            );
        Mockito
            .verify(this.persistenceService, Mockito.never())
            .deleteUnusedClusters(Mockito.anySet(), Mockito.any(Instant.class), Mockito.anyInt());
        Mockito
            .verify(this.persistenceService, Mockito.never())
            .deleteUnusedFiles(Mockito.any(Instant.class), Mockito.any(Instant.class), Mockito.anyInt());
        Mockito
            .verify(this.persistenceService, Mockito.never())
            .deleteUnusedTags(Mockito.any(Instant.class), Mockito.anyInt());
    }
}
