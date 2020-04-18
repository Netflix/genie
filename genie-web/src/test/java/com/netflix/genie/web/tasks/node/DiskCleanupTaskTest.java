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
package com.netflix.genie.web.tasks.node;

import com.google.common.collect.Lists;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.web.data.services.DataServices;
import com.netflix.genie.web.data.services.PersistenceService;
import com.netflix.genie.web.properties.DiskCleanupProperties;
import com.netflix.genie.web.properties.JobsProperties;
import com.netflix.genie.web.tasks.TaskUtils;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.commons.exec.Executor;
import org.apache.commons.lang3.SystemUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.springframework.core.io.Resource;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.Trigger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.UUID;

/**
 * Unit tests for the disk cleanup task.
 *
 * @author tgianos
 * @since 3.0.0
 */
class DiskCleanupTaskTest {

    @Test
    public void cantConstruct() {
        final JobsProperties properties = JobsProperties.getJobsPropertiesDefaults();
        properties.getUsers().setRunAsUserEnabled(false);
        final Resource jobsDir = Mockito.mock(Resource.class);
        Mockito.when(jobsDir.exists()).thenReturn(false);
        final DataServices dataServices = Mockito.mock(DataServices.class);
        Mockito.when(dataServices.getPersistenceService()).thenReturn(Mockito.mock(PersistenceService.class));
        Assertions
            .assertThatIOException()
            .isThrownBy(
                () -> new DiskCleanupTask(
                    new DiskCleanupProperties(),
                    Mockito.mock(TaskScheduler.class),
                    jobsDir,
                    dataServices,
                    properties,
                    Mockito.mock(Executor.class),
                    new SimpleMeterRegistry()
                )
            );
    }

    @Test
    void wontScheduleOnNonUnixWithSudo() throws IOException {
        Assumptions.assumeTrue(!SystemUtils.IS_OS_UNIX);
        final TaskScheduler scheduler = Mockito.mock(TaskScheduler.class);
        final Resource jobsDir = Mockito.mock(Resource.class);
        Mockito.when(jobsDir.exists()).thenReturn(true);
        final DataServices dataServices = Mockito.mock(DataServices.class);
        Mockito.when(dataServices.getPersistenceService()).thenReturn(Mockito.mock(PersistenceService.class));
        Assertions.assertThat(
            new DiskCleanupTask(
                new DiskCleanupProperties(),
                scheduler,
                jobsDir,
                dataServices,
                JobsProperties.getJobsPropertiesDefaults(),
                Mockito.mock(Executor.class),
                new SimpleMeterRegistry()
            )
        ).isNotNull();
        Mockito.verify(scheduler, Mockito.never()).schedule(Mockito.any(Runnable.class), Mockito.any(Trigger.class));
    }

    @Test
    void willScheduleOnUnixWithSudo() throws IOException {
        Assumptions.assumeTrue(SystemUtils.IS_OS_UNIX);
        final TaskScheduler scheduler = Mockito.mock(TaskScheduler.class);
        final Resource jobsDir = Mockito.mock(Resource.class);
        Mockito.when(jobsDir.exists()).thenReturn(true);
        final DataServices dataServices = Mockito.mock(DataServices.class);
        Mockito.when(dataServices.getPersistenceService()).thenReturn(Mockito.mock(PersistenceService.class));
        Assertions.assertThat(
            new DiskCleanupTask(
                new DiskCleanupProperties(),
                scheduler,
                jobsDir,
                dataServices,
                JobsProperties.getJobsPropertiesDefaults(),
                Mockito.mock(Executor.class),
                new SimpleMeterRegistry()
            )
        ).isNotNull();
        Mockito.verify(scheduler, Mockito.times(1)).schedule(Mockito.any(Runnable.class), Mockito.any(Trigger.class));
    }

    @Test
    void willScheduleOnUnixWithoutSudo() throws IOException {
        Assumptions.assumeTrue(SystemUtils.IS_OS_UNIX);
        final JobsProperties properties = JobsProperties.getJobsPropertiesDefaults();
        properties.getUsers().setRunAsUserEnabled(false);
        final TaskScheduler scheduler = Mockito.mock(TaskScheduler.class);
        final Resource jobsDir = Mockito.mock(Resource.class);
        Mockito.when(jobsDir.exists()).thenReturn(true);
        final DataServices dataServices = Mockito.mock(DataServices.class);
        Mockito.when(dataServices.getPersistenceService()).thenReturn(Mockito.mock(PersistenceService.class));
        Assertions.assertThat(
            new DiskCleanupTask(
                new DiskCleanupProperties(),
                scheduler,
                jobsDir,
                dataServices,
                properties,
                Mockito.mock(Executor.class),
                new SimpleMeterRegistry()
            )
        ).isNotNull();
        Mockito.verify(scheduler, Mockito.times(1)).schedule(Mockito.any(Runnable.class), Mockito.any(Trigger.class));
    }

    @Test
    void canRunWithoutSudo(@TempDir final Path tempDir) throws IOException, GenieException {
        final JobsProperties jobsProperties = JobsProperties.getJobsPropertiesDefaults();
        jobsProperties.getUsers().setRunAsUserEnabled(false);

        // Create some random junk file that should be ignored
        final Path testFile = tempDir.resolve(UUID.randomUUID().toString());
        Files.write(testFile, Lists.newArrayList("hi", "bye"));
        final DiskCleanupProperties properties = new DiskCleanupProperties();
        final Instant threshold = TaskUtils.getMidnightUTC().minus(properties.getRetention(), ChronoUnit.DAYS);

        final String job1Id = UUID.randomUUID().toString();
        final String job2Id = UUID.randomUUID().toString();
        final String job3Id = UUID.randomUUID().toString();
        final String job4Id = UUID.randomUUID().toString();
        final String job5Id = UUID.randomUUID().toString();

        final Job job1 = Mockito.mock(Job.class);
        Mockito.when(job1.getStatus()).thenReturn(JobStatus.INIT);
        final Job job2 = Mockito.mock(Job.class);
        Mockito.when(job2.getStatus()).thenReturn(JobStatus.RUNNING);
        final Job job3 = Mockito.mock(Job.class);
        Mockito.when(job3.getStatus()).thenReturn(JobStatus.SUCCEEDED);
        Mockito.when(job3.getFinished()).thenReturn(Optional.of(threshold.minus(1, ChronoUnit.MILLIS)));
        final Job job4 = Mockito.mock(Job.class);
        Mockito.when(job4.getStatus()).thenReturn(JobStatus.FAILED);
        Mockito.when(job4.getFinished()).thenReturn(Optional.of(threshold));

        this.createJobDir(job1Id, tempDir);
        this.createJobDir(job2Id, tempDir);
        this.createJobDir(job3Id, tempDir);
        this.createJobDir(job4Id, tempDir);
        this.createJobDir(job5Id, tempDir);

        final TaskScheduler scheduler = Mockito.mock(TaskScheduler.class);
        final Resource jobDir = Mockito.mock(Resource.class);
        Mockito.when(jobDir.exists()).thenReturn(true);
        Mockito.when(jobDir.getFile()).thenReturn(tempDir.toFile());
        final PersistenceService persistenceService = Mockito.mock(PersistenceService.class);

        Mockito.when(persistenceService.getJob(job1Id)).thenReturn(job1);
        Mockito.when(persistenceService.getJob(job2Id)).thenReturn(job2);
        Mockito.when(persistenceService.getJob(job3Id)).thenReturn(job3);
        Mockito.when(persistenceService.getJob(job4Id)).thenReturn(job4);
        Mockito.when(persistenceService.getJob(job5Id)).thenThrow(new GenieServerException("blah"));

        final DataServices dataServices = Mockito.mock(DataServices.class);
        Mockito.when(dataServices.getPersistenceService()).thenReturn(persistenceService);

        final DiskCleanupTask task = new DiskCleanupTask(
            properties,
            scheduler,
            jobDir,
            dataServices,
            jobsProperties,
            Mockito.mock(Executor.class),
            new SimpleMeterRegistry()
        );
        task.run();
        Assertions.assertThat(new File(jobDir.getFile(), job1Id).exists()).isTrue();
        Assertions.assertThat(new File(jobDir.getFile(), job2Id).exists()).isTrue();
        Assertions.assertThat(new File(jobDir.getFile(), job3Id).exists()).isFalse();
        Assertions.assertThat(new File(jobDir.getFile(), job4Id).exists()).isTrue();
        Assertions.assertThat(new File(jobDir.getFile(), job5Id).exists()).isTrue();
    }

    private void createJobDir(final String id, final Path tmpDir) throws IOException {
        final File dir = Files.createDirectory(tmpDir.resolve(id)).toFile();

        for (int i = 0; i < 5; i++) {
            new File(dir, UUID.randomUUID().toString());
        }
        for (int i = 0; i < 5; i++) {
            final boolean success = new File(dir, UUID.randomUUID().toString()).mkdir();
            if (!success) {
                throw new IOException("Unable to create temporary directory.");
            }
        }
    }
}
