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

import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.genie.web.properties.DiskCleanupProperties;
import com.netflix.genie.web.properties.JobsProperties;
import com.netflix.genie.web.services.JobSearchService;
import com.netflix.genie.web.tasks.TaskUtils;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.commons.exec.Executor;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.springframework.core.io.Resource;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.Trigger;

import java.io.File;
import java.io.IOException;
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
@Category(UnitTest.class)
public class DiskCleanupTaskUnitTests {

    /**
     * Temporary folder used for storing fake job files. Deleted after tests are done.
     */
    @Rule
    public TemporaryFolder tmpJobDir = new TemporaryFolder();

    /**
     * Test the constructor on error case.
     *
     * @throws IOException on error
     */
    @Test(expected = IOException.class)
    public void cantConstruct() throws IOException {
        final JobsProperties properties = JobsProperties.getJobsPropertiesDefaults();
        properties.getUsers().setRunAsUserEnabled(false);
        final Resource jobsDir = Mockito.mock(Resource.class);
        Mockito.when(jobsDir.exists()).thenReturn(false);
        Assert.assertNotNull(
            new DiskCleanupTask(
                new DiskCleanupProperties(),
                Mockito.mock(TaskScheduler.class),
                jobsDir,
                Mockito.mock(JobSearchService.class),
                properties,
                Mockito.mock(Executor.class),
                new SimpleMeterRegistry()
            )
        );
    }

    /**
     * Test the constructor on error case.
     *
     * @throws IOException on error
     */
    @Test
    public void wontScheduleOnNonUnixWithSudo() throws IOException {
        Assume.assumeTrue(!SystemUtils.IS_OS_UNIX);
        final TaskScheduler scheduler = Mockito.mock(TaskScheduler.class);
        final Resource jobsDir = Mockito.mock(Resource.class);
        Mockito.when(jobsDir.exists()).thenReturn(true);
        Assert.assertNotNull(
            new DiskCleanupTask(
                new DiskCleanupProperties(),
                scheduler,
                jobsDir,
                Mockito.mock(JobSearchService.class),
                JobsProperties.getJobsPropertiesDefaults(),
                Mockito.mock(Executor.class),
                new SimpleMeterRegistry()
            )
        );
        Mockito.verify(scheduler, Mockito.never()).schedule(Mockito.any(Runnable.class), Mockito.any(Trigger.class));
    }

    /**
     * Test the constructor.
     *
     * @throws IOException on error
     */
    @Test
    public void willScheduleOnUnixWithSudo() throws IOException {
        Assume.assumeTrue(SystemUtils.IS_OS_UNIX);
        final TaskScheduler scheduler = Mockito.mock(TaskScheduler.class);
        final Resource jobsDir = Mockito.mock(Resource.class);
        Mockito.when(jobsDir.exists()).thenReturn(true);
        Assert.assertNotNull(
            new DiskCleanupTask(
                new DiskCleanupProperties(),
                scheduler,
                jobsDir,
                Mockito.mock(JobSearchService.class),
                JobsProperties.getJobsPropertiesDefaults(),
                Mockito.mock(Executor.class),
                new SimpleMeterRegistry()
            )
        );
        Mockito.verify(scheduler, Mockito.times(1)).schedule(Mockito.any(Runnable.class), Mockito.any(Trigger.class));
    }

    /**
     * Test the constructor.
     *
     * @throws IOException on error
     */
    @Test
    public void willScheduleOnUnixWithoutSudo() throws IOException {
        final JobsProperties properties = JobsProperties.getJobsPropertiesDefaults();
        properties.getUsers().setRunAsUserEnabled(false);
        Assume.assumeTrue(SystemUtils.IS_OS_UNIX);
        final TaskScheduler scheduler = Mockito.mock(TaskScheduler.class);
        final Resource jobsDir = Mockito.mock(Resource.class);
        Mockito.when(jobsDir.exists()).thenReturn(true);
        Assert.assertNotNull(
            new DiskCleanupTask(
                new DiskCleanupProperties(),
                scheduler,
                jobsDir,
                Mockito.mock(JobSearchService.class),
                properties,
                Mockito.mock(Executor.class),
                new SimpleMeterRegistry()
            )
        );
        Mockito.verify(scheduler, Mockito.times(1)).schedule(Mockito.any(Runnable.class), Mockito.any(Trigger.class));
    }

    /**
     * Make sure we can run successfully when runAsUser is false for the system.
     *
     * @throws IOException    on error
     * @throws GenieException on error
     */
    @Test
    public void canRunWithoutSudo() throws IOException, GenieException {
        final JobsProperties jobsProperties = JobsProperties.getJobsPropertiesDefaults();
        jobsProperties.getUsers().setRunAsUserEnabled(false);

        // Create some random junk file that should be ignored
        this.tmpJobDir.newFile(UUID.randomUUID().toString());
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

        this.createJobDir(job1Id);
        this.createJobDir(job2Id);
        this.createJobDir(job3Id);
        this.createJobDir(job4Id);
        this.createJobDir(job5Id);

        final TaskScheduler scheduler = Mockito.mock(TaskScheduler.class);
        final Resource jobDir = Mockito.mock(Resource.class);
        Mockito.when(jobDir.exists()).thenReturn(true);
        Mockito.when(jobDir.getFile()).thenReturn(this.tmpJobDir.getRoot());
        final JobSearchService jobSearchService = Mockito.mock(JobSearchService.class);

        Mockito.when(jobSearchService.getJob(job1Id)).thenReturn(job1);
        Mockito.when(jobSearchService.getJob(job2Id)).thenReturn(job2);
        Mockito.when(jobSearchService.getJob(job3Id)).thenReturn(job3);
        Mockito.when(jobSearchService.getJob(job4Id)).thenReturn(job4);
        Mockito.when(jobSearchService.getJob(job5Id)).thenThrow(new GenieServerException("blah"));

        final DiskCleanupTask task = new DiskCleanupTask(
            properties,
            scheduler,
            jobDir,
            jobSearchService,
            jobsProperties,
            Mockito.mock(Executor.class),
            new SimpleMeterRegistry()
        );
        task.run();
        Assert.assertTrue(new File(jobDir.getFile(), job1Id).exists());
        Assert.assertTrue(new File(jobDir.getFile(), job2Id).exists());
        Assert.assertFalse(new File(jobDir.getFile(), job3Id).exists());
        Assert.assertTrue(new File(jobDir.getFile(), job4Id).exists());
        Assert.assertTrue(new File(jobDir.getFile(), job5Id).exists());
    }

    private void createJobDir(final String id) throws IOException {
        final File dir = this.tmpJobDir.newFolder(id);

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
