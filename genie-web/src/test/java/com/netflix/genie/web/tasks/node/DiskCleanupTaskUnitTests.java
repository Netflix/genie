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
import com.netflix.genie.core.properties.JobsProperties;
import com.netflix.genie.core.services.JobSearchService;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.genie.web.properties.DiskCleanupProperties;
import com.netflix.genie.web.tasks.TaskUtils;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import org.apache.commons.exec.Executor;
import org.apache.commons.lang3.SystemUtils;
import org.hamcrest.Matchers;
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
import java.util.Calendar;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

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
        final JobsProperties properties = new JobsProperties();
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
                Mockito.mock(Registry.class)
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
                new JobsProperties(),
                Mockito.mock(Executor.class),
                Mockito.mock(Registry.class)
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
                new JobsProperties(),
                Mockito.mock(Executor.class),
                Mockito.mock(Registry.class)
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
        final JobsProperties properties = new JobsProperties();
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
                Mockito.mock(Registry.class)
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
        final JobsProperties jobsProperties = new JobsProperties();
        jobsProperties.getUsers().setRunAsUserEnabled(false);

        // Create some random junk file that should be ignored
        this.tmpJobDir.newFile(UUID.randomUUID().toString());
        final DiskCleanupProperties properties = new DiskCleanupProperties();
        final Calendar cal = TaskUtils.getMidnightUTC();
        TaskUtils.subtractDaysFromDate(cal, properties.getRetention());
        final Date threshold = cal.getTime();

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
        Mockito.when(job3.getFinished()).thenReturn(Optional.of(new Date(threshold.getTime() - 1)));
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
        final Registry registry = Mockito.mock(Registry.class);
        final AtomicLong numberOfDeletedJobDirs = new AtomicLong();
        Mockito.when(
            registry.gauge(
                Mockito.eq("genie.tasks.diskCleanup.numberDeletedJobDirs.gauge"),
                Mockito.any(AtomicLong.class)
            )
        ).thenReturn(numberOfDeletedJobDirs);
        final AtomicLong numberOfDirsUnableToDelete = new AtomicLong();
        Mockito.when(
            registry.gauge(
                Mockito.eq("genie.tasks.diskCleanup.numberDirsUnableToDelete.gauge"),
                Mockito.any(AtomicLong.class)
            )
        ).thenReturn(numberOfDirsUnableToDelete);
        final Counter unableToGetJobCounter = Mockito.mock(Counter.class);
        Mockito
            .when(registry.counter("genie.tasks.diskCleanup.unableToGetJobs.rate"))
            .thenReturn(unableToGetJobCounter);
        final Counter unabledToDeleteJobsDir = Mockito.mock(Counter.class);
        Mockito
            .when(registry.counter("genie.tasks.diskCleanup.unableToDeleteJobsDir.rate"))
            .thenReturn(unabledToDeleteJobsDir);

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
            registry
        );
        Assert.assertThat(numberOfDeletedJobDirs.get(), Matchers.is(0L));
        Assert.assertThat(numberOfDirsUnableToDelete.get(), Matchers.is(0L));
        task.run();
        Assert.assertThat(numberOfDeletedJobDirs.get(), Matchers.is(1L));
        Assert.assertThat(numberOfDirsUnableToDelete.get(), Matchers.is(1L));
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
