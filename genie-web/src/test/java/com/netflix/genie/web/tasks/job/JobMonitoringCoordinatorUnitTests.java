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
package com.netflix.genie.web.tasks.job;

import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.internal.dto.v4.Application;
import com.netflix.genie.common.internal.dto.v4.Cluster;
import com.netflix.genie.common.internal.dto.v4.Command;
import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.genie.web.events.GenieEventBus;
import com.netflix.genie.web.events.JobFinishedEvent;
import com.netflix.genie.web.events.JobFinishedReason;
import com.netflix.genie.web.events.JobStartedEvent;
import com.netflix.genie.web.properties.JobsProperties;
import com.netflix.genie.web.services.JobSearchService;
import com.netflix.genie.web.services.JobSubmitterService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.commons.exec.Executor;
import org.assertj.core.util.Lists;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.io.Resource;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.Trigger;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;

/**
 * Tests for the JobMonitoringCoordinator.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class JobMonitoringCoordinatorUnitTests {

    private static final String HOSTNAME = UUID.randomUUID().toString();
    private static final long DELAY = 38023L;

    /**
     * Temporary folder that will be deleted at the end of tests.
     */
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private TaskScheduler scheduler;
    private JobMonitoringCoordinator coordinator;
    private JobSearchService jobSearchService;
    private GenieEventBus genieEventBus;
    private Instant tomorrow;

    // Reusable mocks
    private JobRequest jobRequest = Mockito.mock(JobRequest.class);
    private Cluster cluster = Mockito.mock(Cluster.class);
    private Command command = Mockito.mock(Command.class);
    private List<Application> applications = Lists.newArrayList();

    /**
     * Setup for the tests.
     *
     * @throws IOException on error
     */
    @Before
    public void setup() throws IOException {
        this.tomorrow = Instant.now().plus(1, ChronoUnit.DAYS);
        this.jobSearchService = Mockito.mock(JobSearchService.class);
        final JobSubmitterService jobSubmitterService = Mockito.mock(JobSubmitterService.class);
        final Executor executor = Mockito.mock(Executor.class);
        this.scheduler = Mockito.mock(TaskScheduler.class);
        this.genieEventBus = Mockito.mock(GenieEventBus.class);

        final File jobsFile = this.folder.newFolder();
        final Resource jobsDir = Mockito.mock(Resource.class);
        Mockito.when(jobsDir.getFile()).thenReturn(jobsFile);

        this.coordinator = new JobMonitoringCoordinator(
            new GenieHostInfo(HOSTNAME),
            this.jobSearchService,
            this.genieEventBus,
            this.scheduler,
            executor,
            new SimpleMeterRegistry(),
            jobsDir,
            new JobsProperties(),
            jobSubmitterService
        );
    }

    /**
     * Make sure the system will re-attach to running jobs.
     *
     * @throws GenieException on issue
     */
    @Test
    @SuppressWarnings("unchecked")
    public void canAttachToRunningJobs() throws GenieException {
        final ContextRefreshedEvent event = Mockito.mock(ContextRefreshedEvent.class);

        Mockito.when(this.jobSearchService.getAllActiveJobsOnHost(HOSTNAME)).thenReturn(Sets.newHashSet());
        this.coordinator.onStartup(event);
        Mockito
            .verify(this.scheduler, Mockito.never())
            .scheduleWithFixedDelay(Mockito.any(JobMonitor.class), Mockito.anyLong());

        // Simulate a job being started
        final String job1Id = UUID.randomUUID().toString();
        final String job2Id = UUID.randomUUID().toString();
        final String job3Id = UUID.randomUUID().toString();
        final String job4Id = UUID.randomUUID().toString();
        final String job5Id = UUID.randomUUID().toString();
        final JobExecution.Builder builder = new JobExecution.Builder(UUID.randomUUID().toString())
            .withProcessId(2818)
            .withCheckDelay(DELAY)
            .withMemory(1024)
            .withTimeout(this.tomorrow);
        builder.withId(job1Id);
        final JobExecution job1 = builder.build();
        builder.withId(job2Id);
        final JobExecution job2 = builder.build();
        builder.withId(job3Id);
        final JobExecution job3 = builder.build();

        final JobStartedEvent event1 = new JobStartedEvent(job1, this);
        final ScheduledFuture future = Mockito.mock(ScheduledFuture.class);
        Mockito
            .when(this.scheduler.scheduleWithFixedDelay(Mockito.any(JobMonitor.class), Mockito.eq(DELAY)))
            .thenReturn(future);
        coordinator.init(job1Id);
        coordinator.schedule(job1Id, this.jobRequest, this.cluster, this.command, this.applications, 1024);
        this.coordinator.onJobStarted(event1);
        Mockito
            .verify(this.scheduler, Mockito.times(1))
            .schedule(Mockito.any(JobMonitor.class), Mockito.any(Trigger.class));

        final Job j1 = Mockito.mock(Job.class);
        Mockito.when(j1.getId()).thenReturn(Optional.of(job1Id));
        Mockito.when(j1.getStatus()).thenReturn(JobStatus.RUNNING);
        final Job j2 = Mockito.mock(Job.class);
        Mockito.when(j2.getId()).thenReturn(Optional.of(job2Id));
        Mockito.when(j2.getStatus()).thenReturn(JobStatus.RUNNING);
        final Job j3 = Mockito.mock(Job.class);
        Mockito.when(j3.getId()).thenReturn(Optional.of(job3Id));
        Mockito.when(j3.getStatus()).thenReturn(JobStatus.RUNNING);
        final Job j4 = Mockito.mock(Job.class);
        Mockito.when(j4.getId()).thenReturn(Optional.of(job4Id));
        Mockito.when(j4.getStatus()).thenReturn(JobStatus.RUNNING);
        final Job j5 = Mockito.mock(Job.class);
        Mockito.when(j5.getId()).thenReturn(Optional.of(job5Id));
        Mockito.when(j5.getStatus()).thenReturn(JobStatus.INIT);
        final Set<Job> jobs = Sets.newHashSet(j1, j2, j3, j4, j5);
        Mockito.when(this.jobSearchService.getAllActiveJobsOnHost(HOSTNAME)).thenReturn(jobs);
        Mockito.when(this.jobSearchService.getJobExecution(job1Id)).thenReturn(job1);
        Mockito.when(this.jobSearchService.getJobExecution(job2Id)).thenReturn(job2);
        Mockito.when(this.jobSearchService.getJobExecution(job3Id)).thenReturn(job3);
        Mockito.when(this.jobSearchService.getJobExecution(job4Id)).thenThrow(new GenieNotFoundException("blah"));
        this.coordinator.onStartup(event);

        Mockito
            .verify(this.genieEventBus, Mockito.times(2))
            .publishAsynchronousEvent(Mockito.any(JobFinishedEvent.class));
        Mockito
            .verify(this.scheduler, Mockito.times(3))
            .schedule(Mockito.any(JobMonitor.class), Mockito.any(Trigger.class));
        Assert.assertThat(this.coordinator.getNumActiveJobs(), Matchers.is(3));
        Assert.assertThat(this.coordinator.getUsedMemory(), Matchers.is(3 * 1024));
    }

    /**
     * Make sure when a {@link com.netflix.genie.web.events.JobStartedEvent} is sent a new monitor is spawned.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void canStartJobMonitor() {
        final String job1Id = UUID.randomUUID().toString();
        final String job2Id = UUID.randomUUID().toString();
        final String job3Id = UUID.randomUUID().toString();
        final String job4Id = UUID.randomUUID().toString();
        final JobExecution.Builder builder = new JobExecution.Builder(UUID.randomUUID().toString())
            .withProcessId(2818)
            .withCheckDelay(DELAY)
            .withMemory(1024)
            .withTimeout(this.tomorrow);
        builder.withId(job1Id);
        final JobExecution job1 = builder.build();
        builder.withId(job2Id);
        final JobExecution job2 = builder.build();
        builder.withId(job3Id);
        final JobExecution job3 = builder.build();
        builder.withId(job4Id);
        final JobExecution job4 = builder.build();

        coordinator.init(job1Id);
        coordinator.schedule(job1Id, this.jobRequest, this.cluster, this.command, this.applications, 1024);
        coordinator.init(job2Id);
        coordinator.schedule(job2Id, this.jobRequest, this.cluster, this.command, this.applications, 1024);
        coordinator.init(job3Id);
        coordinator.schedule(job3Id, this.jobRequest, this.cluster, this.command, this.applications, 1024);
        coordinator.init(job4Id);
        coordinator.schedule(job4Id, this.jobRequest, this.cluster, this.command, this.applications, 1024);
        coordinator.schedule(job1Id, this.jobRequest, this.cluster, this.command, this.applications, 1024);

        final JobStartedEvent event1 = new JobStartedEvent(job1, this);
        final JobStartedEvent event2 = new JobStartedEvent(job2, this);
        final JobStartedEvent event3 = new JobStartedEvent(job3, this);
        final JobStartedEvent event4 = new JobStartedEvent(job4, this);
        final JobStartedEvent event5 = new JobStartedEvent(job1, this);

        final ScheduledFuture future = Mockito.mock(ScheduledFuture.class);

        Mockito.when(
            this.scheduler.schedule(Mockito.any(JobMonitor.class), Mockito.any(Trigger.class))
        ).thenReturn(future);

        Assert.assertThat(this.coordinator.getNumActiveJobs(), Matchers.is(4));
        Assert.assertThat(this.coordinator.getUsedMemory(), Matchers.is(4096));
        this.coordinator.onJobStarted(event1);
        Assert.assertThat(this.coordinator.getNumActiveJobs(), Matchers.is(4));
        Assert.assertThat(this.coordinator.getUsedMemory(), Matchers.is(4096));
        this.coordinator.onJobStarted(event2);
        Assert.assertThat(this.coordinator.getNumActiveJobs(), Matchers.is(4));
        Assert.assertThat(this.coordinator.getUsedMemory(), Matchers.is(4096));
        this.coordinator.onJobStarted(event3);
        Assert.assertThat(this.coordinator.getNumActiveJobs(), Matchers.is(4));
        Assert.assertThat(this.coordinator.getUsedMemory(), Matchers.is(4096));
        this.coordinator.onJobStarted(event4);
        Assert.assertThat(this.coordinator.getNumActiveJobs(), Matchers.is(4));
        Assert.assertThat(this.coordinator.getUsedMemory(), Matchers.is(4096));
        this.coordinator.onJobStarted(event5);
        Assert.assertThat(this.coordinator.getNumActiveJobs(), Matchers.is(4));
        Assert.assertThat(this.coordinator.getUsedMemory(), Matchers.is(4096));

        Mockito
            .verify(this.scheduler, Mockito.times(5))
            .schedule(Mockito.any(JobMonitor.class), Mockito.any(Trigger.class));
    }

    /**
     * Make sure when a {@link com.netflix.genie.web.events.JobFinishedEvent} is sent the monitor is cancelled.
     *
     * @throws GenieException on error
     */
    @Test
    @SuppressWarnings("unchecked")
    public void canStopJobMonitor() throws GenieException {
        final String job1Id = UUID.randomUUID().toString();
        final JobExecution.Builder builder = new JobExecution.Builder(UUID.randomUUID().toString())
            .withProcessId(2818)
            .withCheckDelay(DELAY)
            .withMemory(1024)
            .withTimeout(this.tomorrow);
        builder.withId(job1Id);
        final JobExecution job1 = builder.build();
        final String job2Id = UUID.randomUUID().toString();
        builder.withId(job2Id);
        final JobExecution job2 = builder.build();

        final JobStartedEvent startedEvent1 = new JobStartedEvent(job1, this);
        final JobFinishedEvent finishedEvent1
            = new JobFinishedEvent(job1Id, JobFinishedReason.PROCESS_COMPLETED, "something", this);
        final JobStartedEvent startedEvent2 = new JobStartedEvent(job2, this);
        final JobFinishedEvent finishedEvent2
            = new JobFinishedEvent(job2Id, JobFinishedReason.KILLED, "something", this);

        final ScheduledFuture future1 = Mockito.mock(ScheduledFuture.class);
        Mockito.when(future1.cancel(true)).thenReturn(true);
        final ScheduledFuture future2 = Mockito.mock(ScheduledFuture.class);
        Mockito.when(future2.cancel(true)).thenReturn(false);

        Mockito.when(
            this.scheduler.schedule(Mockito.any(JobMonitor.class), Mockito.any(Trigger.class))
        ).thenReturn(future1, future2);

        Assert.assertThat(this.coordinator.getNumActiveJobs(), Matchers.is(0));
        Assert.assertThat(this.coordinator.getUsedMemory(), Matchers.is(0));
        coordinator.init(job1Id);
        coordinator.schedule(job1Id, this.jobRequest, this.cluster, this.command, this.applications, 1024);
        this.coordinator.onJobStarted(startedEvent1);
        Assert.assertThat(this.coordinator.getNumActiveJobs(), Matchers.is(1));
        Assert.assertThat(this.coordinator.getUsedMemory(), Matchers.is(1024));
        coordinator.init(job2Id);
        coordinator.schedule(job2Id, this.jobRequest, this.cluster, this.command, this.applications, 1024);
        this.coordinator.onJobStarted(startedEvent2);
        Assert.assertThat(this.coordinator.getNumActiveJobs(), Matchers.is(2));
        Assert.assertThat(this.coordinator.getUsedMemory(), Matchers.is(2048));

        Mockito
            .verify(this.scheduler, Mockito.times(2))
            .schedule(Mockito.any(JobMonitor.class), Mockito.any(Trigger.class));

        Assert.assertThat(this.coordinator.getNumActiveJobs(), Matchers.is(2));
        Assert.assertThat(this.coordinator.getUsedMemory(), Matchers.is(2048));
        this.coordinator.onJobFinished(finishedEvent1);
        Assert.assertThat(this.coordinator.getNumActiveJobs(), Matchers.is(1));
        Assert.assertThat(this.coordinator.getUsedMemory(), Matchers.is(1024));
        this.coordinator.onJobFinished(finishedEvent2);
        Assert.assertThat(this.coordinator.getNumActiveJobs(), Matchers.is(0));
        Assert.assertThat(this.coordinator.getUsedMemory(), Matchers.is(0));
        this.coordinator.onJobFinished(finishedEvent1);
        Assert.assertThat(this.coordinator.getNumActiveJobs(), Matchers.is(0));
        Assert.assertThat(this.coordinator.getUsedMemory(), Matchers.is(0));

        Mockito.verify(future1, Mockito.times(1)).cancel(true);
        Mockito.verify(future2, Mockito.times(1)).cancel(true);
    }

    /**
     * Make sure when a job is scheduled it counts in active jobs and increases memory.
     */
    @Test
    public void canScheduleJob() {
        final String jobId = UUID.randomUUID().toString();
        Assert.assertThat(this.coordinator.getNumActiveJobs(), Matchers.is(0));
        Assert.assertThat(this.coordinator.getUsedMemory(), Matchers.is(0));
        coordinator.init(jobId);
        coordinator.schedule(jobId, this.jobRequest, this.cluster, this.command, this.applications, 1024);
        Assert.assertThat(this.coordinator.getNumActiveJobs(), Matchers.is(1));
        Assert.assertThat(this.coordinator.getUsedMemory(), Matchers.is(1024));
    }

    /**
     * Make sure we can kill the job init task on job finished event for the job.
     *
     * @throws GenieException on error
     */
    @Test
    @SuppressWarnings("unchecked")
    public void canStopJobTask() throws GenieException {
        final String jobId = UUID.randomUUID().toString();
        final ScheduledFuture task = Mockito.mock(ScheduledFuture.class);
        final JobFinishedEvent jobFinishedEvent
            = new JobFinishedEvent(jobId, JobFinishedReason.FAILED_TO_INIT, "something", this);
        Mockito.when(task.isDone()).thenReturn(true).thenReturn(false).thenReturn(false);
        Mockito.when(task.cancel(true)).thenReturn(true).thenReturn(false);
        Mockito.when(this.scheduler.schedule(Mockito.any(), Mockito.any(Date.class))).thenReturn(task);
        Assert.assertThat(this.coordinator.getNumActiveJobs(), Matchers.is(0));
        Assert.assertThat(this.coordinator.getUsedMemory(), Matchers.is(0));
        coordinator.init(jobId);
        coordinator.schedule(jobId, this.jobRequest, this.cluster, this.command, this.applications, 1024);
        Assert.assertThat(this.coordinator.getNumActiveJobs(), Matchers.is(1));
        Assert.assertThat(this.coordinator.getUsedMemory(), Matchers.is(1024));
        this.coordinator.onJobFinished(jobFinishedEvent);
        Assert.assertThat(this.coordinator.getNumActiveJobs(), Matchers.is(0));
        Assert.assertThat(this.coordinator.getUsedMemory(), Matchers.is(0));
        coordinator.init(jobId);
        coordinator.schedule(jobId, this.jobRequest, this.cluster, this.command, this.applications, 1024);
        Assert.assertThat(this.coordinator.getNumActiveJobs(), Matchers.is(1));
        Assert.assertThat(this.coordinator.getUsedMemory(), Matchers.is(1024));
        this.coordinator.onJobFinished(jobFinishedEvent);
        Assert.assertThat(this.coordinator.getNumActiveJobs(), Matchers.is(0));
        Assert.assertThat(this.coordinator.getUsedMemory(), Matchers.is(0));
        coordinator.init(jobId);
        coordinator.schedule(jobId, this.jobRequest, this.cluster, this.command, this.applications, 1024);
        Assert.assertThat(this.coordinator.getNumActiveJobs(), Matchers.is(1));
        Assert.assertThat(this.coordinator.getUsedMemory(), Matchers.is(1024));
        this.coordinator.onJobFinished(jobFinishedEvent);
        Assert.assertThat(this.coordinator.getNumActiveJobs(), Matchers.is(0));
        Assert.assertThat(this.coordinator.getUsedMemory(), Matchers.is(0));
    }
}
