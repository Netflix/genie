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
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.core.events.JobFinishedEvent;
import com.netflix.genie.core.events.JobStartedEvent;
import com.netflix.genie.core.services.JobSearchService;
import com.netflix.genie.test.categories.UnitTest;
import org.apache.commons.exec.Executor;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.scheduling.TaskScheduler;

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
    private TaskScheduler scheduler;
    private JobMonitoringCoordinator coordinator;
    private JobSearchService jobSearchService;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.jobSearchService = Mockito.mock(JobSearchService.class);
        final Executor executor = Mockito.mock(Executor.class);
        this.scheduler = Mockito.mock(TaskScheduler.class);
        final ApplicationEventPublisher publisher = Mockito.mock(ApplicationEventPublisher.class);

        this.coordinator
            = new JobMonitoringCoordinator(HOSTNAME, this.jobSearchService, publisher, this.scheduler, executor);
    }

    /**
     * Make sure the system will re-attach to running jobs.
     *
     * @throws GenieException on issue
     */
    @Test
    @SuppressWarnings("unchecked")
    public void canAttachToRunningJobs() throws GenieException {
        final ApplicationReadyEvent event = Mockito.mock(ApplicationReadyEvent.class);

        Mockito.when(this.jobSearchService.getAllJobExecutionsOnHost(HOSTNAME)).thenReturn(Sets.newHashSet());
        this.coordinator.attachToRunningJobs(event);
        Mockito
            .verify(this.scheduler, Mockito.never())
            .scheduleWithFixedDelay(Mockito.any(JobMonitor.class), Mockito.eq(1000L));

        // Simulate a job being started
        final String job1Id = UUID.randomUUID().toString();
        final String job2Id = UUID.randomUUID().toString();
        final String job3Id = UUID.randomUUID().toString();
        final String job4Id = UUID.randomUUID().toString();
        final JobExecution.Builder builder = new JobExecution.Builder(UUID.randomUUID().toString(), 2818);
        builder.withId(job1Id);
        final JobExecution job1 = builder.build();
        builder.withId(job2Id);
        final JobExecution job2 = builder.build();
        builder.withId(job3Id);
        final JobExecution job3 = builder.build();
        builder.withId(job4Id);
        final JobExecution job4 = builder.build();

        final JobStartedEvent event1 = new JobStartedEvent(job1, this);
        final ScheduledFuture future = Mockito.mock(ScheduledFuture.class);
        Mockito
            .when(this.scheduler.scheduleWithFixedDelay(Mockito.any(JobMonitor.class), Mockito.eq(1000L)))
            .thenReturn(future);
        this.coordinator.onJobStarted(event1);
        Mockito
            .verify(this.scheduler, Mockito.times(1))
            .scheduleWithFixedDelay(Mockito.any(JobMonitor.class), Mockito.eq(1000L));

        final Set<JobExecution> executions = Sets.newHashSet(job1, job2, job3, job4);
        Mockito.when(this.jobSearchService.getAllJobExecutionsOnHost(HOSTNAME)).thenReturn(executions);
        this.coordinator.attachToRunningJobs(event);

        Mockito
            .verify(this.scheduler, Mockito.times(4))
            .scheduleWithFixedDelay(Mockito.any(JobMonitor.class), Mockito.eq(1000L));

        Mockito
            .when(this.jobSearchService.getAllJobExecutionsOnHost(HOSTNAME))
            .thenThrow(new GenieException(404, "blah"));
        this.coordinator.attachToRunningJobs(event);
        Mockito
            .verify(this.scheduler, Mockito.times(4))
            .scheduleWithFixedDelay(Mockito.any(JobMonitor.class), Mockito.eq(1000L));
        Mockito.verifyNoMoreInteractions(this.scheduler);
    }

    /**
     * Make sure when a {@link com.netflix.genie.core.events.JobStartedEvent} is sent a new monitor is spawned.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void canStartJobMonitor() {
        final String job1Id = UUID.randomUUID().toString();
        final String job2Id = UUID.randomUUID().toString();
        final String job3Id = UUID.randomUUID().toString();
        final String job4Id = UUID.randomUUID().toString();
        final JobExecution.Builder builder = new JobExecution.Builder(UUID.randomUUID().toString(), 2818);
        builder.withId(job1Id);
        final JobExecution job1 = builder.build();
        builder.withId(job2Id);
        final JobExecution job2 = builder.build();
        builder.withId(job3Id);
        final JobExecution job3 = builder.build();
        builder.withId(job4Id);
        final JobExecution job4 = builder.build();

        final JobStartedEvent event1 = new JobStartedEvent(job1, this);
        final JobStartedEvent event2 = new JobStartedEvent(job2, this);
        final JobStartedEvent event3 = new JobStartedEvent(job3, this);
        final JobStartedEvent event4 = new JobStartedEvent(job4, this);
        final JobStartedEvent event5 = new JobStartedEvent(job1, this);

        final ScheduledFuture future = Mockito.mock(ScheduledFuture.class);

        Mockito.when(
            this.scheduler.scheduleWithFixedDelay(Mockito.any(JobMonitor.class), Mockito.eq(1000L))
        ).thenReturn(future);

        this.coordinator.onJobStarted(event1);
        this.coordinator.onJobStarted(event2);
        this.coordinator.onJobStarted(event3);
        this.coordinator.onJobStarted(event4);
        this.coordinator.onJobStarted(event5);

        Mockito
            .verify(this.scheduler, Mockito.times(4))
            .scheduleWithFixedDelay(Mockito.any(JobMonitor.class), Mockito.eq(1000L));
    }

    /**
     * Make sure when a {@link com.netflix.genie.core.events.JobFinishedEvent} is sent the monitor is cancelled.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void canStopJobMonitor() {
        final String job1Id = UUID.randomUUID().toString();
        final JobExecution.Builder builder = new JobExecution.Builder(UUID.randomUUID().toString(), 2818);
        builder.withId(job1Id);
        final JobExecution job1 = builder.build();
        final String job2Id = UUID.randomUUID().toString();
        builder.withId(job2Id);
        final JobExecution job2 = builder.build();

        final JobStartedEvent startedEvent1 = new JobStartedEvent(job1, this);
        final JobFinishedEvent finishedEvent1 = new JobFinishedEvent(job1, this);
        final JobStartedEvent startedEvent2 = new JobStartedEvent(job2, this);
        final JobFinishedEvent finishedEvent2 = new JobFinishedEvent(job2, this);

        final ScheduledFuture future1 = Mockito.mock(ScheduledFuture.class);
        Mockito.when(future1.cancel(true)).thenReturn(true);
        final ScheduledFuture future2 = Mockito.mock(ScheduledFuture.class);
        Mockito.when(future2.cancel(true)).thenReturn(false);

        Mockito.when(
            this.scheduler.scheduleWithFixedDelay(Mockito.any(JobMonitor.class), Mockito.eq(1000L))
        ).thenReturn(future1, future2);

        this.coordinator.onJobStarted(startedEvent1);
        this.coordinator.onJobStarted(startedEvent2);

        Mockito
            .verify(this.scheduler, Mockito.times(2))
            .scheduleWithFixedDelay(Mockito.any(JobMonitor.class), Mockito.eq(1000L));

        this.coordinator.onJobFinished(finishedEvent1);
        this.coordinator.onJobFinished(finishedEvent2);
        this.coordinator.onJobFinished(finishedEvent1);

        Mockito.verify(future1, Mockito.times(1)).cancel(true);
        Mockito.verify(future2, Mockito.times(1)).cancel(true);
    }
}
