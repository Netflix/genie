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

import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.core.events.JobFinishedEvent;
import com.netflix.genie.core.events.KillJobEvent;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.genie.web.tasks.GenieTaskScheduleType;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.Executor;
import org.apache.commons.lang3.SystemUtils;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;

import java.io.IOException;
import java.util.UUID;

/**
 * Unit tests for JobMonitor.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class JobMonitorUnitTests {

    private JobMonitor monitor;
    private JobExecution jobExecution;
    private Executor executor;
    private ApplicationEventPublisher publisher;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        final JobExecution.Builder builder = new JobExecution.Builder(UUID.randomUUID().toString(), 3808);
        builder.withId(UUID.randomUUID().toString());
        this.jobExecution = builder.build();
        this.executor = Mockito.mock(Executor.class);
        this.publisher = Mockito.mock(ApplicationEventPublisher.class);

        this.monitor = new JobMonitor(this.jobExecution, this.executor, this.publisher);
    }

    /**
     * This test should only run on windows machines and asserts that the system fails on Windows.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void cantRunOnWindows() {
        Assume.assumeTrue(SystemUtils.IS_OS_WINDOWS);
        this.monitor.run();
    }

    /**
     * Make sure that a running process doesn't publish anything.
     *
     * @throws IOException on error
     */
    @Test
    public void canCheckRunningProcessOnUnixLikeSystem() throws IOException {
        Assume.assumeTrue(SystemUtils.IS_OS_UNIX);
        Mockito
            .when(this.executor.execute(Mockito.any(CommandLine.class)))
            .thenReturn(0)
            .thenThrow(new IOException())
            .thenReturn(0);

        for (int i = 0; i < 3; i++) {
            this.monitor.run();
        }

        Mockito.verify(this.publisher, Mockito.never()).publishEvent(Mockito.any(ApplicationEvent.class));
    }

    /**
     * Make sure that a finished process sends event.
     *
     * @throws IOException on error
     */
    @Test
    public void canCheckFinishedProcessOnUnixLikeSystem() throws IOException {
        Assume.assumeTrue(SystemUtils.IS_OS_UNIX);
        Mockito.when(this.executor.execute(Mockito.any(CommandLine.class))).thenThrow(new ExecuteException("done", 1));

        this.monitor.run();

        final ArgumentCaptor<JobFinishedEvent> captor = ArgumentCaptor.forClass(JobFinishedEvent.class);
        Mockito
            .verify(this.publisher, Mockito.times(1))
            .publishEvent(captor.capture());

        Assert.assertNotNull(captor.getValue());
        Assert.assertThat(captor.getValue().getJobExecution(), Matchers.is(this.jobExecution));
        Assert.assertThat(captor.getValue().getSource(), Matchers.is(this.monitor));
    }

    /**
     * Make sure that an error doesn't publish anything until it runs too many times then it tries to kill the job.
     *
     * @throws IOException on error
     */
    @Test
    public void cantGetStatusIfErrorOnUnixLikeSystem() throws IOException {
        Assume.assumeTrue(SystemUtils.IS_OS_UNIX);
        Mockito.when(this.executor.execute(Mockito.any(CommandLine.class))).thenThrow(new IOException());

        // Run six times to force error
        for (int i = 0; i < 6; i++) {
            this.monitor.run();
        }

        final ArgumentCaptor<KillJobEvent> captor = ArgumentCaptor.forClass(KillJobEvent.class);
        Mockito
            .verify(this.publisher, Mockito.times(1))
            .publishEvent(captor.capture());
        Assert.assertNotNull(captor.getValue());
        Assert.assertThat(captor.getValue().getId(), Matchers.is(this.jobExecution.getId()));
        Assert.assertThat(captor.getValue().getSource(), Matchers.is(this.monitor));
    }

    /**
     * Make sure the right schedule type is returned.
     */
    @Test
    public void canGetScheduleType() {
        Assert.assertThat(this.monitor.getScheduleType(), Matchers.is(GenieTaskScheduleType.FIXED_DELAY));
    }

    /**
     * Make sure asking for a trigger isn't allowed.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void cantGetTrigger() {
        this.monitor.getTrigger();
    }

    /**
     * Make sure asking for a trigger isn't allowed.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void cantGetFixedRate() {
        this.monitor.getFixedRate();
    }

    /**
     * Make sure the fixed delay value is what we expect.
     */
    @Test
    public void canGetFixedDelay() {
        Assert.assertThat(1000L, Matchers.is(this.monitor.getFixedDelay()));
    }
}
