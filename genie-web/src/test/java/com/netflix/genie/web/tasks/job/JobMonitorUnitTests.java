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
import com.netflix.genie.common.dto.JobStatusMessage;
import com.netflix.genie.core.events.JobFinishedEvent;
import com.netflix.genie.core.events.KillJobEvent;
import com.netflix.genie.core.jobs.JobConstants;
import com.netflix.genie.core.properties.JobsProperties;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.genie.web.tasks.GenieTaskScheduleType;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
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
import org.springframework.context.event.ApplicationEventMulticaster;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;
import java.util.List;
import java.util.UUID;

/**
 * Unit tests for JobMonitor.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class JobMonitorUnitTests {

    private static final long DELAY = 180235L;
    private static final long MAX_STD_OUT_LENGTH = 108234203L;
    private static final long MAX_STD_ERR_LENGTH = 18023482L;

    private JobMonitor monitor;
    private JobExecution jobExecution;
    private Executor executor;
    private ApplicationEventPublisher publisher;
    private ApplicationEventMulticaster eventMulticaster;
    private Registry registry;
    private File stdOut;
    private File stdErr;
    private Counter successfulCheckRate;
    private Counter timeoutRate;
    private Counter finishedRate;
    private Counter unsuccessfulCheckRate;
    private Counter stdOutTooLarge;
    private Counter stdErrTooLarge;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        final Calendar tomorrow = Calendar.getInstance(JobConstants.UTC);
        tomorrow.add(Calendar.DAY_OF_YEAR, 1);
        this.jobExecution = new JobExecution.Builder(UUID.randomUUID().toString())
            .withProcessId(3808)
            .withCheckDelay(DELAY)
            .withTimeout(tomorrow.getTime())
            .withId(UUID.randomUUID().toString())
            .build();
        this.executor = Mockito.mock(Executor.class);
        this.publisher = Mockito.mock(ApplicationEventPublisher.class);
        this.eventMulticaster = Mockito.mock(ApplicationEventMulticaster.class);
        this.successfulCheckRate = Mockito.mock(Counter.class);
        this.timeoutRate = Mockito.mock(Counter.class);
        this.finishedRate = Mockito.mock(Counter.class);
        this.unsuccessfulCheckRate = Mockito.mock(Counter.class);
        this.stdOutTooLarge = Mockito.mock(Counter.class);
        this.stdErrTooLarge = Mockito.mock(Counter.class);
        this.registry = Mockito.mock(Registry.class);
        this.stdOut = Mockito.mock(File.class);
        this.stdErr = Mockito.mock(File.class);
        Mockito
            .when(this.registry.counter("genie.jobs.successfulStatusCheck.rate"))
            .thenReturn(this.successfulCheckRate);
        Mockito
            .when(this.registry.counter("genie.jobs.timeout.rate"))
            .thenReturn(this.timeoutRate);
        Mockito
            .when(this.registry.counter("genie.jobs.finished.rate"))
            .thenReturn(this.finishedRate);
        Mockito
            .when(this.registry.counter("genie.jobs.unsuccessfulStatusCheck.rate"))
            .thenReturn(this.unsuccessfulCheckRate);
        Mockito
            .when(this.registry.counter("genie.jobs.stdOutTooLarge.rate"))
            .thenReturn(this.stdOutTooLarge);
        Mockito
            .when(this.registry.counter("genie.jobs.stdErrTooLarge.rate"))
            .thenReturn(this.stdErrTooLarge);

        final JobsProperties outputMaxProperties = new JobsProperties();
        outputMaxProperties.getMax().setStdOutSize(MAX_STD_OUT_LENGTH);
        outputMaxProperties.getMax().setStdErrSize(MAX_STD_ERR_LENGTH);

        this.monitor = new JobMonitor(
            this.jobExecution,
            this.stdOut,
            this.stdErr,
            this.executor,
            this.publisher,
            this.eventMulticaster,
            this.registry,
            outputMaxProperties
        );
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
     * Make sure that a process whose std out file has grown too large will attempt to be killed.
     *
     * @throws IOException on error
     */
    @Test
    public void canKillProcessOnTooLargeStdOut() throws IOException {
        Assume.assumeTrue(SystemUtils.IS_OS_UNIX);
        Mockito
            .when(this.executor.execute(Mockito.any(CommandLine.class)))
            .thenReturn(0)
            .thenReturn(0)
            .thenReturn(0);

        Mockito.when(this.stdOut.exists()).thenReturn(true);
        Mockito.when(this.stdOut.length())
            .thenReturn(MAX_STD_OUT_LENGTH - 1)
            .thenReturn(MAX_STD_OUT_LENGTH)
            .thenReturn(MAX_STD_OUT_LENGTH + 1);
        Mockito.when(this.stdErr.exists()).thenReturn(false);

        for (int i = 0; i < 3; i++) {
            this.monitor.run();
        }

        Mockito.verify(this.successfulCheckRate, Mockito.times(2)).increment();
        Mockito.verify(this.stdOutTooLarge, Mockito.times(1)).increment();
        Mockito.verify(this.publisher, Mockito.times(1)).publishEvent(Mockito.any(KillJobEvent.class));
    }

    /**
     * Make sure that a process whose std err file has grown too large will attempt to be killed.
     *
     * @throws IOException on error
     */
    @Test
    public void canKillProcessOnTooLargeStdErr() throws IOException {
        Assume.assumeTrue(SystemUtils.IS_OS_UNIX);
        Mockito
            .when(this.executor.execute(Mockito.any(CommandLine.class)))
            .thenReturn(0)
            .thenReturn(0)
            .thenReturn(0);

        Mockito.when(this.stdOut.exists()).thenReturn(false);
        Mockito.when(this.stdErr.exists()).thenReturn(true);
        Mockito.when(this.stdErr.length())
            .thenReturn(MAX_STD_ERR_LENGTH - 1)
            .thenReturn(MAX_STD_ERR_LENGTH)
            .thenReturn(MAX_STD_ERR_LENGTH + 1);

        for (int i = 0; i < 3; i++) {
            this.monitor.run();
        }

        Mockito.verify(this.successfulCheckRate, Mockito.times(2)).increment();
        Mockito.verify(this.stdErrTooLarge, Mockito.times(1)).increment();
        Mockito.verify(this.publisher, Mockito.times(1)).publishEvent(Mockito.any(KillJobEvent.class));
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

        Mockito.when(this.stdOut.exists()).thenReturn(false);
        Mockito.when(this.stdErr.exists()).thenReturn(false);

        for (int i = 0; i < 3; i++) {
            this.monitor.run();
        }

        Mockito.verify(this.successfulCheckRate, Mockito.times(2)).increment();
        Mockito.verify(this.publisher, Mockito.never()).publishEvent(Mockito.any(ApplicationEvent.class));
        Mockito.verify(this.eventMulticaster, Mockito.never()).multicastEvent(Mockito.any(ApplicationEvent.class));
        Mockito.verify(this.unsuccessfulCheckRate, Mockito.times(1)).increment();
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
            .verify(this.eventMulticaster, Mockito.times(1))
            .multicastEvent(captor.capture());

        Assert.assertNotNull(captor.getValue());
        final String jobId = this.jobExecution.getId().orElseThrow(IllegalArgumentException::new);
        Assert.assertThat(
            captor.getValue().getId(),
            Matchers.is(jobId)
        );
        Assert.assertThat(captor.getValue().getSource(), Matchers.is(this.monitor));
        Mockito.verify(this.finishedRate, Mockito.times(1)).increment();
    }

    /**
     * Make sure that a timed out process sends event.
     *
     * @throws IOException on error
     */
    @Test
    public void canTryToKillTimedOutProcess() throws IOException {
        Assume.assumeTrue(SystemUtils.IS_OS_UNIX);

        // Set timeout to yesterday to force timeout when check happens
        final Calendar yesterday = Calendar.getInstance(JobConstants.UTC);
        yesterday.add(Calendar.DAY_OF_YEAR, -1);
        this.jobExecution = new JobExecution.Builder(UUID.randomUUID().toString())
            .withProcessId(3808)
            .withCheckDelay(DELAY)
            .withTimeout(yesterday.getTime())
            .withId(UUID.randomUUID().toString())
            .build();
        this.monitor = new JobMonitor(
            this.jobExecution,
            this.stdOut,
            this.stdErr,
            this.executor,
            this.publisher,
            this.eventMulticaster,
            this.registry,
            new JobsProperties()
        );

        this.monitor.run();

        final ArgumentCaptor<KillJobEvent> captor = ArgumentCaptor.forClass(KillJobEvent.class);
        Mockito
            .verify(this.publisher, Mockito.times(1))
            .publishEvent(captor.capture());

        Assert.assertNotNull(captor.getValue());
        final String jobId = this.jobExecution.getId().orElseThrow(IllegalArgumentException::new);
        Assert.assertThat(
            captor.getValue().getId(),
            Matchers.is(jobId)
        );
        Assert.assertThat(captor.getValue().getReason(), Matchers.is(JobStatusMessage.JOB_EXCEEDED_TIMEOUT));
        Assert.assertThat(captor.getValue().getSource(), Matchers.is(this.monitor));
        Mockito.verify(this.timeoutRate, Mockito.times(1)).increment();
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

        final ArgumentCaptor<KillJobEvent> eventCaptor = ArgumentCaptor.forClass(KillJobEvent.class);
        Mockito.verify(this.publisher, Mockito.times(1)).publishEvent(eventCaptor.capture());
        final List<KillJobEvent> events = eventCaptor.getAllValues();
        Assert.assertThat(events.size(), Matchers.is(1));
        final String jobId = this.jobExecution.getId().orElseThrow(IllegalArgumentException::new);
        Assert.assertThat(
            events.get(0).getId(),
            Matchers.is(jobId)
        );
        Assert.assertThat(events.get(0).getSource(), Matchers.is(this.monitor));

        final ArgumentCaptor<JobFinishedEvent> finishedCaptor = ArgumentCaptor.forClass(JobFinishedEvent.class);
        Mockito.verify(this.eventMulticaster, Mockito.times(1)).multicastEvent(finishedCaptor.capture());
        Assert.assertThat(
            finishedCaptor.getValue().getId(),
            Matchers.is(jobId)
        );
        Assert.assertThat(finishedCaptor.getValue().getSource(), Matchers.is(this.monitor));
        Mockito.verify(this.unsuccessfulCheckRate, Mockito.times(6)).increment();
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
        Assert.assertThat(DELAY, Matchers.is(this.monitor.getFixedDelay()));
    }
}
