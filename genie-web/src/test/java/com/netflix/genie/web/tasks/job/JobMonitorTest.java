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
import com.netflix.genie.common.dto.JobStatusMessages;
import com.netflix.genie.common.exceptions.GenieTimeoutException;
import com.netflix.genie.web.events.GenieEventBus;
import com.netflix.genie.web.events.JobFinishedEvent;
import com.netflix.genie.web.events.KillJobEvent;
import com.netflix.genie.web.properties.JobsProperties;
import com.netflix.genie.web.tasks.GenieTaskScheduleType;
import com.netflix.genie.web.util.ProcessChecker;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.Executor;
import org.apache.commons.lang3.SystemUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.context.ApplicationEvent;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

/**
 * Unit tests for JobMonitor.
 *
 * @author tgianos
 * @since 3.0.0
 */
class JobMonitorTest {

    private static final long DELAY = 180235L;
    private static final long MAX_STD_OUT_LENGTH = 108234203L;
    private static final long MAX_STD_ERR_LENGTH = 18023482L;

    private JobMonitor monitor;
    private JobExecution jobExecution;
    private Executor executor;
    private GenieEventBus genieEventBus;
    private MeterRegistry registry;
    private File stdOut;
    private File stdErr;
    private Counter successfulCheckRate;
    private Counter timeoutRate;
    private Counter finishedRate;
    private Counter unsuccessfulCheckRate;
    private Counter stdOutTooLarge;
    private Counter stdErrTooLarge;
    private ProcessChecker processChecker;

    /**
     * Setup for the tests.
     */
    @BeforeEach
    void setup() {
        final Instant tomorrow = Instant.now().plus(1, ChronoUnit.DAYS);
        this.jobExecution = new JobExecution.Builder(UUID.randomUUID().toString())
            .withProcessId(3808)
            .withCheckDelay(DELAY)
            .withTimeout(tomorrow)
            .withId(UUID.randomUUID().toString())
            .build();
        this.executor = Mockito.mock(Executor.class);
        this.genieEventBus = Mockito.mock(GenieEventBus.class);
        this.successfulCheckRate = Mockito.mock(Counter.class);
        this.timeoutRate = Mockito.mock(Counter.class);
        this.finishedRate = Mockito.mock(Counter.class);
        this.unsuccessfulCheckRate = Mockito.mock(Counter.class);
        this.stdOutTooLarge = Mockito.mock(Counter.class);
        this.stdErrTooLarge = Mockito.mock(Counter.class);
        this.registry = Mockito.mock(MeterRegistry.class);
        this.stdOut = Mockito.mock(File.class);
        this.stdErr = Mockito.mock(File.class);
        this.processChecker = Mockito.mock(ProcessChecker.class);
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

        final JobsProperties outputMaxProperties = JobsProperties.getJobsPropertiesDefaults();
        outputMaxProperties.getMax().setStdOutSize(MAX_STD_OUT_LENGTH);
        outputMaxProperties.getMax().setStdErrSize(MAX_STD_ERR_LENGTH);

        this.monitor = new JobMonitor(
            this.jobExecution,
            this.stdOut,
            this.stdErr,
            this.genieEventBus,
            this.registry,
            outputMaxProperties,
            this.processChecker
        );
    }

    /**
     * This test should only run on windows machines and asserts that the system fails on Windows.
     */
    @Test
    void cantRunOnWindows() {
        Assumptions.assumeTrue(SystemUtils.IS_OS_WINDOWS);
        Assertions.assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> this.monitor.run());
    }

    /**
     * Make sure that a process whose std out file has grown too large will attempt to be killed.
     *
     * @throws IOException on error
     */
    @Test
    void canKillProcessOnTooLargeStdOut() throws IOException {
        Assumptions.assumeTrue(SystemUtils.IS_OS_UNIX);
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
        Mockito.verify(this.genieEventBus, Mockito.times(1)).publishSynchronousEvent(Mockito.any(KillJobEvent.class));
    }

    /**
     * Make sure that a process whose std err file has grown too large will attempt to be killed.
     *
     * @throws IOException on error
     */
    @Test
    void canKillProcessOnTooLargeStdErr() throws IOException {
        Assumptions.assumeTrue(SystemUtils.IS_OS_UNIX);
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
        Mockito.verify(this.genieEventBus, Mockito.times(1)).publishSynchronousEvent(Mockito.any(KillJobEvent.class));
    }

    /**
     * Make sure that a running process doesn't publish anything.
     *
     * @throws Exception on error
     */
    @Test
    void canCheckRunningProcessOnUnixLikeSystem() throws Exception {
        Assumptions.assumeTrue(SystemUtils.IS_OS_UNIX);

        Mockito.when(this.stdOut.exists()).thenReturn(false);
        Mockito.when(this.stdErr.exists()).thenReturn(false);

        for (int i = 0; i < 3; i++) {
            this.monitor.run();
        }

        Mockito.verify(this.processChecker, Mockito.times(3)).checkProcess();
        Mockito.verify(this.successfulCheckRate, Mockito.times(3)).increment();
        Mockito
            .verify(this.genieEventBus, Mockito.never())
            .publishSynchronousEvent(Mockito.any(ApplicationEvent.class));
        Mockito
            .verify(this.genieEventBus, Mockito.never())
            .publishAsynchronousEvent(Mockito.any(ApplicationEvent.class));
    }

    /**
     * Make sure that a finished process sends event.
     *
     * @throws Exception on error
     */
    @Test
    void canCheckFinishedProcessOnUnixLikeSystem() throws Exception {
        Assumptions.assumeTrue(SystemUtils.IS_OS_UNIX);
        Mockito.doThrow(new ExecuteException("done", 1)).when(processChecker).checkProcess();

        this.monitor.run();

        final ArgumentCaptor<JobFinishedEvent> captor = ArgumentCaptor.forClass(JobFinishedEvent.class);
        Mockito
            .verify(this.genieEventBus, Mockito.times(1))
            .publishAsynchronousEvent(captor.capture());

        Assertions.assertThat(captor.getValue()).isNotNull();
        final String jobId = this.jobExecution.getId().orElseThrow(IllegalArgumentException::new);
        Assertions.assertThat(captor.getValue().getId()).isEqualTo(jobId);
        Assertions.assertThat(captor.getValue().getSource()).isEqualTo(this.monitor);
        Mockito.verify(this.finishedRate, Mockito.times(1)).increment();
    }

    /**
     * Make sure that a timed out process sends event.
     *
     * @throws Exception in case of error
     */
    @Test
    void canTryToKillTimedOutProcess() throws Exception {
        Assumptions.assumeTrue(SystemUtils.IS_OS_UNIX);

        // Set timeout to yesterday to force timeout when check happens
        final Instant yesterday = Instant.now().minus(1, ChronoUnit.DAYS);
        this.jobExecution = new JobExecution.Builder(UUID.randomUUID().toString())
            .withProcessId(3808)
            .withCheckDelay(DELAY)
            .withTimeout(yesterday)
            .withId(UUID.randomUUID().toString())
            .build();
        this.monitor = new JobMonitor(
            this.jobExecution,
            this.stdOut,
            this.stdErr,
            this.genieEventBus,
            this.registry,
            JobsProperties.getJobsPropertiesDefaults(),
            processChecker
        );

        Mockito.doThrow(new GenieTimeoutException("...")).when(processChecker).checkProcess();

        this.monitor.run();

        final ArgumentCaptor<KillJobEvent> captor = ArgumentCaptor.forClass(KillJobEvent.class);
        Mockito
            .verify(this.genieEventBus, Mockito.times(1))
            .publishSynchronousEvent(captor.capture());

        Assertions.assertThat(captor.getValue()).isNotNull();
        final String jobId = this.jobExecution.getId().orElseThrow(IllegalArgumentException::new);
        Assertions.assertThat(captor.getValue().getId()).isEqualTo(jobId);
        Assertions.assertThat(captor.getValue().getReason()).isEqualTo(JobStatusMessages.JOB_EXCEEDED_TIMEOUT);
        Assertions.assertThat(captor.getValue().getSource()).isEqualTo(this.monitor);
        Mockito.verify(this.timeoutRate, Mockito.times(1)).increment();
    }

    /**
     * Make sure that an error doesn't publish anything until it runs too many times then it tries to kill the job.
     *
     * @throws Exception on error
     */
    @Test
    void cantGetStatusIfErrorOnUnixLikeSystem() throws Exception {
        Assumptions.assumeTrue(SystemUtils.IS_OS_UNIX);

        Mockito.doThrow(new IOException()).when(processChecker).checkProcess();

        // Run six times to force error
        for (int i = 0; i < 6; i++) {
            this.monitor.run();
        }

        final ArgumentCaptor<KillJobEvent> eventCaptor = ArgumentCaptor.forClass(KillJobEvent.class);
        Mockito.verify(this.genieEventBus, Mockito.times(1)).publishSynchronousEvent(eventCaptor.capture());
        final List<KillJobEvent> events = eventCaptor.getAllValues();
        Assertions.assertThat(events.size()).isEqualTo(1);
        final String jobId = this.jobExecution.getId().orElseThrow(IllegalArgumentException::new);
        Assertions.assertThat(events.get(0).getId()).isEqualTo(jobId);
        Assertions.assertThat(events.get(0).getSource()).isEqualTo(this.monitor);

        final ArgumentCaptor<JobFinishedEvent> finishedCaptor = ArgumentCaptor.forClass(JobFinishedEvent.class);
        Mockito.verify(this.genieEventBus, Mockito.times(1)).publishAsynchronousEvent(finishedCaptor.capture());
        Assertions.assertThat(finishedCaptor.getValue().getId()).isEqualTo(jobId);
        Assertions.assertThat(finishedCaptor.getValue().getSource()).isEqualTo(this.monitor);
        Mockito.verify(this.unsuccessfulCheckRate, Mockito.times(6)).increment();
    }

    /**
     * Make sure the right schedule type is returned.
     */
    @Test
    void canGetScheduleType() {
        Assertions.assertThat(this.monitor.getScheduleType()).isEqualTo(GenieTaskScheduleType.TRIGGER);
    }

    /**
     * Make sure asking for a trigger is returns one.
     */
    @Test
    void canGetTrigger() {
        Assertions.assertThat(this.monitor.getTrigger()).isNotNull();
    }

    /**
     * Make sure asking for a fixed rate isn't allowed.
     */
    @Test
    void cantGetFixedRate() {
        Assertions
            .assertThatExceptionOfType(UnsupportedOperationException.class)
            .isThrownBy(() -> this.monitor.getFixedRate());
    }

    /**
     * Make sure asking for a fixed delay isn't allowed.
     */
    @Test
    void cantGetFixedDelay() {
        Assertions
            .assertThatExceptionOfType(UnsupportedOperationException.class)
            .isThrownBy(() -> this.monitor.getFixedDelay());
    }
}
