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
package com.netflix.genie.web.services.impl;

import com.google.common.io.Files;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.util.GenieObjectMapper;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.genie.web.events.GenieEventBus;
import com.netflix.genie.web.events.JobFinishedEvent;
import com.netflix.genie.web.events.JobFinishedReason;
import com.netflix.genie.web.services.JobSearchService;
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
import org.springframework.core.io.FileSystemResource;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

/**
 * Unit tests for the JobKillServiceV3 class.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class JobKillServiceV3UnitTests {

    private static final String ID = UUID.randomUUID().toString();
    private static final String HOSTNAME = UUID.randomUUID().toString();
    private static final int PID = 18243;
    private static final String KILL_REASON = "Killed by test";
    private CommandLine killCommand;
    private JobSearchService jobSearchService;
    private Executor executor;
    private JobKillServiceV3 service;
    private GenieEventBus genieEventBus;
    private FileSystemResource genieWorkingDir;

    /**
     * Setup for the tests.
     *
     *
     * @throws IOException if the job directory cannot be created
     */
    @Before
    public void setup() throws IOException {
        Assume.assumeTrue(SystemUtils.IS_OS_UNIX);
        final File tempDirectory = Files.createTempDir();
        this.genieWorkingDir = new FileSystemResource(tempDirectory);
        Files.createParentDirs(new File(tempDirectory.getPath() + "/" + ID + "/genie/x"));
        this.jobSearchService = Mockito.mock(JobSearchService.class);
        this.executor = Mockito.mock(Executor.class);
        this.genieEventBus = Mockito.mock(GenieEventBus.class);
        this.service = new JobKillServiceV3(
            HOSTNAME,
            this.jobSearchService,
            this.executor,
            false,
            this.genieEventBus,
            this.genieWorkingDir,
            GenieObjectMapper.getMapper()
        );

        this.killCommand = new CommandLine("kill");
        this.killCommand.addArguments(Integer.toString(PID));
    }

    /**
     * Make sure we don't execute any functionality if the job is already not running.
     *
     * @throws GenieException on any error
     */
    @Test
    public void wontKillJobIfAlreadyNotRunning() throws GenieException {
        final JobExecution jobExecution = Mockito.mock(JobExecution.class);
        Mockito.when(this.jobSearchService.getJobStatus(ID)).thenReturn(JobStatus.RUNNING);
        Mockito.when(jobExecution.getExitCode()).thenReturn(Optional.of(1));
        Mockito.when(this.jobSearchService.getJobExecution(ID)).thenReturn(jobExecution);

        this.service.killJob(ID, KILL_REASON);
    }

    /**
     * Make sure we throw an exception if the job isn't actually running on this host.
     *
     * @throws GenieException On error
     */
    @Test(expected = GeniePreconditionException.class)
    public void cantKillJobIfNotOnThisHost() throws GenieException {
        final JobExecution jobExecution = Mockito.mock(JobExecution.class);
        Mockito.when(jobExecution.getExitCode()).thenReturn(Optional.empty());
        Mockito.when(jobExecution.getHostName()).thenReturn(UUID.randomUUID().toString());
        Mockito.when(this.jobSearchService.getJobStatus(ID)).thenReturn(JobStatus.RUNNING);
        Mockito.when(this.jobSearchService.getJobExecution(ID)).thenReturn(jobExecution);

        this.service.killJob(ID, KILL_REASON);
    }

    /**
     * Make sure that if between the time the job execution was pulled from the database and now the job didn't finish.
     *
     * @throws GenieException on any error
     * @throws IOException    on error in execute
     */
    @Test
    public void cantKillJobIfAlreadyDoneSinceDBCall() throws GenieException, IOException {
        final JobExecution jobExecution = Mockito.mock(JobExecution.class);
        Mockito.when(jobExecution.getExitCode()).thenReturn(Optional.empty());
        Mockito.when(jobExecution.getHostName()).thenReturn(HOSTNAME);
        Mockito.when(jobExecution.getProcessId()).thenReturn(Optional.of(PID));
        Mockito.when(this.jobSearchService.getJobStatus(ID)).thenReturn(JobStatus.RUNNING);
        Mockito.when(this.jobSearchService.getJobExecution(ID)).thenReturn(jobExecution);
        Mockito.when(this.executor.execute(Mockito.any(CommandLine.class))).thenThrow(new ExecuteException("blah", 1));

        this.service.killJob(ID, KILL_REASON);

        Mockito.verify(this.executor, Mockito.never()).execute(this.killCommand);
    }

    /**
     * Make sure that if between the time the job execution was pulled from the database and now the job didn't finish.
     *
     * @throws GenieException on any error
     * @throws IOException    on error in execute
     */
    @Test(expected = GenieServerException.class)
    public void cantKillJobIfCantCheckProcessStatus() throws GenieException, IOException {
        final JobExecution jobExecution = Mockito.mock(JobExecution.class);
        Mockito.when(jobExecution.getExitCode()).thenReturn(Optional.empty());
        Mockito.when(jobExecution.getHostName()).thenReturn(HOSTNAME);
        Mockito.when(jobExecution.getProcessId()).thenReturn(Optional.of(PID));
        Mockito.when(this.jobSearchService.getJobStatus(ID)).thenReturn(JobStatus.RUNNING);
        Mockito.when(this.jobSearchService.getJobExecution(ID)).thenReturn(jobExecution);
        Mockito.when(this.executor.execute(Mockito.any(CommandLine.class))).thenThrow(new IOException());

        this.service.killJob(ID, KILL_REASON);

        Mockito.verify(this.executor, Mockito.never()).execute(this.killCommand);
    }

    /**
     * Make sure that if we can't kill the actual process it throws an exception.
     *
     * @throws GenieException on any error
     * @throws IOException    on error in execute
     */
    @Test(expected = GenieServerException.class)
    public void cantKillJobIfCantKillProcess() throws GenieException, IOException {
        final JobExecution jobExecution = Mockito.mock(JobExecution.class);
        Mockito.when(jobExecution.getExitCode()).thenReturn(Optional.empty());
        Mockito.when(jobExecution.getHostName()).thenReturn(HOSTNAME);
        Mockito.when(jobExecution.getProcessId()).thenReturn(Optional.of(PID));
        Mockito.when(this.jobSearchService.getJobStatus(ID)).thenReturn(JobStatus.RUNNING);
        Mockito.when(this.jobSearchService.getJobExecution(ID)).thenReturn(jobExecution);
        Mockito.when(this.executor.execute(Mockito.any(CommandLine.class))).thenReturn(0).thenThrow(new IOException());

        this.service.killJob(ID, KILL_REASON);
        Mockito.verify(this.executor, Mockito.times(1)).execute(this.killCommand);
    }

    /**
     * Make sure we can kill a job.
     *
     * @throws GenieException On any error
     * @throws IOException    On error in execute
     */
    @Test
    public void canKillJob() throws GenieException, IOException {
        final JobExecution jobExecution = Mockito.mock(JobExecution.class);
        Mockito.when(jobExecution.getExitCode()).thenReturn(Optional.empty());
        Mockito.when(jobExecution.getHostName()).thenReturn(HOSTNAME);
        Mockito.when(jobExecution.getProcessId()).thenReturn(Optional.of(PID));
        Mockito.when(this.jobSearchService.getJobStatus(ID)).thenReturn(JobStatus.RUNNING);
        Mockito.when(this.jobSearchService.getJobExecution(ID)).thenReturn(jobExecution);
        Mockito.when(this.executor.execute(Mockito.any(CommandLine.class))).thenReturn(0, 0);

        this.service.killJob(ID, KILL_REASON);

        Mockito.verify(this.executor, Mockito.times(2)).execute(Mockito.any(CommandLine.class));
    }

    /**
     * Make sure we can kill a job that is running as a user.
     *
     * @throws GenieException On any error
     * @throws IOException    On error in execute
     */
    @Test
    public void canKillJobRunningAsUser() throws GenieException, IOException {
        this.service = new JobKillServiceV3(
            HOSTNAME,
            this.jobSearchService,
            this.executor,
            true,
            this.genieEventBus,
            this.genieWorkingDir,
            GenieObjectMapper.getMapper()
        );

        final JobExecution jobExecution = Mockito.mock(JobExecution.class);
        Mockito.when(jobExecution.getExitCode()).thenReturn(Optional.empty());
        Mockito.when(jobExecution.getHostName()).thenReturn(HOSTNAME);
        Mockito.when(jobExecution.getProcessId()).thenReturn(Optional.of(PID));
        Mockito.when(this.jobSearchService.getJobStatus(ID)).thenReturn(JobStatus.RUNNING);
        Mockito.when(this.jobSearchService.getJobExecution(ID)).thenReturn(jobExecution);

        Mockito.when(this.executor.execute(Mockito.any(CommandLine.class))).thenReturn(0, 0);

        this.service.killJob(ID, KILL_REASON);

        Mockito.verify(this.executor, Mockito.times(2)).execute(Mockito.any(CommandLine.class));
        Mockito.verify(this.jobSearchService, Mockito.times(1)).getJobExecution(ID);
    }

    /**
     * Make sure that if the job status is one that is already finished there is no attempt made to kill it.
     *
     * @throws GenieException on error
     * @throws IOException    on error
     */
    @Test
    public void wontKillJobIfAlreadyFinished() throws GenieException, IOException {
        Mockito
            .when(this.jobSearchService.getJobStatus(ID))
            .thenReturn(JobStatus.SUCCEEDED)
            .thenReturn(JobStatus.FAILED)
            .thenReturn(JobStatus.INVALID)
            .thenReturn(JobStatus.KILLED);

        // Run through the four cases
        this.service.killJob(ID, KILL_REASON);
        this.service.killJob(ID, KILL_REASON);
        this.service.killJob(ID, KILL_REASON);
        this.service.killJob(ID, KILL_REASON);

        Mockito.verify(this.jobSearchService, Mockito.never()).getJobExecution(ID);
        Mockito.verify(this.executor, Mockito.never()).execute(Mockito.any(CommandLine.class));
    }

    /**
     * Test to make sure that if the job is in init state it doesn't do anything but throw a job finished event.
     *
     * @throws GenieException on error
     * @throws IOException    on error
     */
    @Test
    public void canKillJobInInitState() throws GenieException, IOException {
        final ArgumentCaptor<JobFinishedEvent> captor = ArgumentCaptor.forClass(JobFinishedEvent.class);
        Mockito.when(this.jobSearchService.getJobStatus(ID)).thenReturn(JobStatus.INIT);
        this.service.killJob(ID, KILL_REASON);

        Mockito.verify(this.genieEventBus, Mockito.times(1)).publishSynchronousEvent(captor.capture());
        Assert.assertThat(captor.getValue().getId(), Matchers.is(ID));
        Assert.assertThat(captor.getValue().getReason(), Matchers.is(JobFinishedReason.KILLED));
        Mockito.verify(this.jobSearchService, Mockito.never()).getJobExecution(ID);
        Mockito.verify(this.executor, Mockito.never()).execute(Mockito.any(CommandLine.class));
    }
}
