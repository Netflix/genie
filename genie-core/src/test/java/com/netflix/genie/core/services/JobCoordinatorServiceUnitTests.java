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
package com.netflix.genie.core.services;

import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerUnavailableException;
import com.netflix.genie.core.jobs.JobConstants;
import com.netflix.genie.core.jobs.JobLauncher;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.spectator.api.Registry;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.core.task.TaskExecutor;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * Unit tests for JobCoordinatorServiceImpl.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class JobCoordinatorServiceUnitTests {

    private static final int MAX_RUNNING_JOBS = 2;
    private static final String JOB_1_ID = "job1";
    private static final String JOB_1_NAME = "relativity";
    private static final String JOB_1_USER = "einstien";
    private static final String JOB_1_VERSION = "1.0";
    private static final String BASE_ARCHIVE_LOCATION = "file://baselocation";

    private TaskExecutor taskExecutor;
    private JobCoordinatorService jobCoordinatorService;
    private JobPersistenceService jobPersistenceService;
    private JobKillService jobKillService;
    private JobCountService jobCountService;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.taskExecutor = Mockito.mock(TaskExecutor.class);
        this.jobPersistenceService = Mockito.mock(JobPersistenceService.class);
        this.jobKillService = Mockito.mock(JobKillService.class);
        this.jobCountService = Mockito.mock(JobCountService.class);

        this.jobCoordinatorService = new JobCoordinatorService(
            this.taskExecutor,
            this.jobPersistenceService,
            Mockito.mock(JobSubmitterService.class),
            this.jobKillService,
            this.jobCountService,
            BASE_ARCHIVE_LOCATION,
            MAX_RUNNING_JOBS,
            Mockito.mock(Registry.class)
        );
    }

    /**
     * Test the coordinate job method.
     *
     * @throws GenieException If there is any problem
     */
    @Test
    public void testCoordinateJob() throws GenieException {
        final int cpu = 1;
        final int mem = 1;
        final String email = "name@domain.com";
        final String setupFile = "setupFilePath";
        final String group = "group";
        final String description = "job description";
        final Set<String> tags = new HashSet<>();
        final String clientHost = "localhost";
        tags.add("foo");
        tags.add("bar");


        final JobRequest jobRequest = new JobRequest.Builder(
            JOB_1_NAME,
            JOB_1_USER,
            JOB_1_VERSION,
            null,
            null,
            null

        ).withId(JOB_1_ID)
            .withDescription(description)
            .withCpu(cpu)
            .withMemory(mem)
            .withEmail(email)
            .withSetupFile(setupFile)
            .withGroup(group)
            .withTags(tags)
            .withDisableLogArchival(true)
            .build();

        Mockito.when(this.jobPersistenceService.createJobRequest(jobRequest, clientHost)).thenReturn(jobRequest);

        this.jobCoordinatorService.coordinateJob(jobRequest, clientHost);

        Mockito.verify(this.taskExecutor, Mockito.times(1)).execute(Mockito.any(JobLauncher.class));

        final ArgumentCaptor<Job> argument = ArgumentCaptor.forClass(Job.class);
        Mockito.verify(this.jobPersistenceService).createJob(argument.capture());

        Assert.assertEquals(JOB_1_ID, argument.getValue().getId());
        Assert.assertEquals(JOB_1_NAME, argument.getValue().getName());
        Assert.assertEquals(JOB_1_USER, argument.getValue().getUser());
        Assert.assertEquals(JOB_1_VERSION, argument.getValue().getVersion());
        Assert.assertEquals(JobStatus.INIT, argument.getValue().getStatus());
        Assert.assertEquals(description, argument.getValue().getDescription());
    }

    /**
     * Test the coordinate job method with archive location enabled.
     *
     * @throws GenieException If there is any problem
     */
    @Test
    public void testCoordinateJobArchiveLocationEnabled() throws GenieException {
        final String clientHost = "localhost";
        final JobRequest jobRequest = new JobRequest.Builder(
            JOB_1_NAME,
            JOB_1_USER,
            JOB_1_VERSION,
            null,
            null,
            null
        ).withDisableLogArchival(false)
            .withId(JOB_1_ID)
            .build();

        Mockito.when(this.jobPersistenceService.createJobRequest(jobRequest, clientHost)).thenReturn(jobRequest);
        Mockito.when(this.jobCountService.getNumRunningJobs()).thenReturn(MAX_RUNNING_JOBS - 1);
        final ArgumentCaptor<Job> argument = ArgumentCaptor.forClass(Job.class);
        this.jobCoordinatorService.coordinateJob(jobRequest, clientHost);
        Mockito.verify(this.jobPersistenceService).createJob(argument.capture());
        Assert.assertEquals(BASE_ARCHIVE_LOCATION
            + JobConstants.FILE_PATH_DELIMITER
            + JOB_1_ID
            + ".tar.gz",
            argument.getValue().getArchiveLocation());

    }

    /**
     * Test the coordinate job method with archive location disabled.
     *
     * @throws GenieException If there is any problem
     */
    @Test
    public void testCoordinateJobArchiveLocationDisabled() throws GenieException {
        final String clientHost = "localhost";
        final JobRequest jobRequest = new JobRequest.Builder(
            JOB_1_NAME,
            JOB_1_USER,
            JOB_1_VERSION,
            null,
            null,
            null
        ).withDisableLogArchival(true)
            .withId(JOB_1_ID)
            .build();

        Mockito.when(this.jobPersistenceService.createJobRequest(jobRequest, clientHost)).thenReturn(jobRequest);
        Mockito.when(this.jobCountService.getNumRunningJobs()).thenReturn(MAX_RUNNING_JOBS - 1);
        final ArgumentCaptor<Job> argument = ArgumentCaptor.forClass(Job.class);
        this.jobCoordinatorService.coordinateJob(jobRequest, clientHost);
        Mockito.verify(this.jobPersistenceService).createJob(argument.capture());
        Assert.assertNull(argument.getValue().getArchiveLocation());
    }

    /**
     * Make sure if there are already the max running number of jobs running on the node the job is rejected.
     *
     * @throws GenieException On error
     */
    @Test(expected = GenieServerUnavailableException.class)
    public void cantRunJobIfFull() throws GenieException {
        final String clientHost = "localhost";
        final JobRequest jobRequest = new JobRequest.Builder(
            JOB_1_NAME,
            JOB_1_USER,
            JOB_1_VERSION,
            null,
            null,
            null
        ).withDisableLogArchival(true)
            .withId(JOB_1_ID)
            .build();

        Mockito.when(this.jobPersistenceService.createJobRequest(jobRequest, clientHost)).thenReturn(jobRequest);
        Mockito.when(this.jobCountService.getNumRunningJobs()).thenReturn(MAX_RUNNING_JOBS);
        final ArgumentCaptor<Job> argument = ArgumentCaptor.forClass(Job.class);
        this.jobCoordinatorService.coordinateJob(jobRequest, clientHost);
        Mockito.verify(this.jobPersistenceService).createJob(argument.capture());
        Assert.assertThat(argument.getValue().getStatus(), Matchers.is(JobStatus.FAILED));
    }

    /**
     * Test killing a job without throwing an exception.
     *
     * @throws GenieException On any error
     */
    @Test
    public void canKillJob() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.doNothing().when(this.jobKillService).killJob(id);
        this.jobCoordinatorService.killJob(id);
    }

    /**
     * Test killing a job without throwing an exception.
     *
     * @throws GenieException On any error
     */
    @Test(expected = GenieException.class)
    public void cantKillJob() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.doThrow(new GenieException(123, "fake")).when(this.jobKillService).killJob(id);
        this.jobCoordinatorService.killJob(id);
    }
}
