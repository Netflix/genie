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
package com.netflix.genie.core.services.impl;

import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.core.services.JobCoordinatorService;
import com.netflix.genie.core.services.JobPersistenceService;
import com.netflix.genie.core.services.JobSearchService;
import com.netflix.genie.core.services.JobSubmitterService;
import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * Unit tests for JobServiceImpl.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class JobCoordinatorServiceImplUnitTests {

    private static final String JOB_1_ID = "job1";
    private static final String JOB_1_NAME = "relativity";
    private static final String JOB_1_USER = "einstien";
    private static final String JOB_1_VERSION = "1.0";
    private static final String BASE_ARCHIVE_LOCATION = "file://baselocation";

    private JobCoordinatorService jobCoordinatorService;
    private JobPersistenceService jobPersistenceService;
    private JobSearchService jobSearchService;
    private JobSubmitterService jobSubmitterService;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.jobPersistenceService = Mockito.mock(JobPersistenceService.class);
        this.jobSearchService = Mockito.mock(JobSearchService.class);
        this.jobSubmitterService = Mockito.mock(JobSubmitterService.class);

        this.jobCoordinatorService = new JobCoordinatorServiceImpl(
            this.jobPersistenceService,
            this.jobSearchService,
            this.jobSubmitterService,
            this.BASE_ARCHIVE_LOCATION
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

        Mockito.when(this.jobPersistenceService.createJobRequest(Mockito.eq(jobRequest))).thenReturn(jobRequest);
        final ArgumentCaptor<Job> argument = ArgumentCaptor.forClass(Job.class);

        this.jobCoordinatorService.coordinateJob(jobRequest, clientHost);

        Mockito.verify(this.jobPersistenceService, Mockito.times(1)).addClientHostToJobRequest(JOB_1_ID, clientHost);
        Mockito.verify(this.jobSubmitterService, Mockito.times(1)).submitJob(jobRequest);

        Mockito.verify(this.jobPersistenceService).createJob(argument.capture());

        Assert.assertEquals(JOB_1_ID, argument.getValue().getId());
        Assert.assertEquals(JOB_1_NAME, argument.getValue().getName());
        Assert.assertEquals(JOB_1_USER, argument.getValue().getUser());
        Assert.assertEquals(JOB_1_VERSION, argument.getValue().getVersion());
        Assert.assertEquals(JobStatus.INIT, argument.getValue().getStatus());
        Assert.assertEquals(description, argument.getValue().getDescription());
    }

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

        Mockito.when(this.jobPersistenceService.createJobRequest(Mockito.eq(jobRequest))).thenReturn(jobRequest);
        final ArgumentCaptor<Job> argument = ArgumentCaptor.forClass(Job.class);
        this.jobCoordinatorService.coordinateJob(jobRequest, clientHost);
        Mockito.verify(this.jobPersistenceService).createJob(argument.capture());
        Assert.assertEquals(BASE_ARCHIVE_LOCATION + "/" + JOB_1_ID, argument.getValue().getArchiveLocation());

    }

    @Test(expected = GeniePreconditionException.class)
    public void testCoordinateJobArchiveLocationEnabledBaseLocationMissing() throws GenieException {
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

        Mockito.when(this.jobPersistenceService.createJobRequest(Mockito.eq(jobRequest))).thenReturn(jobRequest);
        final ArgumentCaptor<Job> argument = ArgumentCaptor.forClass(Job.class);
        final JobCoordinatorService jobCoordinatorService = new JobCoordinatorServiceImpl(
            this.jobPersistenceService,
            this.jobSearchService,
            this.jobSubmitterService,
            null
        );
        jobCoordinatorService.coordinateJob(jobRequest, clientHost);
        Mockito.verify(this.jobPersistenceService).createJob(argument.capture());
        Assert.assertEquals(BASE_ARCHIVE_LOCATION + "/" + JOB_1_ID, argument.getValue().getArchiveLocation());
    }

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

        Mockito.when(this.jobPersistenceService.createJobRequest(Mockito.eq(jobRequest))).thenReturn(jobRequest);
        final ArgumentCaptor<Job> argument = ArgumentCaptor.forClass(Job.class);
        this.jobCoordinatorService.coordinateJob(jobRequest, clientHost);
        Mockito.verify(this.jobPersistenceService).createJob(argument.capture());
        Assert.assertNull(argument.getValue().getArchiveLocation());
    }

    @Test
    public void testGetJob() throws GenieException {
        final ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
        this.jobCoordinatorService.getJob(JOB_1_ID);
        Mockito.verify(this.jobPersistenceService).getJob(argument.capture());
        Assert.assertEquals(JOB_1_ID, argument.getValue());
    }

    @Test
    @Ignore
    public void testGetJobs() {
    }

    /**
     * Make sure if a job execution isn't found it returns a GenieNotFound exception.
     *
     * @throws GenieException for any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void cantGetJobHostIfNoJobExecution() throws GenieException {
        final String jobId = UUID.randomUUID().toString();
        Mockito.when(this.jobPersistenceService.getJobExecution(Mockito.eq(jobId))).thenReturn(null);
        this.jobCoordinatorService.getJobHost(jobId);
    }

    /**
     * Make sure that if the job execution exists we return a valid host.
     *
     * @throws GenieException on any problem
     */
    @Test
    public void canGetJobHost() throws GenieException {
        final String jobId = UUID.randomUUID().toString();
        final String hostname = UUID.randomUUID().toString();
        final JobExecution jobExecution = Mockito.mock(JobExecution.class);
        Mockito.when(jobExecution.getHostName()).thenReturn(hostname);
        Mockito.when(this.jobPersistenceService.getJobExecution(Mockito.eq(jobId))).thenReturn(jobExecution);

        Assert.assertThat(this.jobCoordinatorService.getJobHost(jobId), Matchers.is(hostname));
    }
}
