/*
 *
 *  Copyright 2015 Netflix, Inc.
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
package com.netflix.genie.core.jpa.services;

import com.google.common.collect.Lists;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.core.jpa.entities.ApplicationEntity;
import com.netflix.genie.core.jpa.entities.ClusterEntity;
import com.netflix.genie.core.jpa.entities.CommandEntity;
import com.netflix.genie.core.jpa.entities.JobEntity;
import com.netflix.genie.core.jpa.entities.JobExecutionEntity;
import com.netflix.genie.core.jpa.entities.JobRequestEntity;
import com.netflix.genie.core.jpa.repositories.JpaApplicationRepository;
import com.netflix.genie.core.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.core.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobExecutionRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobRequestRepository;
import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * Tests for the JpaJobPersistenceServiceImpl.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class JpaJobPersistenceServiceImplUnitTests {

    private static final String JOB_1_ID = "job1";
    private static final String JOB_1_NAME = "relativity";
    private static final String JOB_1_USER = "einstien";
    private static final String JOB_1_VERSION = "1.0";
    private static final String JOB_1_COMMAND_ARGS = "-f hive.q";
    private static final String JOB_1_STATUS_MSG = "Default message";

    private JpaJobRepository jobRepo;
    private JpaJobRequestRepository jobRequestRepo;
    private JpaJobExecutionRepository jobExecutionRepo;
    private JpaApplicationRepository applicationRepo;
    private JpaClusterRepository clusterRepo;
    private JpaCommandRepository commandRepo;

    private JpaJobPersistenceServiceImpl jobPersistenceService;

    /**
     * Setup the tests.
     */
    @Before
    public void setup() {
        this.jobRepo = Mockito.mock(JpaJobRepository.class);
        this.jobRequestRepo = Mockito.mock(JpaJobRequestRepository.class);
        this.jobExecutionRepo = Mockito.mock(JpaJobExecutionRepository.class);
        this.applicationRepo = Mockito.mock(JpaApplicationRepository.class);
        this.clusterRepo = Mockito.mock(JpaClusterRepository.class);
        this.commandRepo = Mockito.mock(JpaCommandRepository.class);

        this.jobPersistenceService = new JpaJobPersistenceServiceImpl(
            this.jobRepo,
            this.jobRequestRepo,
            this.jobExecutionRepo,
            this.applicationRepo,
            this.clusterRepo,
            this.commandRepo);
    }

    /******* Unit Tests for Job methods ********/

    /**
     * Test the createJob method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GeniePreconditionException.class)
    public void testCreateJobIdWithNoId() throws GenieException {
        final Job job = new Job.Builder(JOB_1_NAME, JOB_1_USER, JOB_1_VERSION, JOB_1_COMMAND_ARGS).build();
        this.jobPersistenceService.createJob(job);
    }

    /**
     * Test the createJob method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GeniePreconditionException.class)
    public void testCreateJobIdWithNoStatus() throws GenieException {
        final Job job = new Job.Builder(JOB_1_NAME, JOB_1_USER, JOB_1_VERSION, JOB_1_COMMAND_ARGS)
            .withId(JOB_1_ID)
            .build();
        this.jobPersistenceService.createJob(job);
    }

    /**
     * Test the createJob method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieConflictException.class)
    public void testCreateJobAlreadyExists() throws GenieException {
        final Job job = new Job.Builder(JOB_1_NAME, JOB_1_USER, JOB_1_VERSION, JOB_1_COMMAND_ARGS)
            .withId(JOB_1_ID)
            .build();
        Mockito.when(this.jobRepo.exists(Mockito.eq(JOB_1_ID))).thenReturn(true);
        this.jobPersistenceService.createJob(job);
    }

    /**
     * Test the createJob method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GeniePreconditionException.class)
    public void testCreateJobWithMissingJobRequest() throws GenieException {
        final Job job = new Job.Builder(JOB_1_NAME, JOB_1_USER, JOB_1_VERSION, JOB_1_COMMAND_ARGS)
            .withId(JOB_1_ID)
            .build();

        Mockito.when(this.jobRequestRepo.findOne(Mockito.eq(JOB_1_ID))).thenReturn(null);
        this.jobPersistenceService.createJob(job);
    }

    /**
     * Test the createJob method.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testCreateJob() throws GenieException {
        final Job job = new Job.Builder(JOB_1_NAME, JOB_1_USER, JOB_1_VERSION, JOB_1_COMMAND_ARGS)
            .withId(JOB_1_ID)
            .withStarted(new Date())
            .build();
        final JobRequestEntity jobRequestEntity = Mockito.mock(JobRequestEntity.class);
        final ArgumentCaptor<JobEntity> argument = ArgumentCaptor.forClass(JobEntity.class);

        Mockito.when(this.jobRequestRepo.findOne(Mockito.eq(JOB_1_ID))).thenReturn(jobRequestEntity);

        this.jobPersistenceService.createJob(job);

        Mockito.verify(jobRequestEntity).setJob(argument.capture());
        Assert.assertEquals(JOB_1_NAME, argument.getValue().getName());
        Assert.assertEquals(JOB_1_USER, argument.getValue().getUser());
        Assert.assertEquals(JOB_1_VERSION, argument.getValue().getVersion());
        Assert.assertEquals(JOB_1_ID, argument.getValue().getId());
        Assert.assertEquals(JOB_1_COMMAND_ARGS, argument.getValue().getCommandArgs());
    }

    /**
     * Test the updateJobStatus method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testUpdateJobStatusDoesNotExist() throws GenieException {
        final String id = UUID.randomUUID().toString();

        Mockito.when(this.jobRepo.findOne(Mockito.eq(id))).thenReturn(null);

        this.jobPersistenceService.updateJobStatus(id, JobStatus.RUNNING, JOB_1_STATUS_MSG);
    }

    /**
     * Test the updateJobStatus with status INIT.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testUpdateJobStatusForStatusInit() throws GenieException {
        final String id = UUID.randomUUID().toString();
        final JobEntity jobEntity = new JobEntity();
        final ArgumentCaptor<JobEntity> argument = ArgumentCaptor.forClass(JobEntity.class);

        Mockito.when(this.jobRepo.findOne(Mockito.eq(id))).thenReturn(jobEntity);
        this.jobPersistenceService.updateJobStatus(id, JobStatus.INIT, JOB_1_STATUS_MSG);

        Mockito.verify(jobRepo).save(argument.capture());

        Assert.assertEquals(JobStatus.INIT, argument.getValue().getStatus());
        Assert.assertEquals(JOB_1_STATUS_MSG, argument.getValue().getStatusMsg());
        Assert.assertNull(argument.getValue().getFinished());

        // Started should be null as the status is being set to INIT
        Assert.assertNull(argument.getValue().getStarted());
    }

    /**
     * Test the updateJobStatus with status RUNNING.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testUpdateJobStatusForStatusRunning() throws GenieException {
        final String id = UUID.randomUUID().toString();
        final JobEntity jobEntity = new JobEntity();
        final ArgumentCaptor<JobEntity> argument = ArgumentCaptor.forClass(JobEntity.class);

        Mockito.when(this.jobRepo.findOne(Mockito.eq(id))).thenReturn(jobEntity);
        this.jobPersistenceService.updateJobStatus(id, JobStatus.RUNNING, JOB_1_STATUS_MSG);

        Mockito.verify(this.jobRepo).save(argument.capture());

        Assert.assertEquals(JobStatus.RUNNING, argument.getValue().getStatus());
        Assert.assertEquals(JOB_1_STATUS_MSG, argument.getValue().getStatusMsg());

        Assert.assertNull(argument.getValue().getFinished());

        // Started should not be null as the status is being set to RUNNING
        Assert.assertNotNull(argument.getValue().getStarted());
    }

    /**
     * Test the updateJobStatus with status FAILED.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testUpdateJobStatusForStatusFailed() throws GenieException {
        final String id = UUID.randomUUID().toString();
        final JobEntity jobEntity = new JobEntity();
        final ArgumentCaptor<JobEntity> argument = ArgumentCaptor.forClass(JobEntity.class);

        Mockito.when(this.jobRepo.findOne(Mockito.eq(id))).thenReturn(jobEntity);
        this.jobPersistenceService.updateJobStatus(id, JobStatus.FAILED, JOB_1_STATUS_MSG);

        Mockito.verify(jobRepo).save(argument.capture());

        Assert.assertEquals(JobStatus.FAILED, argument.getValue().getStatus());
        Assert.assertEquals(JOB_1_STATUS_MSG, argument.getValue().getStatusMsg());
        Assert.assertNull(argument.getValue().getFinished());

        // Started should not be set as the status is being set to FAILED
        Assert.assertNull(argument.getValue().getStarted());
    }

    /**
     * Test the updateJobStatus with status KILLED.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testUpdateJobStatusForStatusKilled() throws GenieException {
        final String id = UUID.randomUUID().toString();
        final JobEntity jobEntity = new JobEntity();
        jobEntity.setStarted(new Date(0));
        final ArgumentCaptor<JobEntity> argument = ArgumentCaptor.forClass(JobEntity.class);

        Mockito.when(this.jobRepo.findOne(Mockito.eq(id))).thenReturn(jobEntity);
        this.jobPersistenceService.updateJobStatus(id, JobStatus.KILLED, JOB_1_STATUS_MSG);

        Mockito.verify(jobRepo).save(argument.capture());

        Assert.assertEquals(JobStatus.KILLED, argument.getValue().getStatus());
        Assert.assertEquals(JOB_1_STATUS_MSG, argument.getValue().getStatusMsg());
        Assert.assertNotNull(argument.getValue().getFinished());
    }

    /**
     * Test the updateJobStatus with status SUCCEEDED.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testUpdateJobStatusForStatusSucceeded() throws GenieException {
        final String id = UUID.randomUUID().toString();
        final JobEntity jobEntity = new JobEntity();
        jobEntity.setStarted(new Date(0));
        final ArgumentCaptor<JobEntity> argument = ArgumentCaptor.forClass(JobEntity.class);

        Mockito.when(this.jobRepo.findOne(Mockito.eq(id))).thenReturn(jobEntity);
        this.jobPersistenceService.updateJobStatus(id, JobStatus.SUCCEEDED, JOB_1_STATUS_MSG);

        Mockito.verify(jobRepo).save(argument.capture());

        Assert.assertEquals(JobStatus.SUCCEEDED, argument.getValue().getStatus());
        Assert.assertEquals(JOB_1_STATUS_MSG, argument.getValue().getStatusMsg());
        Assert.assertNotNull(argument.getValue().getFinished());
    }

    /**
     * Test the updateJobStatus method.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testUpdateJobStatusFinishedTimeForSuccess() throws GenieException {
        final String id = UUID.randomUUID().toString();
        final JobEntity jobEntity = new JobEntity();
        jobEntity.setStarted(new Date(0));
        final ArgumentCaptor<JobEntity> argument = ArgumentCaptor.forClass(JobEntity.class);

        Mockito.when(this.jobRepo.findOne(Mockito.eq(id))).thenReturn(jobEntity);
        this.jobPersistenceService.updateJobStatus(id, JobStatus.SUCCEEDED, JOB_1_STATUS_MSG);

        Mockito.verify(jobRepo).save(argument.capture());

        Assert.assertNotNull(argument.getValue().getFinished());
    }

    /**
     * Test the updateJobWithRuntime method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void cantUpdateRuntimeForNonExistentJob() throws GenieException {
        Mockito.when(this.jobRepo.findOne(Mockito.eq(JOB_1_ID))).thenReturn(null);
        this.jobPersistenceService.updateJobWithRuntimeEnvironment(JOB_1_ID, "foo", "bar", Lists.newArrayList());
    }

    /**
     * Test the updateJobWithRuntime method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void cantUpdateRuntimeForNonExistentCluster() throws GenieException {
        final String id = UUID.randomUUID().toString();
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        Mockito.when(this.jobRepo.findOne(JOB_1_ID)).thenReturn(jobEntity);
        Mockito.when(this.clusterRepo.findOne(Mockito.eq(id))).thenReturn(null);
        this.jobPersistenceService.updateJobWithRuntimeEnvironment(JOB_1_ID, id, "bar", Lists.newArrayList());
    }

    /**
     * Test the updateJobWithRuntime method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void cantUpdateRuntimeForNonExistentCommand() throws GenieException {
        final String clusterId = UUID.randomUUID().toString();
        final String commandId = UUID.randomUUID().toString();
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        Mockito.when(this.jobRepo.findOne(JOB_1_ID)).thenReturn(jobEntity);
        Mockito.when(this.clusterRepo.findOne(clusterId)).thenReturn(new ClusterEntity());
        Mockito.when(this.commandRepo.findOne(commandId)).thenReturn(null);
        this.jobPersistenceService
            .updateJobWithRuntimeEnvironment(JOB_1_ID, clusterId, commandId, Lists.newArrayList());
    }

    /**
     * Test the updateJobWithRuntime method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void cantUpdateRuntimeForNonExistentApplication() throws GenieException {
        final String clusterId = UUID.randomUUID().toString();
        final String commandId = UUID.randomUUID().toString();
        final String applicationId1 = UUID.randomUUID().toString();
        final String applicationId2 = UUID.randomUUID().toString();
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        Mockito.when(this.jobRepo.findOne(JOB_1_ID)).thenReturn(jobEntity);
        Mockito.when(this.clusterRepo.findOne(clusterId)).thenReturn(new ClusterEntity());
        Mockito.when(this.commandRepo.findOne(commandId)).thenReturn(new CommandEntity());
        Mockito.when(this.applicationRepo.findOne(applicationId1)).thenReturn(new ApplicationEntity());
        Mockito.when(this.applicationRepo.findOne(applicationId2)).thenReturn(null);
        this.jobPersistenceService.updateJobWithRuntimeEnvironment(
            JOB_1_ID,
            clusterId,
            commandId,
            Lists.newArrayList(applicationId1, applicationId2)
        );
    }

    /******* Unit Tests for Job Request methods ********/

    /**
     * Test the createJobRequest method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieConflictException.class)
    public void testCreateJobRequestAlreadyExists() throws GenieException {
        final JobRequest jobRequest = new JobRequest.Builder(
            JOB_1_NAME,
            JOB_1_USER,
            JOB_1_VERSION,
            null,
            null,
            null

        ).withId(JOB_1_ID)
            .build();

        Mockito.when(this.jobRequestRepo.exists(Mockito.eq(JOB_1_ID))).thenReturn(true);
        this.jobPersistenceService.createJobRequest(jobRequest, null);
    }

    /**
     * Test the createJobRequest method.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testCreateJobRequestWithIdSupplied() throws GenieException {
        final int cpu = 1;
        final int mem = 1;
        final String email = "name@domain.com";
        final String setupFile = "setupFilePath";
        final String group = "group";
        final String description = "job description";
        final Set<String> tags = new HashSet<>();
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
            .build();
        final ArgumentCaptor<JobRequestEntity> argument = ArgumentCaptor.forClass(JobRequestEntity.class);
        this.jobPersistenceService.createJobRequest(jobRequest, UUID.randomUUID().toString());
        Mockito.verify(this.jobRequestRepo).save(argument.capture());
        // Make sure id supplied is used to create the JobRequest
        Assert.assertEquals(JOB_1_ID, argument.getValue().getId());
        Assert.assertEquals(JOB_1_USER, argument.getValue().getUser());
        Assert.assertEquals(JOB_1_VERSION, argument.getValue().getVersion());
        Assert.assertEquals(JOB_1_NAME, argument.getValue().getName());
        Assert.assertEquals(cpu, argument.getValue().getCpu());
        Assert.assertEquals(mem, argument.getValue().getMemory());
        Assert.assertEquals(email, argument.getValue().getEmail());
        Assert.assertEquals(setupFile, argument.getValue().getSetupFile());
        Assert.assertEquals(group, argument.getValue().getGroup());
        Assert.assertEquals(tags, argument.getValue().getTags());
        Assert.assertEquals(description, argument.getValue().getDescription());
        Assert.assertThat(argument.getValue().getApplicationsAsList(), Matchers.empty());
    }

    /******* Unit Tests for Job Execution methods ********/

    /**
     * Test the createJobExecution method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GeniePreconditionException.class)
    public void testCreateJobExecutionIdWithNoId() throws GenieException {
        final JobExecution jobExecution = new JobExecution.Builder("hostname", 123, 1000L, new Date()).build();
        this.jobPersistenceService.createJobExecution(jobExecution);
    }

    /**
     * Test the createJobExecution method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testCreateJobExecutionJobDoesNotExist() throws GenieException {
        final JobExecution jobExecution = new JobExecution.Builder("hostname", 123, 2000L, new Date())
            .withId(JOB_1_ID)
            .build();
        Mockito.when(this.jobRepo.findOne(Mockito.eq(JOB_1_ID))).thenReturn(null);
        this.jobPersistenceService.createJobExecution(jobExecution);
    }

    /**
     * Test the createJobExecution method.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testCreateJobExecution() throws GenieException {
        final String hostname = "hostname";
        final int pid = 123;
        final long checkDelay = 3000L;
        final Date timeout = new Date();
        final JobExecution jobExecution = new JobExecution.Builder(hostname, pid, checkDelay, timeout)
            .withId(JOB_1_ID)
            .build();
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        Mockito.when(this.jobRepo.findOne(Mockito.eq(JOB_1_ID))).thenReturn(jobEntity);
        final ArgumentCaptor<JobExecutionEntity> argument = ArgumentCaptor.forClass(JobExecutionEntity.class);
        final ArgumentCaptor<JobStatus> argument1 = ArgumentCaptor.forClass(JobStatus.class);

        this.jobPersistenceService.createJobExecution(jobExecution);

        Mockito.verify(jobEntity).setExecution(argument.capture());
        Mockito.verify(jobEntity).setStatus(argument1.capture());

        Mockito.verify(jobEntity, Mockito.times(1)).setStarted(Mockito.any());

        Assert.assertEquals(hostname, argument.getValue().getHostName());
        Assert.assertEquals(pid, argument.getValue().getProcessId());
        Assert.assertThat(argument.getValue().getCheckDelay(), Matchers.is(checkDelay));
        Assert.assertThat(argument.getValue().getTimeout(), Matchers.is(timeout));
        Assert.assertEquals(JOB_1_ID, argument.getValue().getId());

        // verify the method sets the status code of the Job to RUNNING
        Assert.assertEquals(JobStatus.RUNNING, argument1.getValue());
    }

    /**
     * Test the getJobExecution method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testSetExitCodeJobDoesNotExist() throws GenieException {
        Mockito.when(this.jobExecutionRepo.findOne(Mockito.eq(JOB_1_ID))).thenReturn(null);
        this.jobPersistenceService.setExitCode(JOB_1_ID, 0);
    }
}
