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
import com.netflix.genie.common.dto.JobMetadata;
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
import com.netflix.genie.core.jpa.entities.JobMetadataEntity;
import com.netflix.genie.core.jpa.entities.JobRequestEntity;
import com.netflix.genie.core.jpa.repositories.JpaApplicationRepository;
import com.netflix.genie.core.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.core.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobExecutionRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobMetadataRepository;
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
import java.util.Optional;
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

    private static final String HOST_NAME = UUID.randomUUID().toString();
    private static final int PROCESS_ID = 38018;
    private static final long CHECK_DELAY = 892139L;
    private static final Date TIMEOUT = new Date();

    private JpaJobRepository jobRepo;
    private JpaJobRequestRepository jobRequestRepo;
    private JpaJobExecutionRepository jobExecutionRepo;
    private JpaJobMetadataRepository jobMetadataRepository;
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
        this.jobMetadataRepository = Mockito.mock(JpaJobMetadataRepository.class);
        this.applicationRepo = Mockito.mock(JpaApplicationRepository.class);
        this.clusterRepo = Mockito.mock(JpaClusterRepository.class);
        this.commandRepo = Mockito.mock(JpaCommandRepository.class);

        this.jobPersistenceService = new JpaJobPersistenceServiceImpl(
            this.jobRepo,
            this.jobRequestRepo,
            this.jobExecutionRepo,
            this.jobMetadataRepository,
            this.applicationRepo,
            this.clusterRepo,
            this.commandRepo
        );
    }

    /* Unit Tests for Job methods */

    /**
     * Test the createJob method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GeniePreconditionException.class)
    public void testCreateJobIdWithNoId() throws GenieException {
        final Job job = new Job.Builder(JOB_1_NAME, JOB_1_USER, JOB_1_VERSION, JOB_1_COMMAND_ARGS).build();
        final JobExecution jobExecution = new JobExecution.Builder(HOST_NAME)
            .withProcessId(PROCESS_ID)
            .withCheckDelay(CHECK_DELAY)
            .withTimeout(TIMEOUT)
            .build();
        this.jobPersistenceService.createJobAndJobExecution(job, jobExecution);
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
        final JobExecution jobExecution = new JobExecution.Builder(
            HOST_NAME
        ).build();
        this.jobPersistenceService.createJobAndJobExecution(job, jobExecution);
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
        final JobExecution jobExecution = new JobExecution.Builder(
            HOST_NAME
        ).build();
        this.jobPersistenceService.createJobAndJobExecution(job, jobExecution);
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
        final JobExecution jobExecution = new JobExecution.Builder(
            HOST_NAME
        ).build();
        this.jobPersistenceService.createJobAndJobExecution(job, jobExecution);
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
        final JobExecution jobExecution = new JobExecution.Builder(
            HOST_NAME
        ).build();
        this.jobPersistenceService.createJobAndJobExecution(job, jobExecution);

        Mockito.verify(jobRequestEntity).setJob(argument.capture());
        Assert.assertEquals(JOB_1_NAME, argument.getValue().getName());
        Assert.assertEquals(JOB_1_USER, argument.getValue().getUser());
        Assert.assertEquals(JOB_1_VERSION, argument.getValue().getVersion());
        Assert.assertEquals(JOB_1_ID, argument.getValue().getId());
        Assert.assertEquals(JOB_1_COMMAND_ARGS, argument.getValue().getCommandArgs());
        Assert.assertNotNull(argument.getValue().getExecution());
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

        Mockito.when(this.jobRepo.findOne(Mockito.eq(id))).thenReturn(jobEntity);
        this.jobPersistenceService.updateJobStatus(id, JobStatus.INIT, JOB_1_STATUS_MSG);

        Assert.assertEquals(JobStatus.INIT, jobEntity.getStatus());
        Assert.assertEquals(JOB_1_STATUS_MSG, jobEntity.getStatusMsg().orElseThrow(IllegalArgumentException::new));
        Assert.assertFalse(jobEntity.getFinished().isPresent());

        // Started should be null as the status is being set to INIT
        Assert.assertFalse(jobEntity.getStarted().isPresent());
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

        Mockito.when(this.jobRepo.findOne(Mockito.eq(id))).thenReturn(jobEntity);
        this.jobPersistenceService.updateJobStatus(id, JobStatus.RUNNING, JOB_1_STATUS_MSG);

        Assert.assertEquals(JobStatus.RUNNING, jobEntity.getStatus());
        Assert.assertEquals(JOB_1_STATUS_MSG, jobEntity.getStatusMsg().orElseThrow(IllegalArgumentException::new));

        Assert.assertFalse(jobEntity.getFinished().isPresent());

        // Started should not be null as the status is being set to RUNNING
        Assert.assertTrue(jobEntity.getStarted().isPresent());
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

        Mockito.when(this.jobRepo.findOne(Mockito.eq(id))).thenReturn(jobEntity);
        this.jobPersistenceService.updateJobStatus(id, JobStatus.FAILED, JOB_1_STATUS_MSG);

        Assert.assertEquals(JobStatus.FAILED, jobEntity.getStatus());
        Assert.assertEquals(JOB_1_STATUS_MSG, jobEntity.getStatusMsg().orElseThrow(IllegalArgumentException::new));
        Assert.assertFalse(jobEntity.getFinished().isPresent());

        // Started should not be set as the status is being set to FAILED
        Assert.assertFalse(jobEntity.getStarted().isPresent());
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

        Mockito.when(this.jobRepo.findOne(Mockito.eq(id))).thenReturn(jobEntity);
        this.jobPersistenceService.updateJobStatus(id, JobStatus.KILLED, JOB_1_STATUS_MSG);

        Assert.assertEquals(JobStatus.KILLED, jobEntity.getStatus());
        Assert.assertEquals(JOB_1_STATUS_MSG, jobEntity.getStatusMsg().orElseThrow(IllegalArgumentException::new));
        Assert.assertNotNull(jobEntity.getFinished());
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

        Mockito.when(this.jobRepo.findOne(Mockito.eq(id))).thenReturn(jobEntity);
        this.jobPersistenceService.updateJobStatus(id, JobStatus.SUCCEEDED, JOB_1_STATUS_MSG);

        Assert.assertEquals(JobStatus.SUCCEEDED, jobEntity.getStatus());
        Assert.assertEquals(JOB_1_STATUS_MSG, jobEntity.getStatusMsg().orElseThrow(IllegalArgumentException::new));
        Assert.assertNotNull(jobEntity.getFinished());
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

        Mockito.when(this.jobRepo.findOne(Mockito.eq(id))).thenReturn(jobEntity);
        this.jobPersistenceService.updateJobStatus(id, JobStatus.SUCCEEDED, JOB_1_STATUS_MSG);

        Assert.assertNotNull(jobEntity.getFinished());
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

    /* Unit Tests for Job Request methods */

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

        final String clientHost = UUID.randomUUID().toString();
        final String userAgent = UUID.randomUUID().toString();
        final int numAttachments = 380;
        final long totalSizeOfAttachments = 830803L;
        final JobMetadata metadata = new JobMetadata
            .Builder()
            .withClientHost(clientHost)
            .withUserAgent(userAgent)
            .withNumAttachments(numAttachments)
            .withTotalSizeOfAttachments(totalSizeOfAttachments)
            .build();

        final ArgumentCaptor<JobRequestEntity> argument = ArgumentCaptor.forClass(JobRequestEntity.class);
        this.jobPersistenceService.createJobRequest(jobRequest, metadata);
        Mockito.verify(this.jobRequestRepo).save(argument.capture());
        // Make sure id supplied is used to create the JobRequest
        Assert.assertEquals(JOB_1_ID, argument.getValue().getId());
        Assert.assertEquals(JOB_1_USER, argument.getValue().getUser());
        Assert.assertEquals(JOB_1_VERSION, argument.getValue().getVersion());
        Assert.assertEquals(JOB_1_NAME, argument.getValue().getName());
        final int actualCpu = argument.getValue().getCpu().orElseThrow(IllegalArgumentException::new);
        Assert.assertThat(actualCpu, Matchers.is(cpu));
        final int actualMemory = argument.getValue().getMemory().orElseThrow(IllegalArgumentException::new);
        Assert.assertThat(actualMemory, Matchers.is(mem));
        final String actualEmail = argument.getValue().getEmail().orElseThrow(IllegalArgumentException::new);
        Assert.assertThat(actualEmail, Matchers.is(email));
        final String actualSetupFile = argument.getValue().getSetupFile().orElseThrow(IllegalArgumentException::new);
        Assert.assertThat(actualSetupFile, Matchers.is(setupFile));
        final String actualGroup = argument.getValue().getGroup().orElseThrow(IllegalArgumentException::new);
        Assert.assertThat(actualGroup, Matchers.is(group));
        Assert.assertEquals(tags, argument.getValue().getTags());
        final String actualDescription
            = argument.getValue().getDescription().orElseThrow(IllegalArgumentException::new);
        Assert.assertThat(actualDescription, Matchers.is(description));
        Assert.assertThat(argument.getValue().getApplicationsAsList(), Matchers.empty());
    }

    /**
     * Make sure we can't update a job if it can't be found.
     *
     * @throws GenieException On error
     */
    @Test(expected = GenieNotFoundException.class)
    public void cantFindJobToUpdateRunningInformationFor() throws GenieException {
        final String id = UUID.randomUUID().toString();

        Mockito.when(this.jobExecutionRepo.findOne(id)).thenReturn(null);
        this.jobPersistenceService.setJobRunningInformation(id, 1, 1, new Date());
    }

    /**
     * Make sure we can update a job if it can be found.
     *
     * @throws GenieException On error
     */
    @Test
    public void canUpdateJobRunningInformation() throws GenieException {
        final String id = UUID.randomUUID().toString();
        final int processId = 28042;
        final long checkDelay = 280234L;
        final Date timeout = new Date();
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        Mockito.when(jobEntity.getStatus()).thenReturn(JobStatus.INIT);
        final JobExecutionEntity jobExecutionEntity = Mockito.mock(JobExecutionEntity.class);
        Mockito.when(this.jobExecutionRepo.findOne(id)).thenReturn(jobExecutionEntity);
        Mockito.when(this.jobRepo.findOne(id)).thenReturn(jobEntity);
        this.jobPersistenceService.setJobRunningInformation(id, processId, checkDelay, timeout);
        Mockito.verify(jobExecutionEntity, Mockito.times(1)).setTimeout(timeout);
        Mockito.verify(jobExecutionEntity, Mockito.times(1)).setProcessId(processId);
        Mockito.verify(jobExecutionEntity, Mockito.times(1)).setCheckDelay(checkDelay);
    }

    /**
     * Test the getJobExecution method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testSetExitCodeJobDoesNotExist() throws GenieException {
        Mockito.when(this.jobExecutionRepo.findOne(Mockito.eq(JOB_1_ID))).thenReturn(null);
        this.jobPersistenceService
            .setJobCompletionInformation(JOB_1_ID, 0, JobStatus.FAILED, UUID.randomUUID().toString(), null, null);
    }

    /**
     * Test the setJobCompletionInformation method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void cantUpdateJobMetadataIfNotExists() throws GenieException {
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        Mockito.when(jobEntity.getStatus()).thenReturn(JobStatus.FAILED);
        Mockito.when(this.jobRepo.findOne(JOB_1_ID)).thenReturn(jobEntity);
        final JobExecutionEntity jobExecutionEntity = Mockito.mock(JobExecutionEntity.class);
        Mockito.when(jobExecutionEntity.getExitCode()).thenReturn(Optional.of(1));
        Mockito.when(this.jobExecutionRepo.findOne(JOB_1_ID)).thenReturn(jobExecutionEntity);
        Mockito.when(this.jobMetadataRepository.findOne(JOB_1_ID)).thenReturn(null);

        this.jobPersistenceService.setJobCompletionInformation(JOB_1_ID, 0, JobStatus.FAILED, "k", 100L, 1L);
    }

    /**
     * Test the setJobCompletionInformation method.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void wontUpdateJobMetadataIfNoSizes() throws GenieException {
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        Mockito.when(jobEntity.getStatus()).thenReturn(JobStatus.FAILED);
        Mockito.when(this.jobRepo.findOne(JOB_1_ID)).thenReturn(jobEntity);
        final JobExecutionEntity jobExecutionEntity = Mockito.mock(JobExecutionEntity.class);
        Mockito.when(jobExecutionEntity.getExitCode()).thenReturn(Optional.of(1));
        Mockito.when(this.jobExecutionRepo.findOne(JOB_1_ID)).thenReturn(jobExecutionEntity);

        this.jobPersistenceService.setJobCompletionInformation(JOB_1_ID, 0, JobStatus.FAILED, "k", null, null);
        Mockito.verify(this.jobMetadataRepository, Mockito.never()).findOne(JOB_1_ID);
    }

    /**
     * Test the setJobCompletionInformation method.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void willUpdateJobMetadataIfOneSize() throws GenieException {
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        Mockito.when(jobEntity.getStatus()).thenReturn(JobStatus.FAILED);
        Mockito.when(this.jobRepo.findOne(JOB_1_ID)).thenReturn(jobEntity);
        final JobExecutionEntity jobExecutionEntity = Mockito.mock(JobExecutionEntity.class);
        Mockito.when(jobExecutionEntity.getExitCode()).thenReturn(Optional.of(1));
        Mockito.when(this.jobExecutionRepo.findOne(JOB_1_ID)).thenReturn(jobExecutionEntity);
        Mockito.when(this.jobMetadataRepository.findOne(JOB_1_ID)).thenReturn(Mockito.mock(JobMetadataEntity.class));

        this.jobPersistenceService.setJobCompletionInformation(JOB_1_ID, 0, JobStatus.FAILED, "k", null, 100L);
        Mockito.verify(this.jobMetadataRepository, Mockito.times(1)).findOne(JOB_1_ID);
    }
}
