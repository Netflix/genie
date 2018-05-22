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
package com.netflix.genie.web.jpa.services;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobMetadata;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.internal.dto.v4.AgentClientMetadata;
import com.netflix.genie.common.internal.dto.v4.JobSpecification;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieApplicationNotFoundException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieClusterNotFoundException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieCommandNotFoundException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieInvalidStatusException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobAlreadyClaimedException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobNotFoundException;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.genie.web.jpa.entities.ApplicationEntity;
import com.netflix.genie.web.jpa.entities.ClusterEntity;
import com.netflix.genie.web.jpa.entities.CommandEntity;
import com.netflix.genie.web.jpa.entities.FileEntity;
import com.netflix.genie.web.jpa.entities.JobEntity;
import com.netflix.genie.web.jpa.entities.TagEntity;
import com.netflix.genie.web.jpa.entities.projections.v4.JobSpecificationProjection;
import com.netflix.genie.web.jpa.entities.projections.v4.V4JobRequestProjection;
import com.netflix.genie.web.jpa.repositories.JpaApplicationRepository;
import com.netflix.genie.web.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.web.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.web.jpa.repositories.JpaJobRepository;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.dao.DuplicateKeyException;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

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
    private static final List<String> JOB_1_COMMAND_ARGS = Lists.newArrayList("-f", "hive.q");
    private static final String JOB_1_STATUS_MSG = "Default message";

    private JpaJobRepository jobRepository;
    private JpaApplicationRepository applicationRepository;
    private JpaClusterRepository clusterRepository;
    private JpaCommandRepository commandRepository;
    private JpaFilePersistenceService filePersistenceService;
    private JpaTagPersistenceService tagPersistenceService;

    private JpaJobPersistenceServiceImpl jobPersistenceService;

    /**
     * Setup the tests.
     */
    @Before
    public void setup() {
        this.jobRepository = Mockito.mock(JpaJobRepository.class);
        this.applicationRepository = Mockito.mock(JpaApplicationRepository.class);
        this.clusterRepository = Mockito.mock(JpaClusterRepository.class);
        this.commandRepository = Mockito.mock(JpaCommandRepository.class);
        this.tagPersistenceService = Mockito.mock(JpaTagPersistenceService.class);
        this.filePersistenceService = Mockito.mock(JpaFilePersistenceService.class);

        this.jobPersistenceService = new JpaJobPersistenceServiceImpl(
            this.tagPersistenceService,
            this.filePersistenceService,
            this.applicationRepository,
            this.clusterRepository,
            this.commandRepository,
            this.jobRepository
        );
    }

    /**
     * Test the createJob method.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testCreateJob() throws GenieException {
        final int cpu = 1;
        final int mem = 1;
        final String email = "name@domain.com";
        final String setupFile = "setupFilePath";
        final String group = "group";
        final String description = "job description";
        final Set<String> tags = Sets.newHashSet("foo", "bar");

        final JobRequest jobRequest = new JobRequest.Builder(
            JOB_1_NAME,
            JOB_1_USER,
            JOB_1_VERSION,
            Lists.newArrayList(),
            Sets.newHashSet()
        ).withId(JOB_1_ID)
            .withDescription(description)
            .withCommandArgs(JOB_1_COMMAND_ARGS)
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

        final Job job = new Job.Builder(JOB_1_NAME, JOB_1_USER, JOB_1_VERSION)
            .withStatus(JobStatus.INIT)
            .withStatusMsg("Job is initializing")
            .withCommandArgs(JOB_1_COMMAND_ARGS)
            .build();

        final JobExecution execution = new JobExecution.Builder(UUID.randomUUID().toString()).build();

        final TagEntity fooTag = new TagEntity();
        fooTag.setTag("foo");
        Mockito.when(this.tagPersistenceService.getTag("foo")).thenReturn(Optional.of(fooTag));
        final TagEntity barTag = new TagEntity();
        barTag.setTag("bar");
        Mockito.when(this.tagPersistenceService.getTag("bar")).thenReturn(Optional.of(barTag));
        final FileEntity setupFileEntity = new FileEntity();
        setupFileEntity.setFile(setupFile);
        Mockito.when(this.filePersistenceService.getFile(setupFile)).thenReturn(Optional.of(setupFileEntity));

        final ArgumentCaptor<JobEntity> argument = ArgumentCaptor.forClass(JobEntity.class);
        this.jobPersistenceService.createJob(jobRequest, metadata, job, execution);
        Mockito.verify(this.jobRepository).save(argument.capture());
        // Make sure id supplied is used to create the JobRequest
        Assert.assertEquals(JOB_1_ID, argument.getValue().getUniqueId());
        Assert.assertEquals(JOB_1_USER, argument.getValue().getUser());
        Assert.assertThat(
            argument.getValue().getVersion(),
            Matchers.is(JOB_1_VERSION)
        );
        Assert.assertEquals(JOB_1_NAME, argument.getValue().getName());
        final int actualCpu = argument.getValue().getRequestedCpu().orElseThrow(IllegalArgumentException::new);
        Assert.assertThat(actualCpu, Matchers.is(cpu));
        final int actualMemory = argument.getValue().getRequestedMemory().orElseThrow(IllegalArgumentException::new);
        Assert.assertThat(actualMemory, Matchers.is(mem));
        final String actualEmail = argument.getValue().getEmail().orElseThrow(IllegalArgumentException::new);
        Assert.assertThat(actualEmail, Matchers.is(email));
        final String actualSetupFile
            = argument.getValue().getSetupFile().orElseThrow(IllegalArgumentException::new).getFile();
        Assert.assertThat(actualSetupFile, Matchers.is(setupFile));
        final String actualGroup = argument.getValue().getGenieUserGroup().orElseThrow(IllegalArgumentException::new);
        Assert.assertThat(actualGroup, Matchers.is(group));
        Assert.assertEquals(
            tags,
            argument.getValue().getTags().stream().map(TagEntity::getTag).collect(Collectors.toSet())
        );
        final String actualDescription
            = argument.getValue().getDescription().orElseThrow(IllegalArgumentException::new);
        Assert.assertThat(actualDescription, Matchers.is(description));
        Assert.assertThat(argument.getValue().getRequestedApplications(), Matchers.empty());
    }

    /**
     * Test the createJob method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GeniePreconditionException.class)
    public void testCreateJobWithNoId() throws GenieException {
        final JobRequest jobRequest = new JobRequest.Builder(
            JOB_1_NAME,
            JOB_1_USER,
            JOB_1_VERSION,
            Lists.newArrayList(),
            Sets.newHashSet()
        )
            .withCommandArgs(JOB_1_COMMAND_ARGS)
            .build();
        this.jobPersistenceService.createJob(
            jobRequest, Mockito.mock(JobMetadata.class), Mockito.mock(Job.class), Mockito.mock(JobExecution.class)
        );
    }

    /**
     * Test the createJob method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieConflictException.class)
    public void testCreateJobAlreadyExists() throws GenieException {
        final JobRequest jobRequest = new JobRequest.Builder(
            JOB_1_NAME,
            JOB_1_USER,
            JOB_1_VERSION,
            Lists.newArrayList(),
            Sets.newHashSet()
        )
            .withId(JOB_1_ID)
            .withCommandArgs(JOB_1_COMMAND_ARGS)
            .build();

        final JobMetadata metadata = new JobMetadata
            .Builder()
            .withClientHost(UUID.randomUUID().toString())
            .withUserAgent(UUID.randomUUID().toString())
            .withNumAttachments(0)
            .withTotalSizeOfAttachments(0L)
            .build();

        final Job job = new Job.Builder(JOB_1_NAME, JOB_1_USER, JOB_1_VERSION)
            .withStatus(JobStatus.INIT)
            .withStatusMsg("Job is initializing")
            .withCommandArgs(JOB_1_COMMAND_ARGS)
            .build();


        final JobExecution execution = new JobExecution.Builder(UUID.randomUUID().toString()).build();

        Mockito
            .when(this.tagPersistenceService.getTag(Mockito.anyString()))
            .thenReturn(Optional.of(new TagEntity(UUID.randomUUID().toString())));
        Mockito
            .when(this.filePersistenceService.getFile(Mockito.anyString()))
            .thenReturn(Optional.of(new FileEntity(UUID.randomUUID().toString())));
        Mockito
            .when(this.jobRepository.save(Mockito.any(JobEntity.class)))
            .thenThrow(new DuplicateKeyException("Duplicate Key"));
        this.jobPersistenceService.createJob(jobRequest, metadata, job, execution);
    }

    /**
     * Test the updateJobStatus method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testUpdateJobStatusDoesNotExist() throws GenieException {
        final String id = UUID.randomUUID().toString();

        Mockito.when(this.jobRepository.findByUniqueId(Mockito.eq(id))).thenReturn(Optional.empty());

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

        Mockito.when(this.jobRepository.findByUniqueId(Mockito.eq(id))).thenReturn(Optional.of(jobEntity));
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

        Mockito.when(this.jobRepository.findByUniqueId(Mockito.eq(id))).thenReturn(Optional.of(jobEntity));
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

        Mockito.when(this.jobRepository.findByUniqueId(Mockito.eq(id))).thenReturn(Optional.of(jobEntity));
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
        jobEntity.setStarted(Instant.EPOCH);

        Mockito.when(this.jobRepository.findByUniqueId(Mockito.eq(id))).thenReturn(Optional.of(jobEntity));
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
        jobEntity.setStarted(Instant.EPOCH);

        Mockito.when(this.jobRepository.findByUniqueId(Mockito.eq(id))).thenReturn(Optional.of(jobEntity));
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
        jobEntity.setStarted(Instant.EPOCH);

        Mockito.when(this.jobRepository.findByUniqueId(Mockito.eq(id))).thenReturn(Optional.of(jobEntity));
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
        Mockito.when(this.jobRepository.findByUniqueId(Mockito.eq(JOB_1_ID))).thenReturn(Optional.empty());
        this.jobPersistenceService.updateJobWithRuntimeEnvironment(JOB_1_ID, "foo", "bar", Lists.newArrayList(), 1);
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
        Mockito.when(this.jobRepository.findByUniqueId(JOB_1_ID)).thenReturn(Optional.of(jobEntity));
        Mockito.when(this.clusterRepository.findByUniqueId(Mockito.eq(id))).thenReturn(Optional.empty());
        this.jobPersistenceService.updateJobWithRuntimeEnvironment(JOB_1_ID, id, "bar", Lists.newArrayList(), 1);
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
        Mockito.when(this.jobRepository.findByUniqueId(JOB_1_ID)).thenReturn(Optional.of(jobEntity));
        Mockito.when(this.clusterRepository.findByUniqueId(clusterId)).thenReturn(Optional.of(new ClusterEntity()));
        Mockito.when(this.commandRepository.findByUniqueId(commandId)).thenReturn(Optional.empty());
        this.jobPersistenceService
            .updateJobWithRuntimeEnvironment(JOB_1_ID, clusterId, commandId, Lists.newArrayList(), 1);
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
        Mockito.when(this.jobRepository.findByUniqueId(JOB_1_ID)).thenReturn(Optional.of(jobEntity));
        Mockito.when(this.clusterRepository.findByUniqueId(clusterId)).thenReturn(Optional.of(new ClusterEntity()));
        Mockito.when(this.commandRepository.findByUniqueId(commandId)).thenReturn(Optional.of(new CommandEntity()));
        Mockito
            .when(this.applicationRepository.findByUniqueId(applicationId1))
            .thenReturn(Optional.of(new ApplicationEntity()));
        Mockito.when(this.applicationRepository.findByUniqueId(applicationId2)).thenReturn(Optional.empty());
        this.jobPersistenceService.updateJobWithRuntimeEnvironment(
            JOB_1_ID,
            clusterId,
            commandId,
            Lists.newArrayList(applicationId1, applicationId2),
            1
        );
    }

    /**
     * Make sure we can't update a job if it can't be found.
     *
     * @throws GenieException On error
     */
    @Test(expected = GenieNotFoundException.class)
    public void cantFindJobToUpdateRunningInformationFor() throws GenieException {
        final String id = UUID.randomUUID().toString();

        Mockito.when(this.jobRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.jobPersistenceService.setJobRunningInformation(id, 1, 1, Instant.now());
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
        final Instant timeout = Instant.now();
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        Mockito.when(jobEntity.getStatus()).thenReturn(JobStatus.INIT);
        Mockito.when(this.jobRepository.findByUniqueId(id)).thenReturn(Optional.of(jobEntity));
        this.jobPersistenceService.setJobRunningInformation(id, processId, checkDelay, timeout);
        Mockito.verify(jobEntity, Mockito.times(1)).setTimeout(timeout);
        Mockito.verify(jobEntity, Mockito.times(1)).setProcessId(processId);
        Mockito.verify(jobEntity, Mockito.times(1)).setCheckDelay(checkDelay);
    }

    /**
     * Make sure we can't update running information for a job if it doesn't exist.
     *
     * @throws GenieException On error
     */
    @Test(expected = GenieNotFoundException.class)
    public void cantUpdateJobRunningInformationIfNoJob() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jobRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        this.jobPersistenceService.setJobRunningInformation(id, 212, 308L, Instant.now());
    }

    /**
     * Test the getJobExecution method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testSetExitCodeJobDoesNotExist() throws GenieException {
        Mockito.when(this.jobRepository.findByUniqueId(JOB_1_ID)).thenReturn(Optional.empty());
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
        Mockito.when(this.jobRepository.findByUniqueId(JOB_1_ID)).thenReturn(Optional.empty());

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
        Mockito.when(this.jobRepository.findByUniqueId(JOB_1_ID)).thenReturn(Optional.of(jobEntity));

        this.jobPersistenceService.setJobCompletionInformation(JOB_1_ID, 0, JobStatus.FAILED, "k", null, null);
        Mockito.verify(jobEntity, Mockito.times(1)).setStdOutSize(null);
        Mockito.verify(jobEntity, Mockito.times(1)).setStdErrSize(null);
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
        Mockito.when(this.jobRepository.findByUniqueId(JOB_1_ID)).thenReturn(Optional.of(jobEntity));
        Mockito.when(jobEntity.getExitCode()).thenReturn(Optional.of(1));

        this.jobPersistenceService.setJobCompletionInformation(JOB_1_ID, 0, JobStatus.FAILED, "k", null, 100L);
        Mockito.verify(jobEntity, Mockito.times(1)).setStdErrSize(100L);
        Mockito.verify(jobEntity, Mockito.times(1)).setStdOutSize(null);
    }

    /**
     * When a request is made for a job that doesn't have a record in the database an empty optional is returned.
     */
    @Test
    public void noJobRequestFoundReturnsEmptyOptional() {
        Mockito
            .when(this.jobRepository.findByUniqueId(Mockito.anyString(), Mockito.eq(V4JobRequestProjection.class)))
            .thenReturn(Optional.empty());

        Assert.assertFalse(this.jobPersistenceService.getJobRequest(UUID.randomUUID().toString()).isPresent());
    }

    /**
     * If a job isn't found to save a specification for an exception is thrown.
     */
    @Test(expected = GenieJobNotFoundException.class)
    public void noJobUnableToSaveJobSpecification() {
        Mockito
            .when(this.jobRepository.findByUniqueId(Mockito.anyString()))
            .thenReturn(Optional.empty());

        this.jobPersistenceService.saveJobSpecification(
            UUID.randomUUID().toString(), Mockito.mock(JobSpecification.class)
        );
    }

    /**
     * If a job is already resolved nothing is done.
     */
    @Test
    public void jobAlreadyResolvedDoesNotSaveSpecificationAgain() {
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        final JobSpecification jobSpecification = Mockito.mock(JobSpecification.class);
        Mockito
            .when(this.jobRepository.findByUniqueId(Mockito.anyString()))
            .thenReturn(Optional.of(jobEntity));

        Mockito.when(jobEntity.isResolved()).thenReturn(true);

        this.jobPersistenceService.saveJobSpecification(UUID.randomUUID().toString(), jobSpecification);

        Mockito.verify(jobEntity, Mockito.never()).setCluster(Mockito.any(ClusterEntity.class));
    }

    /**
     * If a job is already terminal.
     */
    @Test
    public void jobAlreadyTerminalDoesNotSaveJobSpecification() {
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        final JobSpecification jobSpecification = Mockito.mock(JobSpecification.class);
        Mockito
            .when(this.jobRepository.findByUniqueId(Mockito.anyString()))
            .thenReturn(Optional.of(jobEntity));

        Mockito.when(jobEntity.isResolved()).thenReturn(false);
        Mockito.when(jobEntity.getStatus()).thenReturn(JobStatus.KILLED);

        this.jobPersistenceService.saveJobSpecification(UUID.randomUUID().toString(), jobSpecification);

        Mockito.verify(jobEntity, Mockito.never()).setCluster(Mockito.any(ClusterEntity.class));
    }

    /**
     * If a job isn't found to save a specification for an exception is thrown.
     */
    @Test
    public void noResourceToSaveForJobSpecification() {
        final String jobId = UUID.randomUUID().toString();
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        Mockito.when(jobEntity.isResolved()).thenReturn(false);
        Mockito.when(jobEntity.getStatus()).thenReturn(JobStatus.RESERVED);

        final String clusterId = UUID.randomUUID().toString();
        final ClusterEntity clusterEntity = Mockito.mock(ClusterEntity.class);
        final JobSpecification.ExecutionResource clusterResource
            = Mockito.mock(JobSpecification.ExecutionResource.class);
        Mockito.when(clusterResource.getId()).thenReturn(clusterId);
        final String commandId = UUID.randomUUID().toString();
        final CommandEntity commandEntity = Mockito.mock(CommandEntity.class);
        final JobSpecification.ExecutionResource commandResource
            = Mockito.mock(JobSpecification.ExecutionResource.class);
        Mockito.when(commandResource.getId()).thenReturn(commandId);
        final String applicationId = UUID.randomUUID().toString();
        final JobSpecification.ExecutionResource applicationResource
            = Mockito.mock(JobSpecification.ExecutionResource.class);
        Mockito.when(applicationResource.getId()).thenReturn(applicationId);

        final JobSpecification jobSpecification = Mockito.mock(JobSpecification.class);
        Mockito.when(jobSpecification.getCluster()).thenReturn(clusterResource);
        Mockito.when(jobSpecification.getCommand()).thenReturn(commandResource);
        Mockito
            .when(jobSpecification.getApplications())
            .thenReturn(Lists.newArrayList(applicationResource));

        Mockito
            .when(this.jobRepository.findByUniqueId(jobId))
            .thenReturn(Optional.of(jobEntity));

        Mockito
            .when(this.clusterRepository.findByUniqueId(clusterId))
            .thenReturn(Optional.empty())
            .thenReturn(Optional.of(clusterEntity))
            .thenReturn(Optional.of(clusterEntity));

        Mockito
            .when(this.commandRepository.findByUniqueId(commandId))
            .thenReturn(Optional.empty())
            .thenReturn(Optional.of(commandEntity));
        Mockito.when(this.applicationRepository.findByUniqueId(applicationId)).thenReturn(Optional.empty());

        try {
            this.jobPersistenceService.saveJobSpecification(jobId, jobSpecification);
            Assert.fail();
        } catch (final GenieClusterNotFoundException e) {
            // expected
        }
        try {
            this.jobPersistenceService.saveJobSpecification(jobId, jobSpecification);
            Assert.fail();
        } catch (final GenieCommandNotFoundException e) {
            // expected
        }
        try {
            this.jobPersistenceService.saveJobSpecification(jobId, jobSpecification);
            Assert.fail();
        } catch (final GenieApplicationNotFoundException e) {
            // expected
        }
    }

    /**
     * If a job isn't found to retrieve a job specification.
     */
    @Test(expected = GenieJobNotFoundException.class)
    public void noJobUnableToGetJobSpecification() {
        Mockito
            .when(this.jobRepository.findByUniqueId(Mockito.anyString(), Mockito.eq(JobSpecificationProjection.class)))
            .thenReturn(Optional.empty());

        this.jobPersistenceService.getJobSpecification(UUID.randomUUID().toString());
    }


    /**
     * If a job isn't resolved empty {@link Optional} returned.
     */
    @Test
    public void unresolvedJobReturnsEmptyJobSpecificationOptional() {
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        Mockito
            .when(this.jobRepository.findByUniqueId(Mockito.anyString(), Mockito.eq(JobSpecificationProjection.class)))
            .thenReturn(Optional.of(jobEntity));

        Mockito.when(jobEntity.isResolved()).thenReturn(false);

        Assert.assertFalse(this.jobPersistenceService.getJobSpecification(UUID.randomUUID().toString()).isPresent());
    }

    /**
     * Test all the error cases covered in the
     * {@link JpaJobPersistenceServiceImpl#claimJob(String, AgentClientMetadata)} API.
     */
    @Test
    public void testClaimJobErrorCases() {
        Mockito
            .when(this.jobRepository.findByUniqueId(Mockito.anyString()))
            .thenReturn(Optional.empty());

        try {
            this.jobPersistenceService.claimJob(UUID.randomUUID().toString(), Mockito.mock(AgentClientMetadata.class));
            Assert.fail();
        } catch (final GenieJobNotFoundException e) {
            // expected
        }

        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        final String id = UUID.randomUUID().toString();
        Mockito
            .when(this.jobRepository.findByUniqueId(id))
            .thenReturn(Optional.of(jobEntity));

        Mockito.when(jobEntity.isClaimed()).thenReturn(true);

        try {
            this.jobPersistenceService.claimJob(id, Mockito.mock(AgentClientMetadata.class));
            Assert.fail();
        } catch (final GenieJobAlreadyClaimedException e) {
            // expected
        }

        Mockito.when(jobEntity.isClaimed()).thenReturn(false);
        Mockito.when(jobEntity.getStatus()).thenReturn(JobStatus.INVALID);

        try {
            this.jobPersistenceService.claimJob(id, Mockito.mock(AgentClientMetadata.class));
            Assert.fail();
        } catch (final GenieInvalidStatusException e) {
            // expected
        }
    }

    /**
     * Test valid behavior of the {@link JpaJobPersistenceServiceImpl#claimJob(String, AgentClientMetadata)} API.
     */
    @Test
    public void testClaimJobValidBehavior() {
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        final AgentClientMetadata agentClientMetadata = Mockito.mock(AgentClientMetadata.class);
        final String id = UUID.randomUUID().toString();

        Mockito.when(this.jobRepository.findByUniqueId(id)).thenReturn(Optional.of(jobEntity));
        Mockito.when(jobEntity.isClaimed()).thenReturn(false);
        Mockito.when(jobEntity.getStatus()).thenReturn(JobStatus.RESOLVED);

        final String agentHostname = UUID.randomUUID().toString();
        Mockito.when(agentClientMetadata.getHostname()).thenReturn(Optional.of(agentHostname));
        final String agentVersion = UUID.randomUUID().toString();
        Mockito.when(agentClientMetadata.getVersion()).thenReturn(Optional.of(agentVersion));
        final int agentPid = 238;
        Mockito.when(agentClientMetadata.getPid()).thenReturn(Optional.of(agentPid));

        this.jobPersistenceService.claimJob(id, agentClientMetadata);

        Mockito.verify(jobEntity, Mockito.times(1)).setClaimed(true);
        Mockito.verify(jobEntity, Mockito.times(1)).setStatus(JobStatus.CLAIMED);
        Mockito.verify(jobEntity, Mockito.times(1)).setAgentHostname(agentHostname);
        Mockito.verify(jobEntity, Mockito.times(1)).setAgentVersion(agentVersion);
        Mockito.verify(jobEntity, Mockito.times(1)).setAgentPid(agentPid);
    }

    /**
     * Test all the error cases covered in the
     * {@link JpaJobPersistenceServiceImpl#updateJobStatus(String, JobStatus, JobStatus, String)} API.
     */
    @Test
    public void testUpdateJobStatusErrorCases() {
        final String id = UUID.randomUUID().toString();
        try {
            this.jobPersistenceService.updateJobStatus(id, JobStatus.CLAIMED, JobStatus.CLAIMED, null);
            Assert.fail();
        } catch (final GenieInvalidStatusException e) {
            // expected
        }

        Mockito
            .when(this.jobRepository.findByUniqueId(id))
            .thenReturn(Optional.empty());

        try {
            this.jobPersistenceService.updateJobStatus(id, JobStatus.CLAIMED, JobStatus.INIT, null);
            Assert.fail();
        } catch (final GenieJobNotFoundException e) {
            // expected
        }

        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        Mockito
            .when(this.jobRepository.findByUniqueId(id))
            .thenReturn(Optional.of(jobEntity));

        Mockito.when(jobEntity.getStatus()).thenReturn(JobStatus.INIT);

        try {
            this.jobPersistenceService.updateJobStatus(id, JobStatus.CLAIMED, JobStatus.INIT, null);
            Assert.fail();
        } catch (final GenieInvalidStatusException e) {
            // expected
        }
    }

    /**
     * Test the valid behavior of
     * {@link JpaJobPersistenceServiceImpl#updateJobStatus(String, JobStatus, JobStatus, String)}.
     */
    @Test
    public void testUpdateJobValidBehavior() {
        final String id = UUID.randomUUID().toString();
        final String newStatusMessage = UUID.randomUUID().toString();
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);

        Mockito.when(this.jobRepository.findByUniqueId(id)).thenReturn(Optional.of(jobEntity));
        Mockito.when(jobEntity.getStatus()).thenReturn(JobStatus.INIT);

        this.jobPersistenceService.updateJobStatus(id, JobStatus.INIT, JobStatus.RUNNING, newStatusMessage);

        Mockito.verify(jobEntity, Mockito.times(1)).setStatus(JobStatus.RUNNING);
        Mockito.verify(jobEntity, Mockito.times(1)).setStatusMsg(newStatusMessage);
        Mockito.verify(jobEntity, Mockito.times(1)).setStarted(Mockito.any(Instant.class));

        final String finalStatusMessage = UUID.randomUUID().toString();
        Mockito.when(jobEntity.getStatus()).thenReturn(JobStatus.RUNNING);
        Mockito.when(jobEntity.getStarted()).thenReturn(Optional.of(Instant.now()));
        this.jobPersistenceService.updateJobStatus(id, JobStatus.RUNNING, JobStatus.SUCCEEDED, finalStatusMessage);

        Mockito.verify(jobEntity, Mockito.times(1)).setStatus(JobStatus.SUCCEEDED);
        Mockito.verify(jobEntity, Mockito.times(1)).setStatusMsg(finalStatusMessage);
        Mockito.verify(jobEntity, Mockito.times(1)).setFinished(Mockito.any(Instant.class));
    }
}
