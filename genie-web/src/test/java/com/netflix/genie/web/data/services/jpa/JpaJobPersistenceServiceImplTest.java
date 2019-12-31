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
package com.netflix.genie.web.data.services.jpa;

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
import com.netflix.genie.common.internal.dtos.v4.AgentClientMetadata;
import com.netflix.genie.common.internal.dtos.v4.JobSpecification;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieApplicationNotFoundException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieClusterNotFoundException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieCommandNotFoundException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieInvalidStatusException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobAlreadyClaimedException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobNotFoundException;
import com.netflix.genie.web.data.entities.ApplicationEntity;
import com.netflix.genie.web.data.entities.ClusterEntity;
import com.netflix.genie.web.data.entities.CommandEntity;
import com.netflix.genie.web.data.entities.FileEntity;
import com.netflix.genie.web.data.entities.JobEntity;
import com.netflix.genie.web.data.entities.TagEntity;
import com.netflix.genie.web.data.entities.projections.JobApiProjection;
import com.netflix.genie.web.data.entities.projections.StatusProjection;
import com.netflix.genie.web.data.entities.projections.v4.FinishedJobProjection;
import com.netflix.genie.web.data.entities.projections.v4.IsV4JobProjection;
import com.netflix.genie.web.data.entities.projections.v4.JobSpecificationProjection;
import com.netflix.genie.web.data.entities.projections.v4.V4JobRequestProjection;
import com.netflix.genie.web.data.repositories.jpa.JpaApplicationRepository;
import com.netflix.genie.web.data.repositories.jpa.JpaClusterRepository;
import com.netflix.genie.web.data.repositories.jpa.JpaCommandRepository;
import com.netflix.genie.web.data.repositories.jpa.JpaJobRepository;
import com.netflix.genie.web.dtos.ResolvedJob;
import com.netflix.genie.web.services.AttachmentService;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.dao.DuplicateKeyException;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
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
public class JpaJobPersistenceServiceImplTest {
    // TODO the use of a static converter makes this class hard to test. Switch to a non-static converter object.

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
    @BeforeEach
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
            this.jobRepository,
            Mockito.mock(AttachmentService.class)
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
        Assertions.assertThat(argument.getValue().getUniqueId()).isEqualTo(JOB_1_ID);
        Assertions.assertThat(argument.getValue().getUser()).isEqualTo(JOB_1_USER);
        Assertions.assertThat(argument.getValue().getVersion()).isEqualTo(JOB_1_VERSION);
        Assertions.assertThat(argument.getValue().getName()).isEqualTo(JOB_1_NAME);
        final int actualCpu = argument.getValue().getRequestedCpu().orElseThrow(IllegalArgumentException::new);
        Assertions.assertThat(actualCpu).isEqualTo(cpu);
        final int actualMemory = argument.getValue().getRequestedMemory().orElseThrow(IllegalArgumentException::new);
        Assertions.assertThat(actualMemory).isEqualTo(mem);
        final String actualEmail = argument.getValue().getEmail().orElseThrow(IllegalArgumentException::new);
        Assertions.assertThat(actualEmail).isEqualTo(email);
        final String actualSetupFile
            = argument.getValue().getSetupFile().orElseThrow(IllegalArgumentException::new).getFile();
        Assertions.assertThat(actualSetupFile).isEqualTo(setupFile);
        final String actualGroup = argument.getValue().getGenieUserGroup().orElseThrow(IllegalArgumentException::new);
        Assertions.assertThat(actualGroup).isEqualTo(group);
        Assertions
            .assertThat(argument.getValue().getTags().stream().map(TagEntity::getTag).collect(Collectors.toSet()))
            .isEqualTo(tags);
        final String actualDescription
            = argument.getValue().getDescription().orElseThrow(IllegalArgumentException::new);
        Assertions.assertThat(actualDescription).isEqualTo(description);
        Assertions.assertThat(argument.getValue().getRequestedApplications()).isEmpty();
    }

    /**
     * Test the createJob method.
     */
    @Test
    public void testCreateJobWithNoId() {
        final JobRequest jobRequest = new JobRequest.Builder(
            JOB_1_NAME,
            JOB_1_USER,
            JOB_1_VERSION,
            Lists.newArrayList(),
            Sets.newHashSet()
        )
            .withCommandArgs(JOB_1_COMMAND_ARGS)
            .build();
        Assertions
            .assertThatExceptionOfType(GeniePreconditionException.class)
            .isThrownBy(
                () -> this.jobPersistenceService.createJob(
                    jobRequest,
                    Mockito.mock(JobMetadata.class),
                    Mockito.mock(Job.class),
                    Mockito.mock(JobExecution.class)
                )
            );
    }

    /**
     * Test the createJob method.
     */
    @Test
    public void testCreateJobAlreadyExists() {
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
        Assertions
            .assertThatExceptionOfType(GenieConflictException.class)
            .isThrownBy(() -> this.jobPersistenceService.createJob(jobRequest, metadata, job, execution));
    }

    /**
     * Test the updateJobStatus method.
     */
    @Test
    public void testUpdateJobStatusDoesNotExist() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jobRepository.findByUniqueId(Mockito.eq(id))).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.jobPersistenceService.updateJobStatus(id, JobStatus.RUNNING, JOB_1_STATUS_MSG));
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
        jobEntity.setStatus(JobStatus.ACCEPTED.name());

        Mockito.when(this.jobRepository.findByUniqueId(Mockito.eq(id))).thenReturn(Optional.of(jobEntity));
        this.jobPersistenceService.updateJobStatus(id, JobStatus.INIT, JOB_1_STATUS_MSG);

        Assertions.assertThat(jobEntity.getStatus()).isEqualTo(JobStatus.INIT.name());
        Assertions.assertThat(jobEntity.getStatusMsg()).isPresent().contains(JOB_1_STATUS_MSG);
        Assertions.assertThat(jobEntity.getFinished()).isNotPresent();

        // Started should be null as the status is being set to INIT
        Assertions.assertThat(jobEntity.getStarted()).isNotPresent();
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
        jobEntity.setStatus(JobStatus.INIT.name());

        Mockito.when(this.jobRepository.findByUniqueId(Mockito.eq(id))).thenReturn(Optional.of(jobEntity));
        this.jobPersistenceService.updateJobStatus(id, JobStatus.RUNNING, JOB_1_STATUS_MSG);

        Assertions.assertThat(jobEntity.getStatus()).isEqualTo(JobStatus.RUNNING.name());
        Assertions.assertThat(jobEntity.getStatusMsg()).isPresent().contains(JOB_1_STATUS_MSG);

        Assertions.assertThat(jobEntity.getFinished()).isNotPresent();

        // Started should not be null as the status is being set to RUNNING
        Assertions.assertThat(jobEntity.getStarted()).isPresent();
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
        jobEntity.setStatus(JobStatus.INIT.name());

        Mockito.when(this.jobRepository.findByUniqueId(Mockito.eq(id))).thenReturn(Optional.of(jobEntity));
        this.jobPersistenceService.updateJobStatus(id, JobStatus.FAILED, JOB_1_STATUS_MSG);

        Assertions.assertThat(jobEntity.getStatus()).isEqualTo(JobStatus.FAILED.name());
        Assertions.assertThat(jobEntity.getStatusMsg()).isPresent().contains(JOB_1_STATUS_MSG);
        Assertions.assertThat(jobEntity.getFinished()).isNotPresent();

        // Started should not be set as the status is being set to FAILED
        Assertions.assertThat(jobEntity.getStarted()).isNotPresent();
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
        jobEntity.setStatus(JobStatus.RUNNING.name());

        Mockito.when(this.jobRepository.findByUniqueId(Mockito.eq(id))).thenReturn(Optional.of(jobEntity));
        this.jobPersistenceService.updateJobStatus(id, JobStatus.KILLED, JOB_1_STATUS_MSG);

        Assertions.assertThat(jobEntity.getStatus()).isEqualTo(JobStatus.KILLED.name());
        Assertions.assertThat(jobEntity.getStatusMsg()).isPresent().contains(JOB_1_STATUS_MSG);
        Assertions.assertThat(jobEntity.getFinished()).isPresent();
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
        jobEntity.setStatus(JobStatus.RUNNING.name());

        Mockito.when(this.jobRepository.findByUniqueId(Mockito.eq(id))).thenReturn(Optional.of(jobEntity));
        this.jobPersistenceService.updateJobStatus(id, JobStatus.SUCCEEDED, JOB_1_STATUS_MSG);

        Assertions.assertThat(jobEntity.getStatus()).isEqualTo(JobStatus.SUCCEEDED.name());
        Assertions.assertThat(jobEntity.getStatusMsg()).isPresent().contains(JOB_1_STATUS_MSG);
        Assertions.assertThat(jobEntity.getFinished()).isPresent();
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
        jobEntity.setStatus(JobStatus.RUNNING.name());
        jobEntity.setStarted(Instant.EPOCH);

        Mockito.when(this.jobRepository.findByUniqueId(id)).thenReturn(Optional.of(jobEntity));
        this.jobPersistenceService.updateJobStatus(id, JobStatus.SUCCEEDED, JOB_1_STATUS_MSG);

        Assertions.assertThat(jobEntity.getFinished()).isPresent();
    }

    /**
     * Test the updateJobWithRuntime method.
     */
    @Test
    public void cantUpdateRuntimeForNonExistentJob() {
        Mockito.when(this.jobRepository.findByUniqueId(Mockito.eq(JOB_1_ID))).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(
                () -> this.jobPersistenceService.updateJobWithRuntimeEnvironment(
                    JOB_1_ID,
                    "foo",
                    "bar",
                    Lists.newArrayList(),
                    1
                )
            );
    }

    /**
     * Test the updateJobWithRuntime method.
     */
    @Test
    public void cantUpdateRuntimeForNonExistentCluster() {
        final String id = UUID.randomUUID().toString();
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        Mockito.when(this.jobRepository.findByUniqueId(JOB_1_ID)).thenReturn(Optional.of(jobEntity));
        Mockito.when(this.clusterRepository.findByUniqueId(Mockito.eq(id))).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(
                () -> this.jobPersistenceService.updateJobWithRuntimeEnvironment(
                    JOB_1_ID,
                    id,
                    "bar",
                    Lists.newArrayList(),
                    1
                )
            );
    }

    /**
     * Test the updateJobWithRuntime method.
     */
    @Test
    public void cantUpdateRuntimeForNonExistentCommand() {
        final String clusterId = UUID.randomUUID().toString();
        final String commandId = UUID.randomUUID().toString();
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        Mockito.when(this.jobRepository.findByUniqueId(JOB_1_ID)).thenReturn(Optional.of(jobEntity));
        Mockito.when(this.clusterRepository.findByUniqueId(clusterId)).thenReturn(Optional.of(new ClusterEntity()));
        Mockito.when(this.commandRepository.findByUniqueId(commandId)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(
                () -> this.jobPersistenceService.updateJobWithRuntimeEnvironment(
                    JOB_1_ID,
                    clusterId,
                    commandId,
                    Lists.newArrayList(),
                    1
                )
            );
    }

    /**
     * Test the updateJobWithRuntime method.
     */
    @Test
    public void cantUpdateRuntimeForNonExistentApplication() {
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
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(
                () -> this.jobPersistenceService.updateJobWithRuntimeEnvironment(
                    JOB_1_ID,
                    clusterId,
                    commandId,
                    Lists.newArrayList(applicationId1, applicationId2),
                    1
                )
            );
    }

    /**
     * Make sure we can't update a job if it can't be found.
     */
    @Test
    public void cantFindJobToUpdateRunningInformationFor() {
        final String id = UUID.randomUUID().toString();

        Mockito.when(this.jobRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(
                () -> this.jobPersistenceService.setJobRunningInformation(
                    id,
                    1,
                    1,
                    Instant.now()
                )
            );
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
        final Instant started = Instant.now();
        final Instant timeout = Instant.now().plus(50L, ChronoUnit.MINUTES);
        final int timeoutUsed = (int) started.until(timeout, ChronoUnit.SECONDS);
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        Mockito.when(jobEntity.getStatus()).thenReturn(JobStatus.INIT.name());
        Mockito.when(this.jobRepository.findByUniqueId(id)).thenReturn(Optional.of(jobEntity));
        Mockito.when(jobEntity.getStarted()).thenReturn(Optional.of(started));

        this.jobPersistenceService.setJobRunningInformation(id, processId, checkDelay, timeout);
        Mockito.verify(jobEntity, Mockito.times(1)).setTimeoutUsed(timeoutUsed);
        Mockito.verify(jobEntity, Mockito.times(1)).setProcessId(processId);
        Mockito.verify(jobEntity, Mockito.times(1)).setCheckDelay(checkDelay);
    }

    /**
     * Make sure we can't update running information for a job if it doesn't exist.
     */
    @Test
    public void cantUpdateJobRunningInformationIfNoJob() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jobRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(
                () -> this.jobPersistenceService.setJobRunningInformation(
                    id,
                    212,
                    308L,
                    Instant.now()
                )
            );
    }

    /**
     * Test the getJobExecution method.
     */
    @Test
    public void testSetExitCodeJobDoesNotExist() {
        Mockito.when(this.jobRepository.findByUniqueId(JOB_1_ID)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(
                () -> this.jobPersistenceService.setJobCompletionInformation(
                    JOB_1_ID,
                    0,
                    JobStatus.FAILED,
                    UUID.randomUUID().toString(),
                    null,
                    null
                )
            );
    }

    /**
     * Test the setJobCompletionInformation method.
     */
    @Test
    public void cantUpdateJobMetadataIfNotExists() {
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        Mockito.when(jobEntity.getStatus()).thenReturn(JobStatus.FAILED.name());
        Mockito.when(this.jobRepository.findByUniqueId(JOB_1_ID)).thenReturn(Optional.empty());

        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(
                () -> this.jobPersistenceService.setJobCompletionInformation(
                    JOB_1_ID,
                    0,
                    JobStatus.FAILED,
                    "k",
                    100L,
                    1L
                )
            );
    }

    /**
     * Test the setJobCompletionInformation method.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void wontUpdateJobMetadataIfNoSizes() throws GenieException {
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        Mockito.when(jobEntity.getStatus()).thenReturn(JobStatus.FAILED.name());
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
        Mockito.when(jobEntity.getStatus()).thenReturn(JobStatus.FAILED.name());
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

        Assertions.assertThat(this.jobPersistenceService.getJobRequest(UUID.randomUUID().toString())).isNotPresent();
    }

    /**
     * If a job isn't found to save a specification for an exception is thrown.
     */
    @Test
    public void noJobUnableToSaveResolvedJob() {
        Mockito
            .when(this.jobRepository.findByUniqueId(Mockito.anyString()))
            .thenReturn(Optional.empty());

        Assertions
            .assertThatExceptionOfType(GenieJobNotFoundException.class)
            .isThrownBy(
                () -> this.jobPersistenceService.saveResolvedJob(
                    UUID.randomUUID().toString(),
                    Mockito.mock(ResolvedJob.class)
                )
            );
    }

    /**
     * If a job is already resolved nothing is done.
     */
    @Test
    public void jobAlreadyResolvedDoesNotResolvedInformationAgain() {
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        Mockito
            .when(this.jobRepository.findByUniqueId(Mockito.anyString()))
            .thenReturn(Optional.of(jobEntity));

        Mockito.when(jobEntity.isResolved()).thenReturn(true);

        this.jobPersistenceService.saveResolvedJob(UUID.randomUUID().toString(), Mockito.mock(ResolvedJob.class));

        Mockito.verify(jobEntity, Mockito.never()).setCluster(Mockito.any(ClusterEntity.class));
    }

    /**
     * If a job is already terminal.
     */
    @Test
    public void jobAlreadyTerminalDoesNotSaveResolvedJob() {
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        Mockito
            .when(this.jobRepository.findByUniqueId(Mockito.anyString()))
            .thenReturn(Optional.of(jobEntity));

        Mockito.when(jobEntity.isResolved()).thenReturn(false);
        Mockito.when(jobEntity.getStatus()).thenReturn(JobStatus.KILLED.name());

        this.jobPersistenceService.saveResolvedJob(UUID.randomUUID().toString(), Mockito.mock(ResolvedJob.class));

        Mockito.verify(jobEntity, Mockito.never()).setCluster(Mockito.any(ClusterEntity.class));
    }

    /**
     * If a job isn't found to save a specification for an exception is thrown.
     */
    @Test
    public void noResourceToSaveForResolvedJob() {
        final String jobId = UUID.randomUUID().toString();
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        Mockito.when(jobEntity.isResolved()).thenReturn(false);
        Mockito.when(jobEntity.getStatus()).thenReturn(JobStatus.RESERVED.name());

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

        final ResolvedJob resolvedJob = Mockito.mock(ResolvedJob.class);
        final JobSpecification jobSpecification = Mockito.mock(JobSpecification.class);
        Mockito.when(resolvedJob.getJobSpecification()).thenReturn(jobSpecification);
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

        Assertions
            .assertThatExceptionOfType(GenieClusterNotFoundException.class)
            .isThrownBy(() -> this.jobPersistenceService.saveResolvedJob(jobId, resolvedJob));
        Assertions
            .assertThatExceptionOfType(GenieCommandNotFoundException.class)
            .isThrownBy(() -> this.jobPersistenceService.saveResolvedJob(jobId, resolvedJob));
        Assertions
            .assertThatExceptionOfType(GenieApplicationNotFoundException.class)
            .isThrownBy(() -> this.jobPersistenceService.saveResolvedJob(jobId, resolvedJob));
    }

    /**
     * If a job isn't found to retrieve a job specification.
     */
    @Test
    public void noJobUnableToGetJobSpecification() {
        Mockito
            .when(this.jobRepository.findByUniqueId(Mockito.anyString(), Mockito.eq(JobSpecificationProjection.class)))
            .thenReturn(Optional.empty());

        Assertions
            .assertThatExceptionOfType(GenieJobNotFoundException.class)
            .isThrownBy(() -> this.jobPersistenceService.getJobSpecification(UUID.randomUUID().toString()));
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

        Assertions
            .assertThat(this.jobPersistenceService.getJobSpecification(UUID.randomUUID().toString()))
            .isNotPresent();
    }

    /**
     * If a job isn't found on querying for v4 flag.
     */
    @Test
    public void noJobUnableToGetV4() {
        Mockito
            .when(this.jobRepository.findByUniqueId(Mockito.anyString(), Mockito.eq(IsV4JobProjection.class)))
            .thenReturn(Optional.empty());

        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.jobPersistenceService.isV4(UUID.randomUUID().toString()));
    }

    /**
     * If v4 is false in db then return false.
     *
     * @throws GenieNotFoundException if the job is not found
     */
    @Test
    public void v4JobFalseReturnsFalse() throws GenieNotFoundException {
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        Mockito
            .when(this.jobRepository.findByUniqueId(Mockito.anyString(), Mockito.eq(IsV4JobProjection.class)))
            .thenReturn(Optional.of(jobEntity));

        Mockito.when(jobEntity.isV4()).thenReturn(false);

        Assertions.assertThat(this.jobPersistenceService.isV4(UUID.randomUUID().toString())).isFalse();
    }

    /**
     * If v4 is true in db then return true.
     *
     * @throws GenieNotFoundException if the job is not found
     */
    @Test
    public void v4JobTrueReturnsTrue() throws GenieNotFoundException {
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        Mockito
            .when(this.jobRepository.findByUniqueId(Mockito.anyString(), Mockito.eq(IsV4JobProjection.class)))
            .thenReturn(Optional.of(jobEntity));

        Mockito.when(jobEntity.isV4()).thenReturn(true);

        Assertions.assertThat(this.jobPersistenceService.isV4(UUID.randomUUID().toString())).isTrue();
    }

    /**
     * If v4 job is not found in the db then throw a Exception.
     */
    @Test
    public void v4JobNotFoundThrowsException() {
        Mockito
            .when(this.jobRepository.findByUniqueId(Mockito.anyString(), Mockito.eq(IsV4JobProjection.class)))
            .thenReturn(Optional.empty());

        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.jobPersistenceService.isV4(UUID.randomUUID().toString()));
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

        Assertions
            .assertThatExceptionOfType(GenieJobNotFoundException.class)
            .isThrownBy(
                () -> this.jobPersistenceService.claimJob(
                    UUID.randomUUID().toString(),
                    Mockito.mock(AgentClientMetadata.class)
                )
            );

        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        final String id = UUID.randomUUID().toString();
        Mockito
            .when(this.jobRepository.findByUniqueId(id))
            .thenReturn(Optional.of(jobEntity));

        Mockito.when(jobEntity.isClaimed()).thenReturn(true);

        Assertions
            .assertThatExceptionOfType(GenieJobAlreadyClaimedException.class)
            .isThrownBy(() -> this.jobPersistenceService.claimJob(id, Mockito.mock(AgentClientMetadata.class)));

        Mockito.when(jobEntity.isClaimed()).thenReturn(false);
        Mockito.when(jobEntity.getStatus()).thenReturn(JobStatus.INVALID.name());

        Assertions
            .assertThatExceptionOfType(GenieInvalidStatusException.class)
            .isThrownBy(() -> this.jobPersistenceService.claimJob(id, Mockito.mock(AgentClientMetadata.class)));
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
        Mockito.when(jobEntity.getStatus()).thenReturn(JobStatus.RESOLVED.name());

        final String agentHostname = UUID.randomUUID().toString();
        Mockito.when(agentClientMetadata.getHostname()).thenReturn(Optional.of(agentHostname));
        final String agentVersion = UUID.randomUUID().toString();
        Mockito.when(agentClientMetadata.getVersion()).thenReturn(Optional.of(agentVersion));
        final int agentPid = 238;
        Mockito.when(agentClientMetadata.getPid()).thenReturn(Optional.of(agentPid));

        this.jobPersistenceService.claimJob(id, agentClientMetadata);

        Mockito.verify(jobEntity, Mockito.times(1)).setClaimed(true);
        Mockito.verify(jobEntity, Mockito.times(1)).setStatus(JobStatus.CLAIMED.name());
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
        Assertions
            .assertThatExceptionOfType(GenieInvalidStatusException.class)
            .isThrownBy(() -> this.jobPersistenceService.updateJobStatus(
                id,
                JobStatus.CLAIMED,
                JobStatus.CLAIMED,
                null)
            );

        Mockito
            .when(this.jobRepository.findByUniqueId(id))
            .thenReturn(Optional.empty());

        Assertions
            .assertThatExceptionOfType(GenieJobNotFoundException.class)
            .isThrownBy(() -> this.jobPersistenceService.updateJobStatus(
                id,
                JobStatus.CLAIMED,
                JobStatus.INIT,
                null
                )
            );

        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        Mockito
            .when(this.jobRepository.findByUniqueId(id))
            .thenReturn(Optional.of(jobEntity));

        Mockito.when(jobEntity.getStatus()).thenReturn(JobStatus.INIT.name());

        Assertions
            .assertThatExceptionOfType(GenieInvalidStatusException.class)
            .isThrownBy(
                () -> this.jobPersistenceService.updateJobStatus(
                    id,
                    JobStatus.CLAIMED,
                    JobStatus.INIT,
                    null
                )
            );
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
        Mockito.when(jobEntity.getStatus()).thenReturn(JobStatus.INIT.name());

        this.jobPersistenceService.updateJobStatus(id, JobStatus.INIT, JobStatus.RUNNING, newStatusMessage);

        Mockito.verify(jobEntity, Mockito.times(1)).setStatus(JobStatus.RUNNING.name());
        Mockito.verify(jobEntity, Mockito.times(1)).setStatusMsg(newStatusMessage);
        Mockito.verify(jobEntity, Mockito.times(1)).setStarted(Mockito.any(Instant.class));

        final String finalStatusMessage = UUID.randomUUID().toString();
        Mockito.when(jobEntity.getStatus()).thenReturn(JobStatus.RUNNING.name());
        Mockito.when(jobEntity.getStarted()).thenReturn(Optional.of(Instant.now()));
        this.jobPersistenceService.updateJobStatus(id, JobStatus.RUNNING, JobStatus.SUCCEEDED, finalStatusMessage);

        Mockito.verify(jobEntity, Mockito.times(1)).setStatus(JobStatus.SUCCEEDED.name());
        Mockito.verify(jobEntity, Mockito.times(1)).setStatusMsg(finalStatusMessage);
        Mockito.verify(jobEntity, Mockito.times(1)).setFinished(Mockito.any(Instant.class));
    }

    /**
     * Test {@link JpaJobPersistenceServiceImpl#getJobStatus(String)}.
     *
     * @throws GenieNotFoundException if the job doesn't exist
     */
    @Test
    public void testGetJobStatus() throws GenieNotFoundException {
        final String id = UUID.randomUUID().toString();
        final JobStatus status = JobStatus.RUNNING;

        Mockito
            .when(this.jobRepository.findByUniqueId(id, StatusProjection.class))
            .thenReturn(Optional.empty())
            .thenReturn(Optional.of(status::name));

        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.jobPersistenceService.getJobStatus(id));
        Assertions.assertThat(this.jobPersistenceService.getJobStatus(id)).isEqualByComparingTo(status);
    }

    /**
     * Test {@link JpaJobPersistenceServiceImpl#getFinishedJob(String)}.
     */
    @Test
    public void testGetFinishedJobNonExisting() {
        final String id = UUID.randomUUID().toString();

        Mockito
            .when(this.jobRepository.findByUniqueId(id, FinishedJobProjection.class))
            .thenReturn(Optional.empty());

        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.jobPersistenceService.getFinishedJob(id));
    }

    // TODO: JpaJobPersistenceServiceImpl#getFinishedJob(String) for job in non-final state
    // TODO: JpaJobPersistenceServiceImpl#getFinishedJob(String) successful

    /**
     * Make sure when a job can't be found when {@link JpaJobPersistenceServiceImpl#isApiJob(String)} is called that
     * the expected exception is thrown.
     */
    @Test
    public void testIsApiJobNoJob() {
        final String id = UUID.randomUUID().toString();
        Mockito
            .when(this.jobRepository.findByUniqueId(id, JobApiProjection.class))
            .thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.jobPersistenceService.isApiJob(id));
    }
}
