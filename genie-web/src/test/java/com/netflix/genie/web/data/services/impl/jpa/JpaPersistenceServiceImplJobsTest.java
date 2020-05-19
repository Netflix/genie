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
package com.netflix.genie.web.data.services.impl.jpa;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.ClusterCriteria;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobMetadata;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.UserResourcesSummary;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.external.dtos.v4.AgentClientMetadata;
import com.netflix.genie.common.external.dtos.v4.JobSpecification;
import com.netflix.genie.common.external.dtos.v4.JobStatus;
import com.netflix.genie.common.internal.exceptions.checked.GenieCheckedException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieInvalidStatusException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobAlreadyClaimedException;
import com.netflix.genie.web.data.services.impl.jpa.entities.ApplicationEntity;
import com.netflix.genie.web.data.services.impl.jpa.entities.ClusterEntity;
import com.netflix.genie.web.data.services.impl.jpa.entities.CommandEntity;
import com.netflix.genie.web.data.services.impl.jpa.entities.FileEntity;
import com.netflix.genie.web.data.services.impl.jpa.entities.JobEntity;
import com.netflix.genie.web.data.services.impl.jpa.entities.TagEntity;
import com.netflix.genie.web.data.services.impl.jpa.queries.aggregates.UserJobResourcesAggregate;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.AgentHostnameProjection;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.JobApiProjection;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.JobApplicationsProjection;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.JobClusterProjection;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.JobCommandProjection;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.JobProjection;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.StatusProjection;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.UniqueIdProjection;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.v4.FinishedJobProjection;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.v4.IsV4JobProjection;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.v4.JobSpecificationProjection;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.v4.V4JobRequestProjection;
import com.netflix.genie.web.data.services.impl.jpa.repositories.JpaApplicationRepository;
import com.netflix.genie.web.data.services.impl.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.web.data.services.impl.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.web.data.services.impl.jpa.repositories.JpaFileRepository;
import com.netflix.genie.web.data.services.impl.jpa.repositories.JpaJobRepository;
import com.netflix.genie.web.data.services.impl.jpa.repositories.JpaRepositories;
import com.netflix.genie.web.data.services.impl.jpa.repositories.JpaTagRepository;
import com.netflix.genie.web.dtos.ResolvedJob;
import com.netflix.genie.web.exceptions.checked.NotFoundException;
import com.netflix.genie.web.services.AttachmentService;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.dao.DuplicateKeyException;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Tests for the {@link JpaPersistenceServiceImpl} class focusing on the job related APIs.
 *
 * @author tgianos
 * @since 3.0.0
 */
class JpaPersistenceServiceImplJobsTest {
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
    private JpaFileRepository fileRepository;
    private JpaTagRepository tagRepository;

    private JpaPersistenceServiceImpl persistenceService;

    @BeforeEach
    void setup() {
        this.jobRepository = Mockito.mock(JpaJobRepository.class);
        this.applicationRepository = Mockito.mock(JpaApplicationRepository.class);
        this.clusterRepository = Mockito.mock(JpaClusterRepository.class);
        this.commandRepository = Mockito.mock(JpaCommandRepository.class);
        this.tagRepository = Mockito.mock(JpaTagRepository.class);
        this.fileRepository = Mockito.mock(JpaFileRepository.class);

        final JpaRepositories jpaRepositories = Mockito.mock(JpaRepositories.class);
        Mockito.when(jpaRepositories.getApplicationRepository()).thenReturn(this.applicationRepository);
        Mockito.when(jpaRepositories.getClusterRepository()).thenReturn(this.clusterRepository);
        Mockito.when(jpaRepositories.getCommandRepository()).thenReturn(this.commandRepository);
        Mockito.when(jpaRepositories.getJobRepository()).thenReturn(this.jobRepository);
        Mockito.when(jpaRepositories.getFileRepository()).thenReturn(this.fileRepository);
        Mockito.when(jpaRepositories.getTagRepository()).thenReturn(this.tagRepository);

        this.persistenceService = new JpaPersistenceServiceImpl(jpaRepositories, Mockito.mock(AttachmentService.class));
    }

    @Test
    void testCreateJob() throws GenieException {
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
            Lists.newArrayList(new ClusterCriteria(Sets.newHashSet("hi"))),
            Sets.newHashSet("bye")
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
            .withStatus(com.netflix.genie.common.dto.JobStatus.INIT)
            .withStatusMsg("Job is initializing")
            .withCommandArgs(JOB_1_COMMAND_ARGS)
            .build();

        final JobExecution execution = new JobExecution.Builder(UUID.randomUUID().toString()).build();

        final TagEntity fooTag = new TagEntity();
        fooTag.setTag("foo");
        Mockito.when(this.tagRepository.findByTag("foo")).thenReturn(Optional.of(fooTag));
        final TagEntity barTag = new TagEntity();
        barTag.setTag("bar");
        Mockito.when(this.tagRepository.findByTag("bar")).thenReturn(Optional.of(barTag));
        final FileEntity setupFileEntity = new FileEntity();
        setupFileEntity.setFile(setupFile);
        Mockito.when(this.fileRepository.findByFile(setupFile)).thenReturn(Optional.of(setupFileEntity));

        final ArgumentCaptor<JobEntity> argument = ArgumentCaptor.forClass(JobEntity.class);
        this.persistenceService.createJob(jobRequest, metadata, job, execution);
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

    @Test
    void testCreateJobWithNoId() {
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
                () -> this.persistenceService.createJob(
                    jobRequest,
                    Mockito.mock(JobMetadata.class),
                    Mockito.mock(Job.class),
                    Mockito.mock(JobExecution.class)
                )
            );
    }

    @Test
    void testCreateJobAlreadyExists() {
        final JobRequest jobRequest = new JobRequest.Builder(
            JOB_1_NAME,
            JOB_1_USER,
            JOB_1_VERSION,
            Lists.newArrayList(new ClusterCriteria(Sets.newHashSet("hi"))),
            Sets.newHashSet("bye")
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
            .withStatus(com.netflix.genie.common.dto.JobStatus.INIT)
            .withStatusMsg("Job is initializing")
            .withCommandArgs(JOB_1_COMMAND_ARGS)
            .build();

        final JobExecution execution = new JobExecution.Builder(UUID.randomUUID().toString()).build();

        Mockito
            .when(this.tagRepository.findByTag(Mockito.anyString()))
            .thenReturn(Optional.of(new TagEntity(UUID.randomUUID().toString())));
        Mockito
            .when(this.fileRepository.findByFile(Mockito.anyString()))
            .thenReturn(Optional.of(new FileEntity(UUID.randomUUID().toString())));
        Mockito
            .when(this.jobRepository.save(Mockito.any(JobEntity.class)))
            .thenThrow(new DuplicateKeyException("Duplicate Key"));
        Assertions
            .assertThatExceptionOfType(GenieConflictException.class)
            .isThrownBy(() -> this.persistenceService.createJob(jobRequest, metadata, job, execution));
    }

    @Test
    void testUpdateJobStatusDoesNotExist() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jobRepository.findByUniqueId(Mockito.eq(id))).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(
                () -> this.persistenceService.updateJobStatus(
                    id,
                    com.netflix.genie.common.dto.JobStatus.RUNNING,
                    JOB_1_STATUS_MSG
                )
            );
    }

    @Test
    void testUpdateJobStatusForStatusInit() throws GenieException {
        final String id = UUID.randomUUID().toString();
        final JobEntity jobEntity = new JobEntity();
        jobEntity.setStatus(JobStatus.ACCEPTED.name());

        Mockito.when(this.jobRepository.findByUniqueId(Mockito.eq(id))).thenReturn(Optional.of(jobEntity));
        this.persistenceService.updateJobStatus(
            id,
            com.netflix.genie.common.dto.JobStatus.INIT,
            JOB_1_STATUS_MSG
        );

        Assertions.assertThat(jobEntity.getStatus()).isEqualTo(JobStatus.INIT.name());
        Assertions.assertThat(jobEntity.getStatusMsg()).isPresent().contains(JOB_1_STATUS_MSG);
        Assertions.assertThat(jobEntity.getFinished()).isNotPresent();

        // Started should be null as the status is being set to INIT
        Assertions.assertThat(jobEntity.getStarted()).isNotPresent();
    }

    @Test
    void testUpdateJobStatusForStatusRunning() throws GenieException {
        final String id = UUID.randomUUID().toString();
        final JobEntity jobEntity = new JobEntity();
        jobEntity.setStatus(JobStatus.INIT.name());

        Mockito.when(this.jobRepository.findByUniqueId(Mockito.eq(id))).thenReturn(Optional.of(jobEntity));
        this.persistenceService.updateJobStatus(
            id,
            com.netflix.genie.common.dto.JobStatus.RUNNING,
            JOB_1_STATUS_MSG
        );

        Assertions.assertThat(jobEntity.getStatus()).isEqualTo(JobStatus.RUNNING.name());
        Assertions.assertThat(jobEntity.getStatusMsg()).isPresent().contains(JOB_1_STATUS_MSG);

        Assertions.assertThat(jobEntity.getFinished()).isNotPresent();

        // Started should not be null as the status is being set to RUNNING
        Assertions.assertThat(jobEntity.getStarted()).isPresent();
    }

    @Test
    void testUpdateJobStatusForStatusFailed() throws GenieException {
        final String id = UUID.randomUUID().toString();
        final JobEntity jobEntity = new JobEntity();
        jobEntity.setStatus(JobStatus.INIT.name());

        Mockito.when(this.jobRepository.findByUniqueId(Mockito.eq(id))).thenReturn(Optional.of(jobEntity));
        this.persistenceService.updateJobStatus(
            id,
            com.netflix.genie.common.dto.JobStatus.FAILED,
            JOB_1_STATUS_MSG
        );

        Assertions.assertThat(jobEntity.getStatus()).isEqualTo(JobStatus.FAILED.name());
        Assertions.assertThat(jobEntity.getStatusMsg()).isPresent().contains(JOB_1_STATUS_MSG);
        Assertions.assertThat(jobEntity.getFinished()).isNotPresent();

        // Started should not be set as the status is being set to FAILED
        Assertions.assertThat(jobEntity.getStarted()).isNotPresent();
    }

    @Test
    void testUpdateJobStatusForStatusKilled() throws GenieException {
        final String id = UUID.randomUUID().toString();
        final JobEntity jobEntity = new JobEntity();
        jobEntity.setStarted(Instant.EPOCH);
        jobEntity.setStatus(JobStatus.RUNNING.name());

        Mockito.when(this.jobRepository.findByUniqueId(Mockito.eq(id))).thenReturn(Optional.of(jobEntity));
        this.persistenceService.updateJobStatus(
            id,
            com.netflix.genie.common.dto.JobStatus.KILLED,
            JOB_1_STATUS_MSG
        );

        Assertions.assertThat(jobEntity.getStatus()).isEqualTo(JobStatus.KILLED.name());
        Assertions.assertThat(jobEntity.getStatusMsg()).isPresent().contains(JOB_1_STATUS_MSG);
        Assertions.assertThat(jobEntity.getFinished()).isPresent();
    }

    @Test
    void testUpdateJobStatusForStatusSucceeded() throws GenieException {
        final String id = UUID.randomUUID().toString();
        final JobEntity jobEntity = new JobEntity();
        jobEntity.setStarted(Instant.EPOCH);
        jobEntity.setStatus(JobStatus.RUNNING.name());

        Mockito.when(this.jobRepository.findByUniqueId(Mockito.eq(id))).thenReturn(Optional.of(jobEntity));
        this.persistenceService.updateJobStatus(
            id,
            com.netflix.genie.common.dto.JobStatus.SUCCEEDED,
            JOB_1_STATUS_MSG
        );

        Assertions.assertThat(jobEntity.getStatus()).isEqualTo(JobStatus.SUCCEEDED.name());
        Assertions.assertThat(jobEntity.getStatusMsg()).isPresent().contains(JOB_1_STATUS_MSG);
        Assertions.assertThat(jobEntity.getFinished()).isPresent();
    }

    @Test
    void testUpdateJobStatusFinishedTimeForSuccess() throws GenieException {
        final String id = UUID.randomUUID().toString();
        final JobEntity jobEntity = new JobEntity();
        jobEntity.setStatus(JobStatus.RUNNING.name());
        jobEntity.setStarted(Instant.EPOCH);

        Mockito.when(this.jobRepository.findByUniqueId(id)).thenReturn(Optional.of(jobEntity));
        this.persistenceService.updateJobStatus(
            id,
            com.netflix.genie.common.dto.JobStatus.SUCCEEDED,
            JOB_1_STATUS_MSG
        );

        Assertions.assertThat(jobEntity.getFinished()).isPresent();
    }

    @Test
    void cantUpdateRuntimeForNonExistentJob() {
        Mockito.when(this.jobRepository.findByUniqueId(Mockito.eq(JOB_1_ID))).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(
                () -> this.persistenceService.updateJobWithRuntimeEnvironment(
                    JOB_1_ID,
                    "foo",
                    "bar",
                    Lists.newArrayList(),
                    1
                )
            );
    }

    @Test
    void cantUpdateRuntimeForNonExistentCluster() {
        final String id = UUID.randomUUID().toString();
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        Mockito.when(this.jobRepository.findByUniqueId(JOB_1_ID)).thenReturn(Optional.of(jobEntity));
        Mockito.when(this.clusterRepository.findByUniqueId(Mockito.eq(id))).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(
                () -> this.persistenceService.updateJobWithRuntimeEnvironment(
                    JOB_1_ID,
                    id,
                    "bar",
                    Lists.newArrayList(),
                    1
                )
            );
    }

    @Test
    void cantUpdateRuntimeForNonExistentCommand() {
        final String clusterId = UUID.randomUUID().toString();
        final String commandId = UUID.randomUUID().toString();
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        Mockito.when(this.jobRepository.findByUniqueId(JOB_1_ID)).thenReturn(Optional.of(jobEntity));
        Mockito.when(this.clusterRepository.findByUniqueId(clusterId)).thenReturn(Optional.of(new ClusterEntity()));
        Mockito.when(this.commandRepository.findByUniqueId(commandId)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(
                () -> this.persistenceService.updateJobWithRuntimeEnvironment(
                    JOB_1_ID,
                    clusterId,
                    commandId,
                    Lists.newArrayList(),
                    1
                )
            );
    }

    @Test
    void cantUpdateRuntimeForNonExistentApplication() {
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
                () -> this.persistenceService.updateJobWithRuntimeEnvironment(
                    JOB_1_ID,
                    clusterId,
                    commandId,
                    Lists.newArrayList(applicationId1, applicationId2),
                    1
                )
            );
    }

    @Test
    void cantFindJobToUpdateRunningInformationFor() {
        final String id = UUID.randomUUID().toString();

        Mockito.when(this.jobRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(
                () -> this.persistenceService.setJobRunningInformation(
                    id,
                    1,
                    1,
                    Instant.now()
                )
            );
    }

    @Test
    void canUpdateJobRunningInformation() throws GenieException {
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

        this.persistenceService.setJobRunningInformation(id, processId, checkDelay, timeout);
        Mockito.verify(jobEntity, Mockito.times(1)).setTimeoutUsed(timeoutUsed);
        Mockito.verify(jobEntity, Mockito.times(1)).setProcessId(processId);
        Mockito.verify(jobEntity, Mockito.times(1)).setCheckDelay(checkDelay);
    }

    @Test
    void cantUpdateJobRunningInformationIfNoJob() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jobRepository.findByUniqueId(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(
                () -> this.persistenceService.setJobRunningInformation(
                    id,
                    212,
                    308L,
                    Instant.now()
                )
            );
    }

    @Test
    void testSetExitCodeJobDoesNotExist() {
        Mockito.when(this.jobRepository.findByUniqueId(JOB_1_ID)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(
                () -> this.persistenceService.setJobCompletionInformation(
                    JOB_1_ID,
                    0,
                    com.netflix.genie.common.dto.JobStatus.FAILED,
                    UUID.randomUUID().toString(),
                    null,
                    null
                )
            );
    }

    @Test
    void cantUpdateJobMetadataIfNotExists() {
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        Mockito.when(jobEntity.getStatus()).thenReturn(JobStatus.FAILED.name());
        Mockito.when(this.jobRepository.findByUniqueId(JOB_1_ID)).thenReturn(Optional.empty());

        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(
                () -> this.persistenceService.setJobCompletionInformation(
                    JOB_1_ID,
                    0,
                    com.netflix.genie.common.dto.JobStatus.FAILED,
                    "k",
                    100L,
                    1L
                )
            );
    }

    @Test
    void wontUpdateJobMetadataIfNoSizes() throws GenieException {
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        Mockito.when(jobEntity.getStatus()).thenReturn(JobStatus.FAILED.name());
        Mockito.when(this.jobRepository.findByUniqueId(JOB_1_ID)).thenReturn(Optional.of(jobEntity));

        this.persistenceService.setJobCompletionInformation(
            JOB_1_ID,
            0,
            com.netflix.genie.common.dto.JobStatus.FAILED,
            "k",
            null,
            null
        );
        Mockito.verify(jobEntity, Mockito.times(1)).setStdOutSize(null);
        Mockito.verify(jobEntity, Mockito.times(1)).setStdErrSize(null);
    }

    @Test
    void willUpdateJobMetadataIfOneSize() throws GenieException {
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        Mockito.when(jobEntity.getStatus()).thenReturn(JobStatus.FAILED.name());
        Mockito.when(this.jobRepository.findByUniqueId(JOB_1_ID)).thenReturn(Optional.of(jobEntity));
        Mockito.when(jobEntity.getExitCode()).thenReturn(Optional.of(1));

        this.persistenceService.setJobCompletionInformation(
            JOB_1_ID,
            0,
            com.netflix.genie.common.dto.JobStatus.FAILED,
            "k",
            null,
            100L
        );
        Mockito.verify(jobEntity, Mockito.times(1)).setStdErrSize(100L);
        Mockito.verify(jobEntity, Mockito.times(1)).setStdOutSize(null);
    }

    @Test
    void noJobRequestFoundReturnsEmptyOptional() {
        Mockito
            .when(this.jobRepository.findByUniqueId(Mockito.anyString(), Mockito.eq(V4JobRequestProjection.class)))
            .thenReturn(Optional.empty());

        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.persistenceService.getJobRequest(UUID.randomUUID().toString()));
    }

    @Test
    void noJobUnableToSaveResolvedJob() {
        Mockito
            .when(this.jobRepository.findByUniqueId(Mockito.anyString()))
            .thenReturn(Optional.empty());

        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(
                () -> this.persistenceService.saveResolvedJob(
                    UUID.randomUUID().toString(),
                    Mockito.mock(ResolvedJob.class)
                )
            );
    }

    @Test
    void jobAlreadyResolvedDoesNotResolvedInformationAgain() throws GenieCheckedException {
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        Mockito
            .when(this.jobRepository.findByUniqueId(Mockito.anyString()))
            .thenReturn(Optional.of(jobEntity));

        Mockito.when(jobEntity.isResolved()).thenReturn(true);

        this.persistenceService.saveResolvedJob(UUID.randomUUID().toString(), Mockito.mock(ResolvedJob.class));

        Mockito.verify(jobEntity, Mockito.never()).setCluster(Mockito.any(ClusterEntity.class));
    }

    @Test
    void jobAlreadyTerminalDoesNotSaveResolvedJob() throws GenieCheckedException {
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        Mockito
            .when(this.jobRepository.findByUniqueId(Mockito.anyString()))
            .thenReturn(Optional.of(jobEntity));

        Mockito.when(jobEntity.isResolved()).thenReturn(false);
        Mockito.when(jobEntity.getStatus()).thenReturn(JobStatus.KILLED.name());

        this.persistenceService.saveResolvedJob(UUID.randomUUID().toString(), Mockito.mock(ResolvedJob.class));

        Mockito.verify(jobEntity, Mockito.never()).setCluster(Mockito.any(ClusterEntity.class));
    }

    @Test
    void noResourceToSaveForResolvedJob() {
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
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.persistenceService.saveResolvedJob(jobId, resolvedJob));
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.persistenceService.saveResolvedJob(jobId, resolvedJob));
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.persistenceService.saveResolvedJob(jobId, resolvedJob));
    }

    @Test
    void noJobUnableToGetJobSpecification() {
        Mockito
            .when(this.jobRepository.findByUniqueId(Mockito.anyString(), Mockito.eq(JobSpecificationProjection.class)))
            .thenReturn(Optional.empty());

        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.persistenceService.getJobSpecification(UUID.randomUUID().toString()));
    }

    @Test
    void unresolvedJobReturnsEmptyJobSpecificationOptional() throws GenieCheckedException {
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        Mockito
            .when(this.jobRepository.findByUniqueId(Mockito.anyString(), Mockito.eq(JobSpecificationProjection.class)))
            .thenReturn(Optional.of(jobEntity));

        Mockito.when(jobEntity.isResolved()).thenReturn(false);

        Assertions
            .assertThat(this.persistenceService.getJobSpecification(UUID.randomUUID().toString()))
            .isNotPresent();
    }

    @Test
    void noJobUnableToGetV4() {
        Mockito
            .when(this.jobRepository.findByUniqueId(Mockito.anyString(), Mockito.eq(IsV4JobProjection.class)))
            .thenReturn(Optional.empty());

        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.persistenceService.isV4(UUID.randomUUID().toString()));
    }

    @Test
    void v4JobFalseReturnsFalse() throws GenieCheckedException {
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        Mockito
            .when(this.jobRepository.findByUniqueId(Mockito.anyString(), Mockito.eq(IsV4JobProjection.class)))
            .thenReturn(Optional.of(jobEntity));

        Mockito.when(jobEntity.isV4()).thenReturn(false);

        Assertions.assertThat(this.persistenceService.isV4(UUID.randomUUID().toString())).isFalse();
    }

    @Test
    void v4JobTrueReturnsTrue() throws GenieCheckedException {
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        Mockito
            .when(this.jobRepository.findByUniqueId(Mockito.anyString(), Mockito.eq(IsV4JobProjection.class)))
            .thenReturn(Optional.of(jobEntity));

        Mockito.when(jobEntity.isV4()).thenReturn(true);

        Assertions.assertThat(this.persistenceService.isV4(UUID.randomUUID().toString())).isTrue();
    }

    @Test
    void v4JobNotFoundThrowsException() {
        Mockito
            .when(this.jobRepository.findByUniqueId(Mockito.anyString(), Mockito.eq(IsV4JobProjection.class)))
            .thenReturn(Optional.empty());

        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.persistenceService.isV4(UUID.randomUUID().toString()));
    }

    @Test
    void testClaimJobErrorCases() {
        Mockito
            .when(this.jobRepository.findByUniqueId(Mockito.anyString()))
            .thenReturn(Optional.empty());

        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(
                () -> this.persistenceService.claimJob(
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
            .isThrownBy(() -> this.persistenceService.claimJob(id, Mockito.mock(AgentClientMetadata.class)));

        Mockito.when(jobEntity.isClaimed()).thenReturn(false);
        Mockito.when(jobEntity.getStatus()).thenReturn(JobStatus.INVALID.name());

        Assertions
            .assertThatExceptionOfType(GenieInvalidStatusException.class)
            .isThrownBy(() -> this.persistenceService.claimJob(id, Mockito.mock(AgentClientMetadata.class)));
    }

    @Test
    void testClaimJobValidBehavior() throws GenieCheckedException {
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

        this.persistenceService.claimJob(id, agentClientMetadata);

        Mockito.verify(jobEntity, Mockito.times(1)).setClaimed(true);
        Mockito.verify(jobEntity, Mockito.times(1)).setStatus(JobStatus.CLAIMED.name());
        Mockito.verify(jobEntity, Mockito.times(1)).setAgentHostname(agentHostname);
        Mockito.verify(jobEntity, Mockito.times(1)).setAgentVersion(agentVersion);
        Mockito.verify(jobEntity, Mockito.times(1)).setAgentPid(agentPid);
    }

    @Test
    void testUpdateJobStatusErrorCases() {
        final String id = UUID.randomUUID().toString();
        Assertions
            .assertThatExceptionOfType(GenieInvalidStatusException.class)
            .isThrownBy(() -> this.persistenceService.updateJobStatus(
                id,
                JobStatus.CLAIMED,
                JobStatus.CLAIMED,
                null)
            );

        Mockito
            .when(this.jobRepository.findByUniqueId(id))
            .thenReturn(Optional.empty());

        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.persistenceService.updateJobStatus(
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
                () -> this.persistenceService.updateJobStatus(
                    id,
                    JobStatus.CLAIMED,
                    JobStatus.INIT,
                    null
                )
            );
    }

    @Test
    void testUpdateJobValidBehavior() throws GenieCheckedException {
        final String id = UUID.randomUUID().toString();
        final String newStatusMessage = UUID.randomUUID().toString();
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);

        Mockito.when(this.jobRepository.findByUniqueId(id)).thenReturn(Optional.of(jobEntity));
        Mockito.when(jobEntity.getStatus()).thenReturn(JobStatus.INIT.name());

        this.persistenceService.updateJobStatus(id, JobStatus.INIT, JobStatus.RUNNING, newStatusMessage);

        Mockito.verify(jobEntity, Mockito.times(1)).setStatus(JobStatus.RUNNING.name());
        Mockito.verify(jobEntity, Mockito.times(1)).setStatusMsg(newStatusMessage);
        Mockito.verify(jobEntity, Mockito.times(1)).setStarted(Mockito.any(Instant.class));

        final String finalStatusMessage = UUID.randomUUID().toString();
        Mockito.when(jobEntity.getStatus()).thenReturn(JobStatus.RUNNING.name());
        Mockito.when(jobEntity.getStarted()).thenReturn(Optional.of(Instant.now()));
        this.persistenceService.updateJobStatus(id, JobStatus.RUNNING, JobStatus.SUCCEEDED, finalStatusMessage);

        Mockito.verify(jobEntity, Mockito.times(1)).setStatus(JobStatus.SUCCEEDED.name());
        Mockito.verify(jobEntity, Mockito.times(1)).setStatusMsg(finalStatusMessage);
        Mockito.verify(jobEntity, Mockito.times(1)).setFinished(Mockito.any(Instant.class));
    }

    @Test
    void testGetJobStatus() throws GenieCheckedException {
        final String id = UUID.randomUUID().toString();
        final JobStatus status = JobStatus.RUNNING;

        Mockito
            .when(this.jobRepository.findByUniqueId(id, StatusProjection.class))
            .thenReturn(Optional.empty())
            .thenReturn(Optional.of(status::name));

        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.persistenceService.getJobStatus(id));
        Assertions.assertThat(this.persistenceService.getJobStatus(id)).isEqualByComparingTo(status);
    }

    @Test
    void testGetFinishedJobNonExisting() {
        final String id = UUID.randomUUID().toString();

        Mockito
            .when(this.jobRepository.findByUniqueId(id, FinishedJobProjection.class))
            .thenReturn(Optional.empty());

        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.persistenceService.getFinishedJob(id));
    }

    // TODO: JpaJobPersistenceServiceImpl#getFinishedJob(String) for job in non-final state
    // TODO: JpaJobPersistenceServiceImpl#getFinishedJob(String) successful

    @Test
    void testIsApiJobNoJob() {
        final String id = UUID.randomUUID().toString();
        Mockito
            .when(this.jobRepository.findByUniqueId(id, JobApiProjection.class))
            .thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.persistenceService.isApiJob(id));
    }

    @Test
    void cantGetJobIfDoesNotExist() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jobRepository.findByUniqueId(id, JobProjection.class)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.persistenceService.getJob(id));
    }

    @Test
    void canGetJob() throws GenieException {
        final JobEntity jobEntity = new JobEntity();
        final String id = UUID.randomUUID().toString();
        jobEntity.setStatus(com.netflix.genie.common.dto.JobStatus.RUNNING.name());
        jobEntity.setUniqueId(id);
        Mockito.when(this.jobRepository.findByUniqueId(id, JobProjection.class)).thenReturn(Optional.of(jobEntity));
        final Job returnedJob = this.persistenceService.getJob(id);
        Mockito
            .verify(this.jobRepository, Mockito.times(1))
            .findByUniqueId(id, JobProjection.class);
        Assertions.assertThat(returnedJob.getId()).isPresent().contains(id);
    }

    @Test
    void cantGetJobClusterIfJobDoesNotExist() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jobRepository.findByUniqueId(id, JobClusterProjection.class)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.persistenceService.getJobCluster(id));
    }

    @Test
    void cantGetJobClusterIfClusterDoesNotExist() {
        final String id = UUID.randomUUID().toString();
        final JobEntity entity = Mockito.mock(JobEntity.class);
        Mockito.when(entity.getCluster()).thenReturn(Optional.empty());
        Mockito.when(this.jobRepository.findByUniqueId(id, JobClusterProjection.class)).thenReturn(Optional.of(entity));
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.persistenceService.getJobCluster(id));
    }

    @Test
    void cantGetJobCommandIfJobDoesNotExist() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jobRepository.findByUniqueId(id, JobCommandProjection.class)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.persistenceService.getJobCommand(id));
    }

    @Test
    void cantGetJobCommandIfCommandDoesNotExist() {
        final String id = UUID.randomUUID().toString();
        final JobEntity entity = Mockito.mock(JobEntity.class);
        Mockito.when(entity.getCommand()).thenReturn(Optional.empty());
        Mockito.when(this.jobRepository.findByUniqueId(id, JobCommandProjection.class)).thenReturn(Optional.of(entity));
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.persistenceService.getJobCommand(id));
    }

    @Test
    void cantGetJobApplicationsIfJobDoesNotExist() {
        final String id = UUID.randomUUID().toString();
        Mockito
            .when(this.jobRepository.findByUniqueId(id, JobApplicationsProjection.class))
            .thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.persistenceService.getJobApplications(id));
    }

    @Test
    void canGetJobApplications() throws GenieCheckedException {
        final String id = UUID.randomUUID().toString();
        final JobEntity entity = Mockito.mock(JobEntity.class);
        Mockito.when(entity.getApplications()).thenReturn(Lists.newArrayList());
        Mockito
            .when(this.jobRepository.findByUniqueId(id, JobApplicationsProjection.class))
            .thenReturn(Optional.of(entity));
        Assertions.assertThat(this.persistenceService.getJobApplications(id)).isEmpty();
    }

    @Test
    void cantGetJobHostIfNoJobExecution() {
        final String jobId = UUID.randomUUID().toString();
        Mockito
            .when(this.jobRepository.findByUniqueId(jobId, AgentHostnameProjection.class))
            .thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.persistenceService.getJobHost(jobId));
    }

    @Test
    void canGetJobHost() throws GenieCheckedException {
        final String jobId = UUID.randomUUID().toString();
        final String hostName = UUID.randomUUID().toString();
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        Mockito.when(jobEntity.getAgentHostname()).thenReturn(Optional.of(hostName));
        Mockito
            .when(this.jobRepository.findByUniqueId(jobId, AgentHostnameProjection.class))
            .thenReturn(Optional.of(jobEntity));

        Assertions.assertThat(this.persistenceService.getJobHost(jobId)).isEqualTo(hostName);
    }

    @Test
    void canGetUserResourceSummariesNoRecords() {
        Mockito
            .when(this.jobRepository.getUserJobResourcesAggregates())
            .thenReturn(Sets.newHashSet());
        Assertions.assertThat(this.persistenceService.getUserResourcesSummaries()).isEmpty();
    }

    @Test
    void canGetUserResourceSummaries() {
        final UserJobResourcesAggregate p1 = Mockito.mock(UserJobResourcesAggregate.class);
        final UserJobResourcesAggregate p2 = Mockito.mock(UserJobResourcesAggregate.class);

        Mockito.when(p1.getUser()).thenReturn("foo");
        Mockito.when(p1.getRunningJobsCount()).thenReturn(3L);
        Mockito.when(p1.getUsedMemory()).thenReturn(1024L);

        Mockito.when(p2.getUser()).thenReturn("bar");
        Mockito.when(p2.getRunningJobsCount()).thenReturn(5L);
        Mockito.when(p2.getUsedMemory()).thenReturn(2048L);

        Mockito
            .when(this.jobRepository.getUserJobResourcesAggregates())
            .thenReturn(Sets.newHashSet(p1, p2));

        final HashMap<String, UserResourcesSummary> expectedMap = Maps.newHashMap();
        expectedMap.put("foo", new UserResourcesSummary("foo", 3, 1024));
        expectedMap.put("bar", new UserResourcesSummary("bar", 5, 2048));
        Assertions.assertThat(this.persistenceService.getUserResourcesSummaries()).isEqualTo(expectedMap);
    }


    @Test
    void canGetAllocatedMemoryOnHost() {
        final String hostname = UUID.randomUUID().toString();
        final long totalMemory = 213_382L;

        Mockito
            .when(this.jobRepository.getTotalMemoryUsedOnHost(hostname, JpaPersistenceServiceImpl.ACTIVE_STATUS_SET))
            .thenReturn(totalMemory);

        Assertions.assertThat(this.persistenceService.getAllocatedMemoryOnHost(hostname)).isEqualTo(totalMemory);
    }

    @Test
    void canGetUsedMemoryOnHost() {
        final String hostname = UUID.randomUUID().toString();
        final long totalMemory = 213_328L;

        Mockito
            .when(this.jobRepository.getTotalMemoryUsedOnHost(hostname, JpaPersistenceServiceImpl.USING_MEMORY_JOB_SET))
            .thenReturn(totalMemory);

        Assertions.assertThat(this.persistenceService.getUsedMemoryOnHost(hostname)).isEqualTo(totalMemory);
    }

    @Test
    void canGetCountActiveJobsOnHost() {
        final String hostname = UUID.randomUUID().toString();
        final long totalJobs = 32L;

        Mockito
            .when(
                this.jobRepository.countByAgentHostnameAndStatusIn(
                    hostname,
                    JpaPersistenceServiceImpl.ACTIVE_STATUS_SET
                )
            )
            .thenReturn(totalJobs);

        Assertions.assertThat(this.persistenceService.getActiveJobCountOnHost(hostname)).isEqualTo(totalJobs);
    }

    @Test
    void canGetActiveAgentJobs() {
        final String jobId1 = UUID.randomUUID().toString();
        final String jobId2 = UUID.randomUUID().toString();

        final UniqueIdProjection job1 = Mockito.mock(UniqueIdProjection.class);
        final UniqueIdProjection job2 = Mockito.mock(UniqueIdProjection.class);

        Mockito.when(job1.getUniqueId()).thenReturn(jobId1);
        Mockito.when(job2.getUniqueId()).thenReturn(jobId2);

        Mockito
            .when(this.jobRepository.getAgentJobIdsWithStatusIn(JpaPersistenceServiceImpl.ACTIVE_STATUS_SET))
            .thenReturn(Sets.newHashSet(job1, job2));

        Assertions
            .assertThat(this.persistenceService.getActiveAgentJobs())
            .isEqualTo(Sets.newHashSet(jobId1, jobId2));
    }

    @Test
    void canGetActiveAgentJobsWhenEmpty() {

        Mockito
            .when(this.jobRepository.getAgentJobIdsWithStatusIn(JpaPersistenceServiceImpl.ACTIVE_STATUS_SET))
            .thenReturn(Sets.newHashSet());

        Assertions
            .assertThat(this.persistenceService.getActiveAgentJobs())
            .isEqualTo(Sets.newHashSet());
    }

    @Test
    void canGetUnclaimedAgentJobs() {
        final String jobId1 = UUID.randomUUID().toString();
        final String jobId2 = UUID.randomUUID().toString();

        final UniqueIdProjection job1 = Mockito.mock(UniqueIdProjection.class);
        final UniqueIdProjection job2 = Mockito.mock(UniqueIdProjection.class);

        Mockito.when(job1.getUniqueId()).thenReturn(jobId1);
        Mockito.when(job2.getUniqueId()).thenReturn(jobId2);

        Mockito
            .when(this.jobRepository.getAgentJobIdsWithStatusIn(JpaPersistenceServiceImpl.UNCLAIMED_STATUS_SET))
            .thenReturn(Sets.newHashSet(job1, job2));

        Assertions
            .assertThat(this.persistenceService.getUnclaimedAgentJobs())
            .isEqualTo(Sets.newHashSet(jobId1, jobId2));
    }

    @Test
    void canGetUnclaimedAgentJobsWhenEmpty() {

        Mockito
            .when(this.jobRepository.getAgentJobIdsWithStatusIn(JpaPersistenceServiceImpl.UNCLAIMED_STATUS_SET))
            .thenReturn(Sets.newHashSet());

        Assertions
            .assertThat(this.persistenceService.getUnclaimedAgentJobs())
            .isEqualTo(Sets.newHashSet());
    }
}
