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
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.UserResourcesSummary;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.external.dtos.v4.AgentClientMetadata;
import com.netflix.genie.common.external.dtos.v4.ArchiveStatus;
import com.netflix.genie.common.external.dtos.v4.JobSpecification;
import com.netflix.genie.common.external.dtos.v4.JobStatus;
import com.netflix.genie.common.internal.exceptions.checked.GenieCheckedException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieInvalidStatusException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobAlreadyClaimedException;
import com.netflix.genie.common.internal.tracing.brave.BraveTracingComponents;
import com.netflix.genie.web.data.services.impl.jpa.entities.ClusterEntity;
import com.netflix.genie.web.data.services.impl.jpa.entities.CommandEntity;
import com.netflix.genie.web.data.services.impl.jpa.entities.JobEntity;
import com.netflix.genie.web.data.services.impl.jpa.queries.aggregates.UserJobResourcesAggregate;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.JobApiProjection;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.JobApplicationsProjection;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.JobCommandProjection;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.JobProjection;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.v4.FinishedJobProjection;
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
import org.apache.commons.lang3.StringUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.persistence.EntityManager;
import java.time.Instant;
import java.util.HashMap;
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

    private JpaJobRepository jobRepository;
    private JpaApplicationRepository applicationRepository;
    private JpaClusterRepository clusterRepository;
    private JpaCommandRepository commandRepository;

    private JpaPersistenceServiceImpl persistenceService;

    @BeforeEach
    void setup() {
        this.jobRepository = Mockito.mock(JpaJobRepository.class);
        this.applicationRepository = Mockito.mock(JpaApplicationRepository.class);
        this.clusterRepository = Mockito.mock(JpaClusterRepository.class);
        this.commandRepository = Mockito.mock(JpaCommandRepository.class);
        final JpaTagRepository tagRepository = Mockito.mock(JpaTagRepository.class);
        final JpaFileRepository fileRepository = Mockito.mock(JpaFileRepository.class);

        final JpaRepositories jpaRepositories = Mockito.mock(JpaRepositories.class);
        Mockito.when(jpaRepositories.getApplicationRepository()).thenReturn(this.applicationRepository);
        Mockito.when(jpaRepositories.getClusterRepository()).thenReturn(this.clusterRepository);
        Mockito.when(jpaRepositories.getCommandRepository()).thenReturn(this.commandRepository);
        Mockito.when(jpaRepositories.getJobRepository()).thenReturn(this.jobRepository);
        Mockito.when(jpaRepositories.getFileRepository()).thenReturn(fileRepository);
        Mockito.when(jpaRepositories.getTagRepository()).thenReturn(tagRepository);

        this.persistenceService = new JpaPersistenceServiceImpl(
            Mockito.mock(EntityManager.class),
            jpaRepositories,
            Mockito.mock(BraveTracingComponents.class)
        );
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
            .when(this.jobRepository.getJobSpecification(Mockito.anyString()))
            .thenReturn(Optional.of(jobEntity));

        Mockito.when(jobEntity.isResolved()).thenReturn(false);

        Assertions
            .assertThat(this.persistenceService.getJobSpecification(UUID.randomUUID().toString()))
            .isNotPresent();
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
            .when(this.jobRepository.getJobStatus(id))
            .thenReturn(Optional.empty())
            .thenReturn(Optional.of(status.name()));

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
        Mockito.when(this.jobRepository.getV3Job(id)).thenReturn(Optional.of(jobEntity));
        final Job returnedJob = this.persistenceService.getJob(id);
        Mockito
            .verify(this.jobRepository, Mockito.times(1))
            .getV3Job(id);
        Assertions.assertThat(returnedJob.getId()).isPresent().contains(id);
    }

    @Test
    void cantGetJobClusterIfJobDoesNotExist() {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jobRepository.getJobCluster(id)).thenReturn(Optional.empty());
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.persistenceService.getJobCluster(id));
    }

    @Test
    void cantGetJobClusterIfClusterDoesNotExist() {
        final String id = UUID.randomUUID().toString();
        final JobEntity entity = Mockito.mock(JobEntity.class);
        Mockito.when(entity.getCluster()).thenReturn(Optional.empty());
        Mockito.when(this.jobRepository.getJobCluster(id)).thenReturn(Optional.of(entity));
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
            .when(this.jobRepository.getJobApplications(id))
            .thenReturn(Optional.of(entity))
            .thenReturn(Optional.empty());
        Assertions.assertThat(this.persistenceService.getJobApplications(id)).isEmpty();
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.persistenceService.getJobApplications(id));
    }

    @Test
    void cantGetJobHostIfNoJobExecution() {
        final String jobId = UUID.randomUUID().toString();
        Mockito
            .when(this.jobRepository.getJobHostname(jobId))
            .thenReturn(Optional.empty());
    }

    @Test
    void canGetJobHost() {
        final String jobId = UUID.randomUUID().toString();
        final String hostName = UUID.randomUUID().toString();
        Mockito
            .when(this.jobRepository.getJobHostname(jobId))
            .thenReturn(Optional.of(hostName));
    }

    @Test
    void canGetUserResourceSummariesNoRecords() {
        Mockito
            .when(this.jobRepository.getUserJobResourcesAggregates(JpaPersistenceServiceImpl.ACTIVE_STATUS_SET, true))
            .thenReturn(Sets.newHashSet());
        Assertions
            .assertThat(this.persistenceService.getUserResourcesSummaries(JobStatus.getActiveStatuses(), true))
            .isEmpty();
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

        final Set<JobStatus> statuses = JobStatus.getResolvableStatuses();
        final Set<String> statusStrings = statuses.stream().map(JobStatus::name).collect(Collectors.toSet());

        Mockito
            .when(this.jobRepository.getUserJobResourcesAggregates(statusStrings, false))
            .thenReturn(Sets.newHashSet(p1, p2));

        final HashMap<String, UserResourcesSummary> expectedMap = Maps.newHashMap();
        expectedMap.put("foo", new UserResourcesSummary("foo", 3, 1024));
        expectedMap.put("bar", new UserResourcesSummary("bar", 5, 2048));
        Assertions
            .assertThat(this.persistenceService.getUserResourcesSummaries(statuses, false))
            .isEqualTo(expectedMap);
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
    void canGetActiveAgentJobs() {
        final String job1Id = UUID.randomUUID().toString();
        final String job2Id = UUID.randomUUID().toString();

        Mockito
            .when(this.jobRepository.getJobIdsWithStatusIn(JpaPersistenceServiceImpl.ACTIVE_STATUS_SET))
            .thenReturn(Sets.newHashSet(job1Id, job2Id));

        Assertions
            .assertThat(this.persistenceService.getActiveJobs())
            .isEqualTo(Sets.newHashSet(job1Id, job2Id));
    }

    @Test
    void canGetActiveAgentJobsWhenEmpty() {

        Mockito
            .when(this.jobRepository.getJobIdsWithStatusIn(JpaPersistenceServiceImpl.ACTIVE_STATUS_SET))
            .thenReturn(Sets.newHashSet());

        Assertions
            .assertThat(this.persistenceService.getActiveJobs())
            .isEqualTo(Sets.newHashSet());
    }

    @Test
    void canGetUnclaimedAgentJobs() {
        final String jobId1 = UUID.randomUUID().toString();
        final String jobId2 = UUID.randomUUID().toString();

        Mockito
            .when(this.jobRepository.getJobIdsWithStatusIn(JpaPersistenceServiceImpl.UNCLAIMED_STATUS_SET))
            .thenReturn(Sets.newHashSet(jobId1, jobId2));

        Assertions
            .assertThat(this.persistenceService.getUnclaimedJobs())
            .isEqualTo(Sets.newHashSet(jobId1, jobId2));
    }

    @Test
    void canGetUnclaimedAgentJobsWhenEmpty() {
        Mockito
            .when(this.jobRepository.getJobIdsWithStatusIn(JpaPersistenceServiceImpl.UNCLAIMED_STATUS_SET))
            .thenReturn(Sets.newHashSet());

        Assertions
            .assertThat(this.persistenceService.getUnclaimedJobs())
            .isEqualTo(Sets.newHashSet());
    }

    @Test
    void testUpdateJobStatusWithTooLongMessage() throws GenieCheckedException {
        final String id = UUID.randomUUID().toString();
        final JobEntity jobEntity = new JobEntity();
        jobEntity.setStatus(JobStatus.INIT.name());
        final String tooLong = StringUtils.leftPad("a", 256, 'b');

        Mockito.when(this.jobRepository.findByUniqueId(id)).thenReturn(Optional.of(jobEntity));
        this.persistenceService.updateJobStatus(id, JobStatus.INIT, JobStatus.RUNNING, tooLong);

        Assertions.assertThat(jobEntity.getStatus()).isEqualTo(JobStatus.RUNNING.name());
        Assertions.assertThat(jobEntity.getStatusMsg()).isPresent().contains(StringUtils.truncate(tooLong, 255));
    }

    @Test
    void testGetJobsWithStatusAndArchiveStatusUpdatedBefore() {
        Mockito
            .when(
                this.jobRepository.getJobsWithStatusAndArchiveStatusUpdatedBefore(
                    Mockito.anySet(),
                    Mockito.anySet(),
                    Mockito.any(Instant.class)
                ))
            .thenReturn(Sets.newHashSet());
        this.persistenceService.getJobsWithStatusAndArchiveStatusUpdatedBefore(
            Sets.newHashSet(JobStatus.FAILED, JobStatus.KILLED),
            Sets.newHashSet(ArchiveStatus.FAILED),
            Instant.now()
        );
    }
}
