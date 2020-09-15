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
package com.netflix.genie.web.data.services.impl.jpa;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.BaseDTO;
import com.netflix.genie.common.dto.ClusterCriteria;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.UserResourcesSummary;
import com.netflix.genie.common.dto.search.BaseSearchResult;
import com.netflix.genie.common.dto.search.JobSearchResult;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.external.dtos.v4.AgentClientMetadata;
import com.netflix.genie.common.external.dtos.v4.AgentConfigRequest;
import com.netflix.genie.common.external.dtos.v4.ApiClientMetadata;
import com.netflix.genie.common.external.dtos.v4.Application;
import com.netflix.genie.common.external.dtos.v4.ArchiveStatus;
import com.netflix.genie.common.external.dtos.v4.Cluster;
import com.netflix.genie.common.external.dtos.v4.Command;
import com.netflix.genie.common.external.dtos.v4.Criterion;
import com.netflix.genie.common.external.dtos.v4.ExecutionEnvironment;
import com.netflix.genie.common.external.dtos.v4.ExecutionResourceCriteria;
import com.netflix.genie.common.external.dtos.v4.JobEnvironment;
import com.netflix.genie.common.external.dtos.v4.JobEnvironmentRequest;
import com.netflix.genie.common.external.dtos.v4.JobMetadata;
import com.netflix.genie.common.external.dtos.v4.JobRequest;
import com.netflix.genie.common.external.dtos.v4.JobRequestMetadata;
import com.netflix.genie.common.external.dtos.v4.JobSpecification;
import com.netflix.genie.common.external.dtos.v4.JobStatus;
import com.netflix.genie.common.external.util.GenieObjectMapper;
import com.netflix.genie.common.internal.dtos.v4.FinishedJob;
import com.netflix.genie.common.internal.exceptions.checked.GenieCheckedException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieInvalidStatusException;
import com.netflix.genie.test.suppliers.RandomSuppliers;
import com.netflix.genie.web.data.services.impl.jpa.entities.JobEntity;
import com.netflix.genie.web.data.services.impl.jpa.queries.aggregates.JobInfoAggregate;
import com.netflix.genie.web.data.services.impl.jpa.queries.projections.JobMetadataProjection;
import com.netflix.genie.web.dtos.JobSubmission;
import com.netflix.genie.web.dtos.ResolvedJob;
import com.netflix.genie.web.exceptions.checked.IdAlreadyExistsException;
import com.netflix.genie.web.exceptions.checked.NotFoundException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Integration tests for {@link JpaPersistenceServiceImpl} focusing on the jobs APIs.
 *
 * @author tgianos
 * @since 3.0.0
 */
class JpaPersistenceServiceImplJobsIntegrationTest extends JpaPersistenceServiceIntegrationTestBase {

    private static final String JOB_1_ID = "job1";
    private static final String JOB_2_ID = "job2";
    private static final String JOB_3_ID = "job3";
    private static final String AGENT_JOB_1 = "agentJob1";
    private static final String AGENT_JOB_2 = "agentJob2";

    // Job Request fields
    private static final String UNIQUE_ID = UUID.randomUUID().toString();
    private static final String NAME = UUID.randomUUID().toString();
    private static final String USER = UUID.randomUUID().toString();
    private static final String VERSION = UUID.randomUUID().toString();
    private static final String DESCRIPTION = UUID.randomUUID().toString();
    private static final List<String> COMMAND_ARGS = Lists.newArrayList(UUID.randomUUID().toString());
    private static final String GROUP = UUID.randomUUID().toString();
    private static final String SETUP_FILE = UUID.randomUUID().toString();
    private static final String CLUSTER_TAG_1 = UUID.randomUUID().toString();
    private static final String CLUSTER_TAG_2 = UUID.randomUUID().toString();
    private static final String CLUSTER_TAG_3 = UUID.randomUUID().toString();
    private static final List<ClusterCriteria> CLUSTER_CRITERIA = Lists.newArrayList(
        new ClusterCriteria(Sets.newHashSet(CLUSTER_TAG_1)),
        new ClusterCriteria(Sets.newHashSet(CLUSTER_TAG_2)),
        new ClusterCriteria(Sets.newHashSet(CLUSTER_TAG_3))
    );
    private static final String COMMAND_TAG_1 = UUID.randomUUID().toString();
    private static final String COMMAND_TAG_2 = UUID.randomUUID().toString();
    private static final Set<String> COMMAND_CRITERION = Sets.newHashSet(COMMAND_TAG_1, COMMAND_TAG_2);
    private static final String CONFIG_1 = UUID.randomUUID().toString();
    private static final String CONFIG_2 = UUID.randomUUID().toString();
    private static final Set<String> CONFIGS = Sets.newHashSet(CONFIG_1, CONFIG_2);
    private static final String DEPENDENCY = UUID.randomUUID().toString();
    private static final Set<String> DEPENDENCIES = Sets.newHashSet(DEPENDENCY);
    private static final String EMAIL = UUID.randomUUID().toString() + "@" + UUID.randomUUID().toString() + ".com";
    private static final String TAG_1 = UUID.randomUUID().toString();
    private static final String TAG_2 = UUID.randomUUID().toString();
    private static final String TAG_3 = UUID.randomUUID().toString();
    private static final Set<String> TAGS = Sets.newHashSet(TAG_1, TAG_2, TAG_3);
    private static final int CPU_REQUESTED = 2;
    private static final int MEMORY_REQUESTED = 1024;
    private static final String APP_REQUESTED_1 = UUID.randomUUID().toString();
    private static final String APP_REQUESTED_2 = UUID.randomUUID().toString();
    private static final List<String> APPLICATIONS_REQUESTED = Lists.newArrayList(APP_REQUESTED_1, APP_REQUESTED_2);
    private static final int TIMEOUT_REQUESTED = 84500;
    private static final String GROUPING = UUID.randomUUID().toString();
    private static final String GROUPING_INSTANCE = UUID.randomUUID().toString();

    // Job Metadata fields
    private static final String CLIENT_HOST = UUID.randomUUID().toString();
    private static final String USER_AGENT = UUID.randomUUID().toString();
    private static final int NUM_ATTACHMENTS = 3;
    private static final long TOTAL_SIZE_ATTACHMENTS = 38023423L;
    private static final long STD_ERR_SIZE = 98025245L;
    private static final long STD_OUT_SIZE = 78723423L;

    // Job fields
    private static final String ARCHIVE_LOCATION = UUID.randomUUID().toString();
    private static final Instant FINISHED = Instant.now();
    private static final Instant STARTED = Instant.now();
    private static final String STATUS_MSG = UUID.randomUUID().toString();

    // Job Execution fields
    private static final String HOSTNAME = UUID.randomUUID().toString();
    private static final int PROCESS_ID = 3203;
    private static final long CHECK_DELAY = 8728L;
    private static final Instant TIMEOUT = STARTED.plus(50L, ChronoUnit.SECONDS);
    private static final int MEMORY = 2048;

    @Test
    @DatabaseSetup("persistence/jobs/init.xml")
    void canDeleteJobsCreatedBeforeDateWithBatchSizeGreaterThanExpectedTotalDeletions() {
        final Instant cal = ZonedDateTime
            .of(2016, Month.JANUARY.getValue(), 1, 0, 0, 0, 0, ZoneId.of("UTC"))
            .toInstant();

        final long deleted = this.service.deleteJobsCreatedBefore(cal, JobStatus.getActiveStatuses(), 10);

        Assertions.assertThat(deleted).isEqualTo(1L);
        Assertions.assertThat(this.jobRepository.count()).isEqualTo(2L);
        Assertions.assertThat(this.jobRepository.existsByUniqueId(JOB_1_ID)).isFalse();
        Assertions.assertThat(this.jobRepository.existsByUniqueId(JOB_2_ID)).isTrue();
        Assertions.assertThat(this.jobRepository.existsByUniqueId(JOB_3_ID)).isTrue();
    }

    @Test
    @DatabaseSetup("persistence/jobs/init.xml")
    void canDeleteJobsCreatedBeforeDateWithBatchSizeLessThanExpectedTotalDeletions() {
        final Instant cal = ZonedDateTime
            .of(2016, Month.JANUARY.getValue(), 1, 0, 0, 0, 0, ZoneId.of("UTC"))
            .toInstant();

        final long deleted = this.service.deleteJobsCreatedBefore(cal, JobStatus.getActiveStatuses(), 1);

        Assertions.assertThat(deleted).isEqualTo(1L);
        Assertions.assertThat(this.jobRepository.count()).isEqualTo(2L);
        Assertions.assertThat(this.jobRepository.existsByUniqueId(JOB_3_ID)).isTrue();
    }

    @Test
    @DatabaseSetup("persistence/jobs/init.xml")
    void canDeleteJobsRegardlessOfStatus() {
        final Instant cal = ZonedDateTime
            .of(2016, Month.JANUARY.getValue(), 1, 0, 0, 0, 0, ZoneId.of("UTC"))
            .toInstant();

        final long deleted = this.service.deleteJobsCreatedBefore(cal, Sets.newHashSet(), 10);

        Assertions.assertThat(deleted).isEqualTo(2L);
        Assertions.assertThat(this.jobRepository.count()).isEqualTo(1L);
        Assertions.assertThat(this.jobRepository.existsByUniqueId(JOB_1_ID)).isFalse();
        Assertions.assertThat(this.jobRepository.existsByUniqueId(JOB_2_ID)).isFalse();
        Assertions.assertThat(this.jobRepository.existsByUniqueId(JOB_3_ID)).isTrue();
    }

    @Test
    @DatabaseSetup("persistence/jobs/init.xml")
    void canPersistAndGetAJob() throws GenieException {
        // Set up some fields for comparison
        final com.netflix.genie.common.dto.JobRequest jobRequest = new com.netflix.genie.common.dto.JobRequest.Builder(
            NAME, USER, VERSION, CLUSTER_CRITERIA, COMMAND_CRITERION
        )
            .withId(UNIQUE_ID)
            .withDescription(DESCRIPTION)
            .withCommandArgs(COMMAND_ARGS)
            .withGroup(GROUP)
            .withSetupFile(SETUP_FILE)
            .withConfigs(CONFIGS)
            .withDependencies(DEPENDENCIES)
            .withDisableLogArchival(true)
            .withEmail(EMAIL)
            .withTags(TAGS)
            .withCpu(CPU_REQUESTED)
            .withMemory(MEMORY_REQUESTED)
            .withTimeout(TIMEOUT_REQUESTED)
            .withApplications(APPLICATIONS_REQUESTED)
            .withGrouping(GROUPING)
            .withGroupingInstance(GROUPING_INSTANCE)
            .build();

        final com.netflix.genie.common.dto.JobMetadata jobMetadata = new com.netflix.genie.common.dto.JobMetadata
            .Builder()
            .withClientHost(CLIENT_HOST)
            .withUserAgent(USER_AGENT)
            .withNumAttachments(NUM_ATTACHMENTS)
            .withTotalSizeOfAttachments(TOTAL_SIZE_ATTACHMENTS)
            .withStdErrSize(STD_ERR_SIZE)
            .withStdOutSize(STD_OUT_SIZE)
            .build();

        final Job.Builder jobBuilder = new Job.Builder(
            NAME, USER, VERSION
        )
            .withId(UNIQUE_ID)
            .withDescription(DESCRIPTION)
            .withTags(TAGS)
            .withArchiveLocation(ARCHIVE_LOCATION)
            .withStarted(STARTED)
            .withFinished(FINISHED)
            .withStatus(com.netflix.genie.common.dto.JobStatus.RUNNING)
            .withStatusMsg(STATUS_MSG);

        jobBuilder.withCommandArgs(COMMAND_ARGS);
        final Job job = jobBuilder.build();

        final JobExecution jobExecution = new JobExecution.Builder(HOSTNAME)
            .withId(UNIQUE_ID)
            .withCheckDelay(CHECK_DELAY)
            .withTimeout(TIMEOUT)
            .withMemory(MEMORY)
            .withProcessId(PROCESS_ID)
            .withArchiveStatus(ArchiveStatus.DISABLED)
            .build();

        Assertions.assertThat(this.jobRepository.count()).isEqualTo(3L);
        Assertions.assertThat(this.tagRepository.count()).isEqualTo(17L);
        Assertions.assertThat(this.fileRepository.count()).isEqualTo(11L);
        this.service.createJob(jobRequest, jobMetadata, job, jobExecution);

        // Force it to behave as fresh request to API
        this.entityManager.flush();
        this.entityManager.clear();

        Assertions.assertThat(this.jobRepository.count()).isEqualTo(4L);
        Assertions.assertThat(this.tagRepository.count()).isEqualTo(25L);
        Assertions.assertThat(this.fileRepository.count()).isEqualTo(15L);

        Assertions
            .assertThat(this.jobRepository.findByUniqueId(UNIQUE_ID))
            .isPresent()
            .get()
            .extracting(JobEntity::isV4)
            .isEqualTo(false);

        this.validateJobRequest(this.service.getV3JobRequest(UNIQUE_ID));
        this.validateJob(this.service.getJob(UNIQUE_ID));
        this.validateJobExecution(this.service.getJobExecution(UNIQUE_ID));

        Assertions
            .assertThat(this.jobRepository.findByUniqueId(UNIQUE_ID, JobMetadataProjection.class))
            .isPresent()
            .get()
            .satisfies(this::validateJobMetadata);
    }

    @Test
    @DatabaseSetup("persistence/jobs/init.xml")
    void canSaveAndVerifyJobSubmissionWithoutAttachments() throws IOException, GenieCheckedException {
        final String job0Id = UUID.randomUUID().toString();
        final String job3Id = UUID.randomUUID().toString();
        final JobRequest jobRequest0 = this.createJobRequest(job0Id, UUID.randomUUID().toString());
        final JobRequest jobRequest1 = this.createJobRequest(null, UUID.randomUUID().toString());
        final JobRequest jobRequest2 = this.createJobRequest(JOB_3_ID, UUID.randomUUID().toString());
        final JobRequest jobRequest3 = this.createJobRequest(job3Id, null);
        final JobRequestMetadata jobRequestMetadata = this.createJobRequestMetadata(
            true,
            NUM_ATTACHMENTS,
            TOTAL_SIZE_ATTACHMENTS
        );

        String id = this.service.saveJobSubmission(new JobSubmission.Builder(jobRequest0, jobRequestMetadata).build());
        Assertions.assertThat(id).isEqualTo(job0Id);
        this.validateSavedJobSubmission(id, jobRequest0, jobRequestMetadata);

        id = this.service.saveJobSubmission(new JobSubmission.Builder(jobRequest1, jobRequestMetadata).build());
        Assertions.assertThat(id).isNotBlank();
        this.validateSavedJobSubmission(id, jobRequest1, jobRequestMetadata);

        id = this.service.saveJobSubmission(new JobSubmission.Builder(jobRequest3, jobRequestMetadata).build());
        Assertions.assertThat(id).isEqualTo(job3Id);
        this.validateSavedJobSubmission(id, jobRequest3, jobRequestMetadata);

        Assertions
            .assertThatExceptionOfType(IdAlreadyExistsException.class)
            .isThrownBy(
                () -> this.service.saveJobSubmission(
                    new JobSubmission.Builder(jobRequest2, jobRequestMetadata).build()
                )
            );
    }

    @Test
    @DatabaseSetup("persistence/jobs/init.xml")
    void canSaveAndVerifyJobSubmissionWithAttachments(@TempDir final Path tempDir) throws
        GenieCheckedException,
        IOException {
        final JobRequest jobRequest = this.createJobRequest(null, null);
        final int numAttachments = 6;
        long totalAttachmentSize = 0L;
        final Set<Resource> attachments = Sets.newHashSet();
        for (int i = 0; i < numAttachments; i++) {
            final Path attachment = tempDir.resolve(UUID.randomUUID().toString());
            Files.write(attachment, ("Select * FROM my_table where id = " + i + ";").getBytes(StandardCharsets.UTF_8));
            attachments.add(new FileSystemResource(attachment));
            totalAttachmentSize += Files.size(attachment);
        }
        final Set<URI> attachmentURIs = attachments
            .stream()
            .map(
                // checked exceptions are so fun...
                attachment -> {
                    try {
                        return attachment.getURI();
                    } catch (final IOException e) {
                        throw new IllegalArgumentException(e);
                    }
                }
            )
            .collect(Collectors.toSet());
        final JobRequestMetadata jobRequestMetadata = this.createJobRequestMetadata(
            true,
            numAttachments,
            totalAttachmentSize
        );

        // Save the job submission
        final String id = this.service.saveJobSubmission(
            new JobSubmission.Builder(jobRequest, jobRequestMetadata).withAttachments(attachmentURIs).build()
        );

        // Going to assume that most other verification of parameters other than attachments is done in
        // canSaveAndVerifyJobSubmissionWithoutAttachments()
        final JobRequest savedJobRequest = this.service.getJobRequest(id);

        // We should have all the original dependencies
        Assertions
            .assertThat(savedJobRequest.getResources().getDependencies())
            .containsAll(jobRequest.getResources().getDependencies());

        // Filter out the original dependencies so we're left with just the attachments
        final Set<URI> savedAttachmentURIs = savedJobRequest
            .getResources()
            .getDependencies()
            .stream()
            .filter(dependency -> !jobRequest.getResources().getDependencies().contains(dependency))
            .map(
                dependency -> {
                    try {
                        return new URI(dependency);
                    } catch (URISyntaxException e) {
                        throw new IllegalArgumentException(e);
                    }
                }
            )
            .collect(Collectors.toSet());

        Assertions.assertThat(savedAttachmentURIs).isEqualTo(attachmentURIs);
    }

    @Test
    @DatabaseSetup("persistence/jobs/init.xml")
    void canSaveAndRetrieveJobSpecification() throws GenieCheckedException, IOException {
        final String jobId = this.service.saveJobSubmission(
            new JobSubmission.Builder(
                this.createJobRequest(null, UUID.randomUUID().toString()),
                this.createJobRequestMetadata(false, NUM_ATTACHMENTS, TOTAL_SIZE_ATTACHMENTS)
            ).build()
        );

        final JobRequest jobRequest = this.service.getJobRequest(jobId);

        final JobSpecification jobSpecification = this.createJobSpecification(
            jobId,
            jobRequest,
            UUID.randomUUID().toString(),
            null
        );

        final ResolvedJob resolvedJob = new ResolvedJob(
            jobSpecification,
            new JobEnvironment.Builder(1_512).build(),
            jobRequest.getMetadata()
        );

        this.service.saveResolvedJob(jobId, resolvedJob);
        Assertions
            .assertThat(this.service.getJobSpecification(jobId))
            .isPresent()
            .contains(jobSpecification);

        final String jobId2 = this.service.saveJobSubmission(
            new JobSubmission.Builder(
                this.createJobRequest(null, null),
                this.createJobRequestMetadata(false, NUM_ATTACHMENTS, TOTAL_SIZE_ATTACHMENTS)
            ).build()
        );

        final JobRequest jobRequest2 = this.service.getJobRequest(jobId2);

        final JobSpecification jobSpecification2 = this.createJobSpecification(
            jobId2,
            jobRequest2,
            null,
            10_358
        );

        final ResolvedJob resolvedJob1 = new ResolvedJob(
            jobSpecification2,
            new JobEnvironment.Builder(1_1512).build(),
            jobRequest2.getMetadata()
        );
        this.service.saveResolvedJob(jobId2, resolvedJob1);
        Assertions
            .assertThat(this.service.getJobSpecification(jobId2))
            .isPresent()
            .contains(jobSpecification2);
    }

    @Test
    @DatabaseSetup("persistence/jobs/init.xml")
    void canClaimJobAndUpdateStatus() throws GenieCheckedException, IOException {
        final String jobId = this.service.saveJobSubmission(
            new JobSubmission.Builder(
                this.createJobRequest(null, UUID.randomUUID().toString()),
                this.createJobRequestMetadata(true, NUM_ATTACHMENTS, TOTAL_SIZE_ATTACHMENTS)
            ).build()
        );

        final JobRequest jobRequest = this.service.getJobRequest(jobId);

        final JobSpecification jobSpecification = this.createJobSpecification(
            jobId,
            jobRequest,
            UUID.randomUUID().toString(),
            null
        );

        final ResolvedJob resolvedJob = new ResolvedJob(
            jobSpecification,
            new JobEnvironment.Builder(1_512).build(),
            jobRequest.getMetadata()
        );
        this.service.saveResolvedJob(jobId, resolvedJob);

        final JobEntity preClaimedJob = this.jobRepository
            .findByUniqueId(jobId)
            .orElseThrow(IllegalArgumentException::new);

        Assertions.assertThat(preClaimedJob.isApi()).isTrue();
        Assertions.assertThat(preClaimedJob.getStatus()).isEqualTo(JobStatus.RESOLVED.name());
        Assertions.assertThat(preClaimedJob.isResolved()).isTrue();
        Assertions.assertThat(preClaimedJob.isClaimed()).isFalse();
        Assertions.assertThat(preClaimedJob.getAgentHostname()).isNotPresent();
        Assertions.assertThat(preClaimedJob.getAgentVersion()).isNotPresent();
        Assertions.assertThat(preClaimedJob.getAgentPid()).isNotPresent();

        final String agentHostname = UUID.randomUUID().toString();
        final String agentVersion = UUID.randomUUID().toString();
        final int agentPid = RandomSuppliers.INT.get();
        final AgentClientMetadata agentClientMetadata = new AgentClientMetadata(agentHostname, agentVersion, agentPid);

        this.service.claimJob(jobId, agentClientMetadata);

        final JobEntity postClaimedJob = this.jobRepository
            .findByUniqueId(jobId)
            .orElseThrow(IllegalArgumentException::new);

        Assertions.assertThat(postClaimedJob.getStatus()).isEqualTo(JobStatus.CLAIMED.name());
        Assertions.assertThat(postClaimedJob.isResolved()).isTrue();
        Assertions.assertThat(postClaimedJob.isClaimed()).isTrue();
        Assertions.assertThat(postClaimedJob.getAgentHostname()).isPresent().contains(agentHostname);
        Assertions.assertThat(postClaimedJob.getAgentVersion()).isPresent().contains(agentVersion);
        Assertions.assertThat(postClaimedJob.getAgentPid()).isPresent().contains(agentPid);
    }

    @Test
    @DatabaseSetup("persistence/jobs/init.xml")
    void canUpdateJobStatus() throws GenieCheckedException, IOException {
        final String jobId = this.service.saveJobSubmission(
            new JobSubmission.Builder(
                this.createJobRequest(null, UUID.randomUUID().toString()),
                this.createJobRequestMetadata(false, NUM_ATTACHMENTS, TOTAL_SIZE_ATTACHMENTS)
            ).build()
        );

        final JobRequest jobRequest = this.service.getJobRequest(jobId);

        final JobSpecification jobSpecification = this.createJobSpecification(
            jobId,
            jobRequest,
            UUID.randomUUID().toString(),
            null
        );

        final ResolvedJob resolvedJob = new ResolvedJob(
            jobSpecification,
            new JobEnvironment.Builder(1_512).build(),
            jobRequest.getMetadata()
        );
        this.service.saveResolvedJob(jobId, resolvedJob);

        final String agentHostname = UUID.randomUUID().toString();
        final String agentVersion = UUID.randomUUID().toString();
        final int agentPid = RandomSuppliers.INT.get();
        final AgentClientMetadata agentClientMetadata = new AgentClientMetadata(agentHostname, agentVersion, agentPid);

        this.service.claimJob(jobId, agentClientMetadata);

        JobEntity jobEntity = this.jobRepository
            .findByUniqueId(jobId)
            .orElseThrow(IllegalArgumentException::new);

        Assertions.assertThat(jobEntity.getStatus()).isEqualTo(JobStatus.CLAIMED.name());

        // status won't match so it will throw exception
        Assertions
            .assertThatExceptionOfType(GenieInvalidStatusException.class)
            .isThrownBy(
                () -> this.service.updateJobStatus(
                    jobId,
                    JobStatus.RUNNING,
                    JobStatus.FAILED,
                    null
                )
            );

        final String initStatusMessage = "Job is initializing";
        this.service.updateJobStatus(jobId, JobStatus.CLAIMED, JobStatus.INIT, initStatusMessage);

        jobEntity = this.jobRepository
            .findByUniqueId(jobId)
            .orElseThrow(IllegalArgumentException::new);

        Assertions.assertThat(jobEntity.isApi()).isFalse();
        Assertions.assertThat(jobEntity.getStatus()).isEqualTo(JobStatus.INIT.name());
        Assertions.assertThat(jobEntity.getStatusMsg()).isPresent().contains(initStatusMessage);
        Assertions.assertThat(jobEntity.getStarted()).isNotPresent();
        Assertions.assertThat(jobEntity.getFinished()).isNotPresent();

        final String runningStatusMessage = "Job is running";
        this.service.updateJobStatus(jobId, JobStatus.INIT, JobStatus.RUNNING, runningStatusMessage);

        jobEntity = this.jobRepository
            .findByUniqueId(jobId)
            .orElseThrow(IllegalArgumentException::new);

        Assertions.assertThat(jobEntity.getStatus()).isEqualTo(JobStatus.RUNNING.name());
        Assertions.assertThat(jobEntity.getStatusMsg()).isPresent().contains(runningStatusMessage);
        Assertions.assertThat(jobEntity.getStarted()).isPresent();
        Assertions.assertThat(jobEntity.getFinished()).isNotPresent();

        final String successStatusMessage = "Job completed successfully";
        this.service.updateJobStatus(jobId, JobStatus.RUNNING, JobStatus.SUCCEEDED, successStatusMessage);

        jobEntity = this.jobRepository
            .findByUniqueId(jobId)
            .orElseThrow(IllegalArgumentException::new);

        Assertions.assertThat(jobEntity.getStatus()).isEqualTo(JobStatus.SUCCEEDED.name());
        Assertions.assertThat(jobEntity.getStatusMsg()).isPresent().contains(successStatusMessage);
        Assertions.assertThat(jobEntity.getStarted()).isPresent();
        Assertions.assertThat(jobEntity.getFinished()).isPresent();
    }

    @Test
    @DatabaseSetup("persistence/jobs/init.xml")
    void canGetJobStatus() throws GenieCheckedException {
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.getJobStatus(UUID.randomUUID().toString()));
        Assertions.assertThat(this.service.getJobStatus(JOB_1_ID)).isEqualTo(JobStatus.SUCCEEDED);
        Assertions.assertThat(this.service.getJobStatus(JOB_2_ID)).isEqualTo(JobStatus.RUNNING);
        Assertions.assertThat(this.service.getJobStatus(JOB_3_ID)).isEqualTo(JobStatus.RUNNING);
    }

    @Test
    @DatabaseSetup("persistence/jobs/init.xml")
    void canGetJobArchiveLocation() throws GenieCheckedException {
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.getJobArchiveLocation(UUID.randomUUID().toString()));

        Assertions.assertThat(this.service.getJobArchiveLocation(JOB_3_ID)).isNotPresent();
        Assertions
            .assertThat(this.service.getJobArchiveLocation(JOB_1_ID))
            .isPresent()
            .contains("s3://somebucket/genie/logs/1/");
    }

    @Test
    @DatabaseSetup("persistence/jobs/init.xml")
    void cantGetFinishedJobNonExistent() {
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.getFinishedJob(UUID.randomUUID().toString()));
    }

    @Test
    @DatabaseSetup("persistence/jobs/init.xml")
    void cantGetFinishedJobNotFinished() {
        Assertions
            .assertThatExceptionOfType(GenieInvalidStatusException.class)
            .isThrownBy(() -> this.service.getFinishedJob(JOB_3_ID));
    }

    @Test
    @DatabaseSetup("persistence/jobs/init.xml")
    void canGetFinishedJob() throws GenieCheckedException {
        final FinishedJob finishedJob = this.service.getFinishedJob(JOB_1_ID);
        Assertions.assertThat(finishedJob).isNotNull();

        Assertions.assertThat(finishedJob.getUniqueId()).isEqualTo(JOB_1_ID);
        Assertions.assertThat(finishedJob.getUser()).isEqualTo("tgianos");
        Assertions.assertThat(finishedJob.getName()).isEqualTo("testSparkJob");
        Assertions.assertThat(finishedJob.getVersion()).isEqualTo("2.4");
        Assertions.assertThat(finishedJob.getCreated()).isNotNull();
        Assertions.assertThat(finishedJob.getStatus()).isEqualTo(JobStatus.SUCCEEDED);
        Assertions.assertThat(finishedJob.getCommandArgs().size()).isEqualTo(2);
        Assertions.assertThat(finishedJob.getCommandCriterion()).isNotNull();
        Assertions.assertThat(finishedJob.getClusterCriteria()).hasSize(2);
        Assertions.assertThat(finishedJob.getStarted()).isNotPresent();
        Assertions.assertThat(finishedJob.getFinished()).isNotPresent();
        Assertions.assertThat(finishedJob.getGrouping()).isNotPresent();
        Assertions.assertThat(finishedJob.getGroupingInstance()).isNotPresent();
        Assertions.assertThat(finishedJob.getStatusMessage()).isNotPresent();
        Assertions.assertThat(finishedJob.getRequestedMemory()).isPresent().contains(1560);
        Assertions.assertThat(finishedJob.getRequestApiClientHostname()).isNotPresent();
        Assertions.assertThat(finishedJob.getRequestApiClientUserAgent()).isNotPresent();
        Assertions.assertThat(finishedJob.getRequestAgentClientHostname()).isNotPresent();
        Assertions.assertThat(finishedJob.getRequestAgentClientVersion()).isNotPresent();
        Assertions.assertThat(finishedJob.getNumAttachments()).isPresent().contains(2);
        Assertions.assertThat(finishedJob.getExitCode()).isPresent().contains(0);
        Assertions.assertThat(finishedJob.getArchiveLocation()).isPresent().contains("s3://somebucket/genie/logs/1/");
        Assertions.assertThat(finishedJob.getMemoryUsed()).isNotPresent();

        final Command command = finishedJob.getCommand().orElse(null);
        final Cluster cluster = finishedJob.getCluster().orElse(null);
        final List<Application> applications = finishedJob.getApplications();

        Assertions.assertThat(command).isNotNull();
        Assertions.assertThat(cluster).isNotNull();
        Assertions.assertThat(applications).isNotNull();
        Assertions.assertThat(command.getMetadata().getName()).isEqualTo("spark");
        Assertions.assertThat(cluster.getMetadata().getName()).isEqualTo("h2query");
        Assertions.assertThat(applications.size()).isEqualTo(2);
        Assertions.assertThat(applications.get(0).getMetadata().getName()).isEqualTo("hadoop");
        Assertions.assertThat(applications.get(1).getMetadata().getName()).isEqualTo("spark");
    }

    @Test
    @DatabaseSetup("persistence/jobs/init.xml")
    void canDetermineIfIsApiJob() throws GenieCheckedException {
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.isApiJob(UUID.randomUUID().toString()));
        Assertions.assertThat(this.service.isApiJob(JOB_1_ID)).isFalse();
        Assertions.assertThat(this.service.isApiJob(JOB_2_ID)).isTrue();
        Assertions.assertThat(this.service.isApiJob(JOB_3_ID)).isTrue();
    }

    @Test
    @DatabaseSetup("persistence/jobs/init.xml")
    void canGetJobArchiveStatus() throws GenieCheckedException {
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.getJobArchiveStatus(UUID.randomUUID().toString()));
        Assertions.assertThat(this.service.getJobArchiveStatus(JOB_1_ID)).isEqualTo(ArchiveStatus.ARCHIVED);
        Assertions.assertThat(this.service.getJobArchiveStatus(JOB_2_ID)).isEqualTo(ArchiveStatus.PENDING);
        Assertions.assertThat(this.service.getJobArchiveStatus(JOB_3_ID)).isEqualTo(ArchiveStatus.UNKNOWN);
    }

    @Test
    @DatabaseSetup("persistence/jobs/init.xml")
    void canSetJobArchiveStatus() throws GenieCheckedException, GenieException {
        Assertions.assertThat(this.service.getJobArchiveStatus(JOB_2_ID)).isEqualTo(ArchiveStatus.PENDING);
        this.service.updateJobArchiveStatus(JOB_2_ID, ArchiveStatus.ARCHIVED);
        Assertions.assertThat(this.service.getJobArchiveStatus(JOB_2_ID)).isEqualTo(ArchiveStatus.ARCHIVED);
    }

    @Test
    @DatabaseSetup("persistence/jobs/search.xml")
    void canFindJobs() {
        //TODO: add more cases
        final Pageable page = PageRequest.of(0, 10, Sort.Direction.DESC, "updated");
        Page<JobSearchResult> jobs;
        jobs = this.service.findJobs(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            page
        );
        Assertions.assertThat(jobs.getTotalElements()).isEqualTo(5L);

        jobs = this.service.findJobs(
            null,
            null,
            null,
            Sets.newHashSet(com.netflix.genie.common.dto.JobStatus.RUNNING),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            page
        );
        Assertions.assertThat(jobs.getTotalElements()).isEqualTo(3L);
        Assertions.assertThat(jobs.getContent()).hasSize(3).extracting(BaseSearchResult::getId).contains(JOB_3_ID);

        jobs = this.service.findJobs(
            JOB_1_ID,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            page
        );
        Assertions.assertThat(jobs.getTotalElements()).isEqualTo(1L);
        Assertions.assertThat(jobs.getContent()).hasSize(1).extracting(BaseSearchResult::getId).containsOnly(JOB_1_ID);

        jobs = this.service.findJobs(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            "job3Grouping",
            null,
            page
        );
        Assertions.assertThat(jobs.getTotalElements()).isEqualTo(1L);
        Assertions.assertThat(jobs.getContent()).hasSize(1).extracting(BaseSearchResult::getId).containsOnly(JOB_3_ID);

        jobs = this.service.findJobs(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            "job2%",
            page
        );
        Assertions.assertThat(jobs.getTotalElements()).isEqualTo(1L);
        Assertions.assertThat(jobs.getContent()).hasSize(1).extracting(BaseSearchResult::getId).containsOnly(JOB_2_ID);

        jobs = this.service.findJobs(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            "job1%",
            "job2%",
            page
        );
        Assertions.assertThat(jobs.getTotalElements()).isEqualTo(0L);
        Assertions.assertThat(jobs.getContent()).isEmpty();
    }

    @Test
    @DatabaseSetup("persistence/jobs/search.xml")
    void canFindJobsWithTags() {
        final Pageable page = PageRequest.of(0, 10);
        Page<JobSearchResult> jobs;
        jobs = this.service.findJobs(
            null,
            null,
            null,
            null,
            Sets.newHashSet("SparkJob"),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            page
        );
        Assertions.assertThat(jobs.getTotalElements()).isEqualTo(5L);

        jobs = this.service.findJobs(
            null,
            null,
            null,
            null,
            Sets.newHashSet("smoke-test", "SparkJob"),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            page
        );
        Assertions.assertThat(jobs.getTotalElements()).isEqualTo(1L);
        Assertions.assertThat(jobs.getContent()).hasSize(1).extracting(BaseSearchResult::getId).containsOnly(JOB_1_ID);

        jobs = this.service.findJobs(
            null,
            null,
            null,
            null,
            Sets.newHashSet("smoke-test", "SparkJob", "blah"),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            page
        );
        Assertions.assertThat(jobs.getTotalElements()).isEqualTo(0L);
        Assertions.assertThat(jobs.getContent()).isEmpty();

        jobs = this.service.findJobs(
            null,
            null,
            null,
            Sets.newHashSet(com.netflix.genie.common.dto.JobStatus.FAILED),
            Sets.newHashSet("smoke-test", "SparkJob"),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            page
        );
        Assertions.assertThat(jobs.getTotalElements()).isEqualTo(0L);
        Assertions.assertThat(jobs.getContent()).isEmpty();
    }

    @Test
    @DatabaseSetup("persistence/jobs/search.xml")
    void canFindJobsByClusterAndCommand() {
        final String clusterId = "cluster1";
        final String clusterName = "h2query";
        final String commandId = "command1";
        final String commandName = "spark";
        final Pageable page = PageRequest.of(0, 10, Sort.Direction.DESC, "updated");
        Page<JobSearchResult> jobs = this.service.findJobs(
            null,
            null,
            null,
            null,
            null,
            null,
            UUID.randomUUID().toString(),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            page
        );
        Assertions.assertThat(jobs.getTotalElements()).isEqualTo(0L);
        Assertions.assertThat(jobs.getContent()).isEmpty();

        jobs = this.service.findJobs(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            UUID.randomUUID().toString(),
            null,
            null,
            null,
            null,
            null,
            null,
            page
        );
        Assertions.assertThat(jobs.getTotalElements()).isEqualTo(0L);
        Assertions.assertThat(jobs.getContent()).isEmpty();

        jobs = this.service.findJobs(
            null,
            null,
            null,
            null,
            null,
            null,
            clusterId,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            page
        );
        Assertions.assertThat(jobs.getTotalElements()).isEqualTo(5L);
        Assertions.assertThat(jobs.getContent().size()).isEqualTo(5);

        jobs = this.service
            .findJobs(
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                commandId,
                null,
                null,
                null,
                null,
                null,
                null,
                page
            );
        Assertions.assertThat(jobs.getTotalElements()).isEqualTo(5L);
        Assertions.assertThat(jobs.getContent().size()).isEqualTo(5);

        jobs = this.service.findJobs(
            null,
            null,
            null,
            null,
            null,
            null,
            clusterId,
            null,
            commandId,
            null,
            null,
            null,
            null,
            null,
            null,
            page
        );
        Assertions.assertThat(jobs.getTotalElements()).isEqualTo(5L);
        Assertions.assertThat(jobs.getContent().size()).isEqualTo(5);

        jobs = this.service.findJobs(
            null,
            null,
            null,
            null,
            null,
            clusterName,
            clusterId,
            commandName,
            commandId,
            null,
            null,
            null,
            null,
            null,
            null,
            page
        );
        Assertions.assertThat(jobs.getTotalElements()).isEqualTo(5L);
        Assertions.assertThat(jobs.getContent().size()).isEqualTo(5);

        jobs = this.service.findJobs(
            null,
            null,
            null,
            null,
            null,
            UUID.randomUUID().toString(),
            clusterId,
            commandName,
            commandId,
            null,
            null,
            null,
            null,
            null,
            null,
            page
        );
        Assertions.assertThat(jobs.getTotalElements()).isEqualTo(0L);
        Assertions.assertThat(jobs.getContent()).isEmpty();

        jobs = this.service.findJobs(
            null,
            null,
            null,
            null,
            null,
            clusterName,
            clusterId,
            UUID.randomUUID().toString(),
            commandId,
            null,
            null,
            null,
            null,
            null,
            null,
            page
        );
        Assertions.assertThat(jobs.getTotalElements()).isEqualTo(0L);
        Assertions.assertThat(jobs.getContent()).isEmpty();
    }

    @Test
    @DatabaseSetup("persistence/jobs/search.xml")
    void canFindActiveJobsByHostName() {
        final String hostA = "a.netflix.com";
        final String hostB = "b.netflix.com";
        final String hostC = "c.netflix.com";

        Assertions
            .assertThat(this.service.getAllActiveJobsOnHost(hostA))
            .hasSize(1)
            .extracting(BaseDTO::getId)
            .filteredOn(Optional::isPresent)
            .extracting(Optional::get)
            .containsOnly(JOB_2_ID);

        Assertions
            .assertThat(this.service.getAllActiveJobsOnHost(hostB))
            .hasSize(1)
            .extracting(BaseDTO::getId)
            .filteredOn(Optional::isPresent)
            .extracting(Optional::get)
            .containsOnly(JOB_3_ID);

        Assertions.assertThat(this.service.getAllActiveJobsOnHost(hostC)).isEmpty();
    }

    @Test
    @DatabaseSetup("persistence/jobs/search.xml")
    void canFindHostNamesOfActiveJobs() {
        final String hostA = "a.netflix.com";
        final String hostB = "b.netflix.com";

        Assertions
            .assertThat(this.service.getAllHostsWithActiveJobs())
            .hasSize(2)
            .containsExactlyInAnyOrder(hostA, hostB);
    }

    @Test
    @DatabaseSetup("persistence/jobs/search.xml")
    void canGetJobHost() throws NotFoundException {
        final String host = "a.netflix.com";

        Assertions
            .assertThat(this.service.getJobHost(JOB_1_ID))
            .isEqualTo(host);
        Assertions
            .assertThatExceptionOfType(NotFoundException.class)
            .isThrownBy(() -> this.service.getJobHost(UUID.randomUUID().toString()));
    }

    @Test
    @DatabaseSetup("persistence/jobs/search.xml")
    void canGetV3JobRequest() throws GenieException {
        final com.netflix.genie.common.dto.JobRequest job1Request = this.service.getV3JobRequest(JOB_1_ID);
        Assertions.assertThat(job1Request.getCommandArgs()).contains("-f query.q");
        Assertions.assertThat(job1Request.getClusterCriterias()).hasSize(2);
        Assertions
            .assertThat(job1Request.getClusterCriterias().get(0).getTags())
            .hasSize(4)
            .containsExactlyInAnyOrder("genie.id:cluster1", "genie.name:h2query", "sla", "yarn");
        Assertions
            .assertThat(job1Request.getClusterCriterias().get(1).getTags())
            .hasSize(2)
            .containsExactlyInAnyOrder("sla", "adhoc");
        Assertions
            .assertThat(job1Request.getCommandCriteria())
            .hasSize(3)
            .containsExactlyInAnyOrder("type:spark", "ver:1.6.0", "genie.name:spark");
        Assertions.assertThat(this.service.getV3JobRequest(JOB_2_ID).getCommandArgs()).contains("-f spark.jar");
        Assertions.assertThat(this.service.getV3JobRequest(JOB_3_ID).getCommandArgs()).contains("-f spark.jar");
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.service.getV3JobRequest(UUID.randomUUID().toString()));
    }

    @Test
    @DatabaseSetup("persistence/jobs/search.xml")
    void canGetJob() throws GenieException {
        Assertions.assertThat(this.service.getJob(JOB_1_ID).getName()).isEqualTo("testSparkJob");
        Assertions.assertThat(this.service.getJob(JOB_2_ID).getName()).isEqualTo("testSparkJob1");
        Assertions.assertThat(this.service.getJob(JOB_3_ID).getName()).isEqualTo("testSparkJob2");
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.service.getJob(UUID.randomUUID().toString()));
    }

    @Test
    @DatabaseSetup("persistence/jobs/search.xml")
    void canGetJobExecution() throws GenieException {
        Assertions.assertThat(this.service.getJobExecution(JOB_1_ID).getProcessId()).contains(317);
        Assertions.assertThat(this.service.getJobExecution(JOB_2_ID).getProcessId()).contains(318);
        Assertions.assertThat(this.service.getJobExecution(JOB_3_ID).getProcessId()).contains(319);
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.service.getJobExecution(UUID.randomUUID().toString()));
    }

    @Test
    @DatabaseSetup("persistence/jobs/search.xml")
    void canGetJobCluster() throws GenieCheckedException {
        Assertions.assertThat(this.service.getJobCluster(JOB_1_ID).getId()).contains("cluster1");
    }

    @Test
    @DatabaseSetup("persistence/jobs/search.xml")
    void canGetJobCommand() throws GenieCheckedException {
        Assertions.assertThat(this.service.getJobCommand(JOB_1_ID).getId()).contains("command1");
    }

    @Test
    @DatabaseSetup("persistence/jobs/search.xml")
    void canGetJobApplications() throws GenieCheckedException {
        Assertions
            .assertThat(this.service.getJobApplications(JOB_1_ID))
            .hasSize(2)
            .extracting(Application::getId)
            .containsExactly("app1", "app3");
        Assertions
            .assertThat(this.service.getJobApplications(JOB_2_ID))
            .hasSize(2)
            .extracting(Application::getId)
            .containsExactly("app1", "app2");
    }

    @Test
    @DatabaseSetup("persistence/jobs/search.xml")
    void canGetActiveJobCountForUser() {
        Assertions.assertThat(this.service.getActiveJobCountForUser("nobody")).isEqualTo(0L);
        Assertions.assertThat(this.service.getActiveJobCountForUser("tgianos")).isEqualTo(4L);
    }

    @Test
    @DatabaseSetup("persistence/jobs/search.xml")
    void canGetJobMetadata() throws GenieException {
        final com.netflix.genie.common.dto.JobMetadata jobMetadata = this.service.getJobMetadata(JOB_1_ID);
        Assertions.assertThat(jobMetadata.getClientHost()).isNotPresent();
        Assertions.assertThat(jobMetadata.getUserAgent()).isNotPresent();
        Assertions.assertThat(jobMetadata.getNumAttachments()).contains(2);
        Assertions.assertThat(jobMetadata.getTotalSizeOfAttachments()).contains(38083L);
        Assertions.assertThat(jobMetadata.getStdErrSize()).isNotPresent();
        Assertions.assertThat(jobMetadata.getStdOutSize()).isNotPresent();
    }

    @Test
    @DatabaseSetup("persistence/jobs/search.xml")
    void canGetUserResourceSummaries() {
        final Map<String, UserResourcesSummary> summaries = this.service.getUserResourcesSummaries(
            JobStatus.getActiveStatuses(),
            true
        );
        Assertions.assertThat(summaries.keySet()).contains("tgianos");
        final UserResourcesSummary userResourcesSummary = summaries.get("tgianos");
        Assertions.assertThat(userResourcesSummary.getUser()).isEqualTo("tgianos");
        Assertions.assertThat(userResourcesSummary.getRunningJobsCount()).isEqualTo(2L);
        Assertions.assertThat(userResourcesSummary.getUsedMemory()).isEqualTo(4096L);
    }

    @Test
    @DatabaseSetup("persistence/jobs/search.xml")
    void canGetUsedMemoryOnHost() {
        Assertions.assertThat(this.service.getUsedMemoryOnHost("a.netflix.com")).isEqualTo(2048L);
        Assertions.assertThat(this.service.getUsedMemoryOnHost("b.netflix.com")).isEqualTo(2048L);
        Assertions.assertThat(this.service.getUsedMemoryOnHost("agent.netflix.com")).isEqualTo(4096L);
        Assertions.assertThat(this.service.getUsedMemoryOnHost(UUID.randomUUID().toString())).isEqualTo(0L);
    }

    @Test
    @DatabaseSetup("persistence/jobs/search.xml")
    void canGetAgentActiveJobs() {
        Assertions
            .assertThat(this.service.getActiveAgentJobs())
            .hasSize(2)
            .containsExactlyInAnyOrder(AGENT_JOB_1, AGENT_JOB_2);
    }

    @Test
    @DatabaseSetup("persistence/jobs/unclaimed.xml")
    void canGetUnclaimedAgentJobs() {
        Assertions
            .assertThat(this.service.getUnclaimedAgentJobs())
            .hasSize(2)
            .containsExactlyInAnyOrder(AGENT_JOB_1, AGENT_JOB_2);
    }

    @Test
    @DatabaseSetup("persistence/jobs/getHostJobInformation/setup.xml")
    void canGetJobHostInformation() {
        final JobInfoAggregate aInfo = this.service.getHostJobInformation("a.netflix.com");
        Assertions.assertThat(aInfo.getNumberOfActiveJobs()).isEqualTo(1L);
        Assertions.assertThat(aInfo.getTotalMemoryAllocated()).isEqualTo(2048L);
        Assertions.assertThat(aInfo.getTotalMemoryUsed()).isEqualTo(2048L);
        final JobInfoAggregate bInfo = this.service.getHostJobInformation("b.netflix.com");
        Assertions.assertThat(bInfo.getNumberOfActiveJobs()).isEqualTo(1L);
        Assertions.assertThat(bInfo.getTotalMemoryAllocated()).isEqualTo(2048L);
        Assertions.assertThat(bInfo.getTotalMemoryUsed()).isEqualTo(2048L);
        final JobInfoAggregate agentInfo = this.service.getHostJobInformation("agent.netflix.com");
        Assertions.assertThat(agentInfo.getNumberOfActiveJobs()).isEqualTo(2L);
        Assertions.assertThat(agentInfo.getTotalMemoryAllocated()).isEqualTo(4096L);
        Assertions.assertThat(agentInfo.getTotalMemoryUsed()).isEqualTo(4096L);
        final JobInfoAggregate randomInfo = this.service.getHostJobInformation(UUID.randomUUID().toString());
        Assertions.assertThat(randomInfo.getNumberOfActiveJobs()).isEqualTo(0L);
        Assertions.assertThat(randomInfo.getTotalMemoryAllocated()).isEqualTo(0L);
        Assertions.assertThat(randomInfo.getTotalMemoryUsed()).isEqualTo(0L);
    }

    @Test
    @DatabaseSetup("persistence/jobs/archive_status.xml")
    void canGetFinishedJobsWithPendingArchiveStatus() {
        // This is the update timestamp of all jobs in the dataset
        final Instant lastJobUpdate = Instant.parse("2020-01-01T00:00:00.000Z");

        final Set<JobStatus> finishedJobStatuses = JobStatus.getFinishedStatuses();

        final HashSet<ArchiveStatus> pendingJobArchiveStatuses = Sets.newHashSet(ArchiveStatus.PENDING);

        final Instant notMatchingThreshold = lastJobUpdate.minus(1, ChronoUnit.SECONDS);
        final Instant matchingThreshold = lastJobUpdate.plus(1, ChronoUnit.SECONDS);

        Assertions
            .assertThat(
                this.service.getJobsWithStatusAndArchiveStatusUpdatedBefore(
                    finishedJobStatuses,
                    pendingJobArchiveStatuses,
                    lastJobUpdate
                )
            ).isEmpty();

        Assertions
            .assertThat(
                this.service.getJobsWithStatusAndArchiveStatusUpdatedBefore(
                    finishedJobStatuses,
                    pendingJobArchiveStatuses,
                    notMatchingThreshold
                )
            ).isEmpty();

        Assertions
            .assertThat(
                this.service.getJobsWithStatusAndArchiveStatusUpdatedBefore(
                    finishedJobStatuses,
                    pendingJobArchiveStatuses,
                    matchingThreshold
                )
            ).hasSize(2)
            .containsExactlyInAnyOrder(AGENT_JOB_1, AGENT_JOB_2);
    }

    private void validateJobRequest(final com.netflix.genie.common.dto.JobRequest savedJobRequest) {
        Assertions.assertThat(savedJobRequest.getId()).isPresent().contains(UNIQUE_ID);
        Assertions.assertThat(savedJobRequest.getName()).isEqualTo(NAME);
        Assertions.assertThat(savedJobRequest.getUser()).isEqualTo(USER);
        Assertions.assertThat(savedJobRequest.getVersion()).isEqualTo(VERSION);
        Assertions.assertThat(savedJobRequest.getDescription()).isPresent().contains(DESCRIPTION);
        Assertions.assertThat(savedJobRequest.getCommandArgs()).isPresent().contains(COMMAND_ARGS.get(0));
        Assertions.assertThat(savedJobRequest.getGroup()).isPresent().contains(GROUP);
        Assertions.assertThat(savedJobRequest.getSetupFile()).isPresent().contains(SETUP_FILE);
        Assertions.assertThat(savedJobRequest.getClusterCriterias()).isEqualTo(CLUSTER_CRITERIA);
        Assertions.assertThat(savedJobRequest.getCommandCriteria()).isEqualTo(COMMAND_CRITERION);
        Assertions.assertThat(savedJobRequest.getConfigs()).isEqualTo(CONFIGS);
        Assertions.assertThat(savedJobRequest.getDependencies()).isEqualTo(DEPENDENCIES);
        Assertions.assertThat(savedJobRequest.isDisableLogArchival()).isTrue();
        Assertions.assertThat(savedJobRequest.getEmail()).isPresent().contains(EMAIL);
        Assertions.assertThat(savedJobRequest.getTags()).isEqualTo(TAGS);
        Assertions.assertThat(savedJobRequest.getCpu()).isPresent().contains(CPU_REQUESTED);
        Assertions.assertThat(savedJobRequest.getMemory()).isPresent().contains(MEMORY_REQUESTED);
        Assertions.assertThat(savedJobRequest.getApplications()).isEqualTo(APPLICATIONS_REQUESTED);
        Assertions.assertThat(savedJobRequest.getTimeout()).isPresent().contains(TIMEOUT_REQUESTED);
        Assertions.assertThat(savedJobRequest.getGrouping()).isPresent().contains(GROUPING);
        Assertions.assertThat(savedJobRequest.getGroupingInstance()).isPresent().contains(GROUPING_INSTANCE);
    }

    private void validateJob(final Job savedJob) {
        Assertions.assertThat(savedJob.getId()).isPresent().contains(UNIQUE_ID);
        Assertions.assertThat(savedJob.getName()).isEqualTo(NAME);
        Assertions.assertThat(savedJob.getUser()).isEqualTo(USER);
        Assertions.assertThat(savedJob.getVersion()).isEqualTo(VERSION);
        Assertions.assertThat(savedJob.getDescription()).isPresent().contains(DESCRIPTION);
        Assertions.assertThat(savedJob.getCommandArgs()).isPresent().contains(COMMAND_ARGS.get(0));
        Assertions.assertThat(savedJob.getTags()).isEqualTo(TAGS);
        Assertions.assertThat(savedJob.getArchiveLocation()).isPresent().contains(ARCHIVE_LOCATION);
        Assertions.assertThat(savedJob.getStarted()).isPresent().contains(STARTED);
        Assertions.assertThat(savedJob.getFinished()).isPresent().contains(FINISHED);
        Assertions
            .assertThat(savedJob.getStatus())
            .isEqualByComparingTo(com.netflix.genie.common.dto.JobStatus.RUNNING);
        Assertions.assertThat(savedJob.getStatusMsg()).isPresent().contains(STATUS_MSG);
        Assertions.assertThat(savedJob.getGrouping()).isPresent().contains(GROUPING);
        Assertions.assertThat(savedJob.getGroupingInstance()).isPresent().contains(GROUPING_INSTANCE);
    }

    private void validateJobExecution(final JobExecution savedJobExecution) {
        Assertions.assertThat(savedJobExecution.getHostName()).isEqualTo(HOSTNAME);
        Assertions.assertThat(savedJobExecution.getProcessId()).isPresent().contains(PROCESS_ID);
        Assertions.assertThat(savedJobExecution.getCheckDelay()).contains(CHECK_DELAY);
        Assertions.assertThat(savedJobExecution.getTimeout()).contains(TIMEOUT);
        Assertions.assertThat(savedJobExecution.getMemory()).contains(MEMORY);
        Assertions.assertThat(savedJobExecution.getArchiveStatus()).isEqualTo(Optional.of(ArchiveStatus.DISABLED));
    }

    private void validateJobMetadata(final JobMetadataProjection savedMetadata) {
        Assertions.assertThat(savedMetadata.getRequestApiClientHostname()).contains(CLIENT_HOST);
        Assertions.assertThat(savedMetadata.getRequestApiClientUserAgent()).contains(USER_AGENT);
        Assertions.assertThat(savedMetadata.getNumAttachments()).contains(NUM_ATTACHMENTS);
        Assertions.assertThat(savedMetadata.getTotalSizeOfAttachments()).contains(TOTAL_SIZE_ATTACHMENTS);
        Assertions.assertThat(savedMetadata.getStdErrSize()).contains(STD_ERR_SIZE);
        Assertions.assertThat(savedMetadata.getStdOutSize()).contains(STD_OUT_SIZE);
    }

    private void validateSavedJobSubmission(
        final String id,
        final JobRequest jobRequest,
        final JobRequestMetadata jobRequestMetadata
    ) throws GenieCheckedException {
        Assertions.assertThat(this.service.getJobRequest(id)).isEqualTo(jobRequest);
        // TODO: Switch to compare results of a get once implemented to avoid collection transaction problem
        final JobEntity jobEntity = this.jobRepository
            .findByUniqueId(id)
            .orElseThrow(() -> new NotFoundException("No job with id " + id + " found when one was expected"));

        Assertions.assertThat(jobEntity.isResolved()).isFalse();
        Assertions.assertThat(jobEntity.isV4()).isTrue();

        // Job Request Metadata Fields
        jobRequestMetadata.getApiClientMetadata().ifPresent(
            apiClientMetadata -> {
                apiClientMetadata.getHostname().ifPresent(
                    hostname -> Assertions.assertThat(jobEntity.getRequestApiClientHostname()).contains(hostname)
                );
                apiClientMetadata.getUserAgent().ifPresent(
                    userAgent -> Assertions.assertThat(jobEntity.getRequestApiClientUserAgent()).contains(userAgent)
                );
            }
        );
        jobRequestMetadata.getAgentClientMetadata().ifPresent(
            apiClientMetadata -> {
                apiClientMetadata.getHostname().ifPresent(
                    hostname -> Assertions.assertThat(jobEntity.getRequestAgentClientHostname()).contains(hostname)
                );
                apiClientMetadata.getVersion().ifPresent(
                    version -> Assertions.assertThat(jobEntity.getRequestAgentClientVersion()).contains(version)
                );
                apiClientMetadata.getPid().ifPresent(
                    pid -> Assertions.assertThat(jobEntity.getRequestAgentClientPid()).contains(pid)
                );
            }
        );
        Assertions.assertThat(jobEntity.getNumAttachments()).contains(jobRequestMetadata.getNumAttachments());
        Assertions
            .assertThat(jobEntity.getTotalSizeOfAttachments())
            .contains(jobRequestMetadata.getTotalSizeOfAttachments());
    }

    private JobRequest createJobRequest(
        @Nullable final String requestedId,
        @Nullable final String requestedArchivalLocationPrefix
    ) throws IOException {
        final String metadata = "{\"" + UUID.randomUUID().toString() + "\": \"" + UUID.randomUUID().toString() + "\"}";
        final JobMetadata jobMetadata = new JobMetadata
            .Builder(UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString())
            .withMetadata(metadata)
            .withEmail(UUID.randomUUID().toString() + "@" + UUID.randomUUID().toString() + ".com")
            .withGroup(UUID.randomUUID().toString())
            .withGrouping(UUID.randomUUID().toString())
            .withGroupingInstance(UUID.randomUUID().toString())
            .withDescription(UUID.randomUUID().toString())
            .withTags(Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
            .build();

        final List<Criterion> clusterCriteria = Lists.newArrayList(
            new Criterion
                .Builder()
                .withId(UUID.randomUUID().toString())
                .withName(UUID.randomUUID().toString())
                .withStatus(ClusterStatus.OUT_OF_SERVICE.toString())
                .withTags(Sets.newHashSet(UUID.randomUUID().toString()))
                .withVersion(UUID.randomUUID().toString())
                .build(),
            new Criterion
                .Builder()
                .withId(UUID.randomUUID().toString())
                .withName(UUID.randomUUID().toString())
                .withStatus(ClusterStatus.UP.toString())
                .withTags(Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
                .withVersion(UUID.randomUUID().toString())
                .build()
        );
        final Criterion commandCriterion = new Criterion
            .Builder()
            .withId(UUID.randomUUID().toString())
            .withName(UUID.randomUUID().toString())
            .withStatus(CommandStatus.ACTIVE.toString())
            .withTags(Sets.newHashSet(UUID.randomUUID().toString()))
            .withVersion(UUID.randomUUID().toString())
            .build();
        final List<String> requestedApplications = Lists.newArrayList(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );
        final ExecutionResourceCriteria criteria = new ExecutionResourceCriteria(
            clusterCriteria,
            commandCriterion,
            requestedApplications
        );

        final ExecutionEnvironment executionEnvironment = new ExecutionEnvironment(
            Sets.newHashSet(UUID.randomUUID().toString()),
            Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString()),
            UUID.randomUUID().toString()
        );

        final Map<String, String> requestedEnvironmentVariables = ImmutableMap.of(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );
        final String agentEnvironmentExt
            = "{"
            + "\"" + UUID.randomUUID().toString() + "\": \"" + UUID.randomUUID().toString() + "\", "
            + "\"" + UUID.randomUUID().toString() + "\": \"\""
            + "}";
        final JobEnvironmentRequest jobEnvironmentRequest = new JobEnvironmentRequest
            .Builder()
            .withRequestedEnvironmentVariables(requestedEnvironmentVariables)
            .withExt(GenieObjectMapper.getMapper().readTree(agentEnvironmentExt))
            .withRequestedJobCpu(CPU_REQUESTED)
            .withRequestedJobMemory(MEMORY_REQUESTED)
            .build();

        final String agentConfigExt
            = "{\"" + UUID.randomUUID().toString() + "\": \"" + UUID.randomUUID().toString() + "\"}";
        final String requestedJobDirectoryLocation = "/tmp/" + UUID.randomUUID().toString();
        final AgentConfigRequest agentConfigRequest = new AgentConfigRequest
            .Builder()
            .withExt(GenieObjectMapper.getMapper().readTree(agentConfigExt))
            .withInteractive(true)
            .withTimeoutRequested(TIMEOUT_REQUESTED)
            .withArchivingDisabled(true)
            .withRequestedJobDirectoryLocation(requestedJobDirectoryLocation)
            .build();

        return new JobRequest(
            requestedId,
            executionEnvironment,
            Lists.newArrayList(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()
            ),
            jobMetadata,
            criteria,
            jobEnvironmentRequest,
            agentConfigRequest
        );
    }

    private JobRequestMetadata createJobRequestMetadata(
        final boolean api,
        final int numAttachments,
        final long totalAttachmentSize
    ) {
        if (!api) {
            final String agentVersion = UUID.randomUUID().toString();
            final int agentPid = RandomSuppliers.INT.get();
            final AgentClientMetadata agentClientMetadata = new AgentClientMetadata(
                UUID.randomUUID().toString(),
                agentVersion,
                agentPid
            );

            return new JobRequestMetadata(
                null,
                agentClientMetadata,
                numAttachments,
                totalAttachmentSize
            );
        } else {
            final ApiClientMetadata apiClientMetadata = new ApiClientMetadata(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()
            );

            return new JobRequestMetadata(
                apiClientMetadata,
                null,
                numAttachments,
                totalAttachmentSize
            );
        }
    }

    private JobSpecification createJobSpecification(
        final String jobId,
        final JobRequest jobRequest,
        @Nullable final String archiveLocation,
        @Nullable final Integer timeout
    ) throws GenieCheckedException {
        final String clusterId = "cluster1";
        final String commandId = "command1";
        final String application0Id = "app1";
        final String application1Id = "app2";

        final Cluster cluster = this.service.getCluster(clusterId);
        final Command command = this.service.getCommand(commandId);
        final Application application0 = this.service.getApplication(application0Id);
        final Application application1 = this.service.getApplication(application1Id);

        final Map<String, String> environmentVariables = ImmutableMap.of(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );

        final File jobDirectoryLocation = new File("/tmp/genie/jobs/" + jobId);

        return new JobSpecification(
            command.getExecutable(),
            jobRequest.getCommandArgs(),
            new JobSpecification.ExecutionResource(
                jobId,
                jobRequest.getResources()
            ),
            new JobSpecification.ExecutionResource(
                clusterId,
                cluster.getResources()
            ),
            new JobSpecification.ExecutionResource(
                commandId,
                command.getResources()
            ),
            Lists.newArrayList(
                new JobSpecification.ExecutionResource(
                    application0Id,
                    application0.getResources()
                ),
                new JobSpecification.ExecutionResource(
                    application1Id,
                    application1.getResources()
                )
            ),
            environmentVariables,
            jobRequest.getRequestedAgentConfig().isInteractive(),
            jobDirectoryLocation,
            ARCHIVE_LOCATION,
            timeout
        );
    }
}
