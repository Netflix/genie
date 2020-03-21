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
package com.netflix.genie.web.data.services.jpa;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.DatabaseTearDown;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.ClusterCriteria;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.external.dtos.v4.AgentClientMetadata;
import com.netflix.genie.common.external.dtos.v4.AgentConfigRequest;
import com.netflix.genie.common.external.dtos.v4.ApiClientMetadata;
import com.netflix.genie.common.external.dtos.v4.Application;
import com.netflix.genie.common.external.dtos.v4.Cluster;
import com.netflix.genie.common.external.dtos.v4.Command;
import com.netflix.genie.common.external.dtos.v4.Criterion;
import com.netflix.genie.common.external.dtos.v4.ExecutionEnvironment;
import com.netflix.genie.common.external.dtos.v4.ExecutionResourceCriteria;
import com.netflix.genie.common.external.dtos.v4.JobArchivalDataRequest;
import com.netflix.genie.common.external.dtos.v4.JobEnvironment;
import com.netflix.genie.common.external.dtos.v4.JobEnvironmentRequest;
import com.netflix.genie.common.external.dtos.v4.JobMetadata;
import com.netflix.genie.common.external.dtos.v4.JobRequest;
import com.netflix.genie.common.external.dtos.v4.JobRequestMetadata;
import com.netflix.genie.common.external.dtos.v4.JobSpecification;
import com.netflix.genie.common.external.dtos.v4.JobStatus;
import com.netflix.genie.common.external.util.GenieObjectMapper;
import com.netflix.genie.common.internal.dtos.v4.FinishedJob;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieInvalidStatusException;
import com.netflix.genie.test.suppliers.RandomSuppliers;
import com.netflix.genie.web.data.entities.JobEntity;
import com.netflix.genie.web.data.entities.projections.JobMetadataProjection;
import com.netflix.genie.web.data.services.ApplicationPersistenceService;
import com.netflix.genie.web.data.services.ClusterPersistenceService;
import com.netflix.genie.web.data.services.CommandPersistenceService;
import com.netflix.genie.web.data.services.JobSearchService;
import com.netflix.genie.web.dtos.JobSubmission;
import com.netflix.genie.web.dtos.ResolvedJob;
import com.netflix.genie.web.exceptions.checked.IdAlreadyExistsException;
import com.netflix.genie.web.exceptions.checked.SaveAttachmentException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Integration tests for {@link JpaJobPersistenceServiceImpl}.
 *
 * @author tgianos
 * @since 3.0.0
 */
@DatabaseTearDown("cleanup.xml")
class JpaJobPersistenceServiceImplIntegrationTest extends DBIntegrationTestBase {

    private static final String JOB_1_ID = "job1";
    private static final String JOB_2_ID = "job2";
    private static final String JOB_3_ID = "job3";

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

    @Autowired
    private JpaJobPersistenceServiceImpl jobPersistenceService;
    @Autowired
    private JobSearchService jobSearchService;
    @Autowired
    private ClusterPersistenceService clusterPersistenceService;
    @Autowired
    private CommandPersistenceService commandPersistenceService;
    @Autowired
    private ApplicationPersistenceService applicationPersistenceService;

    @BeforeEach
    void setup() {
        Assertions.assertThat(this.jobRepository.count()).isEqualTo(3L);
    }

    @Test
    @DatabaseSetup("JpaJobPersistenceServiceImplIntegrationTest/init.xml")
    void canDeleteJobsCreatedBeforeDateWithMinTransactionAndPageSize() {
        final Instant cal = ZonedDateTime
            .of(2016, Month.JANUARY.getValue(), 1, 0, 0, 0, 0, ZoneId.of("UTC"))
            .toInstant();

        final long deleted = this.jobPersistenceService.deleteBatchOfJobsCreatedBeforeDate(cal, 1, 1);

        Assertions.assertThat(deleted).isEqualTo(1L);
        Assertions.assertThat(this.jobRepository.count()).isEqualTo(2L);
        Assertions.assertThat(this.jobRepository.findByUniqueId(JOB_3_ID)).isPresent();
    }

    @Test
    @DatabaseSetup("JpaJobPersistenceServiceImplIntegrationTest/init.xml")
    void canDeleteJobsCreatedBeforeDateWithPageLargerThanMax() {
        final Instant cal = ZonedDateTime
            .of(2016, Month.JANUARY.getValue(), 1, 0, 0, 0, 0, ZoneId.of("UTC"))
            .toInstant();

        final long deleted = this.jobPersistenceService.deleteBatchOfJobsCreatedBeforeDate(cal, 1, 10);

        Assertions.assertThat(deleted).isEqualTo(2L);
        Assertions.assertThat(this.jobRepository.count()).isEqualTo(1L);
        Assertions.assertThat(this.jobRepository.findByUniqueId(JOB_3_ID)).isPresent();
    }

    @Test
    @DatabaseSetup("JpaJobPersistenceServiceImplIntegrationTest/init.xml")
    void canDeleteJobsCreatedBeforeDateWithMaxLargerThanPage() {
        final Instant cal = ZonedDateTime
            .of(2016, Month.JANUARY.getValue(), 1, 0, 0, 0, 0, ZoneId.of("UTC"))
            .toInstant();

        final long deleted = this.jobPersistenceService.deleteBatchOfJobsCreatedBeforeDate(cal, 10, 1);

        Assertions.assertThat(deleted).isEqualTo(2L);
        Assertions.assertThat(this.jobRepository.count()).isEqualTo(1L);
        Assertions.assertThat(this.jobRepository.findByUniqueId(JOB_3_ID)).isPresent();
    }

    @Test
    @DatabaseSetup("JpaJobPersistenceServiceImplIntegrationTest/init.xml")
    void canDeleteJobsCreatedBeforeDateWithLargeTransactionAndPageSize() {
        final Instant cal = ZonedDateTime
            .of(2016, Month.JANUARY.getValue(), 1, 0, 0, 0, 0, ZoneId.of("UTC"))
            .toInstant();

        final long deleted = this.jobPersistenceService.deleteBatchOfJobsCreatedBeforeDate(cal, 10_000, 1);

        Assertions.assertThat(deleted).isEqualTo(2L);
        Assertions.assertThat(this.jobRepository.count()).isEqualTo(1L);
        Assertions.assertThat(this.jobRepository.findByUniqueId(JOB_3_ID)).isPresent();
    }

    @Test
    @DatabaseSetup("JpaJobPersistenceServiceImplIntegrationTest/init.xml")
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
            .build();

        Assertions.assertThat(this.jobRepository.count()).isEqualTo(3L);
        Assertions.assertThat(this.tagRepository.count()).isEqualTo(17L);
        Assertions.assertThat(this.fileRepository.count()).isEqualTo(11L);
        this.jobPersistenceService.createJob(jobRequest, jobMetadata, job, jobExecution);
        Assertions.assertThat(this.jobRepository.count()).isEqualTo(4L);
        Assertions.assertThat(this.tagRepository.count()).isEqualTo(25L);
        Assertions.assertThat(this.fileRepository.count()).isEqualTo(15L);

        Assertions
            .assertThat(this.jobRepository.findByUniqueId(UNIQUE_ID))
            .isPresent()
            .get()
            .extracting(JobEntity::isV4)
            .isEqualTo(false);

        this.validateJobRequest(this.jobSearchService.getJobRequest(UNIQUE_ID));
        this.validateJob(this.jobSearchService.getJob(UNIQUE_ID));
        this.validateJobExecution(this.jobSearchService.getJobExecution(UNIQUE_ID));

        Assertions
            .assertThat(this.jobRepository.findByUniqueId(UNIQUE_ID, JobMetadataProjection.class))
            .isPresent()
            .get()
            .satisfies(this::validateJobMetadata);
    }

    @Test
    @DatabaseSetup("JpaJobPersistenceServiceImplIntegrationTest/init.xml")
    void canSaveAndVerifyJobSubmissionWithoutAttachments() throws
        IdAlreadyExistsException,
        SaveAttachmentException,
        IOException,
        GenieException {
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

        String id = this.jobPersistenceService.saveJobSubmission(
            new JobSubmission.Builder(jobRequest0, jobRequestMetadata).build()
        );
        Assertions.assertThat(id).isEqualTo(job0Id);
        this.validateSavedJobSubmission(id, jobRequest0, jobRequestMetadata);

        id = this.jobPersistenceService.saveJobSubmission(
            new JobSubmission.Builder(jobRequest1, jobRequestMetadata).build()
        );
        Assertions.assertThat(id).isNotBlank();
        this.validateSavedJobSubmission(id, jobRequest1, jobRequestMetadata);

        Assertions
            .assertThatExceptionOfType(IdAlreadyExistsException.class)
            .isThrownBy(
                () -> this.jobPersistenceService.saveJobSubmission(
                    new JobSubmission.Builder(jobRequest2, jobRequestMetadata).build()
                )
            );

        id = this.jobPersistenceService.saveJobSubmission(
            new JobSubmission.Builder(jobRequest3, jobRequestMetadata).build()
        );
        Assertions.assertThat(id).isEqualTo(job3Id);
        this.validateSavedJobSubmission(id, jobRequest3, jobRequestMetadata);
    }

    @Test
    @DatabaseSetup("JpaJobPersistenceServiceImplIntegrationTest/init.xml")
    void canSaveAndVerifyJobSubmissionWithAttachments(@TempDir final Path tempDir) throws
        IdAlreadyExistsException,
        SaveAttachmentException,
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
        final JobRequestMetadata jobRequestMetadata = this.createJobRequestMetadata(
            true,
            numAttachments,
            totalAttachmentSize
        );

        // Save the job submission
        final String id = this.jobPersistenceService.saveJobSubmission(
            new JobSubmission.Builder(jobRequest, jobRequestMetadata).withAttachments(attachments).build()
        );

        // TODO: Assumption on bean type of the AttachmentService to be the local file system
        // Going to assume that most other verification of parameters other than attachments is done in
        // canSaveAndVerifyJobSubmissionWithoutAttachments()
        final JobRequest savedJobRequest = this.jobPersistenceService
            .getJobRequest(id)
            .orElseThrow(IllegalStateException::new);

        // We should have all the original dependencies
        Assertions
            .assertThat(savedJobRequest.getResources().getDependencies())
            .containsAll(jobRequest.getResources().getDependencies());

        // Filter out the original dependencies so we're left with just the attachments
        final Set<URI> attachmentURIs = savedJobRequest
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

        Assertions.assertThat(attachmentURIs).size().isEqualTo(numAttachments);
        long finalAttachmentsSize = 0L;
        for (final URI attachmentURI : attachmentURIs) {
            final Path attachment = Paths.get(attachmentURI);
            Assertions.assertThat(Files.exists(attachment)).isTrue();
            finalAttachmentsSize += Files.size(attachment);
        }
        Assertions.assertThat(finalAttachmentsSize).isEqualTo(totalAttachmentSize);
    }

    /**
     * Make sure saving and retrieving a job specification works as expected.
     *
     * @throws GenieException           on error
     * @throws IOException              on json error
     * @throws IdAlreadyExistsException If the job ID is already taken
     * @throws SaveAttachmentException  If the attachments can't be saved. God I hate checked exceptions.
     */
    @Test
    @DatabaseSetup("JpaJobPersistenceServiceImplIntegrationTest/init.xml")
    void canSaveAndRetrieveJobSpecification() throws
        GenieException,
        IdAlreadyExistsException,
        SaveAttachmentException,
        IOException {
        final String jobId = this.jobPersistenceService.saveJobSubmission(
            new JobSubmission.Builder(
                this.createJobRequest(null, UUID.randomUUID().toString()),
                this.createJobRequestMetadata(false, NUM_ATTACHMENTS, TOTAL_SIZE_ATTACHMENTS)
            ).build()
        );

        final JobRequest jobRequest = this.jobPersistenceService
            .getJobRequest(jobId)
            .orElseThrow(IllegalArgumentException::new);

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

        this.jobPersistenceService.saveResolvedJob(jobId, resolvedJob);
        Assertions
            .assertThat(this.jobPersistenceService.getJobSpecification(jobId))
            .isPresent()
            .contains(jobSpecification);

        final String jobId2 = this.jobPersistenceService.saveJobSubmission(
            new JobSubmission.Builder(
                this.createJobRequest(null, null),
                this.createJobRequestMetadata(false, NUM_ATTACHMENTS, TOTAL_SIZE_ATTACHMENTS)
            ).build()
        );

        final JobRequest jobRequest2 = this.jobPersistenceService
            .getJobRequest(jobId2)
            .orElseThrow(IllegalArgumentException::new);

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
        this.jobPersistenceService.saveResolvedJob(jobId2, resolvedJob1);
        Assertions
            .assertThat(this.jobPersistenceService.getJobSpecification(jobId2))
            .isPresent()
            .contains(jobSpecification2);
    }

    @Test
    @DatabaseSetup("JpaJobPersistenceServiceImplIntegrationTest/init.xml")
    void canClaimJobAndUpdateStatus() throws
        GenieException,
        IdAlreadyExistsException,
        SaveAttachmentException,
        IOException {
        final String jobId = this.jobPersistenceService.saveJobSubmission(
            new JobSubmission.Builder(
                this.createJobRequest(null, UUID.randomUUID().toString()),
                this.createJobRequestMetadata(true, NUM_ATTACHMENTS, TOTAL_SIZE_ATTACHMENTS)
            ).build()
        );

        final JobRequest jobRequest = this.jobPersistenceService
            .getJobRequest(jobId)
            .orElseThrow(IllegalArgumentException::new);

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
        this.jobPersistenceService.saveResolvedJob(jobId, resolvedJob);

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

        this.jobPersistenceService.claimJob(jobId, agentClientMetadata);

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
    @DatabaseSetup("JpaJobPersistenceServiceImplIntegrationTest/init.xml")
    void canUpdateJobStatus() throws
        GenieException,
        IdAlreadyExistsException,
        SaveAttachmentException,
        IOException {
        final String jobId = this.jobPersistenceService.saveJobSubmission(
            new JobSubmission.Builder(
                this.createJobRequest(null, UUID.randomUUID().toString()),
                this.createJobRequestMetadata(false, NUM_ATTACHMENTS, TOTAL_SIZE_ATTACHMENTS)
            ).build()
        );

        final JobRequest jobRequest = this.jobPersistenceService
            .getJobRequest(jobId)
            .orElseThrow(IllegalArgumentException::new);

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
        this.jobPersistenceService.saveResolvedJob(jobId, resolvedJob);

        final String agentHostname = UUID.randomUUID().toString();
        final String agentVersion = UUID.randomUUID().toString();
        final int agentPid = RandomSuppliers.INT.get();
        final AgentClientMetadata agentClientMetadata = new AgentClientMetadata(agentHostname, agentVersion, agentPid);

        this.jobPersistenceService.claimJob(jobId, agentClientMetadata);

        JobEntity jobEntity = this.jobRepository
            .findByUniqueId(jobId)
            .orElseThrow(IllegalArgumentException::new);

        Assertions.assertThat(jobEntity.getStatus()).isEqualTo(JobStatus.CLAIMED.name());

        // status won't match so it will throw exception
        Assertions
            .assertThatExceptionOfType(GenieInvalidStatusException.class)
            .isThrownBy(
                () -> this.jobPersistenceService.updateJobStatus(
                    jobId,
                    JobStatus.RUNNING,
                    JobStatus.FAILED,
                    null
                )
            );

        final String initStatusMessage = "Job is initializing";
        this.jobPersistenceService.updateJobStatus(jobId, JobStatus.CLAIMED, JobStatus.INIT, initStatusMessage);

        jobEntity = this.jobRepository
            .findByUniqueId(jobId)
            .orElseThrow(IllegalArgumentException::new);

        Assertions.assertThat(jobEntity.isApi()).isFalse();
        Assertions.assertThat(jobEntity.getStatus()).isEqualTo(JobStatus.INIT.name());
        Assertions.assertThat(jobEntity.getStatusMsg()).isPresent().contains(initStatusMessage);
        Assertions.assertThat(jobEntity.getStarted()).isNotPresent();
        Assertions.assertThat(jobEntity.getFinished()).isNotPresent();

        final String runningStatusMessage = "Job is running";
        this.jobPersistenceService.updateJobStatus(jobId, JobStatus.INIT, JobStatus.RUNNING, runningStatusMessage);

        jobEntity = this.jobRepository
            .findByUniqueId(jobId)
            .orElseThrow(IllegalArgumentException::new);

        Assertions.assertThat(jobEntity.getStatus()).isEqualTo(JobStatus.RUNNING.name());
        Assertions.assertThat(jobEntity.getStatusMsg()).isPresent().contains(runningStatusMessage);
        Assertions.assertThat(jobEntity.getStarted()).isPresent();
        Assertions.assertThat(jobEntity.getFinished()).isNotPresent();

        final String successStatusMessage = "Job completed successfully";
        this.jobPersistenceService.updateJobStatus(jobId, JobStatus.RUNNING, JobStatus.SUCCEEDED, successStatusMessage);

        jobEntity = this.jobRepository
            .findByUniqueId(jobId)
            .orElseThrow(IllegalArgumentException::new);

        Assertions.assertThat(jobEntity.getStatus()).isEqualTo(JobStatus.SUCCEEDED.name());
        Assertions.assertThat(jobEntity.getStatusMsg()).isPresent().contains(successStatusMessage);
        Assertions.assertThat(jobEntity.getStarted()).isPresent();
        Assertions.assertThat(jobEntity.getFinished()).isPresent();
    }

    @Test
    @DatabaseSetup("JpaJobPersistenceServiceImplIntegrationTest/init.xml")
    void canGetJobStatus() throws GenieNotFoundException {
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.jobPersistenceService.getJobStatus(UUID.randomUUID().toString()));
        Assertions.assertThat(this.jobPersistenceService.getJobStatus(JOB_1_ID)).isEqualTo(JobStatus.SUCCEEDED);
        Assertions.assertThat(this.jobPersistenceService.getJobStatus(JOB_2_ID)).isEqualTo(JobStatus.RUNNING);
        Assertions.assertThat(this.jobPersistenceService.getJobStatus(JOB_3_ID)).isEqualTo(JobStatus.RUNNING);
    }

    @Test
    @DatabaseSetup("JpaJobPersistenceServiceImplIntegrationTest/init.xml")
    void canGetJobArchiveLocation() throws GenieNotFoundException {
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.jobPersistenceService.getJobArchiveLocation(UUID.randomUUID().toString()));

        Assertions.assertThat(this.jobPersistenceService.getJobArchiveLocation(JOB_3_ID)).isNotPresent();
        Assertions
            .assertThat(this.jobPersistenceService.getJobArchiveLocation(JOB_1_ID))
            .isPresent()
            .contains("s3://somebucket/genie/logs/1/");
    }

    @Test
    @DatabaseSetup("JpaJobPersistenceServiceImplIntegrationTest/init.xml")
    void canGetFinishedJobNonExistent() {
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.jobPersistenceService.getFinishedJob(UUID.randomUUID().toString()));
    }

    @Test
    @DatabaseSetup("JpaJobPersistenceServiceImplIntegrationTest/init.xml")
    void canGetFinishedJobNotFinished() {
        Assertions
            .assertThatExceptionOfType(GenieInvalidStatusException.class)
            .isThrownBy(() -> this.jobPersistenceService.getFinishedJob(JOB_3_ID));
    }

    @Test
    @DatabaseSetup("JpaJobPersistenceServiceImplIntegrationTest/init.xml")
    void canGetFinishedJob() throws GenieNotFoundException {
        final FinishedJob finishedJob = this.jobPersistenceService.getFinishedJob(JOB_1_ID);
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
    @DatabaseSetup("JpaJobPersistenceServiceImplIntegrationTest/init.xml")
    void canDetermineIfIsApiJob() throws GenieNotFoundException {
        Assertions.assertThat(this.jobPersistenceService.isApiJob(JOB_1_ID)).isFalse();
        Assertions.assertThat(this.jobPersistenceService.isApiJob(JOB_2_ID)).isTrue();
        Assertions.assertThat(this.jobPersistenceService.isApiJob(JOB_3_ID)).isTrue();
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
    ) throws GenieException {
        Assertions.assertThat(this.jobPersistenceService.getJobRequest(id)).contains(jobRequest);
        // TODO: Switch to compare results of a get once implemented to avoid collection transaction problem
        final JobEntity jobEntity = this.jobRepository
            .findByUniqueId(id)
            .orElseThrow(() -> new GenieNotFoundException("No job with id " + id + " found when one was expected"));

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
        final JobArchivalDataRequest jobArchivalDataRequest = new JobArchivalDataRequest
            .Builder()
            .withRequestedArchiveLocationPrefix(requestedArchivalLocationPrefix)
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
            agentConfigRequest,
            jobArchivalDataRequest
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
    ) throws GenieException {
        final String clusterId = "cluster1";
        final String commandId = "command1";
        final String application0Id = "app1";
        final String application1Id = "app2";

        final Cluster cluster = this.clusterPersistenceService.getCluster(clusterId);
        final Command command = this.commandPersistenceService.getCommand(commandId);
        final Application application0 = this.applicationPersistenceService.getApplication(application0Id);
        final Application application1 = this.applicationPersistenceService.getApplication(application1Id);

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
