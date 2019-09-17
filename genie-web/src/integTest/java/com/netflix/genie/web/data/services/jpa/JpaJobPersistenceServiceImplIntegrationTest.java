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
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.ClusterCriteria;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.internal.dto.v4.AgentClientMetadata;
import com.netflix.genie.common.internal.dto.v4.AgentConfigRequest;
import com.netflix.genie.common.internal.dto.v4.ApiClientMetadata;
import com.netflix.genie.common.internal.dto.v4.Application;
import com.netflix.genie.common.internal.dto.v4.Cluster;
import com.netflix.genie.common.internal.dto.v4.Command;
import com.netflix.genie.common.internal.dto.v4.Criterion;
import com.netflix.genie.common.internal.dto.v4.ExecutionEnvironment;
import com.netflix.genie.common.internal.dto.v4.ExecutionResourceCriteria;
import com.netflix.genie.common.internal.dto.v4.FinishedJob;
import com.netflix.genie.common.internal.dto.v4.JobArchivalDataRequest;
import com.netflix.genie.common.internal.dto.v4.JobEnvironment;
import com.netflix.genie.common.internal.dto.v4.JobEnvironmentRequest;
import com.netflix.genie.common.internal.dto.v4.JobMetadata;
import com.netflix.genie.common.internal.dto.v4.JobRequest;
import com.netflix.genie.common.internal.dto.v4.JobRequestMetadata;
import com.netflix.genie.common.internal.dto.v4.JobSpecification;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieInvalidStatusException;
import com.netflix.genie.common.util.GenieObjectMapper;
import com.netflix.genie.test.suppliers.RandomSuppliers;
import com.netflix.genie.web.data.entities.JobEntity;
import com.netflix.genie.web.data.entities.projections.JobMetadataProjection;
import com.netflix.genie.web.data.services.ApplicationPersistenceService;
import com.netflix.genie.web.data.services.ClusterPersistenceService;
import com.netflix.genie.web.data.services.CommandPersistenceService;
import com.netflix.genie.web.data.services.JobPersistenceService;
import com.netflix.genie.web.data.services.JobSearchService;
import com.netflix.genie.web.dtos.JobSubmission;
import com.netflix.genie.web.dtos.ResolvedJob;
import com.netflix.genie.web.exceptions.checked.IdAlreadyExistsException;
import com.netflix.genie.web.exceptions.checked.SaveAttachmentException;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
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
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Integration tests for {@link JpaJobPersistenceServiceImpl}.
 *
 * @author tgianos
 * @since 3.0.0
 */
@DatabaseSetup("JpaJobPersistenceServiceImplIntegrationTest/init.xml")
@DatabaseTearDown("cleanup.xml")
public class JpaJobPersistenceServiceImplIntegrationTest extends DBIntegrationTestBase {

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
    private static final JobStatus STATUS = JobStatus.RUNNING;
    private static final String STATUS_MSG = UUID.randomUUID().toString();

    // Job Execution fields
    private static final String HOSTNAME = UUID.randomUUID().toString();
    private static final int PROCESS_ID = 3203;
    private static final long CHECK_DELAY = 8728L;
    private static final Instant TIMEOUT = STARTED.plus(50L, ChronoUnit.SECONDS);
    private static final int MEMORY = 2048;

    /**
     * Creates a temporary folder to use for these tests that is cleaned up after tests are run.
     */
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Autowired
    private JobPersistenceService jobPersistenceService;
    @Autowired
    private JobSearchService jobSearchService;
    @Autowired
    private ClusterPersistenceService clusterPersistenceService;
    @Autowired
    private CommandPersistenceService commandPersistenceService;
    @Autowired
    private ApplicationPersistenceService applicationPersistenceService;

    /**
     * Setup.
     */
    @Before
    public void setup() {
        Assert.assertThat(this.jobRepository.count(), Matchers.is(3L));
    }

    /**
     * Make sure we can delete jobs that were created before a given date.
     */
    @Test
    public void canDeleteJobsCreatedBeforeDateWithMinTransactionAndPageSize() {
        final Instant cal = ZonedDateTime
            .of(2016, Month.JANUARY.getValue(), 1, 0, 0, 0, 0, ZoneId.of("UTC"))
            .toInstant();

        final long deleted = this.jobPersistenceService.deleteBatchOfJobsCreatedBeforeDate(cal, 1, 1);

        Assert.assertThat(deleted, Matchers.is(1L));
        Assert.assertThat(this.jobRepository.count(), Matchers.is(2L));
        Assert.assertTrue(this.jobRepository.findByUniqueId(JOB_3_ID).isPresent());
    }

    /**
     * Make sure we can delete jobs that were created before a given date.
     */
    @Test
    public void canDeleteJobsCreatedBeforeDateWithPageLargerThanMax() {
        final Instant cal = ZonedDateTime
            .of(2016, Month.JANUARY.getValue(), 1, 0, 0, 0, 0, ZoneId.of("UTC"))
            .toInstant();

        final long deleted = this.jobPersistenceService.deleteBatchOfJobsCreatedBeforeDate(cal, 1, 10);

        Assert.assertThat(deleted, Matchers.is(2L));
        Assert.assertThat(this.jobRepository.count(), Matchers.is(1L));
        Assert.assertTrue(this.jobRepository.findByUniqueId(JOB_3_ID).isPresent());
    }

    /**
     * Make sure we can delete jobs that were created before a given date.
     */
    @Test
    public void canDeleteJobsCreatedBeforeDateWithMaxLargerThanPage() {
        final Instant cal = ZonedDateTime
            .of(2016, Month.JANUARY.getValue(), 1, 0, 0, 0, 0, ZoneId.of("UTC"))
            .toInstant();

        final long deleted = this.jobPersistenceService.deleteBatchOfJobsCreatedBeforeDate(cal, 10, 1);

        Assert.assertThat(deleted, Matchers.is(2L));
        Assert.assertThat(this.jobRepository.count(), Matchers.is(1L));
        Assert.assertTrue(this.jobRepository.findByUniqueId(JOB_3_ID).isPresent());
    }

    /**
     * Make sure we can delete jobs that were created before a given date.
     */
    @Test
    public void canDeleteJobsCreatedBeforeDateWithLargeTransactionAndPageSize() {
        final Instant cal = ZonedDateTime
            .of(2016, Month.JANUARY.getValue(), 1, 0, 0, 0, 0, ZoneId.of("UTC"))
            .toInstant();

        final long deleted = this.jobPersistenceService.deleteBatchOfJobsCreatedBeforeDate(cal, 10_000, 1);

        Assert.assertThat(deleted, Matchers.is(2L));
        Assert.assertThat(this.jobRepository.count(), Matchers.is(1L));
        Assert.assertTrue(this.jobRepository.findByUniqueId(JOB_3_ID).isPresent());
    }

    /**
     * Make sure a job can be saved AND criterion are saved properly.
     *
     * @throws GenieException on error
     */
    @Test
    public void canPersistAndGetAJob() throws GenieException {
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
            .withStatus(STATUS)
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

        Assert.assertThat(this.jobRepository.count(), Matchers.is(3L));
        Assert.assertThat(this.tagRepository.count(), Matchers.is(17L));
        Assert.assertThat(this.fileRepository.count(), Matchers.is(11L));
        this.jobPersistenceService.createJob(jobRequest, jobMetadata, job, jobExecution);
        Assert.assertThat(this.jobRepository.count(), Matchers.is(4L));
        Assert.assertThat(this.tagRepository.count(), Matchers.is(25L));
        Assert.assertThat(this.fileRepository.count(), Matchers.is(15L));

        Assert.assertFalse(
            this.jobRepository.findByUniqueId(UNIQUE_ID).orElseThrow(IllegalArgumentException::new).isV4()
        );

        this.validateJobRequest(this.jobSearchService.getJobRequest(UNIQUE_ID));
        this.validateJob(this.jobSearchService.getJob(UNIQUE_ID));
        this.validateJobExecution(this.jobSearchService.getJobExecution(UNIQUE_ID));

        final Optional<JobMetadataProjection> metadataProjection
            = this.jobRepository.findByUniqueId(UNIQUE_ID, JobMetadataProjection.class);
        Assert.assertTrue(metadataProjection.isPresent());
        this.validateJobMetadata(metadataProjection.get());
    }

    /**
     * Test the V4 {@link JobPersistenceService#saveJobSubmission(JobSubmission)} method.
     *
     * @throws GeniePreconditionException On error creating a job request
     * @throws IdAlreadyExistsException   on error saving to database on id conflict
     * @throws SaveAttachmentException    When the attachment can't be saved to the implementation of AttachmentService
     * @throws IOException                on filesystem error
     * @throws GenieException             On any other error
     */
    @Test
    public void canSaveAndVerifyJobSubmissionWithoutAttachments() throws
        GeniePreconditionException,
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
        Assert.assertThat(id, Matchers.is(job0Id));
        this.validateSavedJobSubmission(id, jobRequest0, jobRequestMetadata);

        id = this.jobPersistenceService.saveJobSubmission(
            new JobSubmission.Builder(jobRequest1, jobRequestMetadata).build()
        );
        Assert.assertThat(id, Matchers.notNullValue());
        this.validateSavedJobSubmission(id, jobRequest1, jobRequestMetadata);

        try {
            this.jobPersistenceService.saveJobSubmission(
                new JobSubmission.Builder(jobRequest2, jobRequestMetadata).build()
            );
            Assert.fail();
        } catch (final IdAlreadyExistsException e) {
            // Expected
        }

        id = this.jobPersistenceService.saveJobSubmission(
            new JobSubmission.Builder(jobRequest3, jobRequestMetadata).build()
        );
        Assert.assertThat(id, Matchers.is(job3Id));
        this.validateSavedJobSubmission(id, jobRequest3, jobRequestMetadata);
    }

    /**
     * Test the V4 {@link JobPersistenceService#saveJobSubmission(JobSubmission)} method with attachments.
     *
     * @throws GeniePreconditionException On error creating a job request
     * @throws IdAlreadyExistsException   on error saving to database on id conflict
     * @throws SaveAttachmentException    When the attachment can't be saved to the implementation of AttachmentService
     * @throws IOException                on filesystem error
     */
    @Test
    public void canSaveAndVerifyJobSubmissionWithAttachments() throws
        GeniePreconditionException,
        IdAlreadyExistsException,
        SaveAttachmentException,
        IOException {
        final JobRequest jobRequest = this.createJobRequest(null, null);
        final Path attachmentSource = this.folder.newFolder().toPath();
        final int numAttachments = 6;
        long totalAttachmentSize = 0L;
        final Set<Resource> attachments = Sets.newHashSet();
        for (int i = 0; i < numAttachments; i++) {
            final Path attachment = attachmentSource.resolve(UUID.randomUUID().toString());
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
    public void canSaveAndRetrieveJobSpecification() throws
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
            new JobEnvironment.Builder(1_512).build()
        );

        this.jobPersistenceService.saveResolvedJob(jobId, resolvedJob);
        Assert.assertThat(
            this.jobPersistenceService.getJobSpecification(jobId).orElse(null),
            Matchers.is(jobSpecification)
        );

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
            new JobEnvironment.Builder(1_1512).build()
        );
        this.jobPersistenceService.saveResolvedJob(jobId2, resolvedJob1);
        Assert.assertThat(
            this.jobPersistenceService.getJobSpecification(jobId2).orElse(null),
            Matchers.is(jobSpecification2)
        );
    }

    /**
     * Make sure {@link JpaJobPersistenceServiceImpl#claimJob(String, AgentClientMetadata)} works as expected.
     *
     * @throws GenieException           on error
     * @throws IOException              on json error
     * @throws IdAlreadyExistsException If the job ID is already taken
     * @throws SaveAttachmentException  If the attachments can't be saved.
     */
    @Test
    public void canClaimJobAndUpdateStatus() throws
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
            new JobEnvironment.Builder(1_512).build()
        );
        this.jobPersistenceService.saveResolvedJob(jobId, resolvedJob);

        final JobEntity preClaimedJob = this.jobRepository
            .findByUniqueId(jobId)
            .orElseThrow(IllegalArgumentException::new);

        Assertions.assertThat(preClaimedJob.isApi()).isTrue();
        Assert.assertThat(preClaimedJob.getStatus(), Matchers.is(JobStatus.RESOLVED));
        Assert.assertTrue(preClaimedJob.isResolved());
        Assert.assertFalse(preClaimedJob.isClaimed());
        Assert.assertThat(preClaimedJob.getAgentHostname(), Matchers.is(Optional.empty()));
        Assert.assertThat(preClaimedJob.getAgentVersion(), Matchers.is(Optional.empty()));
        Assert.assertThat(preClaimedJob.getAgentPid(), Matchers.is(Optional.empty()));

        final String agentHostname = UUID.randomUUID().toString();
        final String agentVersion = UUID.randomUUID().toString();
        final int agentPid = RandomSuppliers.INT.get();
        final AgentClientMetadata agentClientMetadata = new AgentClientMetadata(agentHostname, agentVersion, agentPid);

        this.jobPersistenceService.claimJob(jobId, agentClientMetadata);

        final JobEntity postClaimedJob = this.jobRepository
            .findByUniqueId(jobId)
            .orElseThrow(IllegalArgumentException::new);

        Assert.assertThat(postClaimedJob.getStatus(), Matchers.is(JobStatus.CLAIMED));
        Assert.assertTrue(postClaimedJob.isResolved());
        Assert.assertTrue(postClaimedJob.isClaimed());
        Assert.assertThat(postClaimedJob.getAgentHostname(), Matchers.is(Optional.of(agentHostname)));
        Assert.assertThat(postClaimedJob.getAgentVersion(), Matchers.is(Optional.of(agentVersion)));
        Assert.assertThat(postClaimedJob.getAgentPid(), Matchers.is(Optional.of(agentPid)));
    }

    /**
     * Test the {@link JpaJobPersistenceServiceImpl#updateJobStatus(String, JobStatus, JobStatus, String)} method.
     *
     * @throws GenieException           on error
     * @throws IOException              on error
     * @throws IdAlreadyExistsException If the job ID is already taken
     * @throws SaveAttachmentException  If the attachments can't be saved
     */
    @Test
    public void canUpdateJobStatus() throws
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
            new JobEnvironment.Builder(1_512).build()
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

        Assert.assertThat(jobEntity.getStatus(), Matchers.is(JobStatus.CLAIMED));

        try {
            this.jobPersistenceService.updateJobStatus(jobId, JobStatus.RUNNING, JobStatus.FAILED, null);
            Assert.fail();
        } catch (final GenieInvalidStatusException e) {
            // status won't match so it will throw exception
        }

        final String initStatusMessage = "Job is initializing";
        this.jobPersistenceService.updateJobStatus(jobId, JobStatus.CLAIMED, JobStatus.INIT, initStatusMessage);

        jobEntity = this.jobRepository
            .findByUniqueId(jobId)
            .orElseThrow(IllegalArgumentException::new);

        Assertions.assertThat(jobEntity.isApi()).isFalse();
        Assert.assertThat(jobEntity.getStatus(), Matchers.is(JobStatus.INIT));
        Assert.assertThat(jobEntity.getStatusMsg(), Matchers.is(Optional.of(initStatusMessage)));
        Assert.assertFalse(jobEntity.getStarted().isPresent());
        Assert.assertFalse(jobEntity.getFinished().isPresent());

        final String runningStatusMessage = "Job is running";
        this.jobPersistenceService.updateJobStatus(jobId, JobStatus.INIT, JobStatus.RUNNING, runningStatusMessage);

        jobEntity = this.jobRepository
            .findByUniqueId(jobId)
            .orElseThrow(IllegalArgumentException::new);

        Assert.assertThat(jobEntity.getStatus(), Matchers.is(JobStatus.RUNNING));
        Assert.assertThat(jobEntity.getStatusMsg(), Matchers.is(Optional.of(runningStatusMessage)));
        Assert.assertTrue(jobEntity.getStarted().isPresent());
        Assert.assertFalse(jobEntity.getFinished().isPresent());

        final String successStatusMessage = "Job completed successfully";
        this.jobPersistenceService.updateJobStatus(jobId, JobStatus.RUNNING, JobStatus.SUCCEEDED, successStatusMessage);

        jobEntity = this.jobRepository
            .findByUniqueId(jobId)
            .orElseThrow(IllegalArgumentException::new);

        Assert.assertThat(jobEntity.getStatus(), Matchers.is(JobStatus.SUCCEEDED));
        Assert.assertThat(jobEntity.getStatusMsg(), Matchers.is(Optional.of(successStatusMessage)));
        Assert.assertTrue(jobEntity.getStarted().isPresent());
        Assert.assertTrue(jobEntity.getFinished().isPresent());
    }

    /**
     * Test {@link JpaJobPersistenceServiceImpl#getJobStatus(String)}.
     *
     * @throws GenieNotFoundException when the job doesn't exist but should
     */
    @Test
    public void canGetJobStatus() throws GenieNotFoundException {
        try {
            this.jobPersistenceService.getJobStatus(UUID.randomUUID().toString());
            Assert.fail("Should have thrown GenieNotFoundException");
        } catch (final GenieNotFoundException e) {
            // expected
        }
        Assert.assertThat(this.jobPersistenceService.getJobStatus(JOB_1_ID), Matchers.is(JobStatus.SUCCEEDED));
        Assert.assertThat(this.jobPersistenceService.getJobStatus(JOB_2_ID), Matchers.is(JobStatus.RUNNING));
        Assert.assertThat(this.jobPersistenceService.getJobStatus(JOB_3_ID), Matchers.is(JobStatus.RUNNING));
    }

    /**
     * Test {@link JpaJobPersistenceServiceImpl#getJobArchiveLocation(String)}.
     *
     * @throws GenieNotFoundException if the test is broken
     */
    @Test
    public void canGetJobArchiveLocation() throws GenieNotFoundException {
        try {
            this.jobPersistenceService.getJobArchiveLocation(UUID.randomUUID().toString());
            Assert.fail();
        } catch (final GenieNotFoundException gnfe) {
            // expected
        }

        Assert.assertFalse(this.jobPersistenceService.getJobArchiveLocation(JOB_3_ID).isPresent());
        Assert.assertThat(
            this.jobPersistenceService.getJobArchiveLocation(JOB_1_ID).orElseThrow(IllegalStateException::new),
            Matchers.is("s3://somebucket/genie/logs/1/")
        );
    }

    /**
     * Test {@link JpaJobPersistenceServiceImpl#getFinishedJob(String)}.
     *
     * @throws GenieNotFoundException if the test works
     */
    @Test(expected = GenieNotFoundException.class)
    public void canGetFinishedJobNonExistent() throws GenieNotFoundException {
        this.jobPersistenceService.getFinishedJob(UUID.randomUUID().toString());
        Assert.fail();
    }

    /**
     * Test {@link JpaJobPersistenceServiceImpl#getFinishedJob(String)}.
     *
     * @throws GenieNotFoundException if the test is broken
     */
    @Test(expected = GenieInvalidStatusException.class)
    public void canGetFinishedJobNotFinished() throws GenieNotFoundException {
        this.jobPersistenceService.getFinishedJob(JOB_3_ID);
        Assert.fail();
    }

    /**
     * Test {@link JpaJobPersistenceServiceImpl#getFinishedJob(String)}.
     *
     * @throws GenieNotFoundException if the test is broken
     */
    @Test
    public void canGetFinishedJob() throws GenieNotFoundException {
        final FinishedJob finishedJob = this.jobPersistenceService.getFinishedJob(JOB_1_ID);
        Assert.assertNotNull(finishedJob);

        Assert.assertThat(finishedJob.getUniqueId(), Matchers.is(JOB_1_ID));
        Assert.assertThat(finishedJob.getUser(), Matchers.is("tgianos"));
        Assert.assertThat(finishedJob.getName(), Matchers.is("testSparkJob"));
        Assert.assertThat(finishedJob.getVersion(), Matchers.is("2.4"));
        Assert.assertNotNull(finishedJob.getCreated());
        Assert.assertThat(finishedJob.getStatus(), Matchers.is(JobStatus.SUCCEEDED));
        Assert.assertThat(finishedJob.getCommandArgs().size(), Matchers.is(2));
        Assert.assertNotNull(finishedJob.getCommandCriterion());
        Assert.assertThat(finishedJob.getClusterCriteria().size(), Matchers.is(2));
        Assert.assertFalse(finishedJob.getStarted().isPresent());
        Assert.assertFalse(finishedJob.getFinished().isPresent());
        Assert.assertFalse(finishedJob.getGrouping().isPresent());
        Assert.assertFalse(finishedJob.getGroupingInstance().isPresent());
        Assert.assertFalse(finishedJob.getStatusMessage().isPresent());
        Assert.assertThat(finishedJob.getRequestedMemory().orElse(0), Matchers.is(1560));
        Assert.assertFalse(finishedJob.getRequestApiClientHostname().isPresent());
        Assert.assertFalse(finishedJob.getRequestApiClientUserAgent().isPresent());
        Assert.assertFalse(finishedJob.getRequestAgentClientHostname().isPresent());
        Assert.assertFalse(finishedJob.getRequestAgentClientVersion().isPresent());
        Assert.assertThat(finishedJob.getNumAttachments().orElse(0), Matchers.is(2));
        Assert.assertThat(finishedJob.getExitCode().orElse(-1), Matchers.is(0));
        Assert.assertThat(finishedJob.getArchiveLocation().orElse(""), Matchers.is("s3://somebucket/genie/logs/1/"));
        Assert.assertFalse(finishedJob.getMemoryUsed().isPresent());

        final Command command = finishedJob.getCommand().orElse(null);
        final Cluster cluster = finishedJob.getCluster().orElse(null);
        final List<Application> applications = finishedJob.getApplications();

        Assert.assertNotNull(command);
        Assert.assertNotNull(cluster);
        Assert.assertNotNull(applications);
        Assert.assertThat(command.getMetadata().getName(), Matchers.is("spark"));
        Assert.assertThat(cluster.getMetadata().getName(), Matchers.is("h2query"));
        Assert.assertThat(applications.size(), Matchers.is(2));
        Assert.assertThat(applications.get(0).getMetadata().getName(), Matchers.is("hadoop"));
        Assert.assertThat(applications.get(1).getMetadata().getName(), Matchers.is("spark"));
    }

    /**
     * Make sure the {@link JpaJobPersistenceServiceImpl#isApiJob(String)} API behaves as expected against existing
     * data.
     *
     * @throws GenieNotFoundException If a job with the given ID doesn't exist
     */
    @Test
    public void canDetermineIfIsApiJob() throws GenieNotFoundException {
        Assertions.assertThat(this.jobPersistenceService.isApiJob(JOB_1_ID)).isFalse();
        Assertions.assertThat(this.jobPersistenceService.isApiJob(JOB_2_ID)).isTrue();
        Assertions.assertThat(this.jobPersistenceService.isApiJob(JOB_3_ID)).isTrue();
    }

    private void validateJobRequest(final com.netflix.genie.common.dto.JobRequest savedJobRequest) {
        Assert.assertThat(
            savedJobRequest.getId().orElseThrow(IllegalArgumentException::new),
            Matchers.is(UNIQUE_ID)
        );
        Assert.assertThat(savedJobRequest.getName(), Matchers.is(NAME));
        Assert.assertThat(savedJobRequest.getUser(), Matchers.is(USER));
        Assert.assertThat(savedJobRequest.getVersion(), Matchers.is(VERSION));
        Assert.assertThat(
            savedJobRequest.getDescription().orElseThrow(IllegalArgumentException::new),
            Matchers.is(DESCRIPTION)
        );
        Assert.assertThat(
            savedJobRequest.getCommandArgs().orElseThrow(IllegalArgumentException::new),
            Matchers.is(COMMAND_ARGS.get(0))
        );
        Assert.assertThat(
            savedJobRequest.getGroup().orElseThrow(IllegalArgumentException::new),
            Matchers.is(GROUP)
        );
        Assert.assertThat(
            savedJobRequest.getSetupFile().orElseThrow(IllegalArgumentException::new),
            Matchers.is(SETUP_FILE)
        );
        Assert.assertThat(savedJobRequest.getClusterCriterias(), Matchers.is(CLUSTER_CRITERIA));
        Assert.assertThat(savedJobRequest.getCommandCriteria(), Matchers.is(COMMAND_CRITERION));
        Assert.assertThat(savedJobRequest.getConfigs(), Matchers.is(CONFIGS));
        Assert.assertThat(savedJobRequest.getDependencies(), Matchers.is(DEPENDENCIES));
        Assert.assertTrue(savedJobRequest.isDisableLogArchival());
        Assert.assertThat(
            savedJobRequest.getEmail().orElseThrow(IllegalArgumentException::new),
            Matchers.is(EMAIL));
        Assert.assertThat(savedJobRequest.getTags(), Matchers.is(TAGS));
        Assert.assertThat(
            savedJobRequest.getCpu().orElseThrow(IllegalArgumentException::new),
            Matchers.is(CPU_REQUESTED)
        );
        Assert.assertThat(
            savedJobRequest.getMemory().orElseThrow(IllegalArgumentException::new),
            Matchers.is(MEMORY_REQUESTED)
        );
        Assert.assertThat(savedJobRequest.getApplications(), Matchers.is(APPLICATIONS_REQUESTED));
        Assert.assertThat(
            savedJobRequest.getTimeout().orElseThrow(IllegalArgumentException::new),
            Matchers.is(TIMEOUT_REQUESTED)
        );
        Assert.assertThat(
            savedJobRequest.getGrouping().orElseThrow(IllegalArgumentException::new),
            Matchers.is(GROUPING)
        );
        Assert.assertThat(
            savedJobRequest.getGroupingInstance().orElseThrow(IllegalArgumentException::new),
            Matchers.is(GROUPING_INSTANCE)
        );
    }

    private void validateJob(final Job savedJob) {
        Assert.assertThat(savedJob.getId().orElseThrow(IllegalArgumentException::new), Matchers.is(UNIQUE_ID));
        Assert.assertThat(savedJob.getName(), Matchers.is(NAME));
        Assert.assertThat(savedJob.getUser(), Matchers.is(USER));
        Assert.assertThat(savedJob.getVersion(), Matchers.is(VERSION));
        Assert.assertThat(
            savedJob.getDescription().orElseThrow(IllegalArgumentException::new),
            Matchers.is(DESCRIPTION)
        );
        Assert.assertThat(
            savedJob.getCommandArgs().orElseThrow(IllegalArgumentException::new),
            Matchers.is(COMMAND_ARGS.get(0))
        );
        Assert.assertThat(savedJob.getTags(), Matchers.is(TAGS));
        Assert.assertThat(
            savedJob.getArchiveLocation().orElseThrow(IllegalArgumentException::new),
            Matchers.is(ARCHIVE_LOCATION)
        );
        Assert.assertThat(
            savedJob.getStarted().orElseThrow(IllegalArgumentException::new),
            Matchers.is(STARTED)
        );
        Assert.assertThat(
            savedJob.getFinished().orElseThrow(IllegalArgumentException::new),
            Matchers.is(FINISHED)
        );
        Assert.assertThat(savedJob.getStatus(), Matchers.is(STATUS));
        Assert.assertThat(
            savedJob.getStatusMsg().orElseThrow(IllegalArgumentException::new),
            Matchers.is(STATUS_MSG)
        );
        Assert.assertThat(
            savedJob.getGrouping().orElseThrow(IllegalArgumentException::new),
            Matchers.is(GROUPING)
        );
        Assert.assertThat(
            savedJob.getGroupingInstance().orElseThrow(IllegalArgumentException::new),
            Matchers.is(GROUPING_INSTANCE)
        );
    }

    private void validateJobExecution(final JobExecution savedJobExecution) {
        Assert.assertThat(savedJobExecution.getHostName(), Matchers.is(HOSTNAME));
        Assert.assertThat(
            savedJobExecution.getProcessId().orElseThrow(IllegalArgumentException::new),
            Matchers.is(PROCESS_ID)
        );
        Assert.assertThat(
            savedJobExecution.getCheckDelay().orElseThrow(IllegalArgumentException::new),
            Matchers.is(CHECK_DELAY)
        );
        Assert.assertThat(
            savedJobExecution.getTimeout().orElseThrow(IllegalArgumentException::new),
            Matchers.is(TIMEOUT)
        );
        Assert.assertThat(
            savedJobExecution.getMemory().orElseThrow(IllegalArgumentException::new),
            Matchers.is(MEMORY)
        );
    }

    private void validateJobMetadata(final JobMetadataProjection savedMetadata) {
        Assert.assertThat(
            savedMetadata.getRequestApiClientHostname().orElseThrow(IllegalArgumentException::new),
            Matchers.is(CLIENT_HOST)
        );
        Assert.assertThat(
            savedMetadata.getRequestApiClientUserAgent().orElseThrow(IllegalArgumentException::new),
            Matchers.is(USER_AGENT)
        );
        Assert.assertThat(
            savedMetadata.getNumAttachments().orElseThrow(IllegalArgumentException::new),
            Matchers.is(NUM_ATTACHMENTS)
        );
        Assert.assertThat(
            savedMetadata.getTotalSizeOfAttachments().orElseThrow(IllegalArgumentException::new),
            Matchers.is(TOTAL_SIZE_ATTACHMENTS)
        );
        Assert.assertThat(
            savedMetadata.getStdErrSize().orElseThrow(IllegalArgumentException::new),
            Matchers.is(STD_ERR_SIZE)
        );
        Assert.assertThat(
            savedMetadata.getStdOutSize().orElseThrow(IllegalArgumentException::new),
            Matchers.is(STD_OUT_SIZE)
        );
    }

    private void validateSavedJobSubmission(
        final String id,
        final JobRequest jobRequest,
        final JobRequestMetadata jobRequestMetadata
    ) throws GenieException {
        Assert.assertThat(
            this.jobPersistenceService
                .getJobRequest(id)
                .orElseThrow(() -> new GenieNotFoundException("No job request with id " + id + " exists.")),
            Matchers.is(jobRequest)
        );
        // TODO: Switch to compare results of a get once implemented to avoid collection transaction problem
        final JobEntity jobEntity = this.jobRepository
            .findByUniqueId(id)
            .orElseThrow(() -> new GenieNotFoundException("No job with id " + id + " found when one was expected"));

        Assert.assertFalse(jobEntity.isResolved());
        Assert.assertTrue(jobEntity.isV4());

        // Job Request Metadata Fields
        jobRequestMetadata.getApiClientMetadata().ifPresent(
            apiClientMetadata -> {
                apiClientMetadata.getHostname().ifPresent(
                    hostname -> Assert.assertThat(
                        jobEntity.getRequestApiClientHostname().orElse(UUID.randomUUID().toString()),
                        Matchers.is(hostname)
                    )
                );
                apiClientMetadata.getUserAgent().ifPresent(
                    userAgent -> Assert.assertThat(
                        jobEntity.getRequestApiClientUserAgent().orElse(UUID.randomUUID().toString()),
                        Matchers.is(userAgent)
                    )
                );
            }
        );
        jobRequestMetadata.getAgentClientMetadata().ifPresent(
            apiClientMetadata -> {
                apiClientMetadata.getHostname().ifPresent(
                    hostname -> Assert.assertThat(
                        jobEntity.getRequestAgentClientHostname().orElse(UUID.randomUUID().toString()),
                        Matchers.is(hostname)
                    )
                );
                apiClientMetadata.getVersion().ifPresent(
                    version -> Assert.assertThat(
                        jobEntity.getRequestAgentClientVersion().orElse(UUID.randomUUID().toString()),
                        Matchers.is(version)
                    )
                );
                apiClientMetadata.getPid().ifPresent(
                    pid -> Assert.assertThat(jobEntity.getRequestAgentClientPid().orElse(-1), Matchers.is(pid))
                );
            }
        );
        Assert.assertThat(
            jobEntity.getNumAttachments().orElse(-1),
            Matchers.is(jobRequestMetadata.getNumAttachments())
        );
        Assert.assertThat(
            jobEntity.getTotalSizeOfAttachments().orElse(-1L),
            Matchers.is(jobRequestMetadata.getTotalSizeOfAttachments())
        );
    }

    private JobRequest createJobRequest(
        @Nullable final String requestedId,
        @Nullable final String requestedArchivalLocationPrefix
    ) throws GeniePreconditionException, IOException {
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
