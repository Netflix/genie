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
package com.netflix.genie.web.jpa.services;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.DatabaseTearDown;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.ClusterCriteria;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobMetadata;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.test.categories.IntegrationTest;
import com.netflix.genie.web.jpa.entities.projections.JobMetadataProjection;
import com.netflix.genie.web.jpa.repositories.JpaFileRepository;
import com.netflix.genie.web.jpa.repositories.JpaJobRepository;
import com.netflix.genie.web.jpa.repositories.JpaTagRepository;
import com.netflix.genie.web.services.JobPersistenceService;
import com.netflix.genie.web.services.JobSearchService;
import org.assertj.core.util.Lists;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Instant;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Integration tests for JpaJobPersistenceImpl.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(IntegrationTest.class)
@DatabaseSetup("JpaJobPersistenceServiceImplIntegrationTests/init.xml")
@DatabaseTearDown("cleanup.xml")
public class JpaJobPersistenceImplIntegrationTests extends DBUnitTestBase {

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

    // Job Execution fields
    private static final String HOST_NAME = UUID.randomUUID().toString();
    private static final int PROCESS_ID = 3203;
    private static final long CHECK_DELAY = 8728L;
    private static final Instant TIMEOUT = Instant.now();
    private static final int MEMORY = 2048;

    // Job fields
    private static final String ARCHIVE_LOCATION = UUID.randomUUID().toString();
    private static final Instant FINISHED = Instant.now();
    private static final Instant STARTED = Instant.now();
    private static final JobStatus STATUS = JobStatus.RUNNING;
    private static final String STATUS_MSG = UUID.randomUUID().toString();

    @Autowired
    private JpaJobRepository jobRepository;
    @Autowired
    private JpaFileRepository fileRepository;
    @Autowired
    private JpaTagRepository tagRepository;
    @Autowired
    private JobPersistenceService jobPersistenceService;
    @Autowired
    private JobSearchService jobSearchService;

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

        final JobRequest jobRequest = new JobRequest.Builder(
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

        final JobMetadata jobMetadata = new JobMetadata.Builder()
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

        final JobExecution jobExecution = new JobExecution.Builder(HOST_NAME)
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

        this.validateJobRequest(this.jobSearchService.getJobRequest(UNIQUE_ID));
        this.validateJob(this.jobSearchService.getJob(UNIQUE_ID));
        this.validateJobExecution(this.jobSearchService.getJobExecution(UNIQUE_ID));

        final Optional<JobMetadataProjection> metadataProjection
            = this.jobRepository.findByUniqueId(UNIQUE_ID, JobMetadataProjection.class);
        Assert.assertTrue(metadataProjection.isPresent());
        this.validateJobMetadata(metadataProjection.get());
    }

    private void validateJobRequest(final JobRequest savedJobRequest) {
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
        Assert.assertThat(savedJobExecution.getHostName(), Matchers.is(HOST_NAME));
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
            savedMetadata.getClientHost().orElseThrow(IllegalArgumentException::new),
            Matchers.is(CLIENT_HOST)
        );
        Assert.assertThat(
            savedMetadata.getUserAgent().orElseThrow(IllegalArgumentException::new),
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
}
