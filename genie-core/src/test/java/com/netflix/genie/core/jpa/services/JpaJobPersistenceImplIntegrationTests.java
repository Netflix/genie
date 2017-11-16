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
package com.netflix.genie.core.jpa.services;

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
import com.netflix.genie.core.jobs.JobConstants;
import com.netflix.genie.core.jpa.repositories.JpaFileRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobRepository;
import com.netflix.genie.core.jpa.repositories.JpaTagRepository;
import com.netflix.genie.core.services.JobPersistenceService;
import com.netflix.genie.test.categories.IntegrationTest;
import org.assertj.core.util.Lists;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
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

    @Autowired
    private JpaJobRepository jobRepository;
    @Autowired
    private JpaFileRepository fileRepository;
    @Autowired
    private JpaTagRepository tagRepository;
    @Autowired
    private JobPersistenceService jobPersistenceService;

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
        // Try to delete a single job from before Jan 1, 2016
        final Calendar cal = Calendar.getInstance(JobConstants.UTC);
        cal.set(2016, Calendar.JANUARY, 1, 0, 0, 0);
        cal.set(Calendar.MILLISECOND, 0);

        final long deleted = this.jobPersistenceService.deleteBatchOfJobsCreatedBeforeDate(cal.getTime(), 1, 1);

        Assert.assertThat(deleted, Matchers.is(1L));
        Assert.assertThat(this.jobRepository.count(), Matchers.is(2L));
        Assert.assertTrue(this.jobRepository.findByUniqueId(JOB_3_ID).isPresent());
    }

    /**
     * Make sure we can delete jobs that were created before a given date.
     */
    @Test
    public void canDeleteJobsCreatedBeforeDateWithPageLargerThanMax() {
        // Try to delete a all jobs from before Jan 1, 2016
        final Calendar cal = Calendar.getInstance(JobConstants.UTC);
        cal.set(2016, Calendar.JANUARY, 1, 0, 0, 0);
        cal.set(Calendar.MILLISECOND, 0);

        final long deleted = this.jobPersistenceService.deleteBatchOfJobsCreatedBeforeDate(cal.getTime(), 1, 10);

        Assert.assertThat(deleted, Matchers.is(2L));
        Assert.assertThat(this.jobRepository.count(), Matchers.is(1L));
        Assert.assertTrue(this.jobRepository.findByUniqueId(JOB_3_ID).isPresent());
    }

    /**
     * Make sure we can delete jobs that were created before a given date.
     */
    @Test
    public void canDeleteJobsCreatedBeforeDateWithMaxLargerThanPage() {
        // Try to delete a all jobs from before Jan 1, 2016
        final Calendar cal = Calendar.getInstance(JobConstants.UTC);
        cal.set(2016, Calendar.JANUARY, 1, 0, 0, 0);
        cal.set(Calendar.MILLISECOND, 0);

        final long deleted = this.jobPersistenceService.deleteBatchOfJobsCreatedBeforeDate(cal.getTime(), 10, 1);

        Assert.assertThat(deleted, Matchers.is(2L));
        Assert.assertThat(this.jobRepository.count(), Matchers.is(1L));
        Assert.assertTrue(this.jobRepository.findByUniqueId(JOB_3_ID).isPresent());
    }

    /**
     * Make sure we can delete jobs that were created before a given date.
     */
    @Test
    public void canDeleteJobsCreatedBeforeDateWithLargeTransactionAndPageSize() {
        // Try to delete all jobs before Jan 1, 2016
        final Calendar cal = Calendar.getInstance(JobConstants.UTC);
        cal.set(2016, Calendar.JANUARY, 1, 0, 0, 0);
        cal.set(Calendar.MILLISECOND, 0);

        final long deleted = this.jobPersistenceService.deleteBatchOfJobsCreatedBeforeDate(cal.getTime(), 10_000, 1);

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

        // Job Request fields
        final String uniqueId = UUID.randomUUID().toString();
        final String name = UUID.randomUUID().toString();
        final String user = UUID.randomUUID().toString();
        final String version = UUID.randomUUID().toString();
        final String description = UUID.randomUUID().toString();
        final String commandArgs = UUID.randomUUID().toString();
        final String group = UUID.randomUUID().toString();
        final String setupFile = UUID.randomUUID().toString();
        final String clusterTag1 = UUID.randomUUID().toString();
        final String clusterTag2 = UUID.randomUUID().toString();
        final String clusterTag3 = UUID.randomUUID().toString();
        final List<ClusterCriteria> clusterCriteria = Lists.newArrayList(
            new ClusterCriteria(Sets.newHashSet(clusterTag1)),
            new ClusterCriteria(Sets.newHashSet(clusterTag2)),
            new ClusterCriteria(Sets.newHashSet(clusterTag3))
        );
        final String commandTag1 = UUID.randomUUID().toString();
        final String commandTag2 = UUID.randomUUID().toString();
        final Set<String> commandCriterion = Sets.newHashSet(commandTag1, commandTag2);
        final String config1 = UUID.randomUUID().toString();
        final String config2 = UUID.randomUUID().toString();
        final Set<String> configs = Sets.newHashSet(config1, config2);
        final String dependency = UUID.randomUUID().toString();
        final Set<String> dependencies = Sets.newHashSet(dependency);
        final boolean disableLogArchival = true;
        final String email = UUID.randomUUID().toString() + "@" + UUID.randomUUID().toString() + ".com";
        final String tag1 = UUID.randomUUID().toString();
        final String tag2 = UUID.randomUUID().toString();
        final String tag3 = UUID.randomUUID().toString();
        final Set<String> tags = Sets.newHashSet(tag1, tag2, tag3);
        final int cpuRequested = 2;
        final int memoryRequested = 1024;
        final String appRequested1 = UUID.randomUUID().toString();
        final String appRequested2 = UUID.randomUUID().toString();
        final List<String> applicationsRequested = Lists.newArrayList(appRequested1, appRequested2);
        final int timeoutRequested = 84500;

        // TODO: grouping and grouping instance

        final JobRequest jobRequest = new JobRequest.Builder(
            name, user, version, commandArgs, clusterCriteria, commandCriterion
        )
            .withId(uniqueId)
            .withDescription(description)
            .withGroup(group)
            .withSetupFile(setupFile)
            .withConfigs(configs)
            .withDependencies(dependencies)
            .withDisableLogArchival(disableLogArchival)
            .withEmail(email)
            .withTags(tags)
            .withCpu(cpuRequested)
            .withMemory(memoryRequested)
            .withTimeout(timeoutRequested)
            .withApplications(applicationsRequested)
            .build();

        // Job Metadata fields
        final String clientHost = UUID.randomUUID().toString();
        final String userAgent = UUID.randomUUID().toString();
        final int numAttachments = 3;
        final long totalSizeAttachments = 38023423L;
        final long stdErrSize = 98025245L;
        final long stdOutSize = 78723423L;

        final JobMetadata jobMetadata = new JobMetadata.Builder()
            .withClientHost(clientHost)
            .withUserAgent(userAgent)
            .withNumAttachments(numAttachments)
            .withTotalSizeOfAttachments(totalSizeAttachments)
            .withStdErrSize(stdErrSize)
            .withStdOutSize(stdOutSize)
            .build();

        // Job fields
        final String archiveLocation = UUID.randomUUID().toString();
        final Date finished = new Date();
        final Date started = new Date();
        final JobStatus status = JobStatus.RUNNING;
        final String statusMsg = UUID.randomUUID().toString();

        final Job job = new Job.Builder(
            name, user, version, commandArgs
        )
            .withId(uniqueId)
            .withDescription(description)
            .withTags(tags)
            .withArchiveLocation(archiveLocation)
            .withStarted(started)
            .withFinished(finished)
            .withStatus(status)
            .withStatusMsg(statusMsg)
            .build();

        // Job Execution fields
        final String hostName = UUID.randomUUID().toString();
        final int processId = 3203;
        final long checkDelay = 8728L;
        final Date timeout = new Date();
        final int memory = 2048;

        final JobExecution jobExecution = new JobExecution.Builder(hostName)
            .withId(uniqueId)
            .withCheckDelay(checkDelay)
            .withTimeout(timeout)
            .withMemory(memory)
            .withProcessId(processId)
            .build();

        Assert.assertThat(this.jobRepository.count(), Matchers.is(3L));
        Assert.assertThat(this.tagRepository.count(), Matchers.is(17L));
        Assert.assertThat(this.fileRepository.count(), Matchers.is(11L));
        this.jobPersistenceService.createJob(jobRequest, jobMetadata, job, jobExecution);
        Assert.assertThat(this.jobRepository.count(), Matchers.is(4L));
        Assert.assertThat(this.tagRepository.count(), Matchers.is(25L));
        Assert.assertThat(this.fileRepository.count(), Matchers.is(15L));
    }
}
