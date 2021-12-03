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
package com.netflix.genie.client;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.client.apis.SortAttribute;
import com.netflix.genie.client.apis.SortDirection;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.ClusterCriteria;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.dto.search.JobSearchResult;
import com.netflix.genie.common.external.dtos.v4.Criterion;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * Integration tests for {@link JobClient}.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Slf4j
abstract class JobClientIntegrationTest extends ClusterClientIntegrationTest {

    private static final String DATE_TAG = "type:date";
    private static final String ECHO_TAG = "type:echo";
    private static final String SLEEP_TAG = "type:sleep";
    private static final String DUMMY_TAG = "type:dummy";

    @SuppressWarnings("MethodLength")
    @Test
    void canSubmitJob() throws Exception {
        final String dummyClusterId = this.createDummyCluster();
        final String sleepCommandId = this.createSleepCommand();
        final String dateCommandId = this.createDateCommand();
        final String echoCommandId = this.createEchoCommand();

        final List<ClusterCriteria> clusterCriteriaList = Lists.newArrayList(
            new ClusterCriteria(Sets.newHashSet(DUMMY_TAG))
        );

        final JobRequest sleepJob = new JobRequest.Builder(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            clusterCriteriaList,
            Sets.newHashSet(SLEEP_TAG)
        )
            .withCommandArgs(Lists.newArrayList("1"))
            .withDisableLogArchival(true)
            .build();

        final JobRequest killJob = new JobRequest.Builder(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            clusterCriteriaList,
            Sets.newHashSet(SLEEP_TAG)
        )
            .withCommandArgs(Lists.newArrayList("60"))
            .withDisableLogArchival(true)
            .build();

        final JobRequest timeoutJob = new JobRequest.Builder(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            clusterCriteriaList,
            Sets.newHashSet(SLEEP_TAG)
        )
            .withCommandArgs(Lists.newArrayList("60"))
            .withTimeout(1)
            .withDisableLogArchival(true)
            .build();

        final JobRequest dateJob = new JobRequest.Builder(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            clusterCriteriaList,
            Sets.newHashSet(DATE_TAG)
        )
            .withDisableLogArchival(true)
            .build();

        final JobRequest echoJob = new JobRequest.Builder(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            clusterCriteriaList,
            Sets.newHashSet(ECHO_TAG)
        )
            .withCommandArgs(Lists.newArrayList("hello"))
            .withDisableLogArchival(true)
            .build();

        final String sleepJobId;
        final byte[] attachmentBytes = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        try (ByteArrayInputStream bis = new ByteArrayInputStream(attachmentBytes)) {
            final Map<String, InputStream> attachments = ImmutableMap.of("attachment.txt", bis);
            sleepJobId = this.jobClient.submitJobWithAttachments(sleepJob, attachments);
        }
        final String killJobId = this.jobClient.submitJob(killJob);
        final Thread killThread = new Thread(
            () -> {
                try {
                    while (this.jobClient.getJobStatus(killJobId) != JobStatus.RUNNING) {
                        Thread.sleep(10);
                    }
                    this.jobClient.killJob(killJobId);
                } catch (final Exception e) {
                    Assertions.fail(e.getMessage(), e);
                }
            }
        );
        killThread.start();
        final String timeoutJobId = this.jobClient.submitJob(timeoutJob);
        final String dateJobId = this.jobClient.submitJob(dateJob);
        final String echoJobId = this.jobClient.submitJob(echoJob);

        final Map<String, JobStatus> expectedStatuses = ImmutableMap.<String, JobStatus>builder()
            .put(sleepJobId, JobStatus.SUCCEEDED)
            .put(killJobId, JobStatus.KILLED)
            .put(timeoutJobId, JobStatus.KILLED)
            .put(dateJobId, JobStatus.SUCCEEDED)
            .put(echoJobId, JobStatus.SUCCEEDED)
            .build();

        final long waitStart = System.currentTimeMillis();
        final long maxTotalWait = 120000;

        for (final Map.Entry<String, JobStatus> entry : expectedStatuses.entrySet()) {
            final String jobId = entry.getKey();
            final JobStatus status = entry.getValue();

            log.info("Waiting for job: {} (expected final status: {})", jobId, status.name());

            final long timeElapsed = System.currentTimeMillis() - waitStart;
            final long timeLeft = maxTotalWait - timeElapsed;
            if (timeLeft <= 0) {
                throw new TimeoutException("Timed out waiting for jobs to complete");
            }

            Assertions
                .assertThat(this.jobClient.waitForCompletion(jobId, timeLeft, 100))
                .isEqualByComparingTo(entry.getValue());
        }

        // Some basic checking of fields
        Assertions.assertThat(this.jobClient.getJob(sleepJobId).getName()).isEqualTo(sleepJob.getName());
        Assertions.assertThat(this.jobClient.getJobRequest(sleepJobId).getUser()).isEqualTo(sleepJob.getUser());
        Assertions.assertThat(this.jobClient.getJobExecution(sleepJobId)).isNotNull();
        Assertions.assertThat(this.jobClient.getJobMetadata(sleepJobId)).isNotNull();
        Assertions.assertThat(this.jobClient.getJobCluster(sleepJobId).getId()).isPresent().contains(dummyClusterId);
        Assertions.assertThat(this.jobClient.getJobCommand(sleepJobId).getId()).isPresent().contains(sleepCommandId);
        Assertions.assertThat(this.jobClient.getJobCommand(dateJobId).getId()).isPresent().contains(dateCommandId);
        Assertions.assertThat(this.jobClient.getJobCommand(echoJobId).getId()).isPresent().contains(echoCommandId);
        Assertions.assertThat(this.jobClient.getJobApplications(sleepJobId)).isEmpty();
        Assertions
            .assertThat(IOUtils.toString(this.jobClient.getJobStdout(echoJobId), StandardCharsets.UTF_8))
            .isEqualTo("hello\n");
        Assertions
            .assertThat(IOUtils.toString(this.jobClient.getJobStdout(echoJobId, null, null), StandardCharsets.UTF_8))
            .isEqualTo("hello\n");
        Assertions
            .assertThat(IOUtils.toString(this.jobClient.getJobStdout(echoJobId, 4L, null), StandardCharsets.UTF_8))
            .isEqualTo("o\n");
        Assertions
            .assertThat(IOUtils.toString(this.jobClient.getJobStdout(echoJobId, 0L, 3L), StandardCharsets.UTF_8))
            .isEqualTo("hell");
        Assertions
            .assertThat(IOUtils.toString(this.jobClient.getJobStdout(echoJobId, null, 2L), StandardCharsets.UTF_8))
            .isEqualTo("o\n");
        Assertions
            .assertThat(IOUtils.toString(this.jobClient.getJobStderr(echoJobId), StandardCharsets.UTF_8))
            .isBlank();
        Assertions
            .assertThat(IOUtils.toString(
                this.jobClient.getJobOutputFile(echoJobId, "stdout"), StandardCharsets.UTF_8))
            .isEqualTo("hello\n");
        Assertions
            .assertThat(IOUtils.toString(
                this.jobClient.getJobOutputFile(echoJobId, "run"), StandardCharsets.UTF_8))
            .isNotBlank();
        Assertions
            .assertThat(IOUtils.toString(
                this.jobClient.getJobOutputFile(echoJobId, ""), StandardCharsets.UTF_8))
            .isNotBlank();
        Assertions
            .assertThat(IOUtils.toString(
                this.jobClient.getJobOutputFile(echoJobId, null), StandardCharsets.UTF_8))
            .isNotBlank();

        // Some quick find jobs calls
        Assertions
            .assertThat(this.jobClient.getJobs())
            .extracting(JobSearchResult::getId)
            .containsExactlyInAnyOrder(sleepJobId, killJobId, timeoutJobId, dateJobId, echoJobId);
        Assertions
            .assertThat(
                this.jobClient.getJobs(
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    echoCommandId,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null
                )
            )
            .extracting(JobSearchResult::getId)
            .containsExactlyInAnyOrder(echoJobId);
        Assertions
            .assertThat(
                this.jobClient.getJobs(
                    null,
                    null,
                    null,
                    Sets.newHashSet(JobStatus.KILLED.name()),
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null
                )
            )
            .extracting(JobSearchResult::getId)
            .containsExactlyInAnyOrder(killJobId, timeoutJobId);

        final List<String> ids = Lists.newArrayList(sleepJobId, killJobId, timeoutJobId, dateJobId, echoJobId);
        // Paginate, 1 result per page
        for (int i = 0; i < ids.size(); i++) {
            final List<JobSearchResult> page = this.jobClient.getJobs(
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
                1,
                SortAttribute.CREATED,
                SortDirection.ASC,
                i
            );

            Assertions.assertThat(page.size()).isEqualTo(1);
            Assertions.assertThat(page.get(0).getId()).isEqualTo(ids.get(i));
        }

        // Paginate, 1 result per page, reverse order
        Collections.reverse(ids);
        for (int i = 0; i < ids.size(); i++) {
            final List<JobSearchResult> page = this.jobClient.getJobs(
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
                1,
                SortAttribute.CREATED,
                SortDirection.DESC,
                i
            );

            Assertions.assertThat(page.size()).isEqualTo(1);
            Assertions.assertThat(page.get(0).getId()).isEqualTo(ids.get(i));
        }

        // Ask for page beyond end of results
        Assertions.assertThat(
            this.jobClient.getJobs(
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
                10,
                SortAttribute.CREATED,
                SortDirection.DESC,
                1
            )
        ).isEmpty();
    }

    private String createDummyCluster() throws Exception {
        return this.clusterClient.createCluster(
            new Cluster.Builder(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                ClusterStatus.UP
            )
                .withTags(Sets.newHashSet(DUMMY_TAG, UUID.randomUUID().toString()))
                .build()
        );
    }

    private String createSleepCommand() throws Exception {
        return this.commandClient.createCommand(
            new Command.Builder(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                CommandStatus.ACTIVE,
                Lists.newArrayList("sleep"),
                100
            )
                .withTags(Sets.newHashSet(SLEEP_TAG, UUID.randomUUID().toString(), UUID.randomUUID().toString()))
                .withMemory(128)
                .withClusterCriteria(
                    Lists.newArrayList(
                        new Criterion.Builder().withTags(Sets.newHashSet(DUMMY_TAG)).build()
                    )
                )
                .build()
        );
    }

    private String createDateCommand() throws Exception {
        return this.commandClient.createCommand(
            new Command.Builder(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                CommandStatus.ACTIVE,
                Lists.newArrayList("date"),
                100
            )
                .withTags(Sets.newHashSet(DATE_TAG, UUID.randomUUID().toString(), UUID.randomUUID().toString()))
                .withMemory(128)
                .withClusterCriteria(
                    Lists.newArrayList(
                        new Criterion.Builder().withTags(Sets.newHashSet(DUMMY_TAG)).build()
                    )
                )
                .build()
        );
    }

    private String createEchoCommand() throws Exception {
        return this.commandClient.createCommand(
            new Command.Builder(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                CommandStatus.ACTIVE,
                Lists.newArrayList("echo"),
                100
            )
                .withTags(Sets.newHashSet(ECHO_TAG, UUID.randomUUID().toString(), UUID.randomUUID().toString()))
                .withMemory(128)
                .withClusterCriteria(
                    Lists.newArrayList(
                        new Criterion.Builder().withTags(Sets.newHashSet(DUMMY_TAG)).build()
                    )
                )
                .build()
        );
    }
}
