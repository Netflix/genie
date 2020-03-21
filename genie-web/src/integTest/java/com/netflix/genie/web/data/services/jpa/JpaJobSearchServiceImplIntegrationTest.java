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

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.DatabaseTearDown;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.BaseDTO;
import com.netflix.genie.common.dto.JobMetadata;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.dto.UserResourcesSummary;
import com.netflix.genie.common.dto.search.BaseSearchResult;
import com.netflix.genie.common.dto.search.JobSearchResult;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Integration tests for the {@link JpaJobSearchServiceImpl} class.
 *
 * @author tgianos
 * @since 3.0.0
 */
@DatabaseTearDown("cleanup.xml")
class JpaJobSearchServiceImplIntegrationTest extends DBIntegrationTestBase {

    private static final String JOB_1_ID = "job1";
    private static final String JOB_2_ID = "job2";
    private static final String JOB_3_ID = "job3";

    // This needs to be injected as a Spring Bean otherwise transactions don't work as there is no proxy
    @Autowired
    private JpaJobSearchServiceImpl service;

    @Test
    @DatabaseSetup("JpaJobSearchServiceImplIntegrationTest/init.xml")
    void canFindJobs() {
        //TODO: add more cases
        final Pageable page = PageRequest.of(0, 10, Sort.Direction.DESC, "updated");
        Page<JobSearchResult> jobs = this.service.findJobs(
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
            Sets.newHashSet(JobStatus.RUNNING),
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
    @DatabaseSetup("JpaJobSearchServiceImplIntegrationTest/init.xml")
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
    @DatabaseSetup("JpaJobSearchServiceImplIntegrationTest/init.xml")
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
    @DatabaseSetup("JpaJobSearchServiceImplIntegrationTest/init.xml")
    void canFindHostNamesOfActiveJobs() {
        final String hostA = "a.netflix.com";
        final String hostB = "b.netflix.com";

        Assertions
            .assertThat(this.service.getAllHostsWithActiveJobs())
            .hasSize(2)
            .containsExactlyInAnyOrder(hostA, hostB);
    }

    @Test
    @DatabaseSetup("JpaJobSearchServiceImplIntegrationTest/init.xml")
    void canGetJob() throws GenieException {
        Assertions.assertThat(this.service.getJob(JOB_1_ID).getName()).isEqualTo("testSparkJob");
        Assertions.assertThat(this.service.getJob(JOB_2_ID).getName()).isEqualTo("testSparkJob1");
        Assertions.assertThat(this.service.getJob(JOB_3_ID).getName()).isEqualTo("testSparkJob2");
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.service.getJob(UUID.randomUUID().toString()));
    }

    @Test
    @DatabaseSetup("JpaJobSearchServiceImplIntegrationTest/init.xml")
    void canGetJobStatus() throws GenieException {
        Assertions.assertThat(this.service.getJobStatus(JOB_1_ID)).isEqualTo(JobStatus.SUCCEEDED);
        Assertions.assertThat(this.service.getJobStatus(JOB_2_ID)).isEqualTo(JobStatus.INIT);
        Assertions.assertThat(this.service.getJobStatus(JOB_3_ID)).isEqualTo(JobStatus.RUNNING);
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.service.getJobStatus(UUID.randomUUID().toString()));
    }

    @Test
    @DatabaseSetup("JpaJobSearchServiceImplIntegrationTest/init.xml")
    void canGetJobRequest() throws GenieException {
        final JobRequest job1Request = this.service.getJobRequest(JOB_1_ID);
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
        Assertions.assertThat(this.service.getJobRequest(JOB_2_ID).getCommandArgs()).contains("-f spark.jar");
        Assertions.assertThat(this.service.getJobRequest(JOB_3_ID).getCommandArgs()).contains("-f spark.jar");
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.service.getJobRequest(UUID.randomUUID().toString()));
    }

    @Test
    @DatabaseSetup("JpaJobSearchServiceImplIntegrationTest/init.xml")
    void canGetJobExecution() throws GenieException {
        Assertions.assertThat(this.service.getJobExecution(JOB_1_ID).getProcessId()).contains(317);
        Assertions.assertThat(this.service.getJobExecution(JOB_2_ID).getProcessId()).contains(318);
        Assertions.assertThat(this.service.getJobExecution(JOB_3_ID).getProcessId()).contains(319);
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.service.getJobExecution(UUID.randomUUID().toString()));
    }

    @Test
    @DatabaseSetup("JpaJobSearchServiceImplIntegrationTest/init.xml")
    void canGetJobCluster() throws GenieException {
        Assertions.assertThat(this.service.getJobCluster(JOB_1_ID).getId()).contains("cluster1");
    }

    @Test
    @DatabaseSetup("JpaJobSearchServiceImplIntegrationTest/init.xml")
    void canGetJobCommand() throws GenieException {
        Assertions.assertThat(this.service.getJobCommand(JOB_1_ID).getId()).contains("command1");
    }

    @Test
    @DatabaseSetup("JpaJobSearchServiceImplIntegrationTest/init.xml")
    void canGetJobApplications() throws GenieException {
        Assertions
            .assertThat(this.service.getJobApplications(JOB_1_ID))
            .hasSize(2)
            .extracting(BaseDTO::getId)
            .filteredOn(Optional::isPresent)
            .extracting(Optional::get)
            .containsExactly("app1", "app3");
        Assertions
            .assertThat(this.service.getJobApplications(JOB_2_ID))
            .hasSize(2)
            .extracting(BaseDTO::getId)
            .filteredOn(Optional::isPresent)
            .extracting(Optional::get)
            .containsExactly("app1", "app2");
    }

    @Test
    @DatabaseSetup("JpaJobSearchServiceImplIntegrationTest/init.xml")
    void canGetActiveJobCountForUser() throws GenieException {
        Assertions.assertThat(this.service.getActiveJobCountForUser("nobody")).isEqualTo(0L);
        Assertions.assertThat(this.service.getActiveJobCountForUser("tgianos")).isEqualTo(4L);
    }

    @Test
    @DatabaseSetup("JpaJobSearchServiceImplIntegrationTest/init.xml")
    void canGetJobMetadata() throws GenieException {
        final JobMetadata jobMetadata = this.service.getJobMetadata(JOB_1_ID);
        Assertions.assertThat(jobMetadata.getClientHost()).isNotPresent();
        Assertions.assertThat(jobMetadata.getUserAgent()).isNotPresent();
        Assertions.assertThat(jobMetadata.getNumAttachments()).contains(2);
        Assertions.assertThat(jobMetadata.getTotalSizeOfAttachments()).contains(38083L);
        Assertions.assertThat(jobMetadata.getStdErrSize()).isNotPresent();
        Assertions.assertThat(jobMetadata.getStdOutSize()).isNotPresent();
    }

    @Test
    @DatabaseSetup("JpaJobSearchServiceImplIntegrationTest/init.xml")
    void canGetUserResourceSummaries() {
        final Map<String, UserResourcesSummary> summaries = this.service.getUserResourcesSummaries();
        Assertions.assertThat(summaries.keySet()).contains("tgianos");
        final UserResourcesSummary userResourcesSummary = summaries.get("tgianos");
        Assertions.assertThat(userResourcesSummary.getUser()).isEqualTo("tgianos");
        Assertions.assertThat(userResourcesSummary.getRunningJobsCount()).isEqualTo(1L);
        Assertions.assertThat(userResourcesSummary.getUsedMemory()).isEqualTo(2048L);
    }

    @Test
    @DatabaseSetup("JpaJobSearchServiceImplIntegrationTest/init.xml")
    void canGetActiveDisconnectedAgentJobs() {
        Assertions.assertThat(this.service.getActiveDisconnectedAgentJobs()).hasSize(1).containsOnly("agentJob1");
    }

    @Test
    @DatabaseSetup("JpaJobSearchServiceImplIntegrationTest/init.xml")
    void canGetAllocatedMemoryOnHost() {
        Assertions.assertThat(this.service.getAllocatedMemoryOnHost("a.netflix.com")).isEqualTo(2048L);
        Assertions.assertThat(this.service.getAllocatedMemoryOnHost("b.netflix.com")).isEqualTo(2048L);
        Assertions.assertThat(this.service.getAllocatedMemoryOnHost("agent.netflix.com")).isEqualTo(4096L);
        Assertions.assertThat(this.service.getAllocatedMemoryOnHost(UUID.randomUUID().toString())).isEqualTo(0L);
    }

    @Test
    @DatabaseSetup("JpaJobSearchServiceImplIntegrationTest/init.xml")
    void canGetUsedMemoryOnHost() {
        Assertions.assertThat(this.service.getUsedMemoryOnHost("a.netflix.com")).isEqualTo(2048L);
        Assertions.assertThat(this.service.getUsedMemoryOnHost("b.netflix.com")).isEqualTo(2048L);
        Assertions.assertThat(this.service.getUsedMemoryOnHost("agent.netflix.com")).isEqualTo(4096L);
        Assertions.assertThat(this.service.getUsedMemoryOnHost(UUID.randomUUID().toString())).isEqualTo(0L);
    }

    @Test
    @DatabaseSetup("JpaJobSearchServiceImplIntegrationTest/init.xml")
    void canGetCountOfActiveJobsOnHost() {
        Assertions.assertThat(this.service.getActiveJobCountOnHost("a.netflix.com")).isEqualTo(1L);
        Assertions.assertThat(this.service.getActiveJobCountOnHost("b.netflix.com")).isEqualTo(1L);
        Assertions.assertThat(this.service.getActiveJobCountOnHost("agent.netflix.com")).isEqualTo(2L);
        Assertions.assertThat(this.service.getActiveJobCountOnHost(UUID.randomUUID().toString())).isEqualTo(0L);
    }
}
