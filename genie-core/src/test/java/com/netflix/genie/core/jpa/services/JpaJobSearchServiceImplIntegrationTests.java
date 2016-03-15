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
package com.netflix.genie.core.jpa.services;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.DatabaseTearDown;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.dto.search.JobSearchResult;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.core.jpa.repositories.JpaJobExecutionRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobRequestRepository;
import com.netflix.genie.test.categories.IntegrationTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.util.Set;
import java.util.UUID;

/**
 * Integration tests for the Job Search Service using JPA.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(IntegrationTest.class)
@DatabaseSetup("JpaJobSearchServiceImplIntegrationTests/init.xml")
@DatabaseTearDown("cleanup.xml")
public class JpaJobSearchServiceImplIntegrationTests extends DBUnitTestBase {

    private static final String JOB_1_ID = "job1";
    private static final String JOB_2_ID = "job2";
    private static final String JOB_3_ID = "job3";

    @Autowired
    private JpaJobRepository jobRepository;

    @Autowired
    private JpaJobRequestRepository requestRepository;

    @Autowired
    private JpaJobExecutionRepository executionRepository;

    @PersistenceContext
    private EntityManager entityManager;

    private JpaJobSearchServiceImpl service;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.service
            = new JpaJobSearchServiceImpl(this.jobRepository, this.requestRepository, this.executionRepository);
        this.service.setEntityManager(this.entityManager);
    }

    /**
     * Make sure we can search jobs successfully.
     */
    @Test
    public void canFindJobs() {
        //TODO: add more cases
        final Pageable page = new PageRequest(0, 10, Sort.Direction.DESC, "updated");
        Page<JobSearchResult> jobs = this.service
            .findJobs(
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
        Assert.assertThat(jobs.getTotalElements(), Matchers.is(3L));

        jobs = this.service
            .findJobs(
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
                page
            );
        Assert.assertThat(jobs.getTotalElements(), Matchers.is(2L));
        Assert.assertThat(
            jobs
                .getContent()
                .stream()
                .filter(job -> job.getId().equals(JOB_2_ID) || job.getId().equals(JOB_3_ID))
                .count(),
            Matchers.is(2L)
        );

        jobs = this.service
            .findJobs(
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
                page
            );
        Assert.assertThat(jobs.getTotalElements(), Matchers.is(1L));
        Assert.assertThat(
            jobs
                .getContent()
                .stream()
                .filter(job -> job.getId().equals(JOB_1_ID))
                .count(),
            Matchers.is(1L)
        );
    }

    /**
     * Make sure we can get the correct number of job executions which are running on a given host.
     *
     * @throws GenieException on error
     */
    @Test
    public void canFindRunningJobsByHostName() throws GenieException {
        final String hostA = "a.netflix.com";
        final String hostB = "b.netflix.com";
        final String hostC = "c.netflix.com";

        Set<JobExecution> executions = this.service.getAllRunningJobExecutionsOnHost(hostA);
        Assert.assertThat(executions.size(), Matchers.is(1));
        Assert.assertThat(
            executions.stream().filter(jobExecution -> JOB_2_ID.equals(jobExecution.getId())).count(),
            Matchers.is(1L)
        );

        executions = this.service.getAllRunningJobExecutionsOnHost(hostB);
        Assert.assertThat(executions.size(), Matchers.is(1));
        Assert.assertThat(
            executions.stream().filter(jobExecution -> JOB_3_ID.equals(jobExecution.getId())).count(),
            Matchers.is(1L)
        );

        executions = this.service.getAllRunningJobExecutionsOnHost(hostC);
        Assert.assertTrue(executions.isEmpty());
    }

    /**
     * Make sure the getting job method works.
     *
     * @throws GenieException on error
     */
    @Test
    public void canGetJob() throws GenieException {
        Assert.assertThat(this.service.getJob(JOB_1_ID).getName(), Matchers.is("testPigJob"));
        Assert.assertThat(this.service.getJob(JOB_2_ID).getName(), Matchers.is("testSparkJob"));
        Assert.assertThat(this.service.getJob(JOB_3_ID).getName(), Matchers.is("testSparkJob2"));

        try {
            this.service.getJobStatus(UUID.randomUUID().toString());
            Assert.fail();
        } catch (final GenieException ge) {
            Assert.assertTrue(ge instanceof GenieNotFoundException);
        }
    }

    /**
     * Make sure the job status method works.
     *
     * @throws GenieException on error
     */
    @Test
    public void canGetJobStatus() throws GenieException {
        Assert.assertThat(this.service.getJobStatus(JOB_1_ID), Matchers.is(JobStatus.SUCCEEDED));
        Assert.assertThat(this.service.getJobStatus(JOB_2_ID), Matchers.is(JobStatus.RUNNING));
        Assert.assertThat(this.service.getJobStatus(JOB_3_ID), Matchers.is(JobStatus.RUNNING));

        try {
            this.service.getJobStatus(UUID.randomUUID().toString());
            Assert.fail();
        } catch (final GenieException ge) {
            Assert.assertTrue(ge instanceof GenieNotFoundException);
        }
    }

    /**
     * Make sure the getting job request method works.
     *
     * @throws GenieException on error
     */
    @Test
    public void canGetJobRequest() throws GenieException {
        Assert.assertThat(this.service.getJobRequest(JOB_1_ID).getCommandArgs(), Matchers.is("-f query.pig"));
        Assert.assertThat(this.service.getJobRequest(JOB_2_ID).getCommandArgs(), Matchers.is("-f spark.jar"));
        Assert.assertThat(this.service.getJobRequest(JOB_3_ID).getCommandArgs(), Matchers.is("-f spark.jar"));

        try {
            this.service.getJobRequest(UUID.randomUUID().toString());
            Assert.fail();
        } catch (final GenieException ge) {
            Assert.assertTrue(ge instanceof GenieNotFoundException);
        }
    }

    /**
     * Make sure the getting job execution method works.
     *
     * @throws GenieException on error
     */
    @Test
    public void canGetJobExecution() throws GenieException {
        Assert.assertThat(this.service.getJobExecution(JOB_1_ID).getProcessId(), Matchers.is(317));
        Assert.assertThat(this.service.getJobExecution(JOB_2_ID).getProcessId(), Matchers.is(318));
        Assert.assertThat(this.service.getJobExecution(JOB_3_ID).getProcessId(), Matchers.is(319));

        try {
            this.service.getJobExecution(UUID.randomUUID().toString());
            Assert.fail();
        } catch (final GenieException ge) {
            Assert.assertTrue(ge instanceof GenieNotFoundException);
        }
    }
}
