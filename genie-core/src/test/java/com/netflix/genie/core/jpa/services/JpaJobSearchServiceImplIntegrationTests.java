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
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.core.jpa.repositories.JpaJobExecutionRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobRepository;
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

import java.util.Set;

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
    private JpaJobExecutionRepository executionRepository;

    private JpaJobSearchServiceImpl service;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.service = new JpaJobSearchServiceImpl(this.jobRepository, this.executionRepository);
    }

    /**
     * Make sure we can search jobs successfully.
     */
    @Test
    public void canFindJobs() {
        //TODO: add more cases
        final Pageable page = new PageRequest(0, 10, Sort.Direction.DESC, "updated");
        Page<Job> jobs = this.service.getJobs(null, null, null, null, null, null, null, null, null, page);
        Assert.assertThat(jobs.getTotalElements(), Matchers.is(3L));

        jobs = this.service.getJobs(
            null, null, null, Sets.newHashSet(JobStatus.RUNNING), null, null, null, null, null, page
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

        jobs = this.service.getJobs(
            JOB_1_ID, null, null, null, null, null, null, null, null, page
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

        Set<JobExecution> executions = this.service.getAllJobExecutionsOnHost(hostA);
        Assert.assertThat(executions.size(), Matchers.is(1));
        Assert.assertThat(
            executions.stream().filter(jobExecution -> JOB_2_ID.equals(jobExecution.getId())).count(),
            Matchers.is(1L)
        );

        executions = this.service.getAllJobExecutionsOnHost(hostB);
        Assert.assertThat(executions.size(), Matchers.is(1));
        Assert.assertThat(
            executions.stream().filter(jobExecution -> JOB_3_ID.equals(jobExecution.getId())).count(),
            Matchers.is(1L)
        );

        executions = this.service.getAllJobExecutionsOnHost(hostC);
        Assert.assertTrue(executions.isEmpty());
    }
}
