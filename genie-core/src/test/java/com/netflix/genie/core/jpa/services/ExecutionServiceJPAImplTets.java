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

import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.core.jobmanager.JobManagerFactory;
import com.netflix.genie.core.jpa.entities.JobEntity;
import com.netflix.genie.core.jpa.repositories.JobRepository;
import com.netflix.genie.core.metrics.GenieNodeStatistics;
import com.netflix.genie.core.services.JobService;
import com.netflix.genie.core.util.NetUtil;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.UUID;

/**
 * Test for the Genie execution service class.
 *
 * @author tgianos
 */
public class ExecutionServiceJPAImplTets {

    private ExecutionServiceJPAImpl xs;
    private JobRepository jobRepository;

    /**
     * Setup the for tests.
     */
    @Before
    public void setup() {
        this.jobRepository = Mockito.mock(JobRepository.class);
        final GenieNodeStatistics genieNodeStatistics = Mockito.mock(GenieNodeStatistics.class);
        final JobManagerFactory jobManagerFactory = Mockito.mock(JobManagerFactory.class);
        final JobService jobService = Mockito.mock(JobService.class);
        final NetUtil netUtil = Mockito.mock(NetUtil.class);
        this.xs = new ExecutionServiceJPAImpl(
                this.jobRepository,
                genieNodeStatistics,
                jobManagerFactory,
                jobService,
                netUtil
        );
    }

    /**
     * Test submitting a job.
     *
     * @throws GenieException For any problem.
     */
    @Test
    @Ignore
    public void testSubmitJob() throws GenieException {
    }

    /**
     * Test submitting a job that already exists.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieConflictException.class)
    public void testSubmitJobThatExists() throws GenieException {
        final Job job = Mockito.mock(Job.class);
        final String id = UUID.randomUUID().toString();
        Mockito.when(job.getId()).thenReturn(id);
        Mockito.when(this.jobRepository.exists(id)).thenReturn(true);
        this.xs.submitJob(job);
    }

    /**
     * Test to make sure already failed/finished jobs don't get killed again.
     *
     * @throws GenieException For any problem if anything went wrong with the test.
     */
    @Test
    @Ignore
    public void testKillJob() throws GenieException {
    }

    /**
     * Test whether a job kill returns immediately for a finished job.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GeniePreconditionException.class)
    public void testTryingToKillInitializingJob() throws GenieException {
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        Mockito.when(jobEntity.getStatus()).thenReturn(JobStatus.INIT);
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jobRepository.findOne(id)).thenReturn(jobEntity);
        this.xs.killJob(id);
    }

    /**
     * Test killing a job with no job exists.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void testKillJobNoJob() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jobRepository.findOne(id)).thenReturn(null);
        this.xs.killJob(id);
    }

    /**
     * Test killing a job with no kill URI.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GeniePreconditionException.class)
    public void testKillJobNoKillURI() throws GenieException {
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        Mockito.when(jobEntity.getStatus()).thenReturn(JobStatus.RUNNING);
        Mockito.when(jobEntity.getKillURI()).thenReturn(null);
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jobRepository.findOne(id)).thenReturn(jobEntity);
        this.xs.killJob(id);
    }
}
