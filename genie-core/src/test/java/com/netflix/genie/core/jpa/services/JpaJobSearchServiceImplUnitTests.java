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

import com.google.common.collect.Lists;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.core.jpa.entities.JobEntity;
import com.netflix.genie.core.jpa.entities.JobExecutionEntity;
import com.netflix.genie.core.jpa.repositories.JpaJobExecutionRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobRequestRepository;
import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.util.UUID;

/**
 * Unit tests for JpaJobSearchServiceImpl.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class JpaJobSearchServiceImplUnitTests {

    private JpaJobRepository jobRepository;
    private JpaJobRequestRepository jobRequestRepository;
    private JpaJobExecutionRepository jobExecutionRepository;
    private JpaJobSearchServiceImpl service;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.jobRepository = Mockito.mock(JpaJobRepository.class);
        this.jobRequestRepository = Mockito.mock(JpaJobRequestRepository.class);
        this.jobExecutionRepository = Mockito.mock(JpaJobExecutionRepository.class);
        this.service
            = new JpaJobSearchServiceImpl(this.jobRepository, this.jobRequestRepository, this.jobExecutionRepository);
    }

    /**
     * Test the getJob method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void cantGetJobIfDoesNotExist() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jobRepository.findOne(id)).thenReturn(null);
        this.service.getJob(id);
    }

    /**
     * Test the get job method to verify that the id sent is used to fetch from persistence service.
     *
     * @throws GenieException If there is any problem
     */
    @Test
    public void canGetJob() throws GenieException {
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        final Job job = Mockito.mock(Job.class);
        Mockito.when(jobEntity.getDTO()).thenReturn(job);
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jobRepository.findOne(id)).thenReturn(jobEntity);
        final Job returnedJob = this.service.getJob(id);
        Mockito.verify(this.jobRepository, Mockito.times(1)).findOne(id);
        Assert.assertThat(returnedJob, Matchers.is(job));
    }

    /**
     * Test the getJobRequest method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void cantGetJobRequestIfDoesNotExist() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jobRequestRepository.findOne(id)).thenReturn(null);
        this.service.getJobRequest(id);
    }

    /**
     * Test the getJobExecution method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void cantGetJobExecutionIfDoesNotExist() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jobExecutionRepository.findOne(id)).thenReturn(null);
        this.service.getJobExecution(id);
    }

    /**
     * Test the getJobCluster method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void cantGetJobClusterIfJobDoesNotExist() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jobRepository.findOne(id)).thenReturn(null);
        this.service.getJobCluster(id);
    }

    /**
     * Test the getJobCluster method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void cantGetJobClusterIfClusterDoesNotExist() throws GenieException {
        final String id = UUID.randomUUID().toString();
        final JobEntity entity = Mockito.mock(JobEntity.class);
        Mockito.when(entity.getCluster()).thenReturn(null);
        Mockito.when(this.jobRepository.findOne(id)).thenReturn(entity);
        this.service.getJobCluster(id);
    }

    /**
     * Test the getJobCommand method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void cantGetJobCommandIfJobDoesNotExist() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jobRepository.findOne(id)).thenReturn(null);
        this.service.getJobCommand(id);
    }

    /**
     * Test the getJobCommand method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void cantGetJobCommandIfCommandDoesNotExist() throws GenieException {
        final String id = UUID.randomUUID().toString();
        final JobEntity entity = Mockito.mock(JobEntity.class);
        Mockito.when(entity.getCommand()).thenReturn(null);
        Mockito.when(this.jobRepository.findOne(id)).thenReturn(entity);
        this.service.getJobCommand(id);
    }

    /**
     * Test the getJobApplications method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void cantGetJobApplicationsIfJobDoesNotExist() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jobRepository.findOne(id)).thenReturn(null);
        this.service.getJobApplications(id);
    }

    /**
     * Test the getJobApplications method.
     *
     * @throws GenieException For any problem
     */
    @Test()
    public void cantGetJobApplicationsIfApplicationsDoNotExist() throws GenieException {
        final String id = UUID.randomUUID().toString();
        final JobEntity entity = Mockito.mock(JobEntity.class);
        Mockito.when(entity.getApplications()).thenReturn(null);
        Mockito.when(this.jobRepository.findOne(id)).thenReturn(entity);
        Assert.assertTrue(this.service.getJobApplications(id).isEmpty());
    }

    /**
     * Test the getJobApplications method.
     *
     * @throws GenieException For any problem
     */
    @Test()
    public void cantGetJobApplicationsIfApplicationsAreEmpty() throws GenieException {
        final String id = UUID.randomUUID().toString();
        final JobEntity entity = Mockito.mock(JobEntity.class);
        Mockito.when(entity.getApplications()).thenReturn(Lists.newArrayList());
        Mockito.when(this.jobRepository.findOne(id)).thenReturn(entity);
        Assert.assertTrue(this.service.getJobApplications(id).isEmpty());
    }

    /**
     * Make sure if a job execution isn't found it returns a GenieNotFound exception.
     *
     * @throws GenieException for any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void cantGetJobHostIfNoJobExecution() throws GenieException {
        final String jobId = UUID.randomUUID().toString();
        Mockito.when(this.service.getJobExecution(Mockito.eq(jobId))).thenReturn(null);
        this.service.getJobHost(jobId);
    }

    /**
     * Make sure that if the job execution exists we return a valid host.
     *
     * @throws GenieException on any problem
     */
    @Test
    public void canGetJobHost() throws GenieException {
        final String jobId = UUID.randomUUID().toString();
        final String hostName = UUID.randomUUID().toString();
        final JobExecutionEntity jobExecution = Mockito.mock(JobExecutionEntity.class);
        Mockito.when(jobExecution.getHostName()).thenReturn(hostName);
        Mockito.when(this.jobExecutionRepository.findOne(jobId)).thenReturn(jobExecution);

        Assert.assertThat(this.service.getJobHost(jobId), Matchers.is(hostName));
    }
}
