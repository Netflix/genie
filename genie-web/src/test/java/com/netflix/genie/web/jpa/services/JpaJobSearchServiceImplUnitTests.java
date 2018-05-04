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

import com.google.common.collect.Lists;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.genie.web.jpa.entities.JobEntity;
import com.netflix.genie.web.jpa.entities.projections.JobApplicationsProjection;
import com.netflix.genie.web.jpa.entities.projections.JobClusterProjection;
import com.netflix.genie.web.jpa.entities.projections.JobCommandProjection;
import com.netflix.genie.web.jpa.entities.projections.AgentHostnameProjection;
import com.netflix.genie.web.jpa.entities.projections.JobProjection;
import com.netflix.genie.web.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.web.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.web.jpa.repositories.JpaJobRepository;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.util.Optional;
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
    private JpaJobSearchServiceImpl service;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.jobRepository = Mockito.mock(JpaJobRepository.class);
        this.service = new JpaJobSearchServiceImpl(
            this.jobRepository,
            Mockito.mock(JpaClusterRepository.class),
            Mockito.mock(JpaCommandRepository.class)
        );
    }

    /**
     * Test the getJob method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void cantGetJobIfDoesNotExist() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jobRepository.findByUniqueId(id, JobProjection.class)).thenReturn(Optional.empty());
        this.service.getJob(id);
    }

    /**
     * Test the get job method to verify that the id sent is used to fetch from persistence service.
     *
     * @throws GenieException If there is any problem
     */
    @Test
    public void canGetJob() throws GenieException {
        final JobEntity jobEntity = new JobEntity();
        final String id = UUID.randomUUID().toString();
        jobEntity.setUniqueId(id);
        Mockito.when(this.jobRepository.findByUniqueId(id, JobProjection.class)).thenReturn(Optional.of(jobEntity));
        final Job returnedJob = this.service.getJob(id);
        Mockito
            .verify(this.jobRepository, Mockito.times(1))
            .findByUniqueId(id, JobProjection.class);
        Assert.assertThat(returnedJob.getId().orElseThrow(IllegalArgumentException::new), Matchers.is(id));
    }

    /**
     * Test the getJobCluster method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = GenieNotFoundException.class)
    public void cantGetJobClusterIfJobDoesNotExist() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.when(this.jobRepository.findByUniqueId(id, JobClusterProjection.class)).thenReturn(Optional.empty());
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
        Mockito.when(entity.getCluster()).thenReturn(Optional.empty());
        Mockito.when(this.jobRepository.findByUniqueId(id, JobClusterProjection.class)).thenReturn(Optional.of(entity));
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
        Mockito.when(this.jobRepository.findByUniqueId(id, JobCommandProjection.class)).thenReturn(Optional.empty());
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
        Mockito.when(entity.getCommand()).thenReturn(Optional.empty());
        Mockito.when(this.jobRepository.findByUniqueId(id, JobCommandProjection.class)).thenReturn(Optional.of(entity));
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
        Mockito
            .when(this.jobRepository.findByUniqueId(id, JobApplicationsProjection.class))
            .thenReturn(Optional.empty());
        this.service.getJobApplications(id);
    }

    /**
     * Test the getJobApplications method.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void canGetJobApplications() throws GenieException {
        final String id = UUID.randomUUID().toString();
        final JobEntity entity = Mockito.mock(JobEntity.class);
        Mockito.when(entity.getApplications()).thenReturn(Lists.newArrayList());
        Mockito
            .when(this.jobRepository.findByUniqueId(id, JobApplicationsProjection.class))
            .thenReturn(Optional.of(entity));
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
        Mockito
            .when(this.jobRepository.findByUniqueId(jobId, AgentHostnameProjection.class))
            .thenReturn(Optional.empty());
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
        final JobEntity jobEntity = Mockito.mock(JobEntity.class);
        Mockito.when(jobEntity.getAgentHostname()).thenReturn(Optional.of(hostName));
        Mockito
            .when(this.jobRepository.findByUniqueId(jobId, AgentHostnameProjection.class))
            .thenReturn(Optional.of(jobEntity));

        Assert.assertThat(this.service.getJobHost(jobId), Matchers.is(hostName));
    }
}
