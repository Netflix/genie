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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.dto.UserResourcesSummary;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.web.data.entities.JobEntity;
import com.netflix.genie.web.data.entities.aggregates.UserJobResourcesAggregate;
import com.netflix.genie.web.data.entities.projections.AgentHostnameProjection;
import com.netflix.genie.web.data.entities.projections.JobApplicationsProjection;
import com.netflix.genie.web.data.entities.projections.JobClusterProjection;
import com.netflix.genie.web.data.entities.projections.JobCommandProjection;
import com.netflix.genie.web.data.entities.projections.JobProjection;
import com.netflix.genie.web.data.entities.projections.UniqueIdProjection;
import com.netflix.genie.web.data.repositories.jpa.JpaClusterRepository;
import com.netflix.genie.web.data.repositories.jpa.JpaCommandRepository;
import com.netflix.genie.web.data.repositories.jpa.JpaJobRepository;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;

/**
 * Unit tests for JpaJobSearchServiceImpl.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class JpaJobSearchServiceImplTest {

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

    /**
     * Make sure that user resources summaries are returned correctly when there is no active job.
     */
    @Test
    public void canGetUserResourceSummariesNoRecords() {
        Mockito.when(
            jobRepository.getUserJobResourcesAggregates()
        ).thenReturn(
            Sets.newHashSet()
        );
        Assert.assertEquals(
            Maps.newHashMap(),
            this.service.getUserResourcesSummaries()
        );
    }


    /**
     * Make sure that user resources summaries are returned correctly.
     */
    @Test
    public void canGetUserResourceSummaries() {
        final UserJobResourcesAggregate p1 = Mockito.mock(UserJobResourcesAggregate.class);
        final UserJobResourcesAggregate p2 = Mockito.mock(UserJobResourcesAggregate.class);

        Mockito.when(p1.getUser()).thenReturn("foo");
        Mockito.when(p1.getRunningJobsCount()).thenReturn(3L);
        Mockito.when(p1.getUsedMemory()).thenReturn(1024L);

        Mockito.when(p2.getUser()).thenReturn("bar");
        Mockito.when(p2.getRunningJobsCount()).thenReturn(5L);
        Mockito.when(p2.getUsedMemory()).thenReturn(2048L);

        Mockito.when(
            jobRepository.getUserJobResourcesAggregates()
        ).thenReturn(
            Sets.newHashSet(p1, p2)
        );

        final HashMap<String, UserResourcesSummary> expectedMap = Maps.newHashMap();
        expectedMap.put("foo", new UserResourcesSummary("foo", 3, 1024));
        expectedMap.put("bar", new UserResourcesSummary("bar", 5, 2048));
        Assert.assertEquals(
            expectedMap,
            this.service.getUserResourcesSummaries()
        );
    }


    /**
     * Make sure that user disconnected agent ids are returned correctly when there is no active job.
     */
    @Test
    public void canGetDisconnectedAgentJobsNoRecords() {
        Mockito.when(
            jobRepository.getAgentJobIdsWithNoConnectionInState(JobStatus.getActiveStatuses())
        ).thenReturn(
            Sets.newHashSet()
        );
        Assert.assertEquals(
            Sets.newHashSet(),
            this.service.getActiveDisconnectedAgentJobs()
        );
    }

    /**
     * Make sure that user disconnected agent ids are returned correctly.
     */
    @Test
    public void canGetDisconnectedAgentJobs() {
        final UniqueIdProjection p1 = Mockito.mock(UniqueIdProjection.class);
        final UniqueIdProjection p2 = Mockito.mock(UniqueIdProjection.class);

        Mockito.when(p1.getUniqueId()).thenReturn("job1");
        Mockito.when(p2.getUniqueId()).thenReturn("job2");

        Mockito.when(
            jobRepository.getAgentJobIdsWithNoConnectionInState(JobStatus.getActiveStatuses())
        ).thenReturn(
            Sets.newHashSet(p1, p2)
        );

        Assert.assertEquals(
            Sets.newHashSet("job1", "job2"),
            this.service.getActiveDisconnectedAgentJobs()
        );
    }

}
