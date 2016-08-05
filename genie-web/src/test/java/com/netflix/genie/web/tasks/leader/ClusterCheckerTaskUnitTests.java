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
package com.netflix.genie.web.tasks.leader;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.core.services.JobPersistenceService;
import com.netflix.genie.core.services.JobSearchService;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.genie.web.properties.ClusterCheckerProperties;
import com.netflix.genie.web.tasks.GenieTaskScheduleType;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.springframework.boot.actuate.autoconfigure.ManagementServerProperties;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

/**
 * Unit tests for the ClusterCheckerTask class.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class ClusterCheckerTaskUnitTests {

    private ClusterCheckerTask task;
    private String hostName;
    private JobSearchService jobSearchService;
    private JobPersistenceService jobPersistenceService;
    private RestTemplate restTemplate;

    private Counter lostJobCounter;
    private Counter unableToUpdateJobCounter;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.hostName = UUID.randomUUID().toString();
        final ClusterCheckerProperties properties = new ClusterCheckerProperties();
        this.jobSearchService = Mockito.mock(JobSearchService.class);
        this.jobPersistenceService = Mockito.mock(JobPersistenceService.class);
        this.restTemplate = Mockito.mock(RestTemplate.class);
        final ManagementServerProperties serverProperties = Mockito.mock(ManagementServerProperties.class);
        Mockito.when(serverProperties.getContextPath()).thenReturn("/actuator");
        final Registry registry = Mockito.mock(Registry.class);
        this.lostJobCounter = Mockito.mock(Counter.class);
        Mockito.when(registry.counter("genie.tasks.clusterChecker.lostJobs.rate")).thenReturn(this.lostJobCounter);
        this.unableToUpdateJobCounter = Mockito.mock(Counter.class);
        Mockito
            .when(registry.counter("genie.tasks.clusterChecker.unableToUpdateJob.rate"))
            .thenReturn(this.unableToUpdateJobCounter);
        this.task = new ClusterCheckerTask(
            this.hostName,
            properties,
            this.jobSearchService,
            this.jobPersistenceService,
            this.restTemplate,
            serverProperties,
            registry
        );
    }

    /**
     * Make sure run method works.
     *
     * @throws IOException    on error
     * @throws GenieException on error
     */
    @Test
    public void canRun() throws IOException, GenieException {
        final String host1 = UUID.randomUUID().toString();
        final String host2 = UUID.randomUUID().toString();
        final String host3 = UUID.randomUUID().toString();

        // Mock the 9 invocations for 3 calls to run
        Mockito
            .when(this.restTemplate.getForObject(Mockito.anyString(), Mockito.anyObject()))
            .thenReturn("")
            .thenThrow(new RestClientException("blah"))
            .thenReturn("")
            .thenReturn("")
            .thenThrow(new RestClientException("blah"))
            .thenReturn("")
            .thenReturn("")
            .thenThrow(new RestClientException("blah"))
            .thenReturn("");

        final List<String> hostsRunningJobs = Lists.newArrayList(this.hostName, host1, host2, host3);
        Mockito.when(this.jobSearchService.getAllHostsWithActiveJobs()).thenReturn(hostsRunningJobs);

        final Job job1 = Mockito.mock(Job.class);
        final String job1Id = UUID.randomUUID().toString();
        Mockito.when(job1.getId()).thenReturn(job1Id);
        final Job job2 = Mockito.mock(Job.class);
        final String job2Id = UUID.randomUUID().toString();
        Mockito.when(job2.getId()).thenReturn(job2Id);
        final Job job3 = Mockito.mock(Job.class);
        final String job3Id = UUID.randomUUID().toString();
        Mockito.when(job3.getId()).thenReturn(job3Id);
        final Job job4 = Mockito.mock(Job.class);
        final String job4Id = UUID.randomUUID().toString();
        Mockito.when(job4.getId()).thenReturn(job4Id);

        Mockito
            .when(this.jobSearchService.getAllActiveJobsOnHost(host2))
            .thenReturn(Sets.newHashSet(job1, job2));
        Mockito
            .when(this.jobSearchService.getAllActiveJobsOnHost(host3))
            .thenReturn(Sets.newHashSet(job3, job4));

        Mockito
            .doThrow(new GenieServerException("blah"))
            .when(this.jobPersistenceService)
            .setJobCompletionInformation(
                Mockito.eq(job1Id),
                Mockito.eq(JobExecution.LOST_EXIT_CODE),
                Mockito.eq(JobStatus.FAILED), Mockito.anyString()
            );

        this.task.run();
        Assert.assertThat(this.task.getErrorCountsSize(), Matchers.is(1));
        this.task.run();
        Assert.assertThat(this.task.getErrorCountsSize(), Matchers.is(1));
        this.task.run();
        Assert.assertThat(this.task.getErrorCountsSize(), Matchers.is(0));

        Mockito.verify(this.jobPersistenceService, Mockito.times(1))
            .setJobCompletionInformation(
                Mockito.eq(job1Id),
                Mockito.eq(JobExecution.LOST_EXIT_CODE),
                Mockito.eq(JobStatus.FAILED), Mockito.anyString()
            );
        Mockito.verify(this.jobPersistenceService, Mockito.times(1))
            .setJobCompletionInformation(
                Mockito.eq(job2Id),
                Mockito.eq(JobExecution.LOST_EXIT_CODE),
                Mockito.eq(JobStatus.FAILED), Mockito.anyString()
            );
        Mockito.verify(this.lostJobCounter, Mockito.times(1)).increment();
        Mockito.verify(this.unableToUpdateJobCounter, Mockito.times(1)).increment();
    }

    /**
     * Make sure we get the right schedule type.
     */
    @Test
    public void canGetScheduleType() {
        Assert.assertThat(this.task.getScheduleType(), Matchers.is(GenieTaskScheduleType.FIXED_RATE));
    }

    /**
     * Make sure the trigger is null.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void canGetTrigger() {
        this.task.getTrigger();
    }

    /**
     * Make sure the get period returns the correct value.
     */
    @Test
    public void canGetFixedRate() {
        Assert.assertThat(this.task.getFixedRate(), Matchers.is(300000L));
    }

    /**
     * Make sure the trigger is null.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void canGetFixedDelay() {
        this.task.getFixedDelay();
    }
}
