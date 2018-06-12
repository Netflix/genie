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

import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.genie.web.properties.ClusterCheckerProperties;
import com.netflix.genie.web.services.JobPersistenceService;
import com.netflix.genie.web.services.JobSearchService;
import com.netflix.genie.web.tasks.GenieTaskScheduleType;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.springframework.boot.actuate.autoconfigure.endpoint.web.WebEndpointProperties;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.Set;
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
    private String hostname;
    private JobSearchService jobSearchService;
    private JobPersistenceService jobPersistenceService;
    private RestTemplate restTemplate;
    private String scheme;
    private String healthEndpoint;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.hostname = UUID.randomUUID().toString();
        final GenieHostInfo genieHostInfo = new GenieHostInfo(this.hostname);
        final ClusterCheckerProperties properties = new ClusterCheckerProperties();
        properties.setHealthIndicatorsToIgnore("memory,genie ");
        this.jobSearchService = Mockito.mock(JobSearchService.class);
        this.jobPersistenceService = Mockito.mock(JobPersistenceService.class);
        this.restTemplate = Mockito.mock(RestTemplate.class);
        final WebEndpointProperties serverProperties = Mockito.mock(WebEndpointProperties.class);
        Mockito.when(serverProperties.getBasePath()).thenReturn("/actuator");
        this.task = new ClusterCheckerTask(
            genieHostInfo,
            properties,
            this.jobSearchService,
            this.jobPersistenceService,
            this.restTemplate,
            serverProperties,
            new SimpleMeterRegistry()
        );

        this.scheme = properties.getScheme() + "://";
        this.healthEndpoint = ":" + properties.getPort() + "/actuator/health";
    }

    /**
     * Make sure run method works.
     *
     * @throws GenieException on error
     */
    @Test
    public void canRun() throws GenieException {
        final String emptyString = "";
        final String host1 = UUID.randomUUID().toString();
        final String host2 = UUID.randomUUID().toString();
        final String host3 = UUID.randomUUID().toString();

        Mockito
            .when(
                this.restTemplate.getForObject(
                    Mockito.eq(this.scheme + host1 + this.healthEndpoint),
                    Mockito.any()
                )
            )
            .thenReturn(
                emptyString,
                emptyString,
                emptyString,
                emptyString,
                emptyString,
                emptyString
            );

        Mockito
            .when(
                this.restTemplate.getForObject(
                    Mockito.eq(this.scheme + host2 + this.healthEndpoint),
                    Mockito.any()
                )
            )
            .thenThrow(new RestClientException("blah"))
            .thenThrow(new RestClientException("blah"))
            .thenThrow(
                new HttpServerErrorException(
                    HttpStatus.INTERNAL_SERVER_ERROR,
                    "",
                    (
                        "{\"status\":\"OUT_OF_SERVICE\", \"genie\": { \"status\": \"OUT_OF_SERVICE\"}, "
                            + "\"db\": { \"status\": \"OUT_OF_SERVICE\"}}"
                    ).getBytes(StandardCharsets.UTF_8),
                    StandardCharsets.UTF_8
                )
            )
            .thenThrow(
                new HttpServerErrorException(
                    HttpStatus.INTERNAL_SERVER_ERROR,
                    "",
                    (
                        "{\"status\":\"OUT_OF_SERVICE\", \"genie\": { \"status\": \"OUT_OF_SERVICE\"}, "
                            + "\"db\": { \"status\": \"OUT_OF_SERVICE\"}}"
                    ).getBytes(StandardCharsets.UTF_8),
                    StandardCharsets.UTF_8
                )
            )
            .thenThrow(
                new HttpServerErrorException(
                    HttpStatus.INTERNAL_SERVER_ERROR,
                    "",
                    (
                        "{\"status\":\"OUT_OF_SERVICE\", \"genie\": { \"status\": \"OUT_OF_SERVICE\"}, "
                            + "\"db\": { \"status\": \"UP\"}}"
                    ).getBytes(StandardCharsets.UTF_8),
                    StandardCharsets.UTF_8
                )
            )
            .thenThrow(
                new HttpServerErrorException(
                    HttpStatus.INTERNAL_SERVER_ERROR,
                    "",
                    (
                        "{\"status\":\"OUT_OF_SERVICE\", \"genie\": { \"status\": \"OUT_OF_SERVICE\"}, "
                            + "\"db\": { \"status\": \"OUT_OF_SERVICE\"}}"
                    ).getBytes(StandardCharsets.UTF_8),
                    StandardCharsets.UTF_8
                )
            );

        Mockito
            .when(
                this.restTemplate.getForObject(
                    Mockito.eq(this.scheme + host3 + this.healthEndpoint),
                    Mockito.any()
                )
            )
            .thenReturn(
                emptyString,
                emptyString,
                emptyString,
                emptyString,
                emptyString,
                emptyString
            );

        final Set<String> hostsRunningJobs = Sets.newHashSet(this.hostname, host1, host2, host3);
        Mockito.when(this.jobSearchService.getAllHostsWithActiveJobs()).thenReturn(hostsRunningJobs);

        final Job job1 = Mockito.mock(Job.class);
        final String job1Id = UUID.randomUUID().toString();
        Mockito.when(job1.getId()).thenReturn(Optional.of(job1Id));
        final Job job2 = Mockito.mock(Job.class);
        final String job2Id = UUID.randomUUID().toString();
        Mockito.when(job2.getId()).thenReturn(Optional.of(job2Id));
        final Job job3 = Mockito.mock(Job.class);
        final String job3Id = UUID.randomUUID().toString();
        Mockito.when(job3.getId()).thenReturn(Optional.of(job3Id));
        final Job job4 = Mockito.mock(Job.class);
        final String job4Id = UUID.randomUUID().toString();
        Mockito.when(job4.getId()).thenReturn(Optional.of(job4Id));

        Mockito
            .when(this.jobSearchService.getAllActiveJobsOnHost(host2))
            .thenReturn(Sets.newHashSet(job1, job2));
        Mockito
            .when(this.jobSearchService.getAllActiveJobsOnHost(host3))
            .thenReturn(Sets.newHashSet(job3, job4));

        Mockito
            .doThrow(new RuntimeException("blah"))
            .doNothing()
            .when(this.jobPersistenceService)
            .setJobCompletionInformation(
                Mockito.eq(job1Id),
                Mockito.eq(JobExecution.LOST_EXIT_CODE),
                Mockito.eq(JobStatus.FAILED), Mockito.anyString(),
                Mockito.eq(null),
                Mockito.eq(null)
            );

        this.task.run();
        Assert.assertThat(this.task.getErrorCountsSize(), Matchers.is(1));
        this.task.run();
        Assert.assertThat(this.task.getErrorCountsSize(), Matchers.is(1));
        this.task.run();
        Assert.assertThat(this.task.getErrorCountsSize(), Matchers.is(1));
        this.task.run();
        Assert.assertThat(this.task.getErrorCountsSize(), Matchers.is(0));
        this.task.run();
        Assert.assertThat(this.task.getErrorCountsSize(), Matchers.is(0));
        this.task.run();
        Assert.assertThat(this.task.getErrorCountsSize(), Matchers.is(1));

        Mockito.verify(this.jobPersistenceService, Mockito.times(2))
            .setJobCompletionInformation(
                Mockito.eq(job1Id),
                Mockito.eq(JobExecution.LOST_EXIT_CODE),
                Mockito.eq(JobStatus.FAILED), Mockito.anyString(),
                Mockito.eq(null),
                Mockito.eq(null)
            );
        Mockito.verify(this.jobPersistenceService, Mockito.atLeast(1))
            .setJobCompletionInformation(
                Mockito.eq(job2Id),
                Mockito.eq(JobExecution.LOST_EXIT_CODE),
                Mockito.eq(JobStatus.FAILED), Mockito.anyString(),
                Mockito.eq(null),
                Mockito.eq(null)
            );
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
