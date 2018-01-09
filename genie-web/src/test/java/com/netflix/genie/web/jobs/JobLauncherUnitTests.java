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
package com.netflix.genie.web.jobs;

import com.google.common.collect.Lists;
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.genie.web.services.JobSubmitterService;
import com.netflix.genie.web.util.MetricsUtils;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for the JobLauncher class.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class JobLauncherUnitTests {

    private JobLauncher jobLauncher;
    private JobSubmitterService jobSubmitterService;
    private JobRequest jobRequest;
    private Cluster cluster;
    private Command command;
    private List<Application> applications;
    private Timer timer;
    private int memory;
    private Id timerId;
    @Captor
    private ArgumentCaptor<Map<String, String>> tagsCaptor;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        this.jobRequest = Mockito.mock(JobRequest.class);
        this.cluster = Mockito.mock(Cluster.class);
        this.command = Mockito.mock(Command.class);
        this.applications = Lists.newArrayList(Mockito.mock(Application.class));
        this.jobSubmitterService = Mockito.mock(JobSubmitterService.class);
        this.memory = 1_024;
        final Registry registry = Mockito.mock(Registry.class);
        this.timerId = Mockito.mock(Id.class);
        this.timer = Mockito.mock(Timer.class);
        Mockito.when(registry.createId("genie.jobs.submit.timer")).thenReturn(timerId);
        Mockito.when(timerId.withTags(Mockito.anyMapOf(String.class, String.class))).thenReturn(timerId);
        Mockito.when(registry.timer(Mockito.eq(timerId))).thenReturn(timer);

        this.jobLauncher = new JobLauncher(
            this.jobSubmitterService,
            this.jobRequest,
            this.cluster,
            this.command,
            this.applications,
            this.memory,
            registry
        );
    }

    /**
     * Make sure can successfully run.
     *
     * @throws GenieException on error
     */
    @Test
    public void canRun() throws GenieException {
        this.jobLauncher.run();
        Mockito.verify(this.jobSubmitterService, Mockito.times(1))
            .submitJob(this.jobRequest, this.cluster, this.command, this.applications, this.memory);
        Mockito
            .verify(this.timer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.timerId, Mockito.times(1))
            .withTags(tagsCaptor.capture());
        Assert.assertEquals(MetricsUtils.newSuccessTagsMap(), tagsCaptor.getValue());
    }

    /**
     * When an error is thrown the system should just log it.
     *
     * @throws GenieException on error
     */
    @Test
    public void cantRun() throws GenieException {
        final GenieServerException exception = new GenieServerException("test");
        Mockito.doThrow(exception).when(this.jobSubmitterService)
            .submitJob(this.jobRequest, this.cluster, this.command, this.applications, this.memory);
        this.jobLauncher.run();
        Mockito
            .verify(this.timer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.timerId, Mockito.times(1))
            .withTags(tagsCaptor.capture());
        Assert.assertEquals(
            MetricsUtils.newFailureTagsMapForException(new GenieServerException("test")),
            tagsCaptor.getValue()
        );
    }

    /**
     * When a runtime exception is thrown the it gets propagated.
     *
     * @throws GenieException on error
     */
    @Test(expected = RuntimeException.class)
    public void runtimeException() throws GenieException {
        final RuntimeException exception = new RuntimeException("test");
        Mockito.doThrow(exception).when(this.jobSubmitterService)
            .submitJob(this.jobRequest, this.cluster, this.command, this.applications, this.memory);
        try {
            this.jobLauncher.run();
        } finally {
            Mockito
                .verify(this.timer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.timerId, Mockito.times(1))
                .withTags(tagsCaptor.capture());
            Assert.assertEquals(
                MetricsUtils.newFailureTagsMapForException(new RuntimeException("test")),
                tagsCaptor.getValue()
            );
        }
    }
}
