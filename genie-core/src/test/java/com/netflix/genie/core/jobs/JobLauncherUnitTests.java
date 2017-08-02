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
package com.netflix.genie.core.jobs;

import com.google.common.collect.Lists;
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.core.services.JobSubmitterService;
import com.netflix.genie.core.util.MetricsConstants;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.util.List;

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
    private Registry registry;
    private JobRequest jobRequest;
    private Cluster cluster;
    private Command command;
    private List<Application> applications;
    private Timer timer;
    private int memory;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.jobRequest = Mockito.mock(JobRequest.class);
        this.cluster = Mockito.mock(Cluster.class);
        this.command = Mockito.mock(Command.class);
        this.applications = Lists.newArrayList(Mockito.mock(Application.class));
        this.jobSubmitterService = Mockito.mock(JobSubmitterService.class);
        this.registry = new DefaultRegistry();
        this.timer = this.registry.timer("genie.jobs.submit.timer");
        this.memory = 1_024;
        this.jobLauncher = new JobLauncher(
            this.jobSubmitterService,
            this.jobRequest,
            this.cluster,
            this.command,
            this.applications,
            this.memory,
            this.registry
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
        Assert.assertEquals(1, this.timer.count());
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
        Assert.assertEquals(1, this.timer.count());
        final Id counterId = registry.createId("genie.jobs.submit.exception")
            .withTag(MetricsConstants.TagKeys.EXCEPTION_CLASS, exception.getClass().getCanonicalName());
        Assert.assertEquals(1, registry.counter(counterId).count());
    }
}
