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

import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.core.services.JobSubmitterService;
import com.netflix.genie.core.util.MetricsConstants;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;

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
    private Counter counter;
    private JobRequest jobRequest;
    private Timer timer;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.jobRequest = Mockito.mock(JobRequest.class);
        this.jobSubmitterService = Mockito.mock(JobSubmitterService.class);
        this.registry = Mockito.mock(Registry.class);
        this.counter = Mockito.mock(Counter.class);
        this.timer = Mockito.mock(Timer.class);
        // Force the runnable to be invoked within the timer
        Mockito.doAnswer(
            (final InvocationOnMock invocationOnMock) -> {
                final Runnable runnable = (Runnable) invocationOnMock.getArguments()[0];
                runnable.run();
                return null;
            }
        ).when(this.timer).record(Mockito.any(Runnable.class));
        Mockito.when(this.registry.timer("genie.jobs.submit.timer")).thenReturn(this.timer);
        this.jobLauncher = new JobLauncher(this.jobSubmitterService, this.jobRequest, this.registry);
    }

    /**
     * Make sure can successfully run.
     *
     * @throws GenieException on error
     */
    @Test
    public void canRun() throws GenieException {
        this.jobLauncher.run();
        Mockito.verify(this.jobSubmitterService, Mockito.times(1)).submitJob(this.jobRequest);
        Mockito.verify(this.timer, Mockito.times(1)).record(Mockito.any(Runnable.class));
    }

    /**
     * When an error is thrown the system should just log it.
     *
     * @throws GenieException on error
     */
    @Test
    public void cantRun() throws GenieException {
        Mockito.when(this.registry.counter(MetricsConstants.GENIE_EXCEPTIONS_SERVER_RATE)).thenReturn(this.counter);
        Mockito.doThrow(new GenieServerException("test")).when(this.jobSubmitterService).submitJob(this.jobRequest);
        this.jobLauncher.run();
        Mockito.verify(this.registry, Mockito.times(1)).counter(MetricsConstants.GENIE_EXCEPTIONS_SERVER_RATE);
        Mockito.verify(this.counter, Mockito.times(1)).increment();
        Mockito.verify(this.timer, Mockito.times(1)).record(Mockito.any(Runnable.class));
    }
}
