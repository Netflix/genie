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
package com.netflix.genie.web.health;

import com.netflix.genie.core.properties.JobsProperties;
import com.netflix.genie.core.services.JobMetricsService;
import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.springframework.boot.actuate.health.Status;

/**
 * Unit tests for GenieHealthIndicator.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class GenieMemoryHealthIndicatorUnitTests {

    private static final int MAX_SYSTEM_MEMORY = 10_240;
    private static final int DEFAULT_JOB_MEMORY = 1_024;
    private static final int MAX_JOB_MEMORY = 5_120;

    private GenieMemoryHealthIndicator genieMemoryHealthIndicator;
    private JobMetricsService jobMetricsService;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.jobMetricsService = Mockito.mock(JobMetricsService.class);
        final JobsProperties jobsProperties = new JobsProperties();
        jobsProperties.getMemory().setDefaultJobMemory(DEFAULT_JOB_MEMORY);
        jobsProperties.getMemory().setMaxSystemMemory(MAX_SYSTEM_MEMORY);
        jobsProperties.getMemory().setMaxJobMemory(MAX_JOB_MEMORY);

        this.genieMemoryHealthIndicator = new GenieMemoryHealthIndicator(this.jobMetricsService, jobsProperties);
    }

    /**
     * Test to make sure the various health conditions are met.
     */
    @Test
    public void canGetHealth() {
        Mockito.when(this.jobMetricsService.getNumActiveJobs()).thenReturn(1, 2, 3);
        Mockito.when(this.jobMetricsService.getUsedMemory())
            .thenReturn(1024, 2048, MAX_SYSTEM_MEMORY - MAX_JOB_MEMORY + 1);
        Assert.assertThat(this.genieMemoryHealthIndicator.health().getStatus(), Matchers.is(Status.UP));
        Assert.assertThat(this.genieMemoryHealthIndicator.health().getStatus(), Matchers.is(Status.UP));
        Assert.assertThat(this.genieMemoryHealthIndicator.health().getStatus(), Matchers.is(Status.OUT_OF_SERVICE));
    }
}
