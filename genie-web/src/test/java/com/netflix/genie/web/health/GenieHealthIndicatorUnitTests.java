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

import com.netflix.genie.test.categories.UnitTest;
import com.netflix.genie.web.tasks.job.JobMonitoringCoordinator;
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
public class GenieHealthIndicatorUnitTests {

    private GenieHealthIndicator genieHealthIndicator;
    private JobMonitoringCoordinator jobMonitoringCoordinator;
    private int maxRunningJobs;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.jobMonitoringCoordinator = Mockito.mock(JobMonitoringCoordinator.class);
        this.maxRunningJobs = 2;
        this.genieHealthIndicator = new GenieHealthIndicator(this.jobMonitoringCoordinator, this.maxRunningJobs);
    }

    /**
     * Test to make sure the various health conditions are met.
     */
    @Test
    public void canGetHealth() {
        Mockito.when(this.jobMonitoringCoordinator.getNumJobs()).thenReturn(1);
        Assert.assertThat(this.genieHealthIndicator.health().getStatus(), Matchers.is(Status.UP));
        Mockito.when(this.jobMonitoringCoordinator.getNumJobs()).thenReturn(2);
        Assert.assertThat(this.genieHealthIndicator.health().getStatus(), Matchers.is(Status.OUT_OF_SERVICE));
        Mockito.when(this.jobMonitoringCoordinator.getNumJobs()).thenReturn(3);
        Assert.assertThat(this.genieHealthIndicator.health().getStatus(), Matchers.is(Status.OUT_OF_SERVICE));
    }
}
