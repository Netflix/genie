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

import com.netflix.genie.core.services.JobPersistenceService;
import com.netflix.genie.core.services.JobSearchService;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.genie.web.properties.ClusterCheckerProperties;
import com.netflix.genie.web.tasks.GenieTaskScheduleType;
import org.apache.http.client.HttpClient;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.springframework.boot.actuate.autoconfigure.ManagementServerProperties;

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

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        final String hostName = UUID.randomUUID().toString();
        final ClusterCheckerProperties properties = new ClusterCheckerProperties();
        final JobSearchService jobSearchService = Mockito.mock(JobSearchService.class);
        final JobPersistenceService jobPersistenceService = Mockito.mock(JobPersistenceService.class);
        final HttpClient httpClient = Mockito.mock(HttpClient.class);
        final ManagementServerProperties serverProperties = Mockito.mock(ManagementServerProperties.class);
        this.task = new ClusterCheckerTask(
            hostName,
            properties,
            jobSearchService,
            jobPersistenceService,
            httpClient,
            serverProperties
        );
    }

    /**
     * Make sure run method works.
     */
    @Test
    public void canRun() {
        // TODO: flesh out once this is implemented
//        this.task.run();
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
