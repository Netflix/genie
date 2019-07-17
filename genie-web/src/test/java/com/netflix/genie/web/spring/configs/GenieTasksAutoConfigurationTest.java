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
package com.netflix.genie.web.spring.configs;

import com.netflix.genie.web.properties.AgentCleanupProperties;
import com.netflix.genie.web.properties.TasksSchedulerPoolProperties;
import com.netflix.genie.web.properties.UserMetricsProperties;
import com.netflix.genie.web.services.JobPersistenceService;
import com.netflix.genie.web.services.JobSearchService;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for the TaskConfig class.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class GenieTasksAutoConfigurationTest {

    /**
     * Make sure we get a valid process executor to use.
     */
    @Test
    public void canGetExecutor() {
        Assert.assertNotNull(new GenieTasksAutoConfiguration().processExecutor());
    }

    /**
     * Make sure we get a valid task scheduler to use.
     */
    @Test
    public void canGetTaskScheduler() {
        Assert.assertNotNull(new GenieTasksAutoConfiguration().genieTaskScheduler(new TasksSchedulerPoolProperties()));
    }

    /**
     * Make sure we get a valid task.
     */
    @Test
    public void canGetUserMetricsTask() {
        Assert.assertNotNull(
            new GenieTasksAutoConfiguration().userMetricsTask(
                Mockito.mock(MeterRegistry.class),
                Mockito.mock(JobSearchService.class),
                Mockito.mock(UserMetricsProperties.class)
            )
        );
        Assert.assertNotNull(new GenieTasksAutoConfiguration().genieTaskScheduler(new TasksSchedulerPoolProperties()));
    }


    /**
     * Make sure we get a valid task.
     */
    @Test
    public void canGetAgentJobCleanupTask() {
        Assert.assertNotNull(
            new GenieTasksAutoConfiguration().agentJobCleanupTask(
                Mockito.mock(JobSearchService.class),
                Mockito.mock(JobPersistenceService.class),
                Mockito.mock(AgentCleanupProperties.class),
                Mockito.mock(MeterRegistry.class)
            )
        );
    }
}
