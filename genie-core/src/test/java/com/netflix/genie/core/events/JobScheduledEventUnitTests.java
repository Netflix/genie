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
package com.netflix.genie.core.events;

import com.google.common.collect.Lists;
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.util.List;
import java.util.UUID;

/**
 * Tests for the JobScheduledEvent class.
 */
@Category(UnitTest.class)
public class JobScheduledEventUnitTests {

    /**
     * Make sure we can successfully create a Job Started Event.
     */
    @Test
    public void canConstruct() {
        final String jobId = UUID.randomUUID().toString();
        final int memory = 1_034;
        final Object source = new Object();
        final JobRequest jobRequest = Mockito.mock(JobRequest.class);
        final Cluster cluster = Mockito.mock(Cluster.class);
        final Command command = Mockito.mock(Command.class);
        final List<Application> applications = Lists.newArrayList();
        final JobScheduledEvent event = new JobScheduledEvent(jobId, jobRequest, cluster,
            command, applications, memory, source);
        Assert.assertNotNull(event);
        Assert.assertThat(event.getId(), Matchers.is(jobId));
        Assert.assertThat(event.getJobRequest(), Matchers.is(jobRequest));
        Assert.assertThat(event.getMemory(), Matchers.is(memory));
        Assert.assertThat(event.getSource(), Matchers.is(source));
    }
}
