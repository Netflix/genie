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
package com.netflix.genie.web.events;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.UUID;
import java.util.concurrent.Future;

/**
 * Tests for the JobScheduledEvent class.
 */
public class JobScheduledEventTest {

    /**
     * Make sure we can successfully create a Job Started Event.
     */
    @Test
    public void canConstruct() {
        final String jobId = UUID.randomUUID().toString();
        final Future<?> task = Mockito.mock(Future.class);
        final int memory = 1_034;
        final Object source = new Object();
        final JobScheduledEvent event = new JobScheduledEvent(jobId, task, memory, source);
        Assert.assertNotNull(event);
        Assert.assertThat(event.getId(), Matchers.is(jobId));
        Assert.assertThat(event.getTask(), Matchers.is(task));
        Assert.assertThat(event.getMemory(), Matchers.is(memory));
        Assert.assertThat(event.getSource(), Matchers.is(source));
    }
}
