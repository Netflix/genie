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

import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Date;
import java.util.UUID;

/**
 * Tests for the JobStartedEvent class.
 */
@Category(UnitTest.class)
public class JobStartedEventUnitTests {

    /**
     * Make sure we can successfully create a Job Started Event.
     */
    @Test
    public void canConstruct() {
        final JobExecution jobExecution = new JobExecution
            .Builder(UUID.randomUUID().toString())
            .withProcessId(3029)
            .withCheckDelay(238124L)
            .withTimeout(new Date())
            .withId(UUID.randomUUID().toString())
            .build();
        final Object source = new Object();
        final JobStartedEvent event = new JobStartedEvent(jobExecution, source);
        Assert.assertNotNull(event);
        Assert.assertThat(event.getJobExecution(), Matchers.is(jobExecution));
        Assert.assertThat(event.getSource(), Matchers.is(source));
    }
}
