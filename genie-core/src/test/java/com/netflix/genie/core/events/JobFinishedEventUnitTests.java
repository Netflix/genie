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

import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.UUID;

/**
 * Tests for the JobFinishedEvent class.
 */
@Category(UnitTest.class)
public class JobFinishedEventUnitTests {

    /**
     * Make sure we can successfully create a Job Finished Event.
     */
    @Test
    public void canConstruct() {
        final JobExecution.Builder jobExecutionBuilder
            = new JobExecution.Builder(UUID.randomUUID().toString(), 3028, 1808234L);
        jobExecutionBuilder.withId(UUID.randomUUID().toString());
        final JobExecution jobExecution = jobExecutionBuilder.build();
        final Object source = new Object();
        final JobFinishedEvent event = new JobFinishedEvent(jobExecution, source);
        Assert.assertNotNull(event);
        Assert.assertThat(event.getJobExecution(), Matchers.is(jobExecution));
        Assert.assertThat(event.getSource(), Matchers.is(source));
    }
}
