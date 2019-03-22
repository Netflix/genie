/*
 *
 *  Copyright 2015 Netflix, Inc.
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
package com.netflix.genie.common.dto;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.util.UUID;

/**
 * Unit tests for the JobExecution class.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class JobExecutionTest {

    private static final String HOST_NAME = UUID.randomUUID().toString();
    private static final long CHECK_DELAY = 280843L;
    private static final int PROCESS_ID = 134234;
    private static final Instant TIMEOUT = Instant.now();
    private static final int MEMORY = 1_024;

    /**
     * Test to make sure can build a valid JobExecution using the builder.
     */
    @Test
    public void canBuildJob() {
        final JobExecution execution = new JobExecution.Builder(HOST_NAME).build();
        Assert.assertThat(execution.getHostName(), Matchers.is(HOST_NAME));
        Assert.assertFalse(execution.getProcessId().isPresent());
        Assert.assertFalse(execution.getCheckDelay().isPresent());
        Assert.assertFalse(execution.getTimeout().isPresent());
        Assert.assertFalse(execution.getExitCode().isPresent());
        Assert.assertFalse(execution.getCreated().isPresent());
        Assert.assertFalse(execution.getId().isPresent());
        Assert.assertFalse(execution.getUpdated().isPresent());
        Assert.assertFalse(execution.getMemory().isPresent());
    }

    /**
     * Test to make sure can build a valid JobExecution with optional parameters.
     */
    @Test
    public void canBuildJobWithOptionals() {
        final JobExecution.Builder builder = new JobExecution.Builder(HOST_NAME);

        builder.withCheckDelay(CHECK_DELAY);
        builder.withProcessId(PROCESS_ID);
        builder.withTimeout(TIMEOUT);
        builder.withMemory(MEMORY);

        final int exitCode = 0;
        builder.withExitCode(exitCode);

        final Instant created = Instant.now();
        builder.withCreated(created);

        final String id = UUID.randomUUID().toString();
        builder.withId(id);

        final Instant updated = Instant.now();
        builder.withUpdated(updated);

        final JobExecution execution = builder.build();
        Assert.assertThat(execution.getHostName(), Matchers.is(HOST_NAME));
        Assert.assertThat(execution.getProcessId().orElseThrow(IllegalArgumentException::new), Matchers.is(PROCESS_ID));
        Assert.assertThat(
            execution.getCheckDelay().orElseThrow(IllegalArgumentException::new), Matchers.is(CHECK_DELAY)
        );
        Assert.assertThat(execution.getTimeout().orElseThrow(IllegalArgumentException::new), Matchers.is(TIMEOUT));
        Assert.assertThat(execution.getExitCode().orElseThrow(IllegalArgumentException::new), Matchers.is(exitCode));
        Assert.assertThat(execution.getCreated().orElseThrow(IllegalArgumentException::new), Matchers.is(created));
        Assert.assertThat(execution.getId().orElseThrow(IllegalArgumentException::new), Matchers.is(id));
        Assert.assertThat(execution.getUpdated().orElseThrow(IllegalArgumentException::new), Matchers.is(updated));
        Assert.assertThat(execution.getMemory().orElseThrow(IllegalArgumentException::new), Matchers.is(MEMORY));
    }

    /**
     * Test to make sure a JobExecution can be successfully built when nulls are inputted.
     */
    @Test
    public void canBuildJobWithNulls() {
        final JobExecution.Builder builder = new JobExecution.Builder(HOST_NAME);
        builder.withExitCode(null);
        builder.withProcessId(null);
        builder.withCheckDelay(null);
        builder.withTimeout(null);
        builder.withMemory(null);
        builder.withCreated(null);
        builder.withId(null);
        builder.withUpdated(null);

        final JobExecution execution = builder.build();
        Assert.assertThat(execution.getHostName(), Matchers.is(HOST_NAME));
        Assert.assertFalse(execution.getProcessId().isPresent());
        Assert.assertFalse(execution.getCheckDelay().isPresent());
        Assert.assertFalse(execution.getTimeout().isPresent());
        Assert.assertFalse(execution.getExitCode().isPresent());
        Assert.assertFalse(execution.getCreated().isPresent());
        Assert.assertFalse(execution.getId().isPresent());
        Assert.assertFalse(execution.getUpdated().isPresent());
        Assert.assertFalse(execution.getMemory().isPresent());
    }

    /**
     * Test equals.
     */
    @Test
    public void canFindEquality() {
        final JobExecution.Builder builder = new JobExecution.Builder(HOST_NAME);
        builder.withCreated(null);
        builder.withId(UUID.randomUUID().toString());
        builder.withUpdated(null);

        final JobExecution jobExecution1 = builder.build();
        final JobExecution jobExecution2 = builder.build();
        builder.withId(UUID.randomUUID().toString());
        final JobExecution jobExecution3 = builder.build();

        Assert.assertEquals(jobExecution1, jobExecution2);
        Assert.assertEquals(jobExecution2, jobExecution1);
        Assert.assertNotEquals(jobExecution1, jobExecution3);
    }

    /**
     * Test hash code.
     */
    @Test
    public void canUseHashCode() {
        final JobExecution.Builder builder = new JobExecution.Builder(HOST_NAME);
        builder.withCreated(null);
        builder.withId(UUID.randomUUID().toString());
        builder.withUpdated(null);

        final JobExecution jobExecution1 = builder.build();
        final JobExecution jobExecution2 = builder.build();
        builder.withId(UUID.randomUUID().toString());
        final JobExecution jobExecution3 = builder.build();

        Assert.assertEquals(jobExecution1.hashCode(), jobExecution2.hashCode());
        Assert.assertNotEquals(jobExecution1.hashCode(), jobExecution3.hashCode());
    }
}
