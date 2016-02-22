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

import com.google.common.collect.Sets;
import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Date;
import java.util.Set;
import java.util.UUID;

/**
 * Unit tests for the JobExecution class.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class JobExecutionUnitTests {

    private static final String HOST_NAME = UUID.randomUUID().toString();
    private static final int PROCESS_ID = 134234;

    /**
     * Test to make sure can build a valid JobExecution using the builder.
     */
    @Test
    public void canBuildJob() {
        final JobExecution execution = new JobExecution.Builder(HOST_NAME, PROCESS_ID).build();
        Assert.assertThat(execution.getHostName(), Matchers.is(HOST_NAME));
        Assert.assertThat(execution.getProcessId(), Matchers.is(PROCESS_ID));
        Assert.assertThat(execution.getClusterCriteria(), Matchers.empty());
        Assert.assertThat(execution.getExitCode(), Matchers.is(-1));
        Assert.assertThat(execution.getCreated(), Matchers.nullValue());
        Assert.assertThat(execution.getId(), Matchers.nullValue());
        Assert.assertThat(execution.getUpdated(), Matchers.nullValue());
    }

    /**
     * Test to make sure can build a valid JobExecution with optional parameters.
     */
    @Test
    public void canBuildJobWithOptionals() {
        final JobExecution.Builder builder = new JobExecution.Builder(HOST_NAME, PROCESS_ID);

        final Set<String> clusterCriteria = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        builder.withClusterCriteria(clusterCriteria);

        final int exitCode = 0;
        builder.withExitCode(exitCode);

        final Date created = new Date();
        builder.withCreated(created);

        final String id = UUID.randomUUID().toString();
        builder.withId(id);

        final Date updated = new Date();
        builder.withUpdated(updated);

        final JobExecution execution = builder.build();
        Assert.assertThat(execution.getHostName(), Matchers.is(HOST_NAME));
        Assert.assertThat(execution.getProcessId(), Matchers.is(PROCESS_ID));
        Assert.assertThat(execution.getClusterCriteria(), Matchers.is(clusterCriteria));
        Assert.assertThat(execution.getExitCode(), Matchers.is(exitCode));
        Assert.assertThat(execution.getCreated(), Matchers.is(created));
        Assert.assertThat(execution.getId(), Matchers.is(id));
        Assert.assertThat(execution.getUpdated(), Matchers.is(updated));
    }

    /**
     * Test to make sure a JobExecution can be successfully built when nulls are inputted.
     */
    @Test
    public void canBuildJobWithNulls() {
        final JobExecution.Builder builder = new JobExecution.Builder(HOST_NAME, PROCESS_ID);
        builder.withClusterCriteria(null);
        builder.withCreated(null);
        builder.withId(null);
        builder.withUpdated(null);

        final JobExecution execution = builder.build();
        Assert.assertThat(execution.getHostName(), Matchers.is(HOST_NAME));
        Assert.assertThat(execution.getProcessId(), Matchers.is(PROCESS_ID));
        Assert.assertThat(execution.getClusterCriteria(), Matchers.empty());
        Assert.assertThat(execution.getExitCode(), Matchers.is(-1));
        Assert.assertThat(execution.getCreated(), Matchers.nullValue());
        Assert.assertThat(execution.getId(), Matchers.nullValue());
        Assert.assertThat(execution.getUpdated(), Matchers.nullValue());
    }

    /**
     * Test equals.
     */
    @Test
    public void canFindEquality() {
        final JobExecution.Builder builder = new JobExecution.Builder(HOST_NAME, PROCESS_ID);
        builder.withClusterCriteria(null);
        builder.withCreated(null);
        builder.withId(UUID.randomUUID().toString());
        builder.withUpdated(null);

        final JobExecution jobExecution1 = builder.build();
        final JobExecution jobExecution2 = builder.build();
        builder.withId(UUID.randomUUID().toString());
        final JobExecution jobExecution3 = builder.build();

        Assert.assertTrue(jobExecution1.equals(jobExecution2));
        Assert.assertTrue(jobExecution2.equals(jobExecution1));
        Assert.assertFalse(jobExecution1.equals(jobExecution3));
    }

    /**
     * Test hash code.
     */
    @Test
    public void canUseHashCode() {
        final JobExecution.Builder builder = new JobExecution.Builder(HOST_NAME, PROCESS_ID);
        builder.withClusterCriteria(null);
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
