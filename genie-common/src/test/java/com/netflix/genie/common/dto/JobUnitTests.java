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
import org.apache.commons.lang3.StringUtils;
import org.assertj.core.util.Lists;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Unit tests for the Job class.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class JobUnitTests {

    private static final String NAME = UUID.randomUUID().toString();
    private static final String USER = UUID.randomUUID().toString();
    private static final String VERSION = UUID.randomUUID().toString();
    private static final List<String> COMMAND_ARGS = Lists.newArrayList(UUID.randomUUID().toString());

    /**
     * Test to make sure can build a valid Job using the builder.
     */
    @Test
    @SuppressWarnings("deprecation")
    public void canBuildJobDeprecatedConstructor() {
        final Job job = new Job.Builder(NAME, USER, VERSION, StringUtils.join(COMMAND_ARGS, StringUtils.SPACE)).build();
        Assert.assertThat(job.getName(), Matchers.is(NAME));
        Assert.assertThat(job.getUser(), Matchers.is(USER));
        Assert.assertThat(job.getVersion(), Matchers.is(VERSION));
        Assert.assertThat(
            job.getCommandArgs().orElseThrow(IllegalArgumentException::new),
            Matchers.is(StringUtils.join(COMMAND_ARGS, StringUtils.SPACE))
        );
        Assert.assertThat(
            job.getCommandArgsString(),
            Matchers.is(StringUtils.join(COMMAND_ARGS, StringUtils.SPACE))
        );
        Assert.assertFalse(job.getArchiveLocation().isPresent());
        Assert.assertFalse(job.getClusterName().isPresent());
        Assert.assertFalse(job.getCommandName().isPresent());
        Assert.assertFalse(job.getFinished().isPresent());
        Assert.assertFalse(job.getStarted().isPresent());
        Assert.assertThat(job.getStatus(), Matchers.is(JobStatus.INIT));
        Assert.assertFalse(job.getStatusMsg().isPresent());
        Assert.assertFalse(job.getCreated().isPresent());
        Assert.assertFalse(job.getDescription().isPresent());
        Assert.assertFalse(job.getId().isPresent());
        Assert.assertThat(job.getTags(), Matchers.empty());
        Assert.assertFalse(job.getUpdated().isPresent());
        Assert.assertThat(job.getRuntime(), Matchers.is(Duration.ZERO));
        Assert.assertThat(job.getGrouping(), Matchers.is(Optional.empty()));
        Assert.assertThat(job.getGroupingInstance(), Matchers.is(Optional.empty()));
    }

    /**
     * Test to make sure can build a valid Job using the builder.
     */
    @Test
    public void canBuildJob() {
        final Job job = new Job.Builder(NAME, USER, VERSION).build();
        Assert.assertThat(job.getName(), Matchers.is(NAME));
        Assert.assertThat(job.getUser(), Matchers.is(USER));
        Assert.assertThat(job.getVersion(), Matchers.is(VERSION));
        Assert.assertThat(job.getCommandArgs(), Matchers.is(Optional.empty()));
        Assert.assertThat(job.getCommandArgsString(), Matchers.isEmptyString());
        Assert.assertFalse(job.getArchiveLocation().isPresent());
        Assert.assertFalse(job.getClusterName().isPresent());
        Assert.assertFalse(job.getCommandName().isPresent());
        Assert.assertFalse(job.getFinished().isPresent());
        Assert.assertFalse(job.getStarted().isPresent());
        Assert.assertThat(job.getStatus(), Matchers.is(JobStatus.INIT));
        Assert.assertFalse(job.getStatusMsg().isPresent());
        Assert.assertFalse(job.getCreated().isPresent());
        Assert.assertFalse(job.getDescription().isPresent());
        Assert.assertFalse(job.getId().isPresent());
        Assert.assertThat(job.getTags(), Matchers.empty());
        Assert.assertFalse(job.getUpdated().isPresent());
        Assert.assertThat(job.getRuntime(), Matchers.is(Duration.ZERO));
        Assert.assertThat(job.getGrouping(), Matchers.is(Optional.empty()));
        Assert.assertThat(job.getGroupingInstance(), Matchers.is(Optional.empty()));
    }

    /**
     * Test to make sure can build a valid Job with optional parameters.
     *
     * @throws Exception on error
     */
    @Test
    @SuppressWarnings("deprecation")
    public void canBuildJobWithOptionalsDeprecated() throws Exception {
        final Job.Builder builder = new Job.Builder(NAME, USER, VERSION);

        builder.withCommandArgs(StringUtils.join(COMMAND_ARGS, StringUtils.SPACE));

        final String archiveLocation = UUID.randomUUID().toString();
        builder.withArchiveLocation(archiveLocation);

        final String clusterName = UUID.randomUUID().toString();
        builder.withClusterName(clusterName);

        final String commandName = UUID.randomUUID().toString();
        builder.withCommandName(commandName);

        final Date finished = new Date();
        builder.withFinished(finished);

        final Date started = new Date();
        builder.withStarted(started);

        builder.withStatus(JobStatus.SUCCEEDED);

        final String statusMsg = UUID.randomUUID().toString();
        builder.withStatusMsg(statusMsg);

        final Date created = new Date();
        builder.withCreated(created);

        final String description = UUID.randomUUID().toString();
        builder.withDescription(description);

        final String id = UUID.randomUUID().toString();
        builder.withId(id);

        final Set<String> tags = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );
        builder.withTags(tags);

        final Date updated = new Date();
        builder.withUpdated(updated);

        final String grouping = UUID.randomUUID().toString();
        builder.withGrouping(grouping);

        final String groupingInstance = UUID.randomUUID().toString();
        builder.withGroupingInstance(groupingInstance);

        final Job job = builder.build();
        Assert.assertThat(job.getName(), Matchers.is(NAME));
        Assert.assertThat(job.getUser(), Matchers.is(USER));
        Assert.assertThat(job.getVersion(), Matchers.is(VERSION));
        Assert.assertThat(
            job.getCommandArgs().orElseThrow(IllegalArgumentException::new),
            Matchers.is(StringUtils.join(COMMAND_ARGS, StringUtils.SPACE))
        );
        Assert.assertThat(
            job.getCommandArgsString(),
            Matchers.is(StringUtils.join(COMMAND_ARGS, StringUtils.SPACE))
        );
        Assert.assertThat(
            job.getArchiveLocation().orElseThrow(IllegalArgumentException::new), Matchers.is(archiveLocation)
        );
        Assert.assertThat(job.getClusterName().orElseThrow(IllegalArgumentException::new), Matchers.is(clusterName));
        Assert.assertThat(job.getCommandName().orElseThrow(IllegalArgumentException::new), Matchers.is(commandName));
        Assert.assertThat(job.getFinished().orElseThrow(IllegalArgumentException::new), Matchers.is(finished));
        Assert.assertThat(job.getStarted().orElseThrow(IllegalArgumentException::new), Matchers.is(started));
        Assert.assertThat(job.getStatus(), Matchers.is(JobStatus.SUCCEEDED));
        Assert.assertThat(job.getStatusMsg().orElseThrow(IllegalArgumentException::new), Matchers.is(statusMsg));
        Assert.assertThat(job.getCreated().orElseThrow(IllegalArgumentException::new), Matchers.is(created));
        Assert.assertThat(job.getDescription().orElseThrow(IllegalArgumentException::new), Matchers.is(description));
        Assert.assertThat(job.getId().orElseThrow(IllegalArgumentException::new), Matchers.is(id));
        Assert.assertThat(job.getTags(), Matchers.is(tags));
        Assert.assertThat(job.getUpdated().orElseThrow(IllegalArgumentException::new), Matchers.is(updated));
        Assert.assertThat(job.getRuntime(), Matchers.is(Duration.ofMillis(finished.getTime() - started.getTime())));
        Assert.assertThat(job.getGrouping().orElseThrow(IllegalArgumentException::new), Matchers.is(grouping));
        Assert.assertThat(
            job.getGroupingInstance().orElseThrow(IllegalArgumentException::new),
            Matchers.is(groupingInstance)
        );
    }

    /**
     * Test to make sure can build a valid Job with optional parameters.
     *
     * @throws Exception on error
     */
    @Test
    public void canBuildJobWithOptionals() throws Exception {
        final Job.Builder builder = new Job.Builder(NAME, USER, VERSION);

        builder.withCommandArgs(COMMAND_ARGS);

        final String archiveLocation = UUID.randomUUID().toString();
        builder.withArchiveLocation(archiveLocation);

        final String clusterName = UUID.randomUUID().toString();
        builder.withClusterName(clusterName);

        final String commandName = UUID.randomUUID().toString();
        builder.withCommandName(commandName);

        final Date finished = new Date();
        builder.withFinished(finished);

        final Date started = new Date();
        builder.withStarted(started);

        builder.withStatus(JobStatus.SUCCEEDED);

        final String statusMsg = UUID.randomUUID().toString();
        builder.withStatusMsg(statusMsg);

        final Date created = new Date();
        builder.withCreated(created);

        final String description = UUID.randomUUID().toString();
        builder.withDescription(description);

        final String id = UUID.randomUUID().toString();
        builder.withId(id);

        final Set<String> tags = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );
        builder.withTags(tags);

        final Date updated = new Date();
        builder.withUpdated(updated);

        final String grouping = UUID.randomUUID().toString();
        builder.withGrouping(grouping);

        final String groupingInstance = UUID.randomUUID().toString();
        builder.withGroupingInstance(groupingInstance);

        final Job job = builder.build();
        Assert.assertThat(job.getName(), Matchers.is(NAME));
        Assert.assertThat(job.getUser(), Matchers.is(USER));
        Assert.assertThat(job.getVersion(), Matchers.is(VERSION));
        Assert.assertThat(
            job.getCommandArgs().orElseThrow(IllegalArgumentException::new),
            Matchers.is(StringUtils.join(COMMAND_ARGS, StringUtils.SPACE))
        );
        Assert.assertThat(
            job.getCommandArgsString(),
            Matchers.is(StringUtils.join(COMMAND_ARGS, StringUtils.SPACE))
        );
        Assert.assertThat(
            job.getArchiveLocation().orElseThrow(IllegalArgumentException::new), Matchers.is(archiveLocation)
        );
        Assert.assertThat(job.getClusterName().orElseThrow(IllegalArgumentException::new), Matchers.is(clusterName));
        Assert.assertThat(job.getCommandName().orElseThrow(IllegalArgumentException::new), Matchers.is(commandName));
        Assert.assertThat(job.getFinished().orElseThrow(IllegalArgumentException::new), Matchers.is(finished));
        Assert.assertThat(job.getStarted().orElseThrow(IllegalArgumentException::new), Matchers.is(started));
        Assert.assertThat(job.getStatus(), Matchers.is(JobStatus.SUCCEEDED));
        Assert.assertThat(job.getStatusMsg().orElseThrow(IllegalArgumentException::new), Matchers.is(statusMsg));
        Assert.assertThat(job.getCreated().orElseThrow(IllegalArgumentException::new), Matchers.is(created));
        Assert.assertThat(job.getDescription().orElseThrow(IllegalArgumentException::new), Matchers.is(description));
        Assert.assertThat(job.getId().orElseThrow(IllegalArgumentException::new), Matchers.is(id));
        Assert.assertThat(job.getTags(), Matchers.is(tags));
        Assert.assertThat(job.getUpdated().orElseThrow(IllegalArgumentException::new), Matchers.is(updated));
        Assert.assertThat(job.getRuntime(), Matchers.is(Duration.ofMillis(finished.getTime() - started.getTime())));
        Assert.assertThat(job.getGrouping().orElseThrow(IllegalArgumentException::new), Matchers.is(grouping));
        Assert.assertThat(
            job.getGroupingInstance().orElseThrow(IllegalArgumentException::new),
            Matchers.is(groupingInstance)
        );
    }

    /**
     * Test to make sure a Job can be successfully built when nulls are inputted.
     */
    @Test
    public void canBuildJobWithNulls() {
        final Job.Builder builder = new Job.Builder(NAME, USER, VERSION);
        builder.withCommandArgs((List<String>) null);
        builder.withArchiveLocation(null);
        builder.withClusterName(null);
        builder.withCommandName(null);
        builder.withFinished(null);
        builder.withStarted(null);
        builder.withStatus(null);
        builder.withStatusMsg(null);
        builder.withCreated(null);
        builder.withDescription(null);
        builder.withId(null);
        builder.withTags(null);
        builder.withUpdated(null);

        final Job job = builder.build();
        Assert.assertThat(job.getName(), Matchers.is(NAME));
        Assert.assertThat(job.getUser(), Matchers.is(USER));
        Assert.assertThat(job.getVersion(), Matchers.is(VERSION));
        Assert.assertFalse(job.getCommandArgs().isPresent());
        Assert.assertEquals("", job.getCommandArgsString());
        Assert.assertFalse(job.getArchiveLocation().isPresent());
        Assert.assertFalse(job.getClusterName().isPresent());
        Assert.assertFalse(job.getCommandName().isPresent());
        Assert.assertFalse(job.getFinished().isPresent());
        Assert.assertFalse(job.getStarted().isPresent());
        Assert.assertThat(job.getStatus(), Matchers.is(JobStatus.INIT));
        Assert.assertFalse(job.getStatusMsg().isPresent());
        Assert.assertFalse(job.getCreated().isPresent());
        Assert.assertFalse(job.getDescription().isPresent());
        Assert.assertFalse(job.getId().isPresent());
        Assert.assertThat(job.getTags(), Matchers.empty());
        Assert.assertFalse(job.getUpdated().isPresent());
        Assert.assertThat(job.getRuntime(), Matchers.is(Duration.ZERO));
    }

    /**
     * Test equals.
     */
    @Test
    public void canFindEquality() {
        final Job.Builder builder = new Job.Builder(NAME, USER, VERSION);
        builder.withCommandArgs((List<String>) null);
        builder.withArchiveLocation(null);
        builder.withClusterName(null);
        builder.withCommandName(null);
        builder.withFinished(null);
        builder.withStarted(null);
        builder.withStatus(null);
        builder.withStatusMsg(null);
        builder.withCreated(null);
        builder.withDescription(null);
        builder.withId(UUID.randomUUID().toString());
        builder.withTags(null);
        builder.withUpdated(null);

        final Job job1 = builder.build();
        final Job job2 = builder.build();
        builder.withId(UUID.randomUUID().toString());
        final Job job3 = builder.build();

        Assert.assertTrue(job1.equals(job2));
        Assert.assertTrue(job2.equals(job1));
        Assert.assertFalse(job1.equals(job3));
    }

    /**
     * Test hash code.
     */
    @Test
    public void canUseHashCode() {
        final Job.Builder builder = new Job.Builder(NAME, USER, VERSION);
        builder.withCommandArgs((List<String>) null);
        builder.withArchiveLocation(null);
        builder.withClusterName(null);
        builder.withCommandName(null);
        builder.withFinished(null);
        builder.withStarted(null);
        builder.withStatus(null);
        builder.withStatusMsg(null);
        builder.withCreated(null);
        builder.withDescription(null);
        builder.withId(UUID.randomUUID().toString());
        builder.withTags(null);
        builder.withUpdated(null);

        final Job job1 = builder.build();
        final Job job2 = builder.build();
        builder.withId(UUID.randomUUID().toString());
        final Job job3 = builder.build();

        Assert.assertEquals(job1.hashCode(), job2.hashCode());
        Assert.assertNotEquals(job1.hashCode(), job3.hashCode());
    }
}
