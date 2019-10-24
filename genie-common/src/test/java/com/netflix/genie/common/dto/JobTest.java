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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Unit tests for the Job class.
 *
 * @author tgianos
 * @since 3.0.0
 */
class JobTest {

    private static final String NAME = UUID.randomUUID().toString();
    private static final String USER = UUID.randomUUID().toString();
    private static final String VERSION = UUID.randomUUID().toString();
    private static final List<String> COMMAND_ARGS = Lists.newArrayList(UUID.randomUUID().toString());

    /**
     * Test to make sure can build a valid Job using the builder.
     */
    @Test
    @SuppressWarnings("deprecation")
    void canBuildJobDeprecatedConstructor() {
        final Job job = new Job.Builder(NAME, USER, VERSION, StringUtils.join(COMMAND_ARGS, StringUtils.SPACE)).build();
        Assertions.assertThat(job.getName()).isEqualTo(NAME);
        Assertions.assertThat(job.getUser()).isEqualTo(USER);
        Assertions.assertThat(job.getVersion()).isEqualTo(VERSION);
        Assertions
            .assertThat(job.getCommandArgs().orElseThrow(IllegalArgumentException::new))
            .isEqualTo(StringUtils.join(COMMAND_ARGS, StringUtils.SPACE));
        Assertions.assertThat(job.getArchiveLocation().isPresent()).isFalse();
        Assertions.assertThat(job.getClusterName().isPresent()).isFalse();
        Assertions.assertThat(job.getCommandName().isPresent()).isFalse();
        Assertions.assertThat(job.getFinished().isPresent()).isFalse();
        Assertions.assertThat(job.getStarted().isPresent()).isFalse();
        Assertions.assertThat(job.getStatus()).isEqualTo(JobStatus.INIT);
        Assertions.assertThat(job.getStatusMsg().isPresent()).isFalse();
        Assertions.assertThat(job.getCreated().isPresent()).isFalse();
        Assertions.assertThat(job.getDescription().isPresent()).isFalse();
        Assertions.assertThat(job.getId().isPresent()).isFalse();
        Assertions.assertThat(job.getTags()).isEmpty();
        Assertions.assertThat(job.getUpdated().isPresent()).isFalse();
        Assertions.assertThat(job.getRuntime()).isEqualTo(Duration.ZERO);
        Assertions.assertThat(job.getGrouping().isPresent()).isFalse();
        Assertions.assertThat(job.getGroupingInstance().isPresent()).isFalse();
    }

    /**
     * Test to make sure can build a valid Job using the builder.
     */
    @Test
    void canBuildJob() {
        final Job job = new Job.Builder(NAME, USER, VERSION).build();
        Assertions.assertThat(job.getName()).isEqualTo(NAME);
        Assertions.assertThat(job.getUser()).isEqualTo(USER);
        Assertions.assertThat(job.getVersion()).isEqualTo(VERSION);
        Assertions.assertThat(job.getCommandArgs().isPresent()).isFalse();
        Assertions.assertThat(job.getArchiveLocation().isPresent()).isFalse();
        Assertions.assertThat(job.getClusterName().isPresent()).isFalse();
        Assertions.assertThat(job.getCommandName().isPresent()).isFalse();
        Assertions.assertThat(job.getFinished().isPresent()).isFalse();
        Assertions.assertThat(job.getStarted().isPresent()).isFalse();
        Assertions.assertThat(job.getStatus()).isEqualTo(JobStatus.INIT);
        Assertions.assertThat(job.getStatusMsg().isPresent()).isFalse();
        Assertions.assertThat(job.getCreated().isPresent()).isFalse();
        Assertions.assertThat(job.getDescription().isPresent()).isFalse();
        Assertions.assertThat(job.getId().isPresent()).isFalse();
        Assertions.assertThat(job.getTags()).isEmpty();
        Assertions.assertThat(job.getUpdated().isPresent()).isFalse();
        Assertions.assertThat(job.getRuntime()).isEqualTo(Duration.ZERO);
        Assertions.assertThat(job.getGrouping().isPresent()).isFalse();
        Assertions.assertThat(job.getGroupingInstance().isPresent()).isFalse();
    }

    /**
     * Test to make sure can build a valid Job with optional parameters.
     */
    @Test
    @SuppressWarnings("deprecation")
    void canBuildJobWithOptionalsDeprecated() {
        final Job.Builder builder = new Job.Builder(NAME, USER, VERSION);

        builder.withCommandArgs(StringUtils.join(COMMAND_ARGS, StringUtils.SPACE));

        final String archiveLocation = UUID.randomUUID().toString();
        builder.withArchiveLocation(archiveLocation);

        final String clusterName = UUID.randomUUID().toString();
        builder.withClusterName(clusterName);

        final String commandName = UUID.randomUUID().toString();
        builder.withCommandName(commandName);

        final Instant finished = Instant.now();
        builder.withFinished(finished);

        final Instant started = Instant.now();
        builder.withStarted(started);

        builder.withStatus(JobStatus.SUCCEEDED);

        final String statusMsg = UUID.randomUUID().toString();
        builder.withStatusMsg(statusMsg);

        final Instant created = Instant.now();
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

        final Instant updated = Instant.now();
        builder.withUpdated(updated);

        final String grouping = UUID.randomUUID().toString();
        builder.withGrouping(grouping);

        final String groupingInstance = UUID.randomUUID().toString();
        builder.withGroupingInstance(groupingInstance);

        final Job job = builder.build();
        Assertions.assertThat(job.getName()).isEqualTo(NAME);
        Assertions.assertThat(job.getUser()).isEqualTo(USER);
        Assertions.assertThat(job.getVersion()).isEqualTo(VERSION);
        Assertions
            .assertThat(job.getCommandArgs().orElseThrow(IllegalArgumentException::new))
            .isEqualTo(StringUtils.join(COMMAND_ARGS, StringUtils.SPACE));
        Assertions
            .assertThat(job.getArchiveLocation().orElseThrow(IllegalArgumentException::new))
            .isEqualTo(archiveLocation);
        Assertions.assertThat(job.getClusterName().orElseThrow(IllegalArgumentException::new)).isEqualTo(clusterName);
        Assertions.assertThat(job.getCommandName().orElseThrow(IllegalArgumentException::new)).isEqualTo(commandName);
        Assertions.assertThat(job.getFinished().orElseThrow(IllegalArgumentException::new)).isEqualTo(finished);
        Assertions.assertThat(job.getStarted().orElseThrow(IllegalArgumentException::new)).isEqualTo(started);
        Assertions.assertThat(job.getStatus()).isEqualTo(JobStatus.SUCCEEDED);
        Assertions.assertThat(job.getStatusMsg().orElseThrow(IllegalArgumentException::new)).isEqualTo(statusMsg);
        Assertions.assertThat(job.getCreated().orElseThrow(IllegalArgumentException::new)).isEqualTo(created);
        Assertions.assertThat(job.getDescription().orElseThrow(IllegalArgumentException::new)).isEqualTo(description);
        Assertions.assertThat(job.getId().orElseThrow(IllegalArgumentException::new)).isEqualTo(id);
        Assertions.assertThat(job.getTags()).isEqualTo(tags);
        Assertions.assertThat(job.getUpdated().orElseThrow(IllegalArgumentException::new)).isEqualTo(updated);
        Assertions
            .assertThat(job.getRuntime())
            .isEqualByComparingTo(Duration.ofMillis(finished.toEpochMilli() - started.toEpochMilli()));
        Assertions.assertThat(job.getGrouping().orElseThrow(IllegalArgumentException::new)).isEqualTo(grouping);
        Assertions
            .assertThat(job.getGroupingInstance().orElseThrow(IllegalArgumentException::new))
            .isEqualTo(groupingInstance);
    }

    /**
     * Test to make sure can build a valid Job with optional parameters.
     */
    @Test
    void canBuildJobWithOptionals() {
        final Job.Builder builder = new Job.Builder(NAME, USER, VERSION);

        builder.withCommandArgs(COMMAND_ARGS);

        final String archiveLocation = UUID.randomUUID().toString();
        builder.withArchiveLocation(archiveLocation);

        final String clusterName = UUID.randomUUID().toString();
        builder.withClusterName(clusterName);

        final String commandName = UUID.randomUUID().toString();
        builder.withCommandName(commandName);

        final Instant finished = Instant.now();
        builder.withFinished(finished);

        final Instant started = Instant.now();
        builder.withStarted(started);

        builder.withStatus(JobStatus.SUCCEEDED);

        final String statusMsg = UUID.randomUUID().toString();
        builder.withStatusMsg(statusMsg);

        final Instant created = Instant.now();
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

        final Instant updated = Instant.now();
        builder.withUpdated(updated);

        final String grouping = UUID.randomUUID().toString();
        builder.withGrouping(grouping);

        final String groupingInstance = UUID.randomUUID().toString();
        builder.withGroupingInstance(groupingInstance);

        final Job job = builder.build();
        Assertions.assertThat(job.getName()).isEqualTo(NAME);
        Assertions.assertThat(job.getUser()).isEqualTo(USER);
        Assertions.assertThat(job.getVersion()).isEqualTo(VERSION);
        Assertions
            .assertThat(job.getCommandArgs().orElseThrow(IllegalArgumentException::new))
            .isEqualTo(StringUtils.join(COMMAND_ARGS, StringUtils.SPACE));
        Assertions
            .assertThat(job.getArchiveLocation().orElseThrow(IllegalArgumentException::new))
            .isEqualTo(archiveLocation);
        Assertions.assertThat(job.getClusterName().orElseThrow(IllegalArgumentException::new)).isEqualTo(clusterName);
        Assertions.assertThat(job.getCommandName().orElseThrow(IllegalArgumentException::new)).isEqualTo(commandName);
        Assertions.assertThat(job.getFinished().orElseThrow(IllegalArgumentException::new)).isEqualTo(finished);
        Assertions.assertThat(job.getStarted().orElseThrow(IllegalArgumentException::new)).isEqualTo(started);
        Assertions.assertThat(job.getStatus()).isEqualTo(JobStatus.SUCCEEDED);
        Assertions.assertThat(job.getStatusMsg().orElseThrow(IllegalArgumentException::new)).isEqualTo(statusMsg);
        Assertions.assertThat(job.getCreated().orElseThrow(IllegalArgumentException::new)).isEqualTo(created);
        Assertions.assertThat(job.getDescription().orElseThrow(IllegalArgumentException::new)).isEqualTo(description);
        Assertions.assertThat(job.getId().orElseThrow(IllegalArgumentException::new)).isEqualTo(id);
        Assertions.assertThat(job.getTags()).isEqualTo(tags);
        Assertions.assertThat(job.getUpdated().orElseThrow(IllegalArgumentException::new)).isEqualTo(updated);
        Assertions
            .assertThat(job.getRuntime())
            .isEqualByComparingTo(Duration.ofMillis(finished.toEpochMilli() - started.toEpochMilli()));
        Assertions.assertThat(job.getGrouping().orElseThrow(IllegalArgumentException::new)).isEqualTo(grouping);
        Assertions
            .assertThat(job.getGroupingInstance().orElseThrow(IllegalArgumentException::new))
            .isEqualTo(groupingInstance);
    }

    /**
     * Test to make sure a Job can be successfully built when nulls are inputted.
     */
    @Test
    void canBuildJobWithNulls() {
        final Job.Builder builder = new Job.Builder(NAME, USER, VERSION);
        builder.withCommandArgs((List<String>) null);
        builder.withArchiveLocation(null);
        builder.withClusterName(null);
        builder.withCommandName(null);
        builder.withFinished(null);
        builder.withStarted(null);
        builder.withStatus(JobStatus.INIT);
        builder.withStatusMsg(null);
        builder.withCreated(null);
        builder.withDescription(null);
        builder.withId(null);
        builder.withTags(null);
        builder.withUpdated(null);

        final Job job = builder.build();
        Assertions.assertThat(job.getName()).isEqualTo(NAME);
        Assertions.assertThat(job.getUser()).isEqualTo(USER);
        Assertions.assertThat(job.getVersion()).isEqualTo(VERSION);
        Assertions.assertThat(job.getCommandArgs().isPresent()).isFalse();
        Assertions.assertThat(job.getArchiveLocation().isPresent()).isFalse();
        Assertions.assertThat(job.getClusterName().isPresent()).isFalse();
        Assertions.assertThat(job.getCommandName().isPresent()).isFalse();
        Assertions.assertThat(job.getFinished().isPresent()).isFalse();
        Assertions.assertThat(job.getStarted().isPresent()).isFalse();
        Assertions.assertThat(job.getStatus()).isEqualTo(JobStatus.INIT);
        Assertions.assertThat(job.getStatusMsg().isPresent()).isFalse();
        Assertions.assertThat(job.getCreated().isPresent()).isFalse();
        Assertions.assertThat(job.getDescription().isPresent()).isFalse();
        Assertions.assertThat(job.getId().isPresent()).isFalse();
        Assertions.assertThat(job.getTags()).isEmpty();
        Assertions.assertThat(job.getUpdated().isPresent()).isFalse();
        Assertions.assertThat(job.getRuntime()).isEqualTo(Duration.ZERO);
    }

    /**
     * Test equals.
     */
    @Test
    void canFindEquality() {
        final Job.Builder builder = new Job.Builder(NAME, USER, VERSION);
        builder.withCommandArgs((List<String>) null);
        builder.withArchiveLocation(null);
        builder.withClusterName(null);
        builder.withCommandName(null);
        builder.withFinished(null);
        builder.withStarted(null);
        builder.withStatus(JobStatus.INIT);
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

        Assertions.assertThat(job1).isEqualTo(job2);
        Assertions.assertThat(job1).isNotEqualTo(job3);
    }

    /**
     * Test hash code.
     */
    @Test
    void canUseHashCode() {
        final Job.Builder builder = new Job.Builder(NAME, USER, VERSION);
        builder.withCommandArgs((List<String>) null);
        builder.withArchiveLocation(null);
        builder.withClusterName(null);
        builder.withCommandName(null);
        builder.withFinished(null);
        builder.withStarted(null);
        builder.withStatus(JobStatus.INIT);
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

        Assertions.assertThat(job1.hashCode()).isEqualTo(job2.hashCode());
        Assertions.assertThat(job1.hashCode()).isNotEqualTo(job3.hashCode());
    }

    /**
     * Test to prove a bug with command args splitting with trailing whitespace was corrected.
     */
    @Test
    void testCommandArgsEdgeCases() {
        final Job.Builder builder = new Job.Builder(NAME, USER, VERSION);

        String commandArgs = " blah ";
        builder.withCommandArgs(commandArgs);
        Assertions
            .assertThat(builder.build().getCommandArgs().orElseThrow(IllegalArgumentException::new))
            .isEqualTo(" blah ");
        commandArgs = " blah    ";
        builder.withCommandArgs(commandArgs);
        Assertions
            .assertThat(builder.build().getCommandArgs().orElseThrow(IllegalArgumentException::new))
            .isEqualTo(" blah    ");
        commandArgs = "  blah blah     blah\nblah\tblah \"blah\" blah  ";
        builder.withCommandArgs(commandArgs);
        Assertions
            .assertThat(builder.build().getCommandArgs().orElseThrow(IllegalArgumentException::new))
            .isEqualTo("  blah blah     blah\nblah\tblah \"blah\" blah  ");
        builder.withCommandArgs(Lists.newArrayList("blah", "blah", "  blah", "\nblah", "\"blah\""));
        Assertions
            .assertThat(builder.build().getCommandArgs().orElseThrow(IllegalArgumentException::new))
            .isEqualTo("blah blah   blah \nblah \"blah\"");
    }
}
