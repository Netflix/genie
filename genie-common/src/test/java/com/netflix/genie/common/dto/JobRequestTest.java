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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * Unit tests for the {@link JobRequest} class.
 *
 * @author tgianos
 * @since 3.0.0
 */
class JobRequestTest {

    private static final String NAME = UUID.randomUUID().toString();
    private static final String USER = UUID.randomUUID().toString();
    private static final String VERSION = UUID.randomUUID().toString();
    private static final List<String> COMMAND_ARGS = Lists.newArrayList(UUID.randomUUID().toString());
    private static final List<ClusterCriteria> CLUSTER_CRITERIA = Lists.newArrayList(
        new ClusterCriteria(
            Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        ),
        new ClusterCriteria(
            Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        ),
        new ClusterCriteria(
            Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        )
    );
    private static final Set<String> COMMAND_CRITERION = Sets.newHashSet(
        UUID.randomUUID().toString(),
        UUID.randomUUID().toString()
    );

    private static Stream<Arguments> provideCommandArgsConstructorCases() {
        return Stream.of(
            Arguments.of(null, null, null),
            Arguments.of("", null, null),
            Arguments.of(null, Lists.newArrayList(), null),
            Arguments.of("foo bar", null, "foo bar"),
            Arguments.of(null, Lists.newArrayList("foo", "bar"), "foo bar"),
            Arguments.of("...", Lists.newArrayList("foo", "bar"), "foo bar")
        );
    }

    /**
     * Test to make sure can build a valid JobRequest using the builder.
     */
    @Test
    @SuppressWarnings("deprecation")
    void canBuildJobRequestDeprecated() {
        final JobRequest request = new JobRequest.Builder(
            NAME,
            USER,
            VERSION,
            StringUtils.join(COMMAND_ARGS, StringUtils.SPACE),
            CLUSTER_CRITERIA,
            COMMAND_CRITERION
        ).build();
        Assertions.assertThat(request.getName()).isEqualTo(NAME);
        Assertions.assertThat(request.getUser()).isEqualTo(USER);
        Assertions.assertThat(request.getVersion()).isEqualTo(VERSION);
        Assertions
            .assertThat(request.getCommandArgs().orElseThrow(IllegalArgumentException::new))
            .isEqualTo(StringUtils.join(COMMAND_ARGS, StringUtils.SPACE));
        Assertions.assertThat(request.getClusterCriterias()).isEqualTo(CLUSTER_CRITERIA);
        Assertions.assertThat(request.getCommandCriteria()).isEqualTo(COMMAND_CRITERION);
        Assertions.assertThat(request.getCpu().isPresent()).isFalse();
        Assertions.assertThat(request.isDisableLogArchival()).isFalse();
        Assertions.assertThat(request.getEmail().isPresent()).isFalse();
        Assertions.assertThat(request.getConfigs()).isEmpty();
        Assertions.assertThat(request.getDependencies()).isEmpty();
        Assertions.assertThat(request.getGroup().isPresent()).isFalse();
        Assertions.assertThat(request.getMemory().isPresent()).isFalse();
        Assertions.assertThat(request.getSetupFile().isPresent()).isFalse();
        Assertions.assertThat(request.getCreated().isPresent()).isFalse();
        Assertions.assertThat(request.getDescription().isPresent()).isFalse();
        Assertions.assertThat(request.getId().isPresent()).isFalse();
        Assertions.assertThat(request.getTags()).isEmpty();
        Assertions.assertThat(request.getUpdated().isPresent()).isFalse();
        Assertions.assertThat(request.getApplications()).isEmpty();
        Assertions.assertThat(request.getTimeout().isPresent()).isFalse();
        Assertions.assertThat(request.getGrouping().isPresent()).isFalse();
        Assertions.assertThat(request.getGroupingInstance().isPresent()).isFalse();
    }

    /**
     * Test to make sure can build a valid JobRequest using the builder.
     */
    @Test
    void canBuildJobRequest() {
        final JobRequest request
            = new JobRequest.Builder(NAME, USER, VERSION, CLUSTER_CRITERIA, COMMAND_CRITERION).build();
        Assertions.assertThat(request.getName()).isEqualTo(NAME);
        Assertions.assertThat(request.getUser()).isEqualTo(USER);
        Assertions.assertThat(request.getVersion()).isEqualTo(VERSION);
        Assertions.assertThat(request.getCommandArgs().isPresent()).isFalse();
        Assertions.assertThat(request.getClusterCriterias()).isEqualTo(CLUSTER_CRITERIA);
        Assertions.assertThat(request.getCommandCriteria()).isEqualTo(COMMAND_CRITERION);
        Assertions.assertThat(request.getCpu().isPresent()).isFalse();
        Assertions.assertThat(request.isDisableLogArchival()).isEqualTo(false);
        Assertions.assertThat(request.getEmail().isPresent()).isFalse();
        Assertions.assertThat(request.getConfigs()).isEmpty();
        Assertions.assertThat(request.getDependencies()).isEmpty();
        Assertions.assertThat(request.getGroup().isPresent()).isFalse();
        Assertions.assertThat(request.getMemory().isPresent()).isFalse();
        Assertions.assertThat(request.getSetupFile().isPresent()).isFalse();
        Assertions.assertThat(request.getCreated().isPresent()).isFalse();
        Assertions.assertThat(request.getDescription().isPresent()).isFalse();
        Assertions.assertThat(request.getId().isPresent()).isFalse();
        Assertions.assertThat(request.getTags()).isEmpty();
        Assertions.assertThat(request.getUpdated().isPresent()).isFalse();
        Assertions.assertThat(request.getApplications()).isEmpty();
        Assertions.assertThat(request.getTimeout().isPresent()).isFalse();
        Assertions.assertThat(request.getGrouping().isPresent()).isFalse();
        Assertions.assertThat(request.getGroupingInstance().isPresent()).isFalse();
    }

    /**
     * Test to make sure can build a valid JobRequest with optional parameters.
     */
    @Test
    @SuppressWarnings("deprecation")
    void canBuildJobRequestWithOptionalsDeprecated() {
        final JobRequest.Builder builder
            = new JobRequest.Builder(NAME, USER, VERSION, CLUSTER_CRITERIA, COMMAND_CRITERION);

        builder.withCommandArgs(StringUtils.join(COMMAND_ARGS, StringUtils.SPACE));

        final int cpu = 5;
        builder.withCpu(cpu);

        final boolean disableLogArchival = true;
        builder.withDisableLogArchival(disableLogArchival);

        final String email = UUID.randomUUID().toString() + "@netflix.com";
        builder.withEmail(email);

        final Set<String> configs = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );
        builder.withConfigs(configs);

        final Set<String> dependencies = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );
        builder.withDependencies(dependencies);

        final String group = UUID.randomUUID().toString();
        builder.withGroup(group);

        final int memory = 2048;
        builder.withMemory(memory);

        final String setupFile = UUID.randomUUID().toString();
        builder.withSetupFile(setupFile);

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

        final List<String> applications = Lists.newArrayList(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );
        builder.withApplications(applications);

        final int timeout = 8970243;
        builder.withTimeout(timeout);

        final String grouping = UUID.randomUUID().toString();
        builder.withGrouping(grouping);

        final String groupingInstance = UUID.randomUUID().toString();
        builder.withGroupingInstance(groupingInstance);

        final JobRequest request = builder.build();
        Assertions.assertThat(request.getName()).isEqualTo(NAME);
        Assertions.assertThat(request.getUser()).isEqualTo(USER);
        Assertions.assertThat(request.getVersion()).isEqualTo(VERSION);
        Assertions
            .assertThat(request.getCommandArgs().orElseThrow(IllegalArgumentException::new))
            .isEqualTo(StringUtils.join(COMMAND_ARGS, StringUtils.SPACE));
        Assertions.assertThat(request.getClusterCriterias()).isEqualTo(CLUSTER_CRITERIA);
        Assertions.assertThat(request.getCommandCriteria()).isEqualTo(COMMAND_CRITERION);
        Assertions.assertThat(request.getCpu().orElseThrow(IllegalArgumentException::new)).isEqualTo(cpu);
        Assertions.assertThat(request.isDisableLogArchival()).isEqualTo(disableLogArchival);
        Assertions.assertThat(request.getEmail().orElseThrow(IllegalArgumentException::new)).isEqualTo(email);
        Assertions.assertThat(request.getConfigs()).isEqualTo(configs);
        Assertions.assertThat(request.getDependencies()).isEqualTo(dependencies);
        Assertions.assertThat(request.getGroup().orElseThrow(IllegalArgumentException::new)).isEqualTo(group);
        Assertions.assertThat(request.getMemory().orElseThrow(IllegalArgumentException::new)).isEqualTo(memory);
        Assertions.assertThat(request.getSetupFile().orElseThrow(IllegalArgumentException::new)).isEqualTo(setupFile);
        Assertions.assertThat(request.getCreated().orElseThrow(IllegalArgumentException::new)).isEqualTo(created);
        Assertions
            .assertThat(request.getDescription().orElseThrow(IllegalArgumentException::new))
            .isEqualTo(description);
        Assertions.assertThat(request.getId().orElseThrow(IllegalArgumentException::new)).isEqualTo(id);
        Assertions.assertThat(request.getTags()).isEqualTo(tags);
        Assertions.assertThat(request.getUpdated().orElseThrow(IllegalArgumentException::new)).isEqualTo(updated);
        Assertions.assertThat(request.getApplications()).isEqualTo(applications);
        Assertions.assertThat(request.getTimeout().orElseThrow(IllegalArgumentException::new)).isEqualTo(timeout);
        Assertions.assertThat(request.getGrouping().orElseThrow(IllegalArgumentException::new)).isEqualTo(grouping);
        Assertions
            .assertThat(request.getGroupingInstance().orElseThrow(IllegalArgumentException::new))
            .isEqualTo(groupingInstance);
    }

    /**
     * Test to make sure can build a valid JobRequest with optional parameters.
     */
    @Test
    void canBuildJobRequestWithOptionals() {
        final JobRequest.Builder builder
            = new JobRequest.Builder(NAME, USER, VERSION, CLUSTER_CRITERIA, COMMAND_CRITERION);

        builder.withCommandArgs(COMMAND_ARGS);

        final int cpu = 5;
        builder.withCpu(cpu);

        final boolean disableLogArchival = true;
        builder.withDisableLogArchival(disableLogArchival);

        final String email = UUID.randomUUID().toString() + "@netflix.com";
        builder.withEmail(email);

        final Set<String> configs = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );
        builder.withConfigs(configs);

        final Set<String> dependencies = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );
        builder.withDependencies(dependencies);

        final String group = UUID.randomUUID().toString();
        builder.withGroup(group);

        final int memory = 2048;
        builder.withMemory(memory);

        final String setupFile = UUID.randomUUID().toString();
        builder.withSetupFile(setupFile);

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

        final List<String> applications = Lists.newArrayList(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );
        builder.withApplications(applications);

        final int timeout = 8970243;
        builder.withTimeout(timeout);

        final String grouping = UUID.randomUUID().toString();
        builder.withGrouping(grouping);

        final String groupingInstance = UUID.randomUUID().toString();
        builder.withGroupingInstance(groupingInstance);

        final JobRequest request = builder.build();
        Assertions.assertThat(request.getName()).isEqualTo(NAME);
        Assertions.assertThat(request.getUser()).isEqualTo(USER);
        Assertions.assertThat(request.getVersion()).isEqualTo(VERSION);
        Assertions
            .assertThat(request.getCommandArgs().orElseThrow(IllegalArgumentException::new))
            .isEqualTo(StringUtils.join(COMMAND_ARGS, StringUtils.SPACE));
        Assertions.assertThat(request.getClusterCriterias()).isEqualTo(CLUSTER_CRITERIA);
        Assertions.assertThat(request.getCommandCriteria()).isEqualTo(COMMAND_CRITERION);
        Assertions.assertThat(request.getCpu().orElseThrow(IllegalArgumentException::new)).isEqualTo(cpu);
        Assertions.assertThat(request.isDisableLogArchival()).isEqualTo(disableLogArchival);
        Assertions.assertThat(request.getEmail().orElseThrow(IllegalArgumentException::new)).isEqualTo(email);
        Assertions.assertThat(request.getConfigs()).isEqualTo(configs);
        Assertions.assertThat(request.getDependencies()).isEqualTo(dependencies);
        Assertions.assertThat(request.getGroup().orElseThrow(IllegalArgumentException::new)).isEqualTo(group);
        Assertions.assertThat(request.getMemory().orElseThrow(IllegalArgumentException::new)).isEqualTo(memory);
        Assertions.assertThat(request.getSetupFile().orElseThrow(IllegalArgumentException::new)).isEqualTo(setupFile);
        Assertions.assertThat(request.getCreated().orElseThrow(IllegalArgumentException::new)).isEqualTo(created);
        Assertions
            .assertThat(request.getDescription().orElseThrow(IllegalArgumentException::new))
            .isEqualTo(description);
        Assertions.assertThat(request.getId().orElseThrow(IllegalArgumentException::new)).isEqualTo(id);
        Assertions.assertThat(request.getTags()).isEqualTo(tags);
        Assertions.assertThat(request.getUpdated().orElseThrow(IllegalArgumentException::new)).isEqualTo(updated);
        Assertions.assertThat(request.getApplications()).isEqualTo(applications);
        Assertions.assertThat(request.getTimeout().orElseThrow(IllegalArgumentException::new)).isEqualTo(timeout);
        Assertions.assertThat(request.getGrouping().orElseThrow(IllegalArgumentException::new)).isEqualTo(grouping);
        Assertions
            .assertThat(request.getGroupingInstance().orElseThrow(IllegalArgumentException::new))
            .isEqualTo(groupingInstance);
    }

    /**
     * Test to make sure a JobRequest can be successfully built when nulls are inputted.
     */
    @Test
    void canBuildJobRequestWithNulls() {
        final JobRequest.Builder builder
            = new JobRequest.Builder(NAME, USER, VERSION, Lists.newArrayList(), Sets.newHashSet());
        builder.withCommandArgs((List<String>) null);
        builder.withEmail(null);
        builder.withConfigs(null);
        builder.withDependencies(null);
        builder.withGroup(null);
        builder.withSetupFile(null);
        builder.withCreated(null);
        builder.withDescription(null);
        builder.withId(null);
        builder.withTags(null);
        builder.withUpdated(null);
        builder.withApplications(null);

        final JobRequest request = builder.build();
        Assertions.assertThat(request.getName()).isEqualTo(NAME);
        Assertions.assertThat(request.getUser()).isEqualTo(USER);
        Assertions.assertThat(request.getVersion()).isEqualTo(VERSION);
        Assertions.assertThat(request.getCommandArgs().isPresent()).isFalse();
        Assertions.assertThat(request.getClusterCriterias()).isEmpty();
        Assertions.assertThat(request.getCommandCriteria()).isEmpty();
        Assertions.assertThat(request.getCpu().isPresent()).isFalse();
        Assertions.assertThat(request.isDisableLogArchival()).isFalse();
        Assertions.assertThat(request.getEmail().isPresent()).isFalse();
        Assertions.assertThat(request.getDependencies()).isEmpty();
        Assertions.assertThat(request.getDependencies()).isEmpty();
        Assertions.assertThat(request.getGroup().isPresent()).isFalse();
        Assertions.assertThat(request.getMemory().isPresent()).isFalse();
        Assertions.assertThat(request.getSetupFile().isPresent()).isFalse();
        Assertions.assertThat(request.getCreated().isPresent()).isFalse();
        Assertions.assertThat(request.getDescription().isPresent()).isFalse();
        Assertions.assertThat(request.getId().isPresent()).isFalse();
        Assertions.assertThat(request.getTags()).isEmpty();
        Assertions.assertThat(request.getUpdated().isPresent()).isFalse();
        Assertions.assertThat(request.getApplications()).isEmpty();
        Assertions.assertThat(request.getTimeout().isPresent()).isFalse();
    }

    /**
     * Test equals.
     */
    @Test
    void canFindEquality() {
        final JobRequest.Builder builder
            = new JobRequest.Builder(NAME, USER, VERSION, Lists.newArrayList(), Sets.newHashSet());
        builder.withEmail(null);
        builder.withConfigs(null);
        builder.withDependencies(null);
        builder.withGroup(null);
        builder.withSetupFile(null);
        builder.withCreated(null);
        builder.withDescription(null);
        builder.withId(UUID.randomUUID().toString());
        builder.withTags(null);
        builder.withUpdated(null);
        builder.withApplications(null);

        final JobRequest jobRequest1 = builder.build();
        final JobRequest jobRequest2 = builder.build();
        builder.withId(UUID.randomUUID().toString());
        final JobRequest jobRequest3 = builder.build();

        Assertions.assertThat(jobRequest1).isEqualTo(jobRequest2);
        Assertions.assertThat(jobRequest1).isNotEqualTo(jobRequest3);
    }

    /**
     * Test hash code.
     */
    @Test
    void canUseHashCode() {
        final JobRequest.Builder builder
            = new JobRequest.Builder(NAME, USER, VERSION, Lists.newArrayList(), Sets.newHashSet());
        builder.withEmail(null);
        builder.withConfigs(null);
        builder.withDependencies(null);
        builder.withGroup(null);
        builder.withSetupFile(null);
        builder.withCreated(null);
        builder.withDescription(null);
        builder.withId(UUID.randomUUID().toString());
        builder.withTags(null);
        builder.withUpdated(null);
        builder.withApplications(null);

        final JobRequest jobRequest1 = builder.build();
        final JobRequest jobRequest2 = builder.build();
        builder.withId(UUID.randomUUID().toString());
        final JobRequest jobRequest3 = builder.build();

        Assertions.assertThat(jobRequest1.hashCode()).isEqualTo(jobRequest2.hashCode());
        Assertions.assertThat(jobRequest1.hashCode()).isNotEqualTo(jobRequest3.hashCode());
    }

    /**
     * Test to prove a bug with command args splitting with trailing whitespace was corrected.
     */
    @Test
    void testCommandArgsEdgeCases() {
        final JobRequest.Builder builder
            = new JobRequest.Builder(NAME, USER, VERSION, Lists.newArrayList(), Sets.newHashSet());

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

    /**
     * Test edge cases of building a job request with two overlapping fields: the legacy commandArgs (String) and the
     * new commandArguments (List of String).
     */
    @ParameterizedTest
    @MethodSource("provideCommandArgsConstructorCases")
    void testCommandArgsConstructorEdgeCases(
        @Nullable final String commandArgs,
        @Nullable final List<String> commandArguments,
        @Nullable final String expectedCommandArgs
    ) {
        final JobRequest jobRequest = new JobRequest.Builder(
            "NAME",
            "USER",
            "VERSION",
            Lists.newArrayList(),
            Sets.newHashSet(),
            commandArgs,
            commandArguments
        ).build();

        final String message = String.format(
            "Unexpected result when constructing JobRequest with commandArgs: %s and commandArguments: %s",
            commandArgs,
            commandArguments
        );

        Assertions
            .assertThat(jobRequest.getCommandArgs().orElse(null))
            .withFailMessage(message)
            .isEqualTo(expectedCommandArgs);
    }
}
