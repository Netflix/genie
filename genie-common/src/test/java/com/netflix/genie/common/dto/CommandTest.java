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
import com.netflix.genie.common.external.dtos.v4.Criterion;
import org.apache.commons.lang3.StringUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Tests for the {@link Command} class.
 *
 * @author tgianos
 * @since 3.0.0
 */
class CommandTest {

    private static final String NAME = UUID.randomUUID().toString();
    private static final String USER = UUID.randomUUID().toString();
    private static final String VERSION = UUID.randomUUID().toString();
    private static final long CHECK_DELAY = 12380L;
    private static final ArrayList<String> EXECUTABLE_AND_ARGS = Lists.newArrayList("foo-cli", "--verbose");
    private static final String EXECUTABLE = StringUtils.join(EXECUTABLE_AND_ARGS, " ");
    private static final int MEMORY = 10_255;

    /**
     * Test to make sure we can build a command using the default builder constructor.
     */
    @Test
    void canBuildCommand() {
        final Command command
            = new Command.Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY).build();
        Assertions.assertThat(command.getName()).isEqualTo(NAME);
        Assertions.assertThat(command.getUser()).isEqualTo(USER);
        Assertions.assertThat(command.getVersion()).isEqualTo(VERSION);
        Assertions.assertThat(command.getStatus()).isEqualTo(CommandStatus.ACTIVE);
        Assertions.assertThat(command.getExecutable()).isEqualTo(EXECUTABLE);
        Assertions.assertThat(command.getExecutableAndArguments()).isEqualTo(EXECUTABLE_AND_ARGS);
        Assertions.assertThat(command.getSetupFile().isPresent()).isFalse();
        Assertions.assertThat(command.getConfigs()).isEmpty();
        Assertions.assertThat(command.getDependencies()).isEmpty();
        Assertions.assertThat(command.getCreated().isPresent()).isFalse();
        Assertions.assertThat(command.getDescription().isPresent()).isFalse();
        Assertions.assertThat(command.getId().isPresent()).isFalse();
        Assertions.assertThat(command.getTags()).isEmpty();
        Assertions.assertThat(command.getUpdated().isPresent()).isFalse();
        Assertions.assertThat(command.getMemory().isPresent()).isFalse();
        Assertions.assertThat(command.getClusterCriteria()).isEmpty();
    }

    /**
     * Test to make sure we can build a command using the deprecated builder constructor.
     */
    @Test
    void canBuildCommandWithDeprecatedConstructor() {
        final Command command
            = new Command.Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE, CHECK_DELAY).build();
        Assertions.assertThat(command.getName()).isEqualTo(NAME);
        Assertions.assertThat(command.getUser()).isEqualTo(USER);
        Assertions.assertThat(command.getVersion()).isEqualTo(VERSION);
        Assertions.assertThat(command.getStatus()).isEqualTo(CommandStatus.ACTIVE);
        Assertions.assertThat(command.getExecutable()).isEqualTo(EXECUTABLE);
        Assertions.assertThat(command.getExecutableAndArguments()).isEqualTo(EXECUTABLE_AND_ARGS);
        Assertions.assertThat(command.getSetupFile().isPresent()).isFalse();
        Assertions.assertThat(command.getConfigs()).isEmpty();
        Assertions.assertThat(command.getDependencies()).isEmpty();
        Assertions.assertThat(command.getCreated().isPresent()).isFalse();
        Assertions.assertThat(command.getDescription().isPresent()).isFalse();
        Assertions.assertThat(command.getId().isPresent()).isFalse();
        Assertions.assertThat(command.getTags()).isEmpty();
        Assertions.assertThat(command.getUpdated().isPresent()).isFalse();
        Assertions.assertThat(command.getMemory().isPresent()).isFalse();
        Assertions.assertThat(command.getClusterCriteria()).isEmpty();
    }

    /**
     * Test to make sure we can build a command with all optional parameters.
     */
    @Test
    void canBuildCommandWithOptionals() {
        final Command.Builder builder
            = new Command.Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY);

        final String setupFile = UUID.randomUUID().toString();
        builder.withSetupFile(setupFile);

        final Set<String> configs = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        builder.withConfigs(configs);

        final Set<String> dependencies = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        builder.withDependencies(dependencies);

        final Instant created = Instant.now();
        builder.withCreated(created);

        final String description = UUID.randomUUID().toString();
        builder.withDescription(description);

        final String id = UUID.randomUUID().toString();
        builder.withId(id);

        final Set<String> tags = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        builder.withTags(tags);

        final Instant updated = Instant.now();
        builder.withUpdated(updated);

        builder.withMemory(MEMORY);

        final List<Criterion> clusterCriteria = Lists.newArrayList(
            new Criterion.Builder().withId(UUID.randomUUID().toString()).build(),
            new Criterion.Builder().withName(UUID.randomUUID().toString()).build(),
            new Criterion.Builder().withStatus(UUID.randomUUID().toString()).build(),
            new Criterion.Builder().withVersion(UUID.randomUUID().toString()).build(),
            new Criterion.Builder().withTags(Sets.newHashSet(UUID.randomUUID().toString())).build()
        );
        builder.withClusterCriteria(clusterCriteria);

        final Command command = builder.build();
        Assertions.assertThat(command.getName()).isEqualTo(NAME);
        Assertions.assertThat(command.getUser()).isEqualTo(USER);
        Assertions.assertThat(command.getVersion()).isEqualTo(VERSION);
        Assertions.assertThat(command.getStatus()).isEqualTo(CommandStatus.ACTIVE);
        Assertions.assertThat(command.getExecutable()).isEqualTo(EXECUTABLE);
        Assertions.assertThat(command.getExecutableAndArguments()).isEqualTo(EXECUTABLE_AND_ARGS);
        Assertions.assertThat(command.getCheckDelay()).isEqualTo(CHECK_DELAY);
        Assertions.assertThat(command.getSetupFile()).isPresent().contains(setupFile);
        Assertions.assertThat(command.getConfigs()).isEqualTo(configs);
        Assertions.assertThat(command.getDependencies()).isEqualTo(dependencies);
        Assertions.assertThat(command.getCreated()).isPresent().contains(created);
        Assertions.assertThat(command.getDescription()).isPresent().contains(description);
        Assertions.assertThat(command.getId()).isPresent().contains(id);
        Assertions.assertThat(command.getTags()).isEqualTo(tags);
        Assertions.assertThat(command.getUpdated()).isPresent().contains(updated);
        Assertions.assertThat(command.getMemory()).isPresent().contains(MEMORY);
        Assertions.assertThat(command.getClusterCriteria()).isEqualTo(clusterCriteria);
    }

    /**
     * Test to make sure we can build a command with null collection parameters.
     */
    @Test
    void canBuildCommandNullOptionals() {
        final Command.Builder builder
            = new Command.Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY);
        builder.withSetupFile(null);
        builder.withConfigs(null);
        builder.withDependencies(null);
        builder.withCreated(null);
        builder.withDescription(null);
        builder.withId(null);
        builder.withTags(null);
        builder.withUpdated(null);
        builder.withMemory(null);
        builder.withClusterCriteria(null);

        final Command command = builder.build();
        Assertions.assertThat(command.getName()).isEqualTo(NAME);
        Assertions.assertThat(command.getUser()).isEqualTo(USER);
        Assertions.assertThat(command.getVersion()).isEqualTo(VERSION);
        Assertions.assertThat(command.getStatus()).isEqualTo(CommandStatus.ACTIVE);
        Assertions.assertThat(command.getExecutable()).isEqualTo(EXECUTABLE);
        Assertions.assertThat(command.getExecutableAndArguments()).isEqualTo(EXECUTABLE_AND_ARGS);
        Assertions.assertThat(command.getSetupFile()).isNotPresent();
        Assertions.assertThat(command.getConfigs()).isEmpty();
        Assertions.assertThat(command.getDependencies()).isEmpty();
        Assertions.assertThat(command.getCreated()).isNotPresent();
        Assertions.assertThat(command.getDescription()).isNotPresent();
        Assertions.assertThat(command.getId()).isNotPresent();
        Assertions.assertThat(command.getTags()).isEmpty();
        Assertions.assertThat(command.getUpdated()).isNotPresent();
        Assertions.assertThat(command.getMemory()).isNotPresent();
        Assertions.assertThat(command.getClusterCriteria()).isEmpty();
    }

    /**
     * Test equals.
     */
    @Test
    void canFindEquality() {
        final Command.Builder builder
            = new Command.Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY);
        builder.withSetupFile(null);
        builder.withConfigs(null);
        builder.withDependencies(null);
        builder.withCreated(null);
        builder.withDescription(null);
        builder.withId(UUID.randomUUID().toString());
        builder.withTags(null);
        builder.withUpdated(null);
        final Command command1 = builder.build();
        final Command command2 = builder.build();
        builder.withId(UUID.randomUUID().toString());
        final Command command3 = builder.build();

        Assertions.assertThat(command1).isEqualTo(command2);
        Assertions.assertThat(command1).isNotEqualTo(command3);
    }

    /**
     * Test hash code.
     */
    @Test
    void canUseHashCode() {
        final Command.Builder builder
            = new Command.Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY);
        builder.withSetupFile(null);
        builder.withConfigs(null);
        builder.withDependencies(null);
        builder.withCreated(null);
        builder.withDescription(null);
        builder.withId(UUID.randomUUID().toString());
        builder.withTags(null);
        builder.withUpdated(null);
        final Command command1 = builder.build();
        final Command command2 = builder.build();
        builder.withId(UUID.randomUUID().toString());
        final Command command3 = builder.build();

        Assertions.assertThat(command1.hashCode()).isEqualTo(command2.hashCode());
        Assertions.assertThat(command1.hashCode()).isNotEqualTo(command3.hashCode());
    }

    /**
     * Test creation fails without an executable.
     */
    @Test
    void cantBuildWithoutExecutable() {
        Assertions
            .assertThatIllegalArgumentException()
            .isThrownBy(() -> new Command.Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, CHECK_DELAY).build());
    }

    /**
     * Test to make sure we if both executable fields are set, the new one takes precedence.
     */
    @Test
    void canBuildWithConflictingExecutableFields() {
        final ArrayList<String> expectedExecutable = Lists.newArrayList("exec", "arg1", "arg2");

        final Command command
            = new Command.Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY)
            .withExecutable("foo")
            .withExecutableAndArguments(expectedExecutable)
            .build();

        Assertions.assertThat(command.getExecutableAndArguments()).isEqualTo(expectedExecutable);
        Assertions.assertThat(command.getExecutable()).isEqualTo(StringUtils.join(expectedExecutable, ' '));
    }
}
