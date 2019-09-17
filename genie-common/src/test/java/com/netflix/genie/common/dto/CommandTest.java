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
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Set;
import java.util.UUID;

/**
 * Tests for the Command DTO.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class CommandTest {

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
    public void canBuildCommand() {
        final Command command
            = new Command.Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY).build();
        Assert.assertThat(command.getName(), Matchers.is(NAME));
        Assert.assertThat(command.getUser(), Matchers.is(USER));
        Assert.assertThat(command.getVersion(), Matchers.is(VERSION));
        Assert.assertThat(command.getStatus(), Matchers.is(CommandStatus.ACTIVE));
        Assert.assertThat(command.getExecutable(), Matchers.is(EXECUTABLE));
        Assert.assertThat(command.getExecutableAndArguments(), Matchers.is(EXECUTABLE_AND_ARGS));
        Assert.assertFalse(command.getSetupFile().isPresent());
        Assert.assertThat(command.getConfigs(), Matchers.empty());
        Assert.assertThat(command.getDependencies(), Matchers.empty());
        Assert.assertFalse(command.getCreated().isPresent());
        Assert.assertFalse(command.getDescription().isPresent());
        Assert.assertFalse(command.getId().isPresent());
        Assert.assertThat(command.getTags(), Matchers.empty());
        Assert.assertFalse(command.getUpdated().isPresent());
        Assert.assertFalse(command.getMemory().isPresent());
    }

    /**
     * Test to make sure we can build a command using the deprecated builder constructor.
     */
    @Test
    public void canBuildCommandWithDeprecatedConstructor() {
        final Command command
            = new Command.Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE, CHECK_DELAY).build();
        Assert.assertThat(command.getName(), Matchers.is(NAME));
        Assert.assertThat(command.getUser(), Matchers.is(USER));
        Assert.assertThat(command.getVersion(), Matchers.is(VERSION));
        Assert.assertThat(command.getStatus(), Matchers.is(CommandStatus.ACTIVE));
        Assert.assertThat(command.getExecutable(), Matchers.is(EXECUTABLE));
        Assert.assertThat(command.getExecutableAndArguments(), Matchers.is(EXECUTABLE_AND_ARGS));
        Assert.assertFalse(command.getSetupFile().isPresent());
        Assert.assertThat(command.getConfigs(), Matchers.empty());
        Assert.assertThat(command.getDependencies(), Matchers.empty());
        Assert.assertFalse(command.getCreated().isPresent());
        Assert.assertFalse(command.getDescription().isPresent());
        Assert.assertFalse(command.getId().isPresent());
        Assert.assertThat(command.getTags(), Matchers.empty());
        Assert.assertFalse(command.getUpdated().isPresent());
        Assert.assertFalse(command.getMemory().isPresent());
    }

    /**
     * Test to make sure we can build a command with all optional parameters.
     */
    @Test
    public void canBuildCommandWithOptionals() {
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

        final Command command = builder.build();
        Assert.assertThat(command.getName(), Matchers.is(NAME));
        Assert.assertThat(command.getUser(), Matchers.is(USER));
        Assert.assertThat(command.getVersion(), Matchers.is(VERSION));
        Assert.assertThat(command.getStatus(), Matchers.is(CommandStatus.ACTIVE));
        Assert.assertThat(command.getExecutable(), Matchers.is(EXECUTABLE));
        Assert.assertThat(command.getExecutableAndArguments(), Matchers.is(EXECUTABLE_AND_ARGS));
        Assert.assertThat(command.getCheckDelay(), Matchers.is(CHECK_DELAY));
        Assert.assertThat(command.getSetupFile().orElseThrow(IllegalArgumentException::new), Matchers.is(setupFile));
        Assert.assertThat(command.getConfigs(), Matchers.is(configs));
        Assert.assertThat(command.getDependencies(), Matchers.is(dependencies));
        Assert.assertThat(command.getCreated().orElseThrow(IllegalArgumentException::new), Matchers.is(created));
        Assert.assertThat(
            command.getDescription().orElseThrow(IllegalArgumentException::new), Matchers.is(description)
        );
        Assert.assertThat(command.getId().orElseThrow(IllegalArgumentException::new), Matchers.is(id));
        Assert.assertThat(command.getTags(), Matchers.is(tags));
        Assert.assertThat(command.getUpdated().orElseThrow(IllegalArgumentException::new), Matchers.is(updated));
        Assert.assertThat(command.getMemory().orElseThrow(IllegalArgumentException::new), Matchers.is(MEMORY));
    }

    /**
     * Test to make sure we can build a command with null collection parameters.
     */
    @Test
    public void canBuildCommandNullOptionals() {
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

        final Command command = builder.build();
        Assert.assertThat(command.getName(), Matchers.is(NAME));
        Assert.assertThat(command.getUser(), Matchers.is(USER));
        Assert.assertThat(command.getVersion(), Matchers.is(VERSION));
        Assert.assertThat(command.getStatus(), Matchers.is(CommandStatus.ACTIVE));
        Assert.assertThat(command.getExecutable(), Matchers.is(EXECUTABLE));
        Assert.assertThat(command.getExecutableAndArguments(), Matchers.is(EXECUTABLE_AND_ARGS));
        Assert.assertFalse(command.getSetupFile().isPresent());
        Assert.assertThat(command.getConfigs(), Matchers.empty());
        Assert.assertThat(command.getDependencies(), Matchers.empty());
        Assert.assertFalse(command.getCreated().isPresent());
        Assert.assertFalse(command.getDescription().isPresent());
        Assert.assertFalse(command.getId().isPresent());
        Assert.assertThat(command.getTags(), Matchers.empty());
        Assert.assertFalse(command.getUpdated().isPresent());
        Assert.assertFalse(command.getMemory().isPresent());
    }

    /**
     * Test equals.
     */
    @Test
    public void canFindEquality() {
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

        Assert.assertEquals(command1, command2);
        Assert.assertEquals(command2, command1);
        Assert.assertNotEquals(command1, command3);
    }

    /**
     * Test hash code.
     */
    @Test
    public void canUseHashCode() {
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

        Assert.assertEquals(command1.hashCode(), command2.hashCode());
        Assert.assertNotEquals(command1.hashCode(), command3.hashCode());
    }

    /**
     * Test creation fails without an executable.
     */
    @Test(expected = IllegalArgumentException.class)
    public void cantBuildWithoutExecutable() {
        new Command.Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, CHECK_DELAY).build();
    }

    /**
     * Test to make sure we if both executable fields are set, the new one takes precedence.
     */
    @Test
    public void canBuildWithConflictingExecutableFields() {

        final ArrayList<String> expectedExecutable = Lists.newArrayList("exec", "arg1", "arg2");

        final Command command
            = new Command.Builder(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE_AND_ARGS, CHECK_DELAY)
            .withExecutable("foo")
            .withExecutableAndArguments(expectedExecutable)
            .build();

        Assert.assertThat(command.getExecutableAndArguments(), Matchers.is(expectedExecutable));
        Assert.assertThat(command.getExecutable(), Matchers.is(StringUtils.join(expectedExecutable, ' ')));
    }
}
