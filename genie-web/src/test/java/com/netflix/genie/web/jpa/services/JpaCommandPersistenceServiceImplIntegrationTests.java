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
package com.netflix.genie.web.jpa.services;

import com.github.fge.jsonpatch.JsonPatch;
import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.DatabaseTearDown;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.internal.dto.v4.Cluster;
import com.netflix.genie.common.internal.dto.v4.Command;
import com.netflix.genie.common.internal.dto.v4.CommandMetadata;
import com.netflix.genie.common.internal.dto.v4.CommandRequest;
import com.netflix.genie.common.util.GenieObjectMapper;
import com.netflix.genie.test.categories.IntegrationTest;
import com.netflix.genie.web.services.ApplicationPersistenceService;
import com.netflix.genie.web.services.ClusterPersistenceService;
import com.netflix.genie.web.services.CommandPersistenceService;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import javax.validation.ConstraintViolationException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Integration Tests for the CommandServiceJPAImpl.
 *
 * @author tgianos
 */
@Category(IntegrationTest.class)
@DatabaseSetup("JpaCommandPersistenceServiceImplIntegrationTests/init.xml")
@DatabaseTearDown("cleanup.xml")
public class JpaCommandPersistenceServiceImplIntegrationTests extends DBIntegrationTestBase {

    private static final String APP_1_ID = "app1";
    private static final String CLUSTER_1_ID = "cluster1";

    private static final String COMMAND_1_ID = "command1";
    private static final String COMMAND_1_NAME = "pig_13_prod";
    private static final String COMMAND_1_USER = "tgianos";
    private static final String COMMAND_1_VERSION = "1.2.3";
    private static final List<String> COMMAND_1_EXECUTABLE = Lists.newArrayList("pig");
    private static final long COMMAND_1_CHECK_DELAY = 18000L;
    private static final CommandStatus COMMAND_1_STATUS = CommandStatus.ACTIVE;

    private static final String COMMAND_2_ID = "command2";
    private static final String COMMAND_2_NAME = "hive_11_prod";
    private static final String COMMAND_2_USER = "amsharma";
    private static final String COMMAND_2_VERSION = "4.5.6";
    private static final List<String> COMMAND_2_EXECUTABLE = Lists.newArrayList("hive");
    private static final CommandStatus COMMAND_2_STATUS = CommandStatus.INACTIVE;

    private static final String COMMAND_3_ID = "command3";
    private static final String COMMAND_3_NAME = "pig_11_prod";
    private static final String COMMAND_3_USER = "tgianos";
    private static final String COMMAND_3_VERSION = "7.8.9";
    private static final List<String> COMMAND_3_EXECUTABLE = Lists.newArrayList("pig");
    private static final CommandStatus COMMAND_3_STATUS = CommandStatus.DEPRECATED;

    private static final Pageable PAGE = PageRequest.of(0, 10, Sort.Direction.DESC, "updated");

    @Autowired
    private CommandPersistenceService service;

    @Autowired
    private ClusterPersistenceService clusterPersistenceService;

    @Autowired
    private ApplicationPersistenceService appService;

    /**
     * Test the get command method.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testGetCommand() throws GenieException {
        final Command command1 = this.service.getCommand(COMMAND_1_ID);
        Assert.assertEquals(COMMAND_1_ID, command1.getId());
        Assert.assertEquals(COMMAND_1_NAME, command1.getMetadata().getName());
        Assert.assertEquals(COMMAND_1_USER, command1.getMetadata().getUser());
        Assert.assertEquals(COMMAND_1_VERSION, command1.getMetadata().getVersion());
        Assert.assertEquals(COMMAND_1_STATUS, command1.getMetadata().getStatus());
        Assert.assertEquals(COMMAND_1_EXECUTABLE, command1.getExecutable());
        Assert.assertEquals(3, command1.getMetadata().getTags().size());
        Assert.assertEquals(2, command1.getResources().getConfigs().size());
        Assert.assertEquals(0, command1.getResources().getDependencies().size());

        final Command command2 = this.service.getCommand(COMMAND_2_ID);
        Assert.assertEquals(COMMAND_2_ID, command2.getId());
        Assert.assertEquals(COMMAND_2_NAME, command2.getMetadata().getName());
        Assert.assertEquals(COMMAND_2_USER, command2.getMetadata().getUser());
        Assert.assertEquals(COMMAND_2_VERSION, command2.getMetadata().getVersion());
        Assert.assertEquals(COMMAND_2_STATUS, command2.getMetadata().getStatus());
        Assert.assertEquals(COMMAND_2_EXECUTABLE, command2.getExecutable());
        Assert.assertEquals(2, command2.getMetadata().getTags().size());
        Assert.assertEquals(1, command2.getResources().getConfigs().size());
        Assert.assertEquals(1, command2.getResources().getDependencies().size());

        final Command command3 = this.service.getCommand(COMMAND_3_ID);
        Assert.assertEquals(COMMAND_3_ID, command3.getId());
        Assert.assertEquals(COMMAND_3_NAME, command3.getMetadata().getName());
        Assert.assertEquals(COMMAND_3_USER, command3.getMetadata().getUser());
        Assert.assertEquals(COMMAND_3_VERSION, command3.getMetadata().getVersion());
        Assert.assertEquals(COMMAND_3_STATUS, command3.getMetadata().getStatus());
        Assert.assertEquals(COMMAND_3_EXECUTABLE, command3.getExecutable());
        Assert.assertEquals(3, command3.getMetadata().getTags().size());
        Assert.assertEquals(1, command3.getResources().getConfigs().size());
        Assert.assertEquals(2, command3.getResources().getDependencies().size());
    }

    /**
     * Test the get commands method.
     */
    @Test
    public void testGetCommandsByName() {
        final Page<Command> commands = this.service.getCommands(COMMAND_2_NAME, null, null, null, PAGE);
        Assert.assertEquals(1, commands.getNumberOfElements());
        Assert.assertEquals(COMMAND_2_ID, commands.getContent().get(0).getId());
    }

    /**
     * Test the get commands method.
     */
    @Test
    public void testGetCommandsByUserName() {
        final Page<Command> commands = this.service.getCommands(null, COMMAND_1_USER, null, null, PAGE);
        Assert.assertEquals(2, commands.getNumberOfElements());
        Assert.assertEquals(COMMAND_3_ID, commands.getContent().get(0).getId());
        Assert.assertEquals(COMMAND_1_ID, commands.getContent().get(1).getId());
    }

    /**
     * Test the get commands method.
     */
    @Test
    public void testGetCommandsByStatuses() {
        final Set<CommandStatus> statuses = Sets.newHashSet(CommandStatus.INACTIVE, CommandStatus.DEPRECATED);
        final Page<Command> commands = this.service.getCommands(null, null, statuses, null, PAGE);
        Assert.assertEquals(2, commands.getNumberOfElements());
        Assert.assertEquals(COMMAND_2_ID, commands.getContent().get(0).getId());
        Assert.assertEquals(COMMAND_3_ID, commands.getContent().get(1).getId());
    }

    /**
     * Test the get commands method.
     */
    @Test
    public void testGetCommandsByTags() {
        final Set<String> tags = Sets.newHashSet("prod");
        Page<Command> commands = this.service.getCommands(null, null, null, tags, PAGE);
        Assert.assertEquals(3, commands.getNumberOfElements());
        Assert.assertEquals(COMMAND_2_ID, commands.getContent().get(0).getId());
        Assert.assertEquals(COMMAND_3_ID, commands.getContent().get(1).getId());
        Assert.assertEquals(COMMAND_1_ID, commands.getContent().get(2).getId());

        tags.add("pig");
        commands = this.service.getCommands(null, null, null, tags, PAGE);
        Assert.assertEquals(2, commands.getNumberOfElements());
        Assert.assertEquals(COMMAND_3_ID, commands.getContent().get(0).getId());
        Assert.assertEquals(COMMAND_1_ID, commands.getContent().get(1).getId());

        tags.clear();
        tags.add("hive");
        commands = this.service.getCommands(null, null, null, tags, PAGE);
        Assert.assertEquals(1, commands.getNumberOfElements());
        Assert.assertEquals(COMMAND_2_ID, commands.getContent().get(0).getId());

        tags.add("somethingThatWouldNeverReallyExist");
        commands = this.service.getCommands(null, null, null, tags, PAGE);
        Assert.assertTrue(commands.getContent().isEmpty());

        tags.clear();
        commands = this.service.getCommands(null, null, null, tags, PAGE);
        Assert.assertEquals(3, commands.getNumberOfElements());
        Assert.assertEquals(COMMAND_2_ID, commands.getContent().get(0).getId());
        Assert.assertEquals(COMMAND_3_ID, commands.getContent().get(1).getId());
        Assert.assertEquals(COMMAND_1_ID, commands.getContent().get(2).getId());
    }

    /**
     * Test the get commands method with descending sort.
     */
    @Test
    public void testGetCommandsDescending() {
        //Default to order by Updated
        final Page<Command> commands = this.service.getCommands(null, null, null, null, PAGE);
        Assert.assertEquals(3, commands.getNumberOfElements());
        Assert.assertEquals(COMMAND_2_ID, commands.getContent().get(0).getId());
        Assert.assertEquals(COMMAND_3_ID, commands.getContent().get(1).getId());
        Assert.assertEquals(COMMAND_1_ID, commands.getContent().get(2).getId());
    }

    /**
     * Test the get commands method with ascending sort.
     */
    @Test
    public void testGetCommandsAscending() {
        final Pageable ascending = PageRequest.of(0, 10, Sort.Direction.ASC, "updated");
        //Default to order by Updated
        final Page<Command> commands = this.service.getCommands(null, null, null, null, ascending);
        Assert.assertEquals(3, commands.getNumberOfElements());
        Assert.assertEquals(COMMAND_1_ID, commands.getContent().get(0).getId());
        Assert.assertEquals(COMMAND_3_ID, commands.getContent().get(1).getId());
        Assert.assertEquals(COMMAND_2_ID, commands.getContent().get(2).getId());
    }

    /**
     * Test the get commands method order by name.
     */
    @Test
    public void testGetCommandsOrderBysName() {
        final Pageable name = PageRequest.of(0, 10, Sort.Direction.DESC, "name");
        final Page<Command> commands = this.service.getCommands(null, null, null, null, name);
        Assert.assertEquals(3, commands.getNumberOfElements());
        Assert.assertEquals(COMMAND_1_ID, commands.getContent().get(0).getId());
        Assert.assertEquals(COMMAND_3_ID, commands.getContent().get(1).getId());
        Assert.assertEquals(COMMAND_2_ID, commands.getContent().get(2).getId());
    }

    /**
     * Test the get commands method order by an invalid field should return the order by default value (updated).
     */
    @Test(expected = RuntimeException.class)
    public void testGetCommandsOrderBysInvalidField() {
        final Pageable invalid = PageRequest.of(0, 10, Sort.Direction.DESC, "I'mNotAValidField");
        final Page<Command> commands = this.service.getCommands(null, null, null, null, invalid);
        Assert.assertEquals(3, commands.getNumberOfElements());
        Assert.assertEquals(COMMAND_2_ID, commands.getContent().get(0).getId());
        Assert.assertEquals(COMMAND_3_ID, commands.getContent().get(1).getId());
        Assert.assertEquals(COMMAND_1_ID, commands.getContent().get(2).getId());
    }

    /**
     * Test the create method.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testCreateCommand() throws GenieException {
        final String id = UUID.randomUUID().toString();
        final CommandRequest command = new CommandRequest.Builder(
            new CommandMetadata.Builder(
                COMMAND_1_NAME,
                COMMAND_1_USER,
                COMMAND_1_VERSION,
                CommandStatus.ACTIVE
            )
                .build(),
            COMMAND_1_EXECUTABLE
        )
            .withRequestedId(id)
            .withCheckDelay(COMMAND_1_CHECK_DELAY)
            .build();
        final String createdId = this.service.createCommand(command);
        Assert.assertThat(createdId, Matchers.is(id));
        final Command created = this.service.getCommand(id);
        Assert.assertNotNull(this.service.getCommand(id));
        Assert.assertEquals(id, created.getId());
        Assert.assertEquals(COMMAND_1_NAME, created.getMetadata().getName());
        Assert.assertEquals(COMMAND_1_USER, created.getMetadata().getUser());
        Assert.assertEquals(CommandStatus.ACTIVE, created.getMetadata().getStatus());
        Assert.assertEquals(COMMAND_1_EXECUTABLE, created.getExecutable());
        Assert.assertThat(COMMAND_1_CHECK_DELAY, Matchers.is(created.getCheckDelay()));
        Assert.assertFalse(created.getMemory().isPresent());
        this.service.deleteCommand(id);
        try {
            this.service.getCommand(id);
            Assert.fail("Should have thrown exception");
        } catch (final GenieException ge) {
            Assert.assertEquals(
                HttpURLConnection.HTTP_NOT_FOUND,
                ge.getErrorCode()
            );
        }
    }

    /**
     * Test the create method when no id is entered.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testCreateCommandNoId() throws GenieException {
        final int memory = 512;
        final CommandRequest command = new CommandRequest.Builder(
            new CommandMetadata.Builder(
                COMMAND_1_NAME,
                COMMAND_1_USER,
                COMMAND_1_VERSION,
                CommandStatus.ACTIVE
            )
                .build(),
            COMMAND_1_EXECUTABLE
        )
            .withMemory(memory)
            .withCheckDelay(COMMAND_1_CHECK_DELAY)
            .build();
        final String id = this.service.createCommand(command);
        final Command created = this.service.getCommand(id);
        Assert.assertNotNull(this.service.getCommand(created.getId()));
        Assert.assertEquals(COMMAND_1_NAME, created.getMetadata().getName());
        Assert.assertEquals(COMMAND_1_USER, created.getMetadata().getUser());
        Assert.assertEquals(CommandStatus.ACTIVE, created.getMetadata().getStatus());
        Assert.assertEquals(COMMAND_1_EXECUTABLE, created.getExecutable());
        Assert.assertThat(COMMAND_1_CHECK_DELAY, Matchers.is(created.getCheckDelay()));
        Assert.assertThat(created.getMemory().orElse(memory + 1), Matchers.is(memory));
        this.service.deleteCommand(created.getId());
        try {
            this.service.getCommand(created.getId());
            Assert.fail("Should have thrown exception");
        } catch (final GenieException ge) {
            Assert.assertEquals(
                HttpURLConnection.HTTP_NOT_FOUND,
                ge.getErrorCode()
            );
        }
    }

    /**
     * Test to update an command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testUpdateCommand() throws GenieException {
        final Command command = this.service.getCommand(COMMAND_1_ID);
        Assert.assertEquals(COMMAND_1_USER, command.getMetadata().getUser());
        Assert.assertEquals(CommandStatus.ACTIVE, command.getMetadata().getStatus());
        Assert.assertEquals(3, command.getMetadata().getTags().size());
        Assert.assertFalse(command.getMemory().isPresent());
        final Set<String> tags = Sets.newHashSet("yarn", "hadoop");
        tags.addAll(command.getMetadata().getTags());

        final int memory = 1_024;
        final Command updateCommand = new Command(
            command.getId(),
            command.getCreated(),
            command.getUpdated(),
            command.getResources(),
            new CommandMetadata.Builder(
                command.getMetadata().getName(),
                COMMAND_2_USER,
                command.getMetadata().getVersion(),
                CommandStatus.INACTIVE
            )
                .withMetadata(command.getMetadata().getMetadata().orElse(null))
                .withDescription(command.getMetadata().getDescription().orElse(null))
                .withTags(tags)
                .build(),
            command.getExecutable(),
            memory,
            command.getCheckDelay()
        );

        this.service.updateCommand(COMMAND_1_ID, updateCommand);

        final Command updated = this.service.getCommand(COMMAND_1_ID);
        Assert.assertEquals(COMMAND_2_USER, updated.getMetadata().getUser());
        Assert.assertEquals(CommandStatus.INACTIVE, updated.getMetadata().getStatus());
        Assert.assertEquals(5, updated.getMetadata().getTags().size());
        Assert.assertThat(updated.getMemory().orElse(memory + 1), Matchers.is(memory));
    }

    /**
     * Test to update a command with invalid content. Should throw ConstraintViolationException from JPA layer.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testUpdateCommandWithInvalidCommand() throws GenieException {
        final Command command = this.service.getCommand(COMMAND_1_ID);
        Assert.assertEquals(COMMAND_1_USER, command.getMetadata().getUser());
        Assert.assertEquals(CommandStatus.ACTIVE, command.getMetadata().getStatus());
        Assert.assertEquals(3, command.getMetadata().getTags().size());

        final Command updateCommand = new Command(
            command.getId(),
            command.getCreated(),
            command.getUpdated(),
            command.getResources(),
            new CommandMetadata.Builder(
                command.getMetadata().getName(),
                "", //invalid
                command.getMetadata().getVersion(),
                CommandStatus.INACTIVE
            )
                .withMetadata(command.getMetadata().getMetadata().orElse(null))
                .withDescription(command.getMetadata().getDescription().orElse(null))
                .withTags(command.getMetadata().getTags())
                .build(),
            command.getExecutable(),
            null,
            command.getCheckDelay()
        );

        this.service.updateCommand(COMMAND_1_ID, updateCommand);
    }

    /**
     * Test to make sure setting the created and updated outside the system control doesn't change record in database.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testUpdateCreateAndUpdate() throws GenieException {
        final Command init = this.service.getCommand(COMMAND_1_ID);
        final Instant created = init.getCreated();
        final Instant updated = init.getUpdated();

        final Command updateCommand = new Command(
            init.getId(),
            Instant.now(),
            Instant.EPOCH,
            init.getResources(),
            init.getMetadata(),
            init.getExecutable(),
            init.getMemory().orElse(null),
            init.getCheckDelay()
        );

        this.service.updateCommand(COMMAND_1_ID, updateCommand);
        final Command updatedCommand = this.service.getCommand(COMMAND_1_ID);
        Assert.assertEquals(created, updatedCommand.getCreated());
        Assert.assertNotEquals(updated, updatedCommand.getUpdated());
        Assert.assertNotEquals(Instant.EPOCH, updatedCommand.getUpdated());
    }

    /**
     * Test to patch a command.
     *
     * @throws GenieException For any problem
     * @throws IOException    For Json serialization problem
     */
    @Test
    public void testPatchCommand() throws GenieException, IOException {
        final Command getCommand = this.service.getCommand(COMMAND_1_ID);
        Assert.assertThat(getCommand.getMetadata().getName(), Matchers.is(COMMAND_1_NAME));
        final Instant updateTime = getCommand.getUpdated();

        final String patchString
            = "[{ \"op\": \"replace\", \"path\": \"/metadata/name\", \"value\": \"" + COMMAND_2_NAME + "\" }]";
        final JsonPatch patch = JsonPatch.fromJson(GenieObjectMapper.getMapper().readTree(patchString));

        this.service.patchCommand(COMMAND_1_ID, patch);

        final Command updated = this.service.getCommand(COMMAND_1_ID);
        Assert.assertNotEquals(updated.getUpdated(), Matchers.is(updateTime));
        Assert.assertThat(updated.getMetadata().getName(), Matchers.is(COMMAND_2_NAME));
    }

    /**
     * Test delete all.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testDeleteAll() throws GenieException {
        Assert.assertEquals(3, this.service.getCommands(null, null, null, null, PAGE).getNumberOfElements());
        this.service.deleteAllCommands();
        Assert.assertTrue(this.service.getCommands(null, null, null, null, PAGE).getContent().isEmpty());
    }

    /**
     * Test delete.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testDelete() throws GenieException {
        List<Command> commands = this.clusterPersistenceService.getCommandsForCluster(CLUSTER_1_ID, null);
        Assert.assertEquals(3, commands.size());
        boolean found = false;
        for (final Command command : commands) {
            if (COMMAND_1_ID.equals(command.getId())) {
                found = true;
                break;
            }
        }
        Assert.assertTrue(found);
        // TODO: Fix once Command service goes to V4
        Set<Command> appCommands
            = this.appService.getCommandsForApplication(APP_1_ID, null);
        Assert.assertEquals(1, appCommands.size());
        found = false;
        for (final Command command : appCommands) {
            if (COMMAND_1_ID.equals(command.getId())) {
                found = true;
                break;
            }
        }
        Assert.assertTrue(found);

        //Actually delete it
        this.service.deleteCommand(COMMAND_1_ID);

        commands = this.clusterPersistenceService.getCommandsForCluster(CLUSTER_1_ID, null);
        Assert.assertEquals(2, commands.size());
        found = false;
        for (final Command command : commands) {
            if (COMMAND_1_ID.equals(command.getId())) {
                found = true;
                break;
            }
        }
        Assert.assertFalse(found);
        appCommands = this.appService.getCommandsForApplication(APP_1_ID, null);
        Assert.assertTrue(appCommands.isEmpty());

        //Test a case where the app has no commands to
        //make sure that also works.
        this.service.deleteCommand(COMMAND_3_ID);
    }

    /**
     * Test add configurations to command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testAddConfigsToCommand() throws GenieException {
        final String newConfig1 = UUID.randomUUID().toString();
        final String newConfig2 = UUID.randomUUID().toString();
        final String newConfig3 = UUID.randomUUID().toString();

        final Set<String> newConfigs = Sets.newHashSet(newConfig1, newConfig2, newConfig3);

        Assert.assertEquals(2, this.service.getConfigsForCommand(COMMAND_1_ID).size());
        this.service.addConfigsForCommand(COMMAND_1_ID, newConfigs);
        final Set<String> finalConfigs = this.service.getConfigsForCommand(COMMAND_1_ID);
        Assert.assertEquals(5, finalConfigs.size());
        Assert.assertTrue(finalConfigs.contains(newConfig1));
        Assert.assertTrue(finalConfigs.contains(newConfig2));
        Assert.assertTrue(finalConfigs.contains(newConfig3));
    }

    /**
     * Test update configurations for command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testUpdateConfigsForCommand() throws GenieException {
        final String newConfig1 = UUID.randomUUID().toString();
        final String newConfig2 = UUID.randomUUID().toString();
        final String newConfig3 = UUID.randomUUID().toString();

        final Set<String> newConfigs = Sets.newHashSet(newConfig1, newConfig2, newConfig3);

        Assert.assertEquals(2, this.service.getConfigsForCommand(COMMAND_1_ID).size());
        this.service.updateConfigsForCommand(COMMAND_1_ID, newConfigs);
        final Set<String> finalConfigs = this.service.getConfigsForCommand(COMMAND_1_ID);
        Assert.assertEquals(3, finalConfigs.size());
        Assert.assertTrue(finalConfigs.contains(newConfig1));
        Assert.assertTrue(finalConfigs.contains(newConfig2));
        Assert.assertTrue(finalConfigs.contains(newConfig3));
    }

    /**
     * Test get configurations for command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testGetConfigsForCommand() throws GenieException {
        Assert.assertEquals(2,
            this.service.getConfigsForCommand(COMMAND_1_ID).size());
    }

    /**
     * Test remove all configurations for command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testRemoveAllConfigsForCommand() throws GenieException {
        Assert.assertEquals(2, this.service.getConfigsForCommand(COMMAND_1_ID).size());
        this.service.removeAllConfigsForCommand(COMMAND_1_ID);
        Assert.assertEquals(0, this.service.getConfigsForCommand(COMMAND_1_ID).size());
    }

    /**
     * Test remove configuration for command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testRemoveConfigForCommand() throws GenieException {
        final Set<String> configs = this.service.getConfigsForCommand(COMMAND_1_ID);
        Assert.assertEquals(2, configs.size());
        final String removedConfig = configs.iterator().next();
        this.service.removeConfigForCommand(COMMAND_1_ID, removedConfig);
        Assert.assertFalse(this.service.getConfigsForCommand(COMMAND_1_ID).contains(removedConfig));
    }

    /**
     * Test add dependencies to command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testAddDependenciesToCommand() throws GenieException {
        final String newDependency1 = UUID.randomUUID().toString();
        final String newDependency2 = UUID.randomUUID().toString();
        final String newDependency3 = UUID.randomUUID().toString();

        final Set<String> newDependencies = Sets.newHashSet(newDependency1, newDependency2, newDependency3);

        Assert.assertEquals(2, this.service.getDependenciesForCommand(COMMAND_3_ID).size());
        this.service.addDependenciesForCommand(COMMAND_3_ID, newDependencies);
        final Set<String> finalDependencies = this.service.getDependenciesForCommand(COMMAND_3_ID);
        Assert.assertEquals(5, finalDependencies.size());
        Assert.assertTrue(finalDependencies.contains(newDependency1));
        Assert.assertTrue(finalDependencies.contains(newDependency2));
        Assert.assertTrue(finalDependencies.contains(newDependency3));
    }

    /**
     * Test update dependencies for command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testUpdateDependenciesForCommand() throws GenieException {
        final String newDependency1 = UUID.randomUUID().toString();
        final String newDependency2 = UUID.randomUUID().toString();
        final String newDependency3 = UUID.randomUUID().toString();

        final Set<String> newDependencies = Sets.newHashSet(newDependency1, newDependency2, newDependency3);

        Assert.assertEquals(0, this.service.getDependenciesForCommand(COMMAND_1_ID).size());
        this.service.updateDependenciesForCommand(COMMAND_1_ID, newDependencies);
        final Set<String> finalDependencies = this.service.getDependenciesForCommand(COMMAND_1_ID);
        Assert.assertEquals(3, finalDependencies.size());
        Assert.assertTrue(finalDependencies.contains(newDependency1));
        Assert.assertTrue(finalDependencies.contains(newDependency2));
        Assert.assertTrue(finalDependencies.contains(newDependency3));
    }

    /**
     * Test get dependencies for command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testGetDependenciesForCommand() throws GenieException {
        Assert.assertEquals(1,
            this.service.getDependenciesForCommand(COMMAND_2_ID).size());
    }

    /**
     * Test remove all dependencies for command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testRemoveAllDependenciesForCommand() throws GenieException {
        Assert.assertEquals(2, this.service.getDependenciesForCommand(COMMAND_3_ID).size());
        this.service.removeAllDependenciesForCommand(COMMAND_3_ID);
        Assert.assertEquals(0, this.service.getDependenciesForCommand(COMMAND_3_ID).size());
    }

    /**
     * Test remove dependencies for command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testRemoveDependencyForCommand() throws GenieException {
        final Set<String> dependencies = this.service.getDependenciesForCommand(COMMAND_3_ID);
        Assert.assertEquals(2, dependencies.size());
        final String removedDependency = dependencies.iterator().next();
        this.service.removeDependencyForCommand(COMMAND_3_ID, removedDependency);
        Assert.assertFalse(this.service.getDependenciesForCommand(COMMAND_3_ID).contains(removedDependency));
    }

    /**
     * Test setting the applications for a given command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testAddApplicationsForCommand() throws GenieException {
        Assert.assertTrue(this.service.getApplicationsForCommand(COMMAND_2_ID).isEmpty());

        final List<String> appIds = new ArrayList<>();
        appIds.add(APP_1_ID);
        final Set<Command> preCommands
            = this.appService.getCommandsForApplication(APP_1_ID, null);
        Assert.assertEquals(1, preCommands.size());
        Assert.assertEquals(1, preCommands
            .stream()
            .filter(command -> COMMAND_1_ID.equals(command.getId()))
            .count()
        );

        this.service.addApplicationsForCommand(COMMAND_2_ID, appIds);

        final Set<Command> savedCommands
            = this.appService.getCommandsForApplication(APP_1_ID, null);
        Assert.assertEquals(2, savedCommands.size());
        Assert.assertThat(
            this.service.getApplicationsForCommand(COMMAND_2_ID).get(0).getId(),
            Matchers.is(APP_1_ID)
        );
    }

    /**
     * Test setting the applications for a given command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testSetApplicationsForCommand() throws GenieException {
        Assert.assertTrue(this.service.getApplicationsForCommand(COMMAND_2_ID).isEmpty());

        final List<String> appIds = Lists.newArrayList(APP_1_ID);
        final Set<Command> preCommands
            = this.appService.getCommandsForApplication(APP_1_ID, null);
        Assert.assertEquals(1, preCommands.size());
        Assert.assertEquals(1, preCommands
            .stream()
            .filter(command -> COMMAND_1_ID.equals(command.getId()))
            .count()
        );

        this.service.setApplicationsForCommand(COMMAND_2_ID, appIds);

        final Set<Command> savedCommands
            = this.appService.getCommandsForApplication(APP_1_ID, null);
        Assert.assertEquals(2, savedCommands.size());
        Assert.assertEquals(
            1,
            this.service.getApplicationsForCommand(COMMAND_2_ID)
                .stream()
                .filter(application -> APP_1_ID.equals(application.getId()))
                .count()
        );
    }

    /**
     * Test get applications for command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testGetApplicationsForCommand() throws GenieException {
        Assert.assertEquals(1, this.service.getApplicationsForCommand(COMMAND_1_ID)
            .stream()
            .filter(application -> APP_1_ID.equals(application.getId()))
            .count()
        );
    }

    /**
     * Test remove applications for command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testRemoveApplicationsForCommand() throws GenieException {
        Assert.assertEquals(1, this.service.getApplicationsForCommand(COMMAND_1_ID).size());
        this.service.removeApplicationsForCommand(COMMAND_1_ID);
        Assert.assertEquals(0, this.service.getApplicationsForCommand(COMMAND_1_ID).size());
    }

    /**
     * Test remove application for command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testRemoveApplicationForCommand() throws GenieException {
        Assert.assertEquals(1, this.service.getApplicationsForCommand(COMMAND_1_ID).size());
        this.service.removeApplicationForCommand(COMMAND_1_ID, APP_1_ID);
        Assert.assertEquals(0, this.service.getApplicationsForCommand(COMMAND_1_ID).size());
    }

    /**
     * Test add tags to command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testAddTagsToCommand() throws GenieException {
        final String newTag1 = UUID.randomUUID().toString();
        final String newTag2 = UUID.randomUUID().toString();
        final String newTag3 = UUID.randomUUID().toString();

        final Set<String> newTags = Sets.newHashSet(newTag1, newTag2, newTag3);

        Assert.assertEquals(3, this.service.getTagsForCommand(COMMAND_1_ID).size());
        this.service.addTagsForCommand(COMMAND_1_ID, newTags);
        final Set<String> finalTags = this.service.getTagsForCommand(COMMAND_1_ID);
        Assert.assertEquals(6, finalTags.size());
        Assert.assertTrue(finalTags.contains(newTag1));
        Assert.assertTrue(finalTags.contains(newTag2));
        Assert.assertTrue(finalTags.contains(newTag3));
    }

    /**
     * Test update tags for command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testUpdateTagsForCommand() throws GenieException {
        final String newTag1 = UUID.randomUUID().toString();
        final String newTag2 = UUID.randomUUID().toString();
        final String newTag3 = UUID.randomUUID().toString();

        final Set<String> newTags = Sets.newHashSet(newTag1, newTag2, newTag3);

        Assert.assertEquals(3, this.service.getTagsForCommand(COMMAND_1_ID).size());
        this.service.updateTagsForCommand(COMMAND_1_ID, newTags);
        final Set<String> finalTags = this.service.getTagsForCommand(COMMAND_1_ID);
        Assert.assertEquals(3, finalTags.size());
        Assert.assertTrue(finalTags.contains(newTag1));
        Assert.assertTrue(finalTags.contains(newTag2));
        Assert.assertTrue(finalTags.contains(newTag3));
    }

    /**
     * Test get tags for command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testGetTagsForCommand() throws GenieException {
        Assert.assertEquals(3, this.service.getTagsForCommand(COMMAND_1_ID).size());
    }

    /**
     * Test remove all tags for command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testRemoveAllTagsForCommand() throws GenieException {
        Assert.assertEquals(3, this.service.getTagsForCommand(COMMAND_1_ID).size());
        this.service.removeAllTagsForCommand(COMMAND_1_ID);
        Assert.assertEquals(0, this.service.getTagsForCommand(COMMAND_1_ID).size());
    }

    /**
     * Test remove tag for command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testRemoveTagForCommand() throws GenieException {
        Assert.assertTrue(this.service.getTagsForCommand(COMMAND_1_ID).contains("tez"));
        this.service.removeTagForCommand(COMMAND_1_ID, "tez");
        Assert.assertFalse(this.service.getTagsForCommand(COMMAND_1_ID).contains("tez"));
    }

    /**
     * Test the Get clusters for command function.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testGetCommandsForCommand() throws GenieException {
        final Set<Cluster> clusters = this.service.getClustersForCommand(COMMAND_1_ID, null);
        Assert.assertEquals(1, clusters.size());
        Assert.assertEquals(CLUSTER_1_ID, clusters.iterator().next().getId());
    }

    /**
     * Test the Get clusters for command function.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testGetClustersForCommandNoId() throws GenieException {
        this.service.getClustersForCommand("", null);
    }
}
