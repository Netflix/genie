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
package com.netflix.genie.web.data.services.jpa;

import com.github.fge.jsonpatch.JsonPatch;
import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.DatabaseTearDown;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.external.dtos.v4.Application;
import com.netflix.genie.common.external.dtos.v4.Cluster;
import com.netflix.genie.common.external.dtos.v4.Command;
import com.netflix.genie.common.external.dtos.v4.CommandMetadata;
import com.netflix.genie.common.external.dtos.v4.CommandRequest;
import com.netflix.genie.common.external.dtos.v4.CommandStatus;
import com.netflix.genie.common.external.util.GenieObjectMapper;
import com.netflix.genie.web.data.services.ApplicationPersistenceService;
import com.netflix.genie.web.data.services.ClusterPersistenceService;
import com.netflix.genie.web.data.services.CommandPersistenceService;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import javax.validation.ConstraintViolationException;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Integration Tests for the {@link JpaCommandPersistenceServiceImpl} class.
 *
 * @author tgianos
 */
@DatabaseSetup("JpaCommandPersistenceServiceImplIntegrationTest/init.xml")
@DatabaseTearDown("cleanup.xml")
public class JpaCommandPersistenceServiceImplIntegrationTest extends DBIntegrationTestBase {

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
        Assertions.assertThat(command1.getId()).isEqualTo(COMMAND_1_ID);
        Assertions.assertThat(command1.getMetadata().getName()).isEqualTo(COMMAND_1_NAME);
        Assertions.assertThat(command1.getMetadata().getUser()).isEqualTo(COMMAND_1_USER);
        Assertions.assertThat(command1.getMetadata().getVersion()).isEqualTo(COMMAND_1_VERSION);
        Assertions.assertThat(command1.getMetadata().getStatus()).isEqualTo(COMMAND_1_STATUS);
        Assertions.assertThat(command1.getExecutable()).isEqualTo(COMMAND_1_EXECUTABLE);
        Assertions.assertThat(command1.getMetadata().getTags().size()).isEqualTo(3);
        Assertions.assertThat(command1.getResources().getConfigs().size()).isEqualTo(2);
        Assertions.assertThat(command1.getResources().getDependencies()).isEmpty();

        final Command command2 = this.service.getCommand(COMMAND_2_ID);
        Assertions.assertThat(command2.getId()).isEqualTo(COMMAND_2_ID);
        Assertions.assertThat(command2.getMetadata().getName()).isEqualTo(COMMAND_2_NAME);
        Assertions.assertThat(command2.getMetadata().getUser()).isEqualTo(COMMAND_2_USER);
        Assertions.assertThat(command2.getMetadata().getVersion()).isEqualTo(COMMAND_2_VERSION);
        Assertions.assertThat(command2.getMetadata().getStatus()).isEqualTo(COMMAND_2_STATUS);
        Assertions.assertThat(command2.getExecutable()).isEqualTo(COMMAND_2_EXECUTABLE);
        Assertions.assertThat(command2.getMetadata().getTags().size()).isEqualTo(2);
        Assertions.assertThat(command2.getResources().getConfigs().size()).isEqualTo(1);
        Assertions.assertThat(command2.getResources().getDependencies().size()).isEqualTo(1);

        final Command command3 = this.service.getCommand(COMMAND_3_ID);
        Assertions.assertThat(command3.getId()).isEqualTo(COMMAND_3_ID);
        Assertions.assertThat(command3.getMetadata().getName()).isEqualTo(COMMAND_3_NAME);
        Assertions.assertThat(command3.getMetadata().getUser()).isEqualTo(COMMAND_3_USER);
        Assertions.assertThat(command3.getMetadata().getVersion()).isEqualTo(COMMAND_3_VERSION);
        Assertions.assertThat(command3.getMetadata().getStatus()).isEqualTo(COMMAND_3_STATUS);
        Assertions.assertThat(command3.getExecutable()).isEqualTo(COMMAND_3_EXECUTABLE);
        Assertions.assertThat(command3.getMetadata().getTags().size()).isEqualTo(3);
        Assertions.assertThat(command3.getResources().getConfigs().size()).isEqualTo(1);
        Assertions.assertThat(command3.getResources().getDependencies().size()).isEqualTo(2);
    }

    /**
     * Test the get commands method.
     */
    @Test
    public void testGetCommandsByName() {
        final Page<Command> commands = this.service.getCommands(COMMAND_2_NAME, null, null, null, PAGE);
        Assertions.assertThat(commands.getNumberOfElements()).isEqualTo(1);
        Assertions.assertThat(commands.getContent().get(0).getId()).isEqualTo(COMMAND_2_ID);
    }

    /**
     * Test the get commands method.
     */
    @Test
    public void testGetCommandsByUserName() {
        final Page<Command> commands = this.service.getCommands(null, COMMAND_1_USER, null, null, PAGE);
        Assertions.assertThat(commands.getNumberOfElements()).isEqualTo(2);
        Assertions.assertThat(commands.getContent().get(0).getId()).isEqualTo(COMMAND_3_ID);
        Assertions.assertThat(commands.getContent().get(1).getId()).isEqualTo(COMMAND_1_ID);
    }

    /**
     * Test the get commands method.
     */
    @Test
    public void testGetCommandsByStatuses() {
        final Set<CommandStatus> statuses = Sets.newHashSet(CommandStatus.INACTIVE, CommandStatus.DEPRECATED);
        final Page<Command> commands = this.service.getCommands(null, null, statuses, null, PAGE);
        Assertions.assertThat(commands.getNumberOfElements()).isEqualTo(2);
        Assertions.assertThat(commands.getContent().get(0).getId()).isEqualTo(COMMAND_2_ID);
        Assertions.assertThat(commands.getContent().get(1).getId()).isEqualTo(COMMAND_3_ID);
    }

    /**
     * Test the get commands method.
     */
    @Test
    public void testGetCommandsByTags() {
        final Set<String> tags = Sets.newHashSet("prod");
        Page<Command> commands = this.service.getCommands(null, null, null, tags, PAGE);
        Assertions.assertThat(commands.getNumberOfElements()).isEqualTo(3);
        Assertions.assertThat(commands.getContent().get(0).getId()).isEqualTo(COMMAND_2_ID);
        Assertions.assertThat(commands.getContent().get(1).getId()).isEqualTo(COMMAND_3_ID);
        Assertions.assertThat(commands.getContent().get(2).getId()).isEqualTo(COMMAND_1_ID);

        tags.add("pig");
        commands = this.service.getCommands(null, null, null, tags, PAGE);
        Assertions.assertThat(commands.getNumberOfElements()).isEqualTo(2);
        Assertions.assertThat(commands.getContent().get(0).getId()).isEqualTo(COMMAND_3_ID);
        Assertions.assertThat(commands.getContent().get(1).getId()).isEqualTo(COMMAND_1_ID);

        tags.clear();
        tags.add("hive");
        commands = this.service.getCommands(null, null, null, tags, PAGE);
        Assertions.assertThat(commands.getNumberOfElements()).isEqualTo(1);
        Assertions.assertThat(commands.getContent().get(0).getId()).isEqualTo(COMMAND_2_ID);

        tags.add("somethingThatWouldNeverReallyExist");
        commands = this.service.getCommands(null, null, null, tags, PAGE);
        Assertions.assertThat(commands.getContent()).isEmpty();

        tags.clear();
        commands = this.service.getCommands(null, null, null, tags, PAGE);
        Assertions.assertThat(commands.getNumberOfElements()).isEqualTo(3);
        Assertions.assertThat(commands.getContent().get(0).getId()).isEqualTo(COMMAND_2_ID);
        Assertions.assertThat(commands.getContent().get(1).getId()).isEqualTo(COMMAND_3_ID);
        Assertions.assertThat(commands.getContent().get(2).getId()).isEqualTo(COMMAND_1_ID);
    }

    /**
     * Test the get commands method with descending sort.
     */
    @Test
    public void testGetCommandsDescending() {
        //Default to order by Updated
        final Page<Command> commands = this.service.getCommands(null, null, null, null, PAGE);
        Assertions.assertThat(commands.getNumberOfElements()).isEqualTo(3);
        Assertions.assertThat(commands.getContent().get(0).getId()).isEqualTo(COMMAND_2_ID);
        Assertions.assertThat(commands.getContent().get(1).getId()).isEqualTo(COMMAND_3_ID);
        Assertions.assertThat(commands.getContent().get(2).getId()).isEqualTo(COMMAND_1_ID);
    }

    /**
     * Test the get commands method with ascending sort.
     */
    @Test
    public void testGetCommandsAscending() {
        final Pageable ascending = PageRequest.of(0, 10, Sort.Direction.ASC, "updated");
        //Default to order by Updated
        final Page<Command> commands = this.service.getCommands(null, null, null, null, ascending);
        Assertions.assertThat(commands.getNumberOfElements()).isEqualTo(3);
        Assertions.assertThat(commands.getContent().get(0).getId()).isEqualTo(COMMAND_1_ID);
        Assertions.assertThat(commands.getContent().get(1).getId()).isEqualTo(COMMAND_3_ID);
        Assertions.assertThat(commands.getContent().get(2).getId()).isEqualTo(COMMAND_2_ID);
    }

    /**
     * Test the get commands method order by name.
     */
    @Test
    public void testGetCommandsOrderBysName() {
        final Pageable name = PageRequest.of(0, 10, Sort.Direction.DESC, "name");
        final Page<Command> commands = this.service.getCommands(null, null, null, null, name);
        Assertions.assertThat(commands.getNumberOfElements()).isEqualTo(3);
        Assertions.assertThat(commands.getContent().get(0).getId()).isEqualTo(COMMAND_1_ID);
        Assertions.assertThat(commands.getContent().get(1).getId()).isEqualTo(COMMAND_3_ID);
        Assertions.assertThat(commands.getContent().get(2).getId()).isEqualTo(COMMAND_2_ID);
    }

    /**
     * Test the get commands method order by an invalid field should return the order by default value (updated).
     */
    @Test(expected = RuntimeException.class)
    public void testGetCommandsOrderBysInvalidField() {
        final Pageable invalid = PageRequest.of(0, 10, Sort.Direction.DESC, "I'mNotAValidField");
        final Page<Command> commands = this.service.getCommands(null, null, null, null, invalid);
        Assertions.assertThat(commands.getNumberOfElements()).isEqualTo(3);
        Assertions.assertThat(commands.getContent().get(0).getId()).isEqualTo(COMMAND_2_ID);
        Assertions.assertThat(commands.getContent().get(1).getId()).isEqualTo(COMMAND_3_ID);
        Assertions.assertThat(commands.getContent().get(2).getId()).isEqualTo(COMMAND_1_ID);
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
        Assertions.assertThat(createdId).isEqualTo(id);
        final Command created = this.service.getCommand(id);
        Assertions.assertThat(created).isNotNull();
        Assertions.assertThat(created.getId()).isEqualTo(id);
        Assertions.assertThat(created.getMetadata().getName()).isEqualTo(COMMAND_1_NAME);
        Assertions.assertThat(created.getMetadata().getUser()).isEqualTo(COMMAND_1_USER);
        Assertions.assertThat(created.getMetadata().getStatus()).isEqualByComparingTo(CommandStatus.ACTIVE);
        Assertions.assertThat(created.getExecutable()).isEqualTo(COMMAND_1_EXECUTABLE);
        Assertions.assertThat(created.getCheckDelay()).isEqualTo(COMMAND_1_CHECK_DELAY);
        Assertions.assertThat(created.getMemory()).isNotPresent();
        this.service.deleteCommand(id);
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.service.getCommand(id));
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
        Assertions.assertThat(created).isNotNull();
        Assertions.assertThat(created.getMetadata().getName()).isEqualTo(COMMAND_1_NAME);
        Assertions.assertThat(created.getMetadata().getUser()).isEqualTo(COMMAND_1_USER);
        Assertions.assertThat(created.getMetadata().getStatus()).isEqualByComparingTo(CommandStatus.ACTIVE);
        Assertions.assertThat(created.getExecutable()).isEqualTo(COMMAND_1_EXECUTABLE);
        Assertions.assertThat(created.getCheckDelay()).isEqualTo(COMMAND_1_CHECK_DELAY);
        Assertions.assertThat(created.getMemory()).isPresent().contains(memory);
        this.service.deleteCommand(created.getId());
        Assertions
            .assertThatExceptionOfType(GenieNotFoundException.class)
            .isThrownBy(() -> this.service.getCommand(id));
    }

    /**
     * Test to update an command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testUpdateCommand() throws GenieException {
        final Command command = this.service.getCommand(COMMAND_1_ID);
        Assertions.assertThat(command.getMetadata().getUser()).isEqualTo(COMMAND_1_USER);
        Assertions.assertThat(command.getMetadata().getStatus()).isEqualByComparingTo(CommandStatus.ACTIVE);
        Assertions.assertThat(command.getMetadata().getTags().size()).isEqualTo(3);
        Assertions.assertThat(command.getMemory()).isNotPresent();
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
            command.getCheckDelay(),
            null
        );

        this.service.updateCommand(COMMAND_1_ID, updateCommand);

        final Command updated = this.service.getCommand(COMMAND_1_ID);
        Assertions.assertThat(updated.getMetadata().getUser()).isEqualTo(COMMAND_2_USER);
        Assertions.assertThat(updated.getMetadata().getStatus()).isEqualByComparingTo(CommandStatus.INACTIVE);
        Assertions.assertThat(updated.getMetadata().getTags().size()).isEqualTo(5);
        Assertions.assertThat(updated.getMemory()).isPresent().contains(memory);
    }

    /**
     * Test to update a command with invalid content. Should throw ConstraintViolationException from JPA layer.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testUpdateCommandWithInvalidCommand() throws GenieException {
        final Command command = this.service.getCommand(COMMAND_1_ID);

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
            command.getCheckDelay(),
            null
        );

        Assertions
            .assertThatExceptionOfType(ConstraintViolationException.class)
            .isThrownBy(() -> this.service.updateCommand(COMMAND_1_ID, updateCommand));
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
            init.getCheckDelay(),
            init.getClusterCriteria()
        );

        this.service.updateCommand(COMMAND_1_ID, updateCommand);
        final Command updatedCommand = this.service.getCommand(COMMAND_1_ID);
        Assertions.assertThat(updatedCommand.getCreated()).isEqualTo(created);
        Assertions.assertThat(updatedCommand.getUpdated()).isNotEqualTo(updated);
        Assertions.assertThat(updatedCommand.getUpdated()).isNotEqualTo(Instant.EPOCH);
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
        Assertions.assertThat(getCommand.getMetadata().getName()).isEqualTo(COMMAND_1_NAME);
        final Instant updateTime = getCommand.getUpdated();

        final String patchString
            = "[{ \"op\": \"replace\", \"path\": \"/metadata/name\", \"value\": \"" + COMMAND_2_NAME + "\" }]";
        final JsonPatch patch = JsonPatch.fromJson(GenieObjectMapper.getMapper().readTree(patchString));

        this.service.patchCommand(COMMAND_1_ID, patch);

        final Command updated = this.service.getCommand(COMMAND_1_ID);
        Assertions.assertThat(updated.getUpdated()).isNotEqualTo(updateTime);
        Assertions.assertThat(updated.getMetadata().getName()).isEqualTo(COMMAND_2_NAME);
    }

    /**
     * Test delete all.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testDeleteAll() throws GenieException {
        Assertions
            .assertThat(this.service.getCommands(null, null, null, null, PAGE).getNumberOfElements())
            .isEqualTo(3);
        this.service.deleteAllCommands();
        Assertions.assertThat(this.service.getCommands(null, null, null, null, PAGE).getContent()).isEmpty();
    }

    /**
     * Test delete.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testDelete() throws GenieException {
        List<Command> commands = this.clusterPersistenceService.getCommandsForCluster(CLUSTER_1_ID, null);
        Assertions.assertThat(commands).hasSize(3);
        boolean found = false;
        for (final Command command : commands) {
            if (COMMAND_1_ID.equals(command.getId())) {
                found = true;
                break;
            }
        }
        Assertions.assertThat(found).isTrue();
        // TODO: Fix once Command service goes to V4
        Set<Command> appCommands = this.appService.getCommandsForApplication(APP_1_ID, null);
        Assertions.assertThat(appCommands).hasSize(1);
        found = false;
        for (final Command command : appCommands) {
            if (COMMAND_1_ID.equals(command.getId())) {
                found = true;
                break;
            }
        }
        Assertions.assertThat(found).isTrue();

        //Actually delete it
        this.service.deleteCommand(COMMAND_1_ID);

        commands = this.clusterPersistenceService.getCommandsForCluster(CLUSTER_1_ID, null);
        Assertions.assertThat(commands).hasSize(2);
        found = false;
        for (final Command command : commands) {
            if (COMMAND_1_ID.equals(command.getId())) {
                found = true;
                break;
            }
        }
        Assertions.assertThat(found).isFalse();
        appCommands = this.appService.getCommandsForApplication(APP_1_ID, null);
        Assertions.assertThat(appCommands).isEmpty();

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

        Assertions.assertThat(this.service.getConfigsForCommand(COMMAND_1_ID)).hasSize(2);
        this.service.addConfigsForCommand(COMMAND_1_ID, newConfigs);
        final Set<String> finalConfigs = this.service.getConfigsForCommand(COMMAND_1_ID);
        Assertions.assertThat(finalConfigs).hasSize(5).contains(newConfig1, newConfig2, newConfig3);
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

        Assertions.assertThat(this.service.getConfigsForCommand(COMMAND_1_ID)).hasSize(2);
        this.service.updateConfigsForCommand(COMMAND_1_ID, newConfigs);
        final Set<String> finalConfigs = this.service.getConfigsForCommand(COMMAND_1_ID);
        Assertions.assertThat(finalConfigs).hasSize(3).contains(newConfig1, newConfig2, newConfig3);
    }

    /**
     * Test get configurations for command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testGetConfigsForCommand() throws GenieException {
        Assertions.assertThat(this.service.getConfigsForCommand(COMMAND_1_ID)).hasSize(2);
    }

    /**
     * Test remove all configurations for command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testRemoveAllConfigsForCommand() throws GenieException {
        Assertions.assertThat(this.service.getConfigsForCommand(COMMAND_1_ID)).hasSize(2);
        this.service.removeAllConfigsForCommand(COMMAND_1_ID);
        Assertions.assertThat(this.service.getConfigsForCommand(COMMAND_1_ID)).isEmpty();
    }

    /**
     * Test remove configuration for command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testRemoveConfigForCommand() throws GenieException {
        final Set<String> configs = this.service.getConfigsForCommand(COMMAND_1_ID);
        Assertions.assertThat(configs).hasSize(2);
        final String removedConfig = configs.iterator().next();
        this.service.removeConfigForCommand(COMMAND_1_ID, removedConfig);
        Assertions.assertThat(this.service.getConfigsForCommand(COMMAND_1_ID)).doesNotContain(removedConfig);
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

        Assertions.assertThat(this.service.getDependenciesForCommand(COMMAND_3_ID)).hasSize(2);
        this.service.addDependenciesForCommand(COMMAND_3_ID, newDependencies);
        final Set<String> finalDependencies = this.service.getDependenciesForCommand(COMMAND_3_ID);
        Assertions.assertThat(finalDependencies).hasSize(5).contains(newDependency1, newDependency2, newDependency3);
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

        Assertions.assertThat(this.service.getDependenciesForCommand(COMMAND_1_ID)).isEmpty();
        this.service.updateDependenciesForCommand(COMMAND_1_ID, newDependencies);
        final Set<String> finalDependencies = this.service.getDependenciesForCommand(COMMAND_1_ID);
        Assertions.assertThat(finalDependencies).hasSize(3).contains(newDependency1, newDependency2, newDependency3);
    }

    /**
     * Test get dependencies for command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testGetDependenciesForCommand() throws GenieException {
        Assertions.assertThat(this.service.getDependenciesForCommand(COMMAND_2_ID)).hasSize(1);
    }

    /**
     * Test remove all dependencies for command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testRemoveAllDependenciesForCommand() throws GenieException {
        Assertions.assertThat(this.service.getDependenciesForCommand(COMMAND_3_ID)).hasSize(2);
        this.service.removeAllDependenciesForCommand(COMMAND_3_ID);
        Assertions.assertThat(this.service.getDependenciesForCommand(COMMAND_3_ID)).isEmpty();
    }

    /**
     * Test remove dependencies for command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testRemoveDependencyForCommand() throws GenieException {
        final Set<String> dependencies = this.service.getDependenciesForCommand(COMMAND_3_ID);
        Assertions.assertThat(dependencies).hasSize(2);
        final String removedDependency = dependencies.iterator().next();
        this.service.removeDependencyForCommand(COMMAND_3_ID, removedDependency);
        Assertions.assertThat(this.service.getDependenciesForCommand(COMMAND_3_ID)).doesNotContain(removedDependency);
    }

    /**
     * Test setting the applications for a given command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testAddApplicationsForCommand() throws GenieException {
        Assertions.assertThat(this.service.getApplicationsForCommand(COMMAND_2_ID)).isEmpty();

        final List<String> appIds = Lists.newArrayList(APP_1_ID);
        final Set<Command> preCommands = this.appService.getCommandsForApplication(APP_1_ID, null);
        Assertions
            .assertThat(preCommands)
            .hasSize(1)
            .hasOnlyOneElementSatisfying(command -> Assertions.assertThat(command.getId()).isEqualTo(COMMAND_1_ID));

        this.service.addApplicationsForCommand(COMMAND_2_ID, appIds);

        final Set<Command> savedCommands = this.appService.getCommandsForApplication(APP_1_ID, null);
        Assertions.assertThat(savedCommands).hasSize(2);
        Assertions
            .assertThat(this.service.getApplicationsForCommand(COMMAND_2_ID))
            .element(0)
            .extracting(Application::getId)
            .isEqualTo(APP_1_ID);
    }

    /**
     * Test setting the applications for a given command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testSetApplicationsForCommand() throws GenieException {
        Assertions.assertThat(this.service.getApplicationsForCommand(COMMAND_2_ID)).isEmpty();

        final List<String> appIds = Lists.newArrayList(APP_1_ID);
        final Set<Command> preCommands = this.appService.getCommandsForApplication(APP_1_ID, null);
        Assertions
            .assertThat(preCommands)
            .hasSize(1)
            .hasOnlyOneElementSatisfying(command -> Assertions.assertThat(command.getId()).isEqualTo(COMMAND_1_ID));

        this.service.setApplicationsForCommand(COMMAND_2_ID, appIds);

        final Set<Command> savedCommands = this.appService.getCommandsForApplication(APP_1_ID, null);
        Assertions.assertThat(savedCommands).hasSize(2);
        Assertions
            .assertThat(this.service.getApplicationsForCommand(COMMAND_2_ID))
            .hasOnlyOneElementSatisfying(application -> Assertions.assertThat(application.getId()).isEqualTo(APP_1_ID));
    }

    /**
     * Test get applications for command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testGetApplicationsForCommand() throws GenieException {
        Assertions
            .assertThat(this.service.getApplicationsForCommand(COMMAND_1_ID))
            .hasOnlyOneElementSatisfying(application -> Assertions.assertThat(application.getId()).isEqualTo(APP_1_ID));
    }

    /**
     * Test remove applications for command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testRemoveApplicationsForCommand() throws GenieException {
        Assertions.assertThat(this.service.getApplicationsForCommand(COMMAND_1_ID)).hasSize(1);
        this.service.removeApplicationsForCommand(COMMAND_1_ID);
        Assertions.assertThat(this.service.getApplicationsForCommand(COMMAND_1_ID)).isEmpty();
    }

    /**
     * Test remove application for command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testRemoveApplicationForCommand() throws GenieException {
        Assertions.assertThat(this.service.getApplicationsForCommand(COMMAND_1_ID)).hasSize(1);
        this.service.removeApplicationForCommand(COMMAND_1_ID, APP_1_ID);
        Assertions.assertThat(this.service.getApplicationsForCommand(COMMAND_1_ID)).isEmpty();
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

        Assertions.assertThat(this.service.getTagsForCommand(COMMAND_1_ID)).hasSize(3);
        this.service.addTagsForCommand(COMMAND_1_ID, newTags);
        final Set<String> finalTags = this.service.getTagsForCommand(COMMAND_1_ID);
        Assertions.assertThat(finalTags).hasSize(6).contains(newTag1, newTag2, newTag3);
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

        Assertions.assertThat(this.service.getTagsForCommand(COMMAND_1_ID)).hasSize(3);
        this.service.updateTagsForCommand(COMMAND_1_ID, newTags);
        final Set<String> finalTags = this.service.getTagsForCommand(COMMAND_1_ID);
        Assertions.assertThat(finalTags).hasSize(3).contains(newTag1, newTag2, newTag3);
    }

    /**
     * Test get tags for command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testGetTagsForCommand() throws GenieException {
        Assertions.assertThat(this.service.getTagsForCommand(COMMAND_1_ID)).hasSize(3);
    }

    /**
     * Test remove all tags for command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testRemoveAllTagsForCommand() throws GenieException {
        Assertions.assertThat(this.service.getTagsForCommand(COMMAND_1_ID)).hasSize(3);
        this.service.removeAllTagsForCommand(COMMAND_1_ID);
        Assertions.assertThat(this.service.getTagsForCommand(COMMAND_1_ID)).isEmpty();
    }

    /**
     * Test remove tag for command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testRemoveTagForCommand() throws GenieException {
        Assertions.assertThat(this.service.getTagsForCommand(COMMAND_1_ID)).contains("tez");
        this.service.removeTagForCommand(COMMAND_1_ID, "tez");
        Assertions.assertThat(this.service.getTagsForCommand(COMMAND_1_ID)).doesNotContain("tez");
    }

    /**
     * Test the Get clusters for command function.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testGetCommandsForCommand() throws GenieException {
        final Set<Cluster> clusters = this.service.getClustersForCommand(COMMAND_1_ID, null);
        Assertions
            .assertThat(clusters)
            .hasSize(1)
            .hasOnlyOneElementSatisfying(cluster -> Assertions.assertThat(cluster.getId()).isEqualTo(CLUSTER_1_ID));
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
