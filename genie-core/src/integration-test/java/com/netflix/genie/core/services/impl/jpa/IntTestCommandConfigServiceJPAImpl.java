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
package com.netflix.genie.core.services.impl.jpa;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.DatabaseTearDown;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.model.Cluster;
import com.netflix.genie.common.model.Command;
import com.netflix.genie.common.model.CommandStatus;
import com.netflix.genie.core.services.ApplicationConfigService;
import com.netflix.genie.core.services.ClusterConfigService;
import com.netflix.genie.core.services.CommandConfigService;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import javax.validation.ConstraintViolationException;
import java.net.HttpURLConnection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Integration Tests for the CommandConfigServiceJPAImpl.
 *
 * @author tgianos
 */
@DatabaseSetup("IntTestCommandConfigServiceJPAImpl/init.xml")
@DatabaseTearDown("cleanup.xml")
public class IntTestCommandConfigServiceJPAImpl extends DBUnitTestBase {

    private static final String APP_1_ID = "app1";
    private static final String CLUSTER_1_ID = "cluster1";

    private static final String COMMAND_1_ID = "command1";
    private static final String COMMAND_1_NAME = "pig_13_prod";
    private static final String COMMAND_1_USER = "tgianos";
    private static final String COMMAND_1_VERSION = "1.2.3";
    private static final String COMMAND_1_EXECUTABLE = "pig";
    private static final String COMMAND_1_JOB_TYPE = "yarn";
    private static final CommandStatus COMMAND_1_STATUS = CommandStatus.ACTIVE;

    private static final String COMMAND_2_ID = "command2";
    private static final String COMMAND_2_NAME = "hive_11_prod";
    private static final String COMMAND_2_USER = "amsharma";
    private static final String COMMAND_2_VERSION = "4.5.6";
    private static final String COMMAND_2_EXECUTABLE = "hive";
    private static final String COMMAND_2_JOB_TYPE = "yarn";
    private static final CommandStatus COMMAND_2_STATUS = CommandStatus.INACTIVE;

    private static final String COMMAND_3_ID = "command3";
    private static final String COMMAND_3_NAME = "pig_11_prod";
    private static final String COMMAND_3_USER = "tgianos";
    private static final String COMMAND_3_VERSION = "7.8.9";
    private static final String COMMAND_3_EXECUTABLE = "pig";
    private static final String COMMAND_3_JOB_TYPE = "yarn";
    private static final CommandStatus COMMAND_3_STATUS = CommandStatus.DEPRECATED;

    @Autowired
    private CommandConfigService service;

    @Autowired
    private ClusterConfigService clusterService;

    @Autowired
    private ApplicationConfigService appService;

    /**
     * Test the get command method.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testGetCommand() throws GenieException {
        final Command command1 = this.service.getCommand(COMMAND_1_ID);
        Assert.assertEquals(COMMAND_1_ID, command1.getId());
        Assert.assertEquals(COMMAND_1_NAME, command1.getName());
        Assert.assertEquals(COMMAND_1_USER, command1.getUser());
        Assert.assertEquals(COMMAND_1_VERSION, command1.getVersion());
        Assert.assertEquals(COMMAND_1_STATUS, command1.getStatus());
        Assert.assertEquals(COMMAND_1_EXECUTABLE, command1.getExecutable());
        Assert.assertEquals(COMMAND_1_JOB_TYPE, command1.getJobType());
        Assert.assertNotNull(command1.getApplications());
        Assert.assertEquals(1, command1
                        .getApplications()
                        .stream()
                        .filter(application -> APP_1_ID.equals(application.getId()))
                        .count()
        );
        Assert.assertEquals(5, command1.getTags().size());
        Assert.assertEquals(2, command1.getConfigs().size());

        final Command command2 = this.service.getCommand(COMMAND_2_ID);
        Assert.assertEquals(COMMAND_2_ID, command2.getId());
        Assert.assertEquals(COMMAND_2_NAME, command2.getName());
        Assert.assertEquals(COMMAND_2_USER, command2.getUser());
        Assert.assertEquals(COMMAND_2_VERSION, command2.getVersion());
        Assert.assertEquals(COMMAND_2_STATUS, command2.getStatus());
        Assert.assertEquals(COMMAND_2_EXECUTABLE, command2.getExecutable());
        Assert.assertEquals(COMMAND_2_JOB_TYPE, command2.getJobType());
        Assert.assertTrue(command2.getApplications().isEmpty());
        Assert.assertEquals(4, command2.getTags().size());
        Assert.assertEquals(1, command2.getConfigs().size());

        final Command command3 = this.service.getCommand(COMMAND_3_ID);
        Assert.assertEquals(COMMAND_3_ID, command3.getId());
        Assert.assertEquals(COMMAND_3_NAME, command3.getName());
        Assert.assertEquals(COMMAND_3_USER, command3.getUser());
        Assert.assertEquals(COMMAND_3_VERSION, command3.getVersion());
        Assert.assertEquals(COMMAND_3_STATUS, command3.getStatus());
        Assert.assertEquals(COMMAND_3_EXECUTABLE, command3.getExecutable());
        Assert.assertEquals(COMMAND_3_JOB_TYPE, command3.getJobType());
        Assert.assertTrue(command3.getApplications().isEmpty());
        Assert.assertEquals(5, command3.getTags().size());
        Assert.assertEquals(1, command3.getConfigs().size());
    }

    /**
     * Test the get command method.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testGetCommandNull() throws GenieException {
        this.service.getCommand(null);
    }

    /**
     * Test the get commands method.
     */
    @Test
    public void testGetCommandsByName() {
        final List<Command> commands = this.service.getCommands(COMMAND_2_NAME, null, null, null, 0, 10, true, null);
        Assert.assertEquals(1, commands.size());
        Assert.assertEquals(COMMAND_2_ID, commands.get(0).getId());
    }

    /**
     * Test the get commands method.
     */
    @Test
    public void testGetCommandsByUserName() {
        final List<Command> apps = this.service.getCommands(null, COMMAND_1_USER, null, null, -1, -5000, true, null);
        Assert.assertEquals(2, apps.size());
        Assert.assertEquals(COMMAND_3_ID, apps.get(0).getId());
        Assert.assertEquals(COMMAND_1_ID, apps.get(1).getId());
    }

    /**
     * Test the get commands method.
     */
    @Test
    public void testGetCommandsByStatuses() {
        final Set<CommandStatus> statuses = new HashSet<>();
        statuses.add(CommandStatus.INACTIVE);
        statuses.add(CommandStatus.DEPRECATED);
        final List<Command> apps = this.service.getCommands(null, null, statuses, null, -1, -5000, true, null);
        Assert.assertEquals(2, apps.size());
        Assert.assertEquals(COMMAND_2_ID, apps.get(0).getId());
        Assert.assertEquals(COMMAND_3_ID, apps.get(1).getId());
    }

    /**
     * Test the get commands method.
     */
    @Test
    public void testGetCommandsByTags() {
        final Set<String> tags = new HashSet<>();
        tags.add("prod");
        List<Command> commands = this.service.getCommands(null, null, null, tags, 0, 10, true, null);
        Assert.assertEquals(3, commands.size());
        Assert.assertEquals(COMMAND_2_ID, commands.get(0).getId());
        Assert.assertEquals(COMMAND_3_ID, commands.get(1).getId());
        Assert.assertEquals(COMMAND_1_ID, commands.get(2).getId());

        tags.add("pig");
        commands = this.service.getCommands(null, null, null, tags, 0, 10, true, null);
        Assert.assertEquals(2, commands.size());
        Assert.assertEquals(COMMAND_3_ID, commands.get(0).getId());
        Assert.assertEquals(COMMAND_1_ID, commands.get(1).getId());

        tags.clear();
        tags.add("hive");
        commands = this.service.getCommands(null, null, null, tags, 0, 10, true, null);
        Assert.assertEquals(1, commands.size());
        Assert.assertEquals(COMMAND_2_ID, commands.get(0).getId());

        tags.add("somethingThatWouldNeverReallyExist");
        commands = this.service.getCommands(null, null, null, tags, 0, 10, true, null);
        Assert.assertTrue(commands.isEmpty());

        tags.clear();
        commands = this.service.getCommands(null, null, null, tags, 0, 10, true, null);
        Assert.assertEquals(3, commands.size());
        Assert.assertEquals(COMMAND_2_ID, commands.get(0).getId());
        Assert.assertEquals(COMMAND_3_ID, commands.get(1).getId());
        Assert.assertEquals(COMMAND_1_ID, commands.get(2).getId());
    }

    /**
     * Test the get commands method with descending sort.
     */
    @Test
    public void testGetClustersDescending() {
        //Default to order by Updated
        final List<Command> commands = this.service.getCommands(null, null, null, null, 0, 10, true, null);
        Assert.assertEquals(3, commands.size());
        Assert.assertEquals(COMMAND_2_ID, commands.get(0).getId());
        Assert.assertEquals(COMMAND_3_ID, commands.get(1).getId());
        Assert.assertEquals(COMMAND_1_ID, commands.get(2).getId());
    }

    /**
     * Test the get commands method with ascending sort.
     */
    @Test
    public void testGetClustersAscending() {
        //Default to order by Updated
        final List<Command> commands = this.service.getCommands(null, null, null, null, 0, 10, false, null);
        Assert.assertEquals(3, commands.size());
        Assert.assertEquals(COMMAND_1_ID, commands.get(0).getId());
        Assert.assertEquals(COMMAND_3_ID, commands.get(1).getId());
        Assert.assertEquals(COMMAND_2_ID, commands.get(2).getId());
    }

    /**
     * Test the get commands method default order by.
     */
    @Test
    public void testGetClustersOrderBysDefault() {
        //Default to order by Updated
        final List<Command> commands = this.service.getCommands(null, null, null, null, 0, 10, true, null);
        Assert.assertEquals(3, commands.size());
        Assert.assertEquals(COMMAND_2_ID, commands.get(0).getId());
        Assert.assertEquals(COMMAND_3_ID, commands.get(1).getId());
        Assert.assertEquals(COMMAND_1_ID, commands.get(2).getId());
    }

    /**
     * Test the get commands method order by updated.
     */
    @Test
    public void testGetClustersOrderBysUpdated() {
        final Set<String> orderBys = new HashSet<>();
        orderBys.add("updated");
        final List<Command> commands = this.service.getCommands(null, null, null, null, 0, 10, true, orderBys);
        Assert.assertEquals(3, commands.size());
        Assert.assertEquals(COMMAND_2_ID, commands.get(0).getId());
        Assert.assertEquals(COMMAND_3_ID, commands.get(1).getId());
        Assert.assertEquals(COMMAND_1_ID, commands.get(2).getId());
    }

    /**
     * Test the get commands method order by name.
     */
    @Test
    public void testGetClustersOrderBysName() {
        final Set<String> orderBys = new HashSet<>();
        orderBys.add("name");
        final List<Command> commands = this.service.getCommands(null, null, null, null, 0, 10, true, orderBys);
        Assert.assertEquals(3, commands.size());
        Assert.assertEquals(COMMAND_1_ID, commands.get(0).getId());
        Assert.assertEquals(COMMAND_3_ID, commands.get(1).getId());
        Assert.assertEquals(COMMAND_2_ID, commands.get(2).getId());
    }

    /**
     * Test the get commands method order by an invalid field should return the order by default value (updated).
     */
    @Test
    public void testGetClustersOrderBysInvalidField() {
        final Set<String> orderBys = new HashSet<>();
        orderBys.add("I'mNotAValidField");
        final List<Command> commands = this.service.getCommands(null, null, null, null, 0, 10, true, orderBys);
        Assert.assertEquals(3, commands.size());
        Assert.assertEquals(COMMAND_2_ID, commands.get(0).getId());
        Assert.assertEquals(COMMAND_3_ID, commands.get(1).getId());
        Assert.assertEquals(COMMAND_1_ID, commands.get(2).getId());
    }

    /**
     * Test the get commands method order by a collection field should return the order by default value (updated).
     */
    @Test
    public void testGetClustersOrderBysCollectionField() {
        final Set<String> orderBys = new HashSet<>();
        orderBys.add("tags");
        final List<Command> commands = this.service.getCommands(null, null, null, null, 0, 10, true, orderBys);
        Assert.assertEquals(3, commands.size());
        Assert.assertEquals(COMMAND_2_ID, commands.get(0).getId());
        Assert.assertEquals(COMMAND_3_ID, commands.get(1).getId());
        Assert.assertEquals(COMMAND_1_ID, commands.get(2).getId());
    }

    /**
     * Test the create method.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testCreateCommand() throws GenieException {
        final Command command = new Command(
                COMMAND_1_NAME,
                COMMAND_1_USER,
                COMMAND_1_VERSION,
                CommandStatus.ACTIVE,
                COMMAND_1_EXECUTABLE
        );
        final String id = UUID.randomUUID().toString();
        command.setId(id);
        final Command created = this.service.createCommand(command);
        Assert.assertNotNull(this.service.getCommand(id));
        Assert.assertEquals(id, created.getId());
        Assert.assertEquals(COMMAND_1_NAME, created.getName());
        Assert.assertEquals(COMMAND_1_USER, created.getUser());
        Assert.assertEquals(CommandStatus.ACTIVE, created.getStatus());
        Assert.assertEquals(COMMAND_1_EXECUTABLE, created.getExecutable());
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
        final Command command = new Command(
                COMMAND_1_NAME,
                COMMAND_1_USER,
                COMMAND_1_VERSION,
                CommandStatus.ACTIVE,
                COMMAND_1_EXECUTABLE
        );
        final Command created = this.service.createCommand(command);
        Assert.assertNotNull(this.service.getCommand(created.getId()));
        Assert.assertEquals(COMMAND_1_NAME, created.getName());
        Assert.assertEquals(COMMAND_1_USER, created.getUser());
        Assert.assertEquals(CommandStatus.ACTIVE, created.getStatus());
        Assert.assertEquals(COMMAND_1_EXECUTABLE, created.getExecutable());
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
     * Test to make sure an exception is thrown when null is entered.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testCreateCommandNull() throws GenieException {
        this.service.createCommand(null);
    }

    /**
     * Test to update an command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testUpdateCommand() throws GenieException {
        final Command updateCommand = this.service.getCommand(COMMAND_1_ID);
        Assert.assertEquals(COMMAND_1_USER, updateCommand.getUser());
        Assert.assertEquals(CommandStatus.ACTIVE, updateCommand.getStatus());
        Assert.assertEquals(5, updateCommand.getTags().size());

        updateCommand.setStatus(CommandStatus.INACTIVE);
        updateCommand.setUser(COMMAND_2_USER);
        final Set<String> tags = updateCommand.getTags();
        tags.add("yarn");
        tags.add("hadoop");
        this.service.updateCommand(COMMAND_1_ID, updateCommand);

        final Command updated = this.service.getCommand(COMMAND_1_ID);
        Assert.assertEquals(COMMAND_2_USER, updated.getUser());
        Assert.assertEquals(CommandStatus.INACTIVE, updated.getStatus());
        Assert.assertEquals(7, updated.getTags().size());
    }

    /**
     * Test to update a command with invalid content. Should throw ConstraintViolationException from JPA layer.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testUpdateCommandWithInvalidCommand() throws GenieException {
        final Command updateCommand = this.service.getCommand(COMMAND_1_ID);
        Assert.assertEquals(COMMAND_1_USER, updateCommand.getUser());
        Assert.assertEquals(CommandStatus.ACTIVE, updateCommand.getStatus());
        Assert.assertEquals(5, updateCommand.getTags().size());

        updateCommand.setStatus(CommandStatus.INACTIVE);
        updateCommand.setVersion("");
        final Set<String> tags = updateCommand.getTags();
        tags.add("prod");
        tags.add("tez");
        tags.add("yarn");
        tags.add("hadoop");
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
        final Date created = init.getCreated();
        final Date updated = init.getUpdated();

        init.setCreated(new Date());
        final Date zero = new Date(0);
        init.setUpdated(zero);

        final Command updatedCommand = this.service.updateCommand(COMMAND_1_ID, init);
        Assert.assertEquals(created, updatedCommand.getCreated());
        Assert.assertNotEquals(updated, updatedCommand.getUpdated());
        Assert.assertNotEquals(zero, updatedCommand.getUpdated());
    }

    /**
     * Test to update an command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testUpdateCommandNullId() throws GenieException {
        this.service.updateCommand(null, this.service.getCommand(COMMAND_1_ID));
    }

    /**
     * Test to update an command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testUpdateCommandNullUpdateCommand() throws GenieException {
        this.service.updateCommand(COMMAND_1_ID, null);
    }

    /**
     * Test delete all.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testDeleteAll() throws GenieException {
        Assert.assertEquals(3, this.service.getCommands(null, null, null, null, 0, 10, true, null).size());
        Assert.assertEquals(3, this.service.deleteAllCommands().size());
        Assert.assertTrue(this.service.getCommands(null, null, null, null, 0, 10, true, null).isEmpty());
    }

    /**
     * Test delete.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testDelete() throws GenieException {
        List<Command> commands
                = this.clusterService.getCommandsForCluster(CLUSTER_1_ID, null);
        Assert.assertEquals(3, commands.size());
        boolean found = false;
        for (final Command command : commands) {
            if (COMMAND_1_ID.equals(command.getId())) {
                found = true;
                break;
            }
        }
        Assert.assertTrue(found);
        List<Command> appCommands
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
        Assert.assertEquals(COMMAND_1_ID,
                this.service.deleteCommand(COMMAND_1_ID).getId());

        commands = this.clusterService.getCommandsForCluster(CLUSTER_1_ID, null);
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
        Assert.assertEquals(COMMAND_3_ID,
                this.service.deleteCommand(COMMAND_3_ID).getId());
    }

    /**
     * Test delete.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testDeleteNoId() throws GenieException {
        this.service.deleteCommand(null);
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

        final Set<String> newConfigs = new HashSet<>();
        newConfigs.add(newConfig1);
        newConfigs.add(newConfig2);
        newConfigs.add(newConfig3);

        Assert.assertEquals(2,
                this.service.getConfigsForCommand(COMMAND_1_ID).size());
        final Set<String> finalConfigs
                = this.service.addConfigsForCommand(COMMAND_1_ID, newConfigs);
        Assert.assertEquals(5, finalConfigs.size());
        Assert.assertTrue(finalConfigs.contains(newConfig1));
        Assert.assertTrue(finalConfigs.contains(newConfig2));
        Assert.assertTrue(finalConfigs.contains(newConfig3));
    }

    /**
     * Test add configurations to command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testAddConfigsToCommandNoId() throws GenieException {
        this.service.addConfigsForCommand(null, new HashSet<>());
    }

    /**
     * Test add configurations to command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testAddConfigsToCommandNoConfigs() throws GenieException {
        this.service.addConfigsForCommand(COMMAND_1_ID, null);
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

        final Set<String> newConfigs = new HashSet<>();
        newConfigs.add(newConfig1);
        newConfigs.add(newConfig2);
        newConfigs.add(newConfig3);

        Assert.assertEquals(2,
                this.service.getConfigsForCommand(COMMAND_1_ID).size());
        final Set<String> finalConfigs
                = this.service.updateConfigsForCommand(COMMAND_1_ID, newConfigs);
        Assert.assertEquals(3, finalConfigs.size());
        Assert.assertTrue(finalConfigs.contains(newConfig1));
        Assert.assertTrue(finalConfigs.contains(newConfig2));
        Assert.assertTrue(finalConfigs.contains(newConfig3));
    }

    /**
     * Test update configurations for command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testUpdateConfigsForCommandNoId() throws GenieException {
        this.service.updateConfigsForCommand(null, new HashSet<>());
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
     * Test get configurations to command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testGetConfigsForCommandNoId() throws GenieException {
        this.service.getConfigsForCommand(null);
    }

    /**
     * Test remove all configurations for command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testRemoveAllConfigsForCommand() throws GenieException {
        Assert.assertEquals(2,
                this.service.getConfigsForCommand(COMMAND_1_ID).size());
        Assert.assertEquals(0,
                this.service.removeAllConfigsForCommand(COMMAND_1_ID).size());
    }

    /**
     * Test remove all configurations for command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testRemoveAllConfigsForCommandNoId() throws GenieException {
        this.service.removeAllConfigsForCommand(null);
    }

    /**
     * Test remove configuration for command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testRemoveConfigForCommand() throws GenieException {
        final Set<String> configs
                = this.service.getConfigsForCommand(COMMAND_1_ID);
        Assert.assertEquals(2, configs.size());
        Assert.assertEquals(1,
                this.service.removeConfigForCommand(
                        COMMAND_1_ID,
                        configs.iterator().next()).size());
    }

    /**
     * Test remove configuration for command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testRemoveConfigForCommandNullConfig() throws GenieException {
        this.service.removeConfigForCommand(COMMAND_1_ID, null);
    }

    /**
     * Test remove configuration for command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testRemoveConfigForCommandNoId() throws GenieException {
        this.service.removeConfigForCommand(null, "something");
    }

    /**
     * Test setting the applications for a given command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testSetApplicationsForCommand() throws GenieException {
        final Command command2 = this.service.getCommand(COMMAND_2_ID);
        Assert.assertTrue(command2.getApplications().isEmpty());

        final Set<String> appIds = new HashSet<>();
        appIds.add(APP_1_ID);
        final List<Command> preCommands = this.appService.getCommandsForApplication(APP_1_ID, null);
        Assert.assertEquals(1, preCommands.size());
        Assert.assertEquals(1, preCommands
                        .stream()
                        .filter(command -> COMMAND_1_ID.equals(command.getId()))
                        .count()
        );

        this.service.setApplicationsForCommand(COMMAND_2_ID, appIds);

        final List<Command> savedCommands = this.appService.getCommandsForApplication(APP_1_ID, null);
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
     * Test setting the applications for a given command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testSetApplicationsForCommandNoId() throws GenieException {
        this.service.setApplicationsForCommand(null, new HashSet<>());
    }

    /**
     * Test setting the applications for a given command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testSetApplicationsForCommandNoCommand() throws GenieException {
        this.service.setApplicationsForCommand(COMMAND_2_ID, null);
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
     * Test get applications for command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testGetApplicationsForCommandNoId() throws GenieException {
        this.service.getApplicationsForCommand(null);
    }

    /**
     * Test remove applications for command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testRemoveApplicationsForCommand() throws GenieException {
        Assert.assertEquals(1, this.service.getApplicationsForCommand(COMMAND_1_ID).size());
        Assert.assertEquals(1, this.service.removeApplicationsForCommand(COMMAND_1_ID).size());
        Assert.assertEquals(0, this.service.getApplicationsForCommand(COMMAND_1_ID).size());
    }

    /**
     * Test remove applications for command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testRemoveApplicationsForCommandNoId() throws GenieException {
        this.service.removeApplicationsForCommand(null);
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

        final Set<String> newTags = new HashSet<>();
        newTags.add(newTag1);
        newTags.add(newTag2);
        newTags.add(newTag3);

        Assert.assertEquals(5,
                this.service.getTagsForCommand(COMMAND_1_ID).size());
        final Set<String> finalTags
                = this.service.addTagsForCommand(COMMAND_1_ID, newTags);
        Assert.assertEquals(8, finalTags.size());
        Assert.assertTrue(finalTags.contains(newTag1));
        Assert.assertTrue(finalTags.contains(newTag2));
        Assert.assertTrue(finalTags.contains(newTag3));
    }

    /**
     * Test add tags to command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testAddTagsToCommandNoId() throws GenieException {
        this.service.addTagsForCommand(null, new HashSet<>());
    }

    /**
     * Test add tags to command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testAddTagsToCommandNoTags() throws GenieException {
        this.service.addTagsForCommand(COMMAND_1_ID, null);
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

        final Set<String> newTags = new HashSet<>();
        newTags.add(newTag1);
        newTags.add(newTag2);
        newTags.add(newTag3);

        Assert.assertEquals(5,
                this.service.getTagsForCommand(COMMAND_1_ID).size());
        final Set<String> finalTags
                = this.service.updateTagsForCommand(COMMAND_1_ID, newTags);
        Assert.assertEquals(5, finalTags.size());
        Assert.assertTrue(finalTags.contains(newTag1));
        Assert.assertTrue(finalTags.contains(newTag2));
        Assert.assertTrue(finalTags.contains(newTag3));
    }

    /**
     * Test update tags for command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testUpdateTagsForCommandNoId() throws GenieException {
        this.service.updateTagsForCommand(null, new HashSet<>());
    }

    /**
     * Test get tags for command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testGetTagsForCommand() throws GenieException {
        Assert.assertEquals(5,
                this.service.getTagsForCommand(COMMAND_1_ID).size());
    }

    /**
     * Test get tags to command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testGetTagsForCommandNoId() throws GenieException {
        this.service.getTagsForCommand(null);
    }

    /**
     * Test remove all tags for command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testRemoveAllTagsForCommand() throws GenieException {
        Assert.assertEquals(5,
                this.service.getTagsForCommand(COMMAND_1_ID).size());
        final Set<String> finalTags
                = this.service.removeAllTagsForCommand(COMMAND_1_ID);
        Assert.assertEquals(2,
                finalTags.size());
    }

    /**
     * Test remove all tags for command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testRemoveAllTagsForCommandNoId() throws GenieException {
        this.service.removeAllTagsForCommand(null);
    }

    /**
     * Test remove tag for command.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testRemoveTagForCommand() throws GenieException {
        final Set<String> tags
                = this.service.getTagsForCommand(COMMAND_1_ID);
        Assert.assertEquals(5, tags.size());
        Assert.assertEquals(4,
                this.service.removeTagForCommand(
                        COMMAND_1_ID,
                        "tez").size()
        );
    }

    /**
     * Test remove tag for command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testRemoveTagForCommandNullTag() throws GenieException {
        this.service.removeTagForCommand(COMMAND_1_ID, null);
    }

    /**
     * Test remove configuration for command.
     *
     * @throws GenieException For any problem
     */
    @Test(expected = ConstraintViolationException.class)
    public void testRemoveTagForCommandNoId() throws GenieException {
        this.service.removeTagForCommand(null, "something");
    }

    /**
     * Test the Get clusters for command function.
     *
     * @throws GenieException For any problem
     */
    @Test
    public void testGetCommandsForCommand() throws GenieException {
        final List<Cluster> clusters
                = this.service.getClustersForCommand(COMMAND_1_ID, null);
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
