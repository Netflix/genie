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
package com.netflix.genie.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.JsonPatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.ApplicationStatus;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.util.GenieObjectMapper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Integration Tests for Command Client.
 *
 * @author amsharma
 */
public class CommandClientIntegrationTest extends GenieClientIntegrationTestBase {

    private ClusterClient clusterClient;
    private CommandClient commandClient;
    private ApplicationClient applicationClient;

    /**
     * Setup for tests.
     *
     * @throws Exception If there is an error.
     */
    @Before
    public void setup() throws Exception {
        clusterClient = new ClusterClient(getBaseUrl(), null, null);
        commandClient = new CommandClient(getBaseUrl(), null, null);
        applicationClient = new ApplicationClient(getBaseUrl(), null, null);
    }

    /**
     * Delete all commands and applications between tests.
     *
     * @throws Exception If there is any problem.
     */
    @After
    public void cleanUp() throws Exception {
        clusterClient.deleteAllClusters();
        commandClient.deleteAllCommands();
        applicationClient.deleteAllApplications();
    }

    /**
     * Integration test to get all applications from Genie.
     *
     * @throws Exception If there is any problem.
     */
    @Test
    public void testCanCreateAndGetCommand() throws Exception {

        final String id = UUID.randomUUID().toString();
        final Command command = constructCommandDTO(id);

        final String commandId = commandClient.createCommand(command);
        Assert.assertEquals(commandId, id);

        // Make sure Genie Not Found Exception is not thrown for this call.
        final Command cmd = commandClient.getCommand(id);

        // Make sure the object returned is exactly what was sent to be created
        Assert.assertEquals(command.getId(), cmd.getId());
        Assert.assertEquals(command.getName(), cmd.getName());
        Assert.assertEquals(command.getDescription(), cmd.getDescription());
        Assert.assertEquals(command.getConfigs(), cmd.getConfigs());
        Assert.assertEquals(command.getSetupFile(), cmd.getSetupFile());
        Assert.assertTrue(cmd.getTags().contains("foo"));
        Assert.assertTrue(cmd.getTags().contains("bar"));
        Assert.assertEquals(command.getStatus(), cmd.getStatus());
    }

    /**
     * Test getting the commands using the various query parameters.
     *
     * @throws Exception If there is any problem.
     */
    @Test
    public void testGetCommandsUsingParams() throws Exception {
        final String command1Id = UUID.randomUUID().toString();
        final String command2Id = UUID.randomUUID().toString();

        final Set<String> command1Tags = Sets.newHashSet("foo", "pi");
        final Set<String> command2Tags = Sets.newHashSet("bar", "pi");

        final Command command1 = new Command.Builder(
            "command1name",
            "command1user",
            "1.0",
            CommandStatus.ACTIVE,
            "exec",
            1000
        )
            .withId(command1Id)
            .withTags(command1Tags)
            .build();

        final Command command2 =
            new Command.Builder(
                "command2name",
                "command2user",
                "2.0",
                CommandStatus.INACTIVE,
                "exec",
                1000
            )
                .withId(command2Id)
                .withTags(command2Tags)
                .build();

        commandClient.createCommand(command1);
        commandClient.createCommand(command2);

        // Test get by tags
        List<Command> commandList = commandClient.getCommands(
            null,
            null,
            null,
            Lists.newArrayList("foo")
        );
        Assert.assertEquals(1, commandList.size());
        Assert.assertEquals(command1Id, commandList.get(0).getId().orElse(UUID.randomUUID().toString()));

        commandList = commandClient.getCommands(
            null,
            null,
            null,
            Lists.newArrayList("pi")
        );

        Assert.assertEquals(2, commandList.size());
        Assert.assertEquals(command2Id, commandList.get(0).getId().orElse(UUID.randomUUID().toString()));
        Assert.assertEquals(command1Id, commandList.get(1).getId().orElse(UUID.randomUUID().toString()));

        // Test get by name
        commandList = commandClient.getCommands(
            "command1name",
            null,
            null,
            null
        );

        Assert.assertEquals(1, commandList.size());

        // Test get by status
        commandList = commandClient.getCommands(
            null,
            null,
            Lists.newArrayList(CommandStatus.ACTIVE.toString()),
            null
        );

        Assert.assertEquals(1, commandList.size());

        commandList = commandClient.getCommands(
            null,
            null,
            Arrays.asList(CommandStatus.ACTIVE.toString(), CommandStatus.INACTIVE.toString()),
            null
        );

        Assert.assertEquals(2, commandList.size());
    }

    /**
     * Test to confirm getting an exception for non existent command.
     *
     * @throws Exception If there is a problem.
     */
    @Test(expected = IOException.class)
    public void testCommandNotExist() throws Exception {
        commandClient.getCommand("foo");
    }

    /**
     * Test get all commands.
     *
     * @throws Exception If there is problem.
     */
    @Test
    public void testGetAllAndDeleteAllCommands() throws Exception {
        final List<Command> initialCommandList = commandClient.getCommands();
        Assert.assertEquals(initialCommandList.size(), 0);

        final Command command1 = constructCommandDTO(null);
        final Command command2 = constructCommandDTO(null);

        commandClient.createCommand(command1);
        commandClient.createCommand(command2);

        final List<Command> finalCommandList = commandClient.getCommands();
        Assert.assertEquals(finalCommandList.size(), 2);

        Assert.assertEquals(command1.getId(), finalCommandList.get(1).getId());
        Assert.assertEquals(command2.getId(), finalCommandList.get(0).getId());

        commandClient.deleteAllCommands();
        Assert.assertEquals(commandClient.getCommands().size(), 0);
    }

    /**
     * Test whether we can delete a command in Genie.
     *
     * @throws Exception If there is any problem.
     */
    @Test(expected = IOException.class)
    public void testDeleteCommand() throws Exception {
        final Command command1 = constructCommandDTO(null);
        commandClient.createCommand(command1);

        final Command command2 = commandClient.getCommand(command1.getId().orElseThrow(IllegalArgumentException::new));
        Assert.assertEquals(command2.getId(), command1.getId());

        commandClient.deleteCommand(command1.getId().orElseThrow(IllegalArgumentException::new));
        commandClient.getCommand(command1.getId().orElseThrow(IllegalArgumentException::new));
    }

    /**
     * Test to verify if the update command method is working correctly.
     *
     * @throws Exception If there is any problem.
     */
    @Test
    public void testUpdateCommand() throws Exception {
        final Command command1 = constructCommandDTO(null);
        commandClient.createCommand(command1);

        final Command command2 = commandClient.getCommand(command1.getId().orElseThrow(IllegalArgumentException::new));
        Assert.assertEquals(command2.getName(), command1.getName());

        final Command command3 = new
            Command.Builder("newname", "newuser", "new version", CommandStatus.ACTIVE, "exec", 1000)
            .withId(command1.getId().orElseThrow(IllegalArgumentException::new))
            .build();

        commandClient.updateCommand(command1.getId().orElseThrow(IllegalArgumentException::new), command3);

        final Command command4 = commandClient.getCommand(command1.getId().orElseThrow(IllegalArgumentException::new));

        Assert.assertEquals("newname", command4.getName());
        Assert.assertEquals("newuser", command4.getUser());
        Assert.assertEquals("new version", command4.getVersion());
        Assert.assertEquals(CommandStatus.ACTIVE, command4.getStatus());
        Assert.assertFalse(command4.getSetupFile().isPresent());
        Assert.assertFalse(command4.getDescription().isPresent());
        Assert.assertEquals(Collections.emptySet(), command4.getConfigs());
        Assert.assertFalse(command4.getTags().contains("foo"));
    }

    /**
     * Test all the methods that manipulate tags for a command in genie.
     *
     * @throws Exception If there is any problem.
     */
    @Test
    public void testCommandTagsMethods() throws Exception {

        final Set<String> initialTags = Sets.newHashSet("foo", "bar");
        final Set<String> configList = Sets.newHashSet("config1", "configs2");

        final Command command = new Command.Builder("name", "user", "1.0", CommandStatus.ACTIVE, "exec", 1000)
            .withId("command1")
            .withDescription("client Test")
            .withSetupFile("path to set up file")
            .withTags(initialTags)
            .withConfigs(configList)
            .build();

        commandClient.createCommand(command);

        // Test getTags for command
        Set<String> tags = commandClient.getTagsForCommand("command1");
        Assert.assertEquals(4, tags.size());
        Assert.assertTrue(tags.contains("foo"));
        Assert.assertTrue(tags.contains("bar"));

        // Test adding a tag for command
        final Set<String> moreTags = Sets.newHashSet("pi");

        commandClient.addTagsToCommand("command1", moreTags);
        tags = commandClient.getTagsForCommand("command1");
        Assert.assertEquals(5, tags.size());
        Assert.assertTrue(tags.contains("foo"));
        Assert.assertTrue(tags.contains("bar"));
        Assert.assertTrue(tags.contains("pi"));

        // Test removing a tag for command
        commandClient.removeTagFromCommand("command1", "bar");
        tags = commandClient.getTagsForCommand("command1");
        Assert.assertEquals(4, tags.size());
        Assert.assertTrue(tags.contains("foo"));
        Assert.assertTrue(tags.contains("pi"));

        // Test update tags for a command
        commandClient.updateTagsForCommand("command1", initialTags);
        tags = commandClient.getTagsForCommand("command1");
        Assert.assertEquals(4, tags.size());
        Assert.assertTrue(tags.contains("foo"));
        Assert.assertTrue(tags.contains("bar"));

        // Test delete all tags in a command
        commandClient.removeAllTagsForCommand("command1");
        tags = commandClient.getTagsForCommand("command1");
        Assert.assertEquals(2, tags.size());
    }

    /**
     * Test all the methods that manipulate configs for a command in genie.
     *
     * @throws Exception If there is any problem.
     */
    @Test
    public void testCommandConfigsMethods() throws Exception {

        final Set<String> initialConfigs = Sets.newHashSet("foo", "bar");

        final Command command = new Command.Builder("name", "user", "1.0", CommandStatus.ACTIVE, "exec", 1000)
            .withId("command1")
            .withDescription("client Test")
            .withSetupFile("path to set up file")
            .withConfigs(initialConfigs)
            .build();

        commandClient.createCommand(command);

        // Test getConfigs for command
        Set<String> configs = commandClient.getConfigsForCommand("command1");
        Assert.assertEquals(2, configs.size());
        Assert.assertTrue(configs.contains("foo"));
        Assert.assertTrue(configs.contains("bar"));

        // Test adding a config for command
        final Set<String> moreConfigs = Sets.newHashSet("pi");

        commandClient.addConfigsToCommand("command1", moreConfigs);
        configs = commandClient.getConfigsForCommand("command1");
        Assert.assertEquals(3, configs.size());
        Assert.assertTrue(configs.contains("foo"));
        Assert.assertTrue(configs.contains("bar"));
        Assert.assertTrue(configs.contains("pi"));

        // Test update configs for a command
        commandClient.updateConfigsForCommand("command1", initialConfigs);
        configs = commandClient.getConfigsForCommand("command1");
        Assert.assertEquals(2, configs.size());
        Assert.assertTrue(configs.contains("foo"));
        Assert.assertTrue(configs.contains("bar"));

        // Test delete all configs in a command
        commandClient.removeAllConfigsForCommand("command1");
        configs = commandClient.getConfigsForCommand("command1");
        Assert.assertEquals(0, configs.size());
    }

    /**
     * Test all the methods that manipulate dependencies for a command in genie.
     *
     * @throws Exception If there is any problem.
     */
    @Test
    public void testCommandDependenciesMethods() throws Exception {

        final Set<String> initialDependencies = Sets.newHashSet("foo", "bar");

        final Command command = new Command.Builder("name", "user", "1.0", CommandStatus.ACTIVE, "exec", 1000)
            .withId("command1")
            .withDescription("client Test")
            .withSetupFile("path to set up file")
            .withDependencies(initialDependencies)
            .build();

        commandClient.createCommand(command);

        // Test getDependencies for command
        Set<String> dependencies = commandClient.getDependenciesForCommand("command1");
        Assert.assertEquals(2, dependencies.size());
        Assert.assertTrue(dependencies.contains("foo"));
        Assert.assertTrue(dependencies.contains("bar"));

        // Test adding a dependency for command
        final Set<String> moreDependencies = Sets.newHashSet("pi");

        commandClient.addDependenciesToCommand("command1", moreDependencies);
        dependencies = commandClient.getDependenciesForCommand("command1");
        Assert.assertEquals(3, dependencies.size());
        Assert.assertTrue(dependencies.contains("foo"));
        Assert.assertTrue(dependencies.contains("bar"));
        Assert.assertTrue(dependencies.contains("pi"));

        // Test update dependencies for a command
        commandClient.updateDependenciesForCommand("command1", initialDependencies);
        dependencies = commandClient.getDependenciesForCommand("command1");
        Assert.assertEquals(2, dependencies.size());
        Assert.assertTrue(dependencies.contains("foo"));
        Assert.assertTrue(dependencies.contains("bar"));

        // Test delete all dependencies in a command
        commandClient.removeAllDependenciesForCommand("command1");
        dependencies = commandClient.getDependenciesForCommand("command1");
        Assert.assertEquals(0, dependencies.size());
    }

    /**
     * Test all the methods that manipulate applications for a command in genie.
     *
     * @throws Exception If there is any problem.
     */
    @Test
    public void testCommandApplicationsMethods() throws Exception {

        final Application foo = new Application.Builder(
            "name",
            "user",
            "version",
            ApplicationStatus.ACTIVE
        ).withId("foo")
            .build();

        applicationClient.createApplication(foo);

        final Application bar = new Application.Builder(
            "name",
            "user",
            "version",
            ApplicationStatus.ACTIVE
        ).withId("bar")
            .build();

        applicationClient.createApplication(bar);

        final Application pi = new Application.Builder(
            "name",
            "user",
            "version",
            ApplicationStatus.ACTIVE
        ).withId("pi")
            .build();

        applicationClient.createApplication(pi);

        final Command command = constructCommandDTO("command1");

        commandClient.createCommand(command);

        // Test add Applications to command
        final List<String> initialApplications = new ArrayList<>();
        initialApplications.add("foo");
        initialApplications.add("bar");
        initialApplications.add("pi");

        commandClient.addApplicationsToCommand("command1", initialApplications);

        List<Application> applications = commandClient.getApplicationsForCommand("command1");
        Assert.assertEquals(3, applications.size());
        Assert.assertEquals("foo", applications.get(0).getId().orElseThrow(IllegalArgumentException::new));
        Assert.assertEquals("bar", applications.get(1).getId().orElseThrow(IllegalArgumentException::new));
        Assert.assertEquals("pi", applications.get(2).getId().orElseThrow(IllegalArgumentException::new));

        // Test removing a application for command
        commandClient.removeApplicationFromCommand("command1", "pi");

        applications = commandClient.getApplicationsForCommand("command1");
        Assert.assertEquals(2, applications.size());
        Assert.assertEquals("foo", applications.get(0).getId().orElseThrow(IllegalArgumentException::new));
        Assert.assertEquals("bar", applications.get(1).getId().orElseThrow(IllegalArgumentException::new));

        final List<String> updatedApplications = new ArrayList<>();
        updatedApplications.add("foo");
        updatedApplications.add("pi");

        // Test update applications for a command
        commandClient.updateApplicationsForCommand("command1", updatedApplications);
        applications = commandClient.getApplicationsForCommand("command1");
        Assert.assertEquals(2, applications.size());
        Assert.assertEquals("foo", applications.get(0).getId().orElseThrow(IllegalArgumentException::new));
        Assert.assertEquals("pi", applications.get(1).getId().orElseThrow(IllegalArgumentException::new));

        // Test delete all applications in a command
        commandClient.removeAllApplicationsForCommand("command1");
        applications = commandClient.getApplicationsForCommand("command1");
        Assert.assertEquals(0, applications.size());
    }

    /**
     * Test the command patch method.
     *
     * @throws Exception If there is any error.
     */
    @Test
    public void testCommandPatchMethod() throws Exception {
        final ObjectMapper mapper = GenieObjectMapper.getMapper();
        final String newName = UUID.randomUUID().toString();
        final String patchString = "[{ \"op\": \"replace\", \"path\": \"/name\", \"value\": \"" + newName + "\" }]";
        final JsonPatch patch = JsonPatch.fromJson(mapper.readTree(patchString));

        final Command command = constructCommandDTO("command1");

        commandClient.createCommand(command);
        commandClient.patchCommand("command1", patch);

        Assert.assertEquals(newName, commandClient.getCommand("command1").getName());
    }

    /**
     * Test to fetch the clusters to which a command is linked.
     *
     * @throws Exception If there is any problem.
     */
    @Test
    public void testCanGetClustersForCommand() throws Exception {
        final Cluster cluster1 = constructClusterDTO(null);
        final Cluster cluster2 = constructClusterDTO(null);

        final Command command = constructCommandDTO(null);

        commandClient.createCommand(command);

        clusterClient.createCluster(cluster1);
        clusterClient.createCluster(cluster2);

        clusterClient.addCommandsToCluster(
            cluster1.getId().orElseThrow(IllegalArgumentException::new),
            Lists.newArrayList(command.getId().orElseThrow(IllegalArgumentException::new))
        );
        clusterClient.addCommandsToCluster(
            cluster2.getId().orElseThrow(IllegalArgumentException::new),
            Lists.newArrayList(command.getId().orElseThrow(IllegalArgumentException::new))
        );

        final List<Cluster> clusterList
            = commandClient.getClustersForCommand(command.getId().orElseThrow(IllegalArgumentException::new));

        Assert.assertEquals(2, clusterList.size());
    }
}
