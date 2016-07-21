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
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;


/**
 * Integration Tests for Cluster Client.
 *
 * @author amsharma
 */
public class ClusterClientIntegrationTests extends GenieClientsIntegrationTestsBase {

    private ClusterClient clusterClient;
    private CommandClient commandClient;

    /**
     * Setup for tests.
     *
     * @throws Exception If there is an error.
     */
    @Before
    public void setup() throws Exception {
        clusterClient = new ClusterClient(getBaseUrl(), null, null);
        commandClient = new CommandClient(getBaseUrl(), null, null);
    }

    /**
     * Delete all clusters and commands between tests.
     *
     * @throws Exception If there is any problem.
     */
    @After
    public void cleanUp() throws Exception {
        clusterClient.deleteAllClusters();
        commandClient.deleteAllCommands();
    }

    /**
     * Integration test to get all applications from Genie.
     *
     * @throws Exception If there is any problem.
     */
    @Test
    public void testCanCreateAndGetCluster() throws Exception {

        final String id = UUID.randomUUID().toString();
        final Cluster cluster = constructClusterDTO(id);

        final String clusterId = clusterClient.createCluster(cluster);
        Assert.assertEquals(clusterId, id);

        // Make sure Genie Not Found Exception is not thrown for this call.
        final Cluster cstr = clusterClient.getCluster(id);

        // Make sure the object returned is exactly what was sent to be created
        Assert.assertEquals(cluster.getId(), cstr.getId());
        Assert.assertEquals(cluster.getName(), cstr.getName());
        Assert.assertEquals(cluster.getDescription(), cstr.getDescription());
        Assert.assertEquals(cluster.getConfigs(), cstr.getConfigs());
        Assert.assertEquals(cluster.getSetupFile(), cstr.getSetupFile());
        Assert.assertEquals(cstr.getTags().contains("foo"), true);
        Assert.assertEquals(cstr.getTags().contains("bar"), true);
        Assert.assertEquals(cluster.getStatus(), cstr.getStatus());
    }

    /**
     * Test getting the clusters using the various query parameters.
     *
     * @throws Exception If there is any problem.
     */
    @Test
    public void testGetClustersUsingParams() throws Exception {
        final String cluster1Id = UUID.randomUUID().toString();
        final String cluster2Id = UUID.randomUUID().toString();

        final Set<String> cluster1Tags = new HashSet<>();
        cluster1Tags.add("foo");
        cluster1Tags.add("pi");

        final Set<String> cluster2Tags = new HashSet<>();
        cluster2Tags.add("bar");
        cluster2Tags.add("pi");


        final Cluster cluster1 = new Cluster.Builder("cluster1name", "cluster1user", "1.0", ClusterStatus.UP)
            .withId(cluster1Id)
            .withTags(cluster1Tags)
            .build();

        final Cluster cluster2 =
            new Cluster.Builder("cluster2name", "cluster2user", "2.0", ClusterStatus.OUT_OF_SERVICE)
            .withId(cluster2Id)
            .withTags(cluster2Tags)
            .build();

        clusterClient.createCluster(cluster1);
        clusterClient.createCluster(cluster2);

        // Test get by tags
        List<Cluster> clusterList = clusterClient.getClusters(
            null,
            null,
            Arrays.asList("foo"),
            null,
            null
        );
        Assert.assertEquals(1, clusterList.size());
        Assert.assertEquals(cluster1Id, clusterList.get(0).getId());

        clusterList = clusterClient.getClusters(
            null,
            null,
            Arrays.asList("pi"),
            null,
            null
        );

        Assert.assertEquals(2, clusterList.size());
        Assert.assertEquals(cluster2Id, clusterList.get(0).getId());
        Assert.assertEquals(cluster1Id, clusterList.get(1).getId());

        // Test get by name
        clusterList = clusterClient.getClusters(
            "cluster1name",
            null,
            null,
            null,
            null
        );

        Assert.assertEquals(1, clusterList.size());

        // Test get by status
        clusterList = clusterClient.getClusters(
            null,
            Arrays.asList(ClusterStatus.UP.toString()),
            null,
            null,
            null
        );

        Assert.assertEquals(1, clusterList.size());

        clusterList = clusterClient.getClusters(
            null,
            Arrays.asList(ClusterStatus.UP.toString(), ClusterStatus.OUT_OF_SERVICE.toString()),
            null,
            null,
            null
        );

        Assert.assertEquals(2, clusterList.size());
    }

    /**
     * Test to confirm getting an exception for non existent cluster.
     *
     * @throws Exception If there is a problem.
     */
    @Test(expected = IOException.class)
    public void testClusterNotExist() throws Exception {
        clusterClient.getCluster("foo");
    }

    /**
     * Test get all clusters.
     *
     * @throws Exception If there is problem.
     */
    @Test
    public void testGetAllAndDeleteAllClusters() throws Exception {
        final List<Cluster> initialClusterList = clusterClient.getClusters();
        Assert.assertEquals(initialClusterList.size(), 0);

        final Cluster cluster1 = constructClusterDTO(null);
        final Cluster cluster2 = constructClusterDTO(null);

        clusterClient.createCluster(cluster1);
        clusterClient.createCluster(cluster2);

        final List<Cluster> finalClusterList = clusterClient.getClusters();
        Assert.assertEquals(finalClusterList.size(), 2);

        Assert.assertEquals(cluster1.getId(), finalClusterList.get(1).getId());
        Assert.assertEquals(cluster2.getId(), finalClusterList.get(0).getId());

        clusterClient.deleteAllClusters();
        Assert.assertEquals(clusterClient.getClusters().size(), 0);
    }

    /**
     * Test whether we can delete a cluster in Genie.
     *
     * @throws Exception If there is any problem.
     */
    @Test(expected = IOException.class)
    public void testDeleteCluster() throws Exception {
        final Cluster cluster1 = constructClusterDTO(null);
        clusterClient.createCluster(cluster1);

        final Cluster cluster2 = clusterClient.getCluster(cluster1.getId());
        Assert.assertEquals(cluster2.getId(), cluster1.getId());

        clusterClient.deleteCluster(cluster1.getId());
        clusterClient.getCluster(cluster1.getId());
    }

    /**
     * Test to verify if the update cluster method is working correctly.
     *
     * @throws Exception If there is any problem.
     */
    @Test
    public void testUpdateCluster() throws Exception {
        final Cluster cluster1 = constructClusterDTO(null);
        clusterClient.createCluster(cluster1);

        final Cluster cluster2 = clusterClient.getCluster(cluster1.getId());
        Assert.assertEquals(cluster2.getName(), cluster1.getName());

        final Cluster cluster3 = new
            Cluster.Builder("newname", "newuser", "new version", ClusterStatus.OUT_OF_SERVICE)
            .withId(cluster1.getId())
            .build();

        clusterClient.updateCluster(cluster1.getId(), cluster3);

        final Cluster cluster4 = clusterClient.getCluster(cluster1.getId());

        Assert.assertEquals("newname", cluster4.getName());
        Assert.assertEquals("newuser", cluster4.getUser());
        Assert.assertEquals("new version", cluster4.getVersion());
        Assert.assertEquals(ClusterStatus.OUT_OF_SERVICE, cluster4.getStatus());
        Assert.assertEquals(null, cluster4.getSetupFile());
        Assert.assertEquals(null, cluster4.getDescription());
        Assert.assertEquals(Collections.emptySet(), cluster4.getConfigs());
        Assert.assertEquals(cluster4.getTags().contains("foo"), false);
    }

    /**
     * Test all the methods that manipulate tags for a cluster in genie.
     *
     * @throws Exception If there is any problem.
     */
    @Test
    public void testClusterTagsMethods() throws Exception {

        final Set<String> initialTags = new HashSet<>();
        initialTags.add("foo");
        initialTags.add("bar");

        final Set<String> configList = new HashSet<>();
        configList.add("config1");
        configList.add("configs2");

        final Cluster cluster = new Cluster.Builder("name", "user", "1.0", ClusterStatus.UP)
            .withId("cluster1")
            .withDescription("client Test")
            .withSetupFile("path to set up file")
            .withTags(initialTags)
            .withConfigs(configList)
            .build();

        clusterClient.createCluster(cluster);

        // Test getTags for cluster
        Set<String> tags = clusterClient.getTagsForCluster("cluster1");
        Assert.assertEquals(4, tags.size());
        Assert.assertEquals(tags.contains("foo"), true);
        Assert.assertEquals(tags.contains("bar"), true);

        // Test adding a tag for cluster
        final Set<String> moreTags = new HashSet<>();
        moreTags.add("pi");

        clusterClient.addTagsToCluster("cluster1", moreTags);
        tags = clusterClient.getTagsForCluster("cluster1");
        Assert.assertEquals(5, tags.size());
        Assert.assertEquals(tags.contains("foo"), true);
        Assert.assertEquals(tags.contains("bar"), true);
        Assert.assertEquals(tags.contains("pi"), true);

        // Test removing a tag for cluster
        clusterClient.removeTagFromCluster("cluster1", "bar");
        tags = clusterClient.getTagsForCluster("cluster1");
        Assert.assertEquals(4, tags.size());
        Assert.assertEquals(tags.contains("foo"), true);
        Assert.assertEquals(tags.contains("pi"), true);

        // Test update tags for a cluster
        clusterClient.updateTagsForCluster("cluster1", initialTags);
        tags = clusterClient.getTagsForCluster("cluster1");
        Assert.assertEquals(4, tags.size());
        Assert.assertEquals(tags.contains("foo"), true);
        Assert.assertEquals(tags.contains("bar"), true);

        // Test delete all tags in a cluster
        clusterClient.removeAllTagsForCluster("cluster1");
        tags = clusterClient.getTagsForCluster("cluster1");
        Assert.assertEquals(2, tags.size());
    }

    /**
     * Test all the methods that manipulate configs for a cluster in genie.
     *
     * @throws Exception If there is any problem.
     */
    @Test
    public void testClusterConfigsMethods() throws Exception {

        final Set<String> initialConfigs = new HashSet<>();
        initialConfigs.add("foo");
        initialConfigs.add("bar");

        final Cluster cluster = new Cluster.Builder("name", "user", "1.0", ClusterStatus.UP)
            .withId("cluster1")
            .withDescription("client Test")
            .withSetupFile("path to set up file")
            .withConfigs(initialConfigs)
            .build();

        clusterClient.createCluster(cluster);

        // Test getConfigs for cluster
        Set<String> configs = clusterClient.getConfigsForCluster("cluster1");
        Assert.assertEquals(2, configs.size());
        Assert.assertEquals(configs.contains("foo"), true);
        Assert.assertEquals(configs.contains("bar"), true);

        // Test adding a config for cluster
        final Set<String> moreConfigs = new HashSet<>();
        moreConfigs.add("pi");

        clusterClient.addConfigsToCluster("cluster1", moreConfigs);
        configs = clusterClient.getConfigsForCluster("cluster1");
        Assert.assertEquals(3, configs.size());
        Assert.assertEquals(configs.contains("foo"), true);
        Assert.assertEquals(configs.contains("bar"), true);
        Assert.assertEquals(configs.contains("pi"), true);

        // Test update configs for a cluster
        clusterClient.updateConfigsForCluster("cluster1", initialConfigs);
        configs = clusterClient.getConfigsForCluster("cluster1");
        Assert.assertEquals(2, configs.size());
        Assert.assertEquals(configs.contains("foo"), true);
        Assert.assertEquals(configs.contains("bar"), true);

        // Test delete all configs in a cluster
        clusterClient.removeAllConfigsForCluster("cluster1");
        configs = clusterClient.getConfigsForCluster("cluster1");
        Assert.assertEquals(0, configs.size());
    }

    /**
     * Test all the methods that manipulate commands for a cluster in genie.
     *
     * @throws Exception If there is any problem.
     */
    @Test
    public void testClusterCommandsMethods() throws Exception {

        final Command foo = new Command.Builder(
            "name",
            "user",
            "version",
            CommandStatus.ACTIVE,
            "exec",
            5
        ).withId("foo")
            .build();

        commandClient.createCommand(foo);

        final Command bar = new Command.Builder(
            "name",
            "user",
            "version",
            CommandStatus.ACTIVE,
            "exec",
            5
        ).withId("bar")
            .build();

        commandClient.createCommand(bar);


        final Command pi = new Command.Builder(
            "name",
            "user",
            "version",
            CommandStatus.ACTIVE,
            "exec",
            5
        ).withId("pi")
            .build();

        commandClient.createCommand(pi);

        final Cluster cluster = new Cluster.Builder("name", "user", "1.0", ClusterStatus.UP)
            .withId("cluster1")
            .withDescription("client Test")
            .withSetupFile("path to set up file")
            .build();

        clusterClient.createCluster(cluster);

        // Test add Commands to cluster
        final List<String> initialCommands = new ArrayList<>();
        initialCommands.add("foo");
        initialCommands.add("bar");
        initialCommands.add("pi");

        clusterClient.addCommandsToCluster("cluster1", initialCommands);

        List<Command> commands = clusterClient.getCommandsForCluster("cluster1");
        Assert.assertEquals(3, commands.size());
        Assert.assertEquals("foo", commands.get(0).getId());
        Assert.assertEquals("bar", commands.get(1).getId());
        Assert.assertEquals("pi", commands.get(2).getId());

        // Test removing a command for cluster
        clusterClient.removeCommandFromCluster("cluster1", "pi");

        commands = clusterClient.getCommandsForCluster("cluster1");
        Assert.assertEquals(2, commands.size());
        Assert.assertEquals("foo", commands.get(0).getId());
        Assert.assertEquals("bar", commands.get(1).getId());

        final List<String> updatedCommands = new ArrayList<>();
        updatedCommands.add("foo");
        updatedCommands.add("pi");

        // Test update commands for a cluster
        clusterClient.updateCommandsForCluster("cluster1", updatedCommands);
        commands = clusterClient.getCommandsForCluster("cluster1");
        Assert.assertEquals(2, commands.size());
        Assert.assertEquals("foo", commands.get(0).getId());
        Assert.assertEquals("pi", commands.get(1).getId());

        // Test delete all commands in a cluster
        clusterClient.removeAllCommandsForCluster("cluster1");
        commands = clusterClient.getCommandsForCluster("cluster1");
        Assert.assertEquals(0, commands.size());
    }

    /**
     * Test the cluster patch method.
     *
     * @throws Exception If there is any error.
     */
    @Test
    public void testClusterPatchMethod() throws Exception {
        final ObjectMapper mapper = new ObjectMapper();
        final String newName = UUID.randomUUID().toString();
        final String patchString = "[{ \"op\": \"replace\", \"path\": \"/name\", \"value\": \"" + newName + "\" }]";
        final JsonPatch patch = JsonPatch.fromJson(mapper.readTree(patchString));

        final Cluster cluster = new Cluster.Builder("name", "user", "1.0", ClusterStatus.UP)
            .withId("cluster1")
            .withDescription("client Test")
            .withSetupFile("path to set up file")
            .build();

        clusterClient.createCluster(cluster);
        clusterClient.patchCluster("cluster1", patch);

        Assert.assertEquals(newName, clusterClient.getCluster("cluster1").getName());
    }
}
