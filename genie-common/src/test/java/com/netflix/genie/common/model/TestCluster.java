/*
 *
 *  Copyright 2014 Netflix, Inc.
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
package com.netflix.genie.common.model;

import com.netflix.genie.common.exceptions.GenieException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Test the Cluster class.
 *
 * @author tgianos
 */
public class TestCluster {
    private static final String NAME = "h2prod";
    private static final String USER = "tgianos";
    private static final String CONFIG = "s3://netflix/clusters/configs/config1";
    private static final String CLUSTER_TYPE = "Hadoop";
    private static final String VERSION = "1.2.3";

    private Cluster c;
    private Set<String> configs;

    /**
     * Setup the tests.
     */
    @Before
    public void setup() {
        this.c = new Cluster();
        this.configs = new HashSet<String>();
        this.configs.add(CONFIG);
    }

    /**
     * Test the default Constructor.
     */
    @Test
    public void testDefaultConstructor() {
        Assert.assertNull(this.c.getClusterType());
        Assert.assertNull(this.c.getCommands());
        Assert.assertNull(this.c.getConfigs());
        Assert.assertNull(this.c.getName());
        Assert.assertEquals(ClusterStatus.OUT_OF_SERVICE, this.c.getStatus());
        Assert.assertNull(this.c.getTags());
        Assert.assertNull(this.c.getUser());
        Assert.assertNull(this.c.getVersion());
    }

    /**
     * Test the argument Constructor.
     * @throws GenieException 
     */
    @Test
    public void testConstructor() throws GenieException {
        this.c = new Cluster(NAME, USER, ClusterStatus.UP, CLUSTER_TYPE, this.configs, VERSION);
        Assert.assertEquals(CLUSTER_TYPE, this.c.getClusterType());
        Assert.assertNull(this.c.getCommands());
        Assert.assertEquals(this.configs, this.c.getConfigs());
        Assert.assertEquals(NAME, this.c.getName());
        Assert.assertEquals(ClusterStatus.UP, this.c.getStatus());
        Assert.assertNull(this.c.getTags());
        Assert.assertEquals(USER, this.c.getUser());
        Assert.assertEquals(VERSION, this.c.getVersion());
    }

    /**
     * Test to make sure validation works.
     *
     * @throws GenieException
     */
    @Test
    public void testOnCreateOrUpdate() throws GenieException {
        this.c = new Cluster(NAME, USER, ClusterStatus.UP, CLUSTER_TYPE, this.configs, VERSION);
        this.c.onCreateOrUpdate();
    }

    /**
     * Test to make sure validation works.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testOnCreateOrUpdateWithNothing() throws GenieException {
        this.c.onCreateOrUpdate();
    }

    /**
     * Test to make sure validation works when no name entered.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testOnCreateOrUpdateNoName() throws GenieException {
        this.c = new Cluster(null, USER, ClusterStatus.UP, CLUSTER_TYPE, this.configs, VERSION);
        this.c.onCreateOrUpdate();
    }

    /**
     * Test to make sure validation works and throws exception when no User entered.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testOnCreateOrUpdateNoUser() throws GenieException {
        this.c = new Cluster(NAME, null, ClusterStatus.UP, CLUSTER_TYPE, this.configs, VERSION);
        this.c.onCreateOrUpdate();
    }

    /**
     * Test to make sure validation works and throws exception when no status entered.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testOnCreateOrUpdateNoStatus() throws GenieException {
        this.c = new Cluster(NAME, USER, null, CLUSTER_TYPE, this.configs, VERSION);
        this.c.onCreateOrUpdate();
    }

    /**
     * Test to make sure validation works and throws exception when no type entered.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testOnCreateOrUpdateNoType() throws GenieException {
        this.c = new Cluster(NAME, USER, ClusterStatus.UP, null, this.configs, VERSION);
        this.c.onCreateOrUpdate();
    }

    /**
     * Test to make sure validation works and throws exception when no configs entered.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testOnCreateOrUpdateEmptyConfigs() throws GenieException {
        this.c = new Cluster(
                NAME,
                USER,
                ClusterStatus.UP,
                CLUSTER_TYPE,
                new HashSet<String>(),
                VERSION
        );
        this.c.onCreateOrUpdate();
    }

    /**
     * Test to make sure validation works and throws exception when no configs entered.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testOnCreateOrUpdateNullConfigs() throws GenieException {
        this.c = new Cluster(NAME, USER, ClusterStatus.UP, CLUSTER_TYPE, null, VERSION);
        this.c.onCreateOrUpdate();
    }

    /**
     * Test to make sure validation works and throws exception when no version entered.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testOnCreateOrUpdateNoVersion() throws GenieException {
        this.c = new Cluster(NAME, USER, ClusterStatus.UP, CLUSTER_TYPE, this.configs, null);
        this.c.onCreateOrUpdate();
    }

    /**
     * Make sure validation works on valid cluster.
     *
     * @throws GenieException
     */
    @Test
    public void testValidate() throws GenieException {
        this.c = new Cluster(NAME, USER, ClusterStatus.UP, CLUSTER_TYPE, this.configs, VERSION);
        this.c.validate();
    }

    /**
     * Test setting the name.
     */
    @Test
    public void testSetName() {
        Assert.assertNull(this.c.getName());
        this.c.setName(NAME);
        Assert.assertEquals(NAME, this.c.getName());
    }

    /**
     * Test setting the user.
     * @throws GenieException 
     */
    @Test
    public void testSetUser() throws GenieException {
        Assert.assertNull(this.c.getUser());
        this.c.setUser(USER);
        Assert.assertEquals(USER, this.c.getUser());
    }

    /**
     * Test setting the status.
     */
    @Test
    public void testSetStatus() {
        Assert.assertEquals(ClusterStatus.OUT_OF_SERVICE, this.c.getStatus());
        this.c.setStatus(ClusterStatus.TERMINATED);
        Assert.assertEquals(ClusterStatus.TERMINATED, this.c.getStatus());
    }

    /**
     * Test setting the cluster type.
     */
    @Test
    public void testSetClusterType() {
        Assert.assertNull(this.c.getClusterType());
        this.c.setClusterType(CLUSTER_TYPE);
        Assert.assertEquals(CLUSTER_TYPE, this.c.getClusterType());
    }

    /**
     * Test setting the version.
     */
    @Test
    public void testSetVersion() {
        Assert.assertNull(this.c.getVersion());
        this.c.setVersion(VERSION);
        Assert.assertEquals(VERSION, this.c.getVersion());
    }

    /**
     * Test setting the tags.
     * @throws GenieException 
     */
    @Test
    public void testSetTags() throws GenieException {
        Assert.assertNull(this.c.getTags());
        final Set<String> tags = new HashSet<String>();
        tags.add("prod");
        tags.add("sla");
        this.c.setTags(tags);
        Assert.assertEquals(tags, this.c.getTags());
    }

    /**
     * Test setting the configs.
     */
    @Test
    public void testSetConfigs() {
        Assert.assertNull(this.c.getConfigs());
        this.c.setConfigs(this.configs);
        Assert.assertEquals(this.configs, this.c.getConfigs());
    }

    /**
     * Test setting the commands.
     *
     * @throws GenieException
     */
    @Test
    public void testSetCommands() throws GenieException {
        Assert.assertNull(this.c.getCommands());
        final Command one = new Command();
        one.setId("one");
        final Command two = new Command();
        two.setId("two");
        final List<Command> commands = new ArrayList<Command>();
        commands.add(one);
        commands.add(two);
        this.c.setCommands(commands);
        Assert.assertEquals(commands, this.c.getCommands());
        Assert.assertTrue(one.getClusters().contains(this.c));
        Assert.assertTrue(two.getClusters().contains(this.c));
        this.c.setCommands(null);
        Assert.assertNull(this.c.getCommands());
        Assert.assertFalse(one.getClusters().contains(this.c));
        Assert.assertFalse(two.getClusters().contains(this.c));
    }

    /**
     * Test adding a command.
     *
     * @throws GenieException
     */
    @Test
    public void testAddCommand() throws GenieException {
        final Command command = new Command();
        command.setId("commandId");
        Assert.assertNull(this.c.getCommands());
        this.c.addCommand(command);
        Assert.assertTrue(this.c.getCommands().contains(command));
        Assert.assertTrue(command.getClusters().contains(this.c));
    }

    /**
     * Test to make sure you can't add a null command.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testAddNullCommand() throws GenieException {
        this.c.addCommand(null);
    }

    /**
     * Test removing a command.
     *
     * @throws GenieException
     */
    @Test
    public void testRemoveCommand() throws GenieException {
        final Command one = new Command();
        one.setId("one");
        final Command two = new Command();
        two.setId("two");
        Assert.assertNull(this.c.getCommands());
        this.c.addCommand(one);
        Assert.assertTrue(this.c.getCommands().contains(one));
        Assert.assertFalse(this.c.getCommands().contains(two));
        Assert.assertTrue(one.getClusters().contains(this.c));
        Assert.assertNull(two.getClusters());
        this.c.addCommand(two);
        Assert.assertTrue(this.c.getCommands().contains(one));
        Assert.assertTrue(this.c.getCommands().contains(two));
        Assert.assertTrue(one.getClusters().contains(this.c));
        Assert.assertTrue(two.getClusters().contains(this.c));

        this.c.removeCommand(one);
        Assert.assertFalse(this.c.getCommands().contains(one));
        Assert.assertTrue(this.c.getCommands().contains(two));
        Assert.assertFalse(one.getClusters().contains(this.c));
        Assert.assertTrue(two.getClusters().contains(this.c));
    }

    /**
     * Make sure you can't remove a null command.
     */
    @Test(expected = GenieException.class)
    public void testRemoveNullCommand() throws GenieException {
        this.c.removeCommand(null);
    }

    /**
     * Test removing all the commands.
     *
     * @throws GenieException
     */
    @Test
    public void testRemoveAllCommands() throws GenieException {
        Assert.assertNull(this.c.getCommands());
        final Command one = new Command();
        one.setId("one");
        final Command two = new Command();
        two.setId("two");
        final List<Command> commands = new ArrayList<Command>();
        commands.add(one);
        commands.add(two);
        this.c.setCommands(commands);
        Assert.assertEquals(commands, this.c.getCommands());
        Assert.assertTrue(one.getClusters().contains(this.c));
        Assert.assertTrue(two.getClusters().contains(this.c));

        this.c.removeAllCommands();
        Assert.assertEquals(0, this.c.getCommands().size());
        Assert.assertTrue(this.c.getCommands().isEmpty());
        Assert.assertFalse(one.getClusters().contains(this.c));
        Assert.assertFalse(two.getClusters().contains(this.c));
    }
}
