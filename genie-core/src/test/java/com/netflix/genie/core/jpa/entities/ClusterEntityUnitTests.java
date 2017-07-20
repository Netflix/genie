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
package com.netflix.genie.core.jpa.entities;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.genie.test.suppliers.RandomSuppliers;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.validation.ConstraintViolationException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Test the Cluster class.
 *
 * @author tgianos
 */
@Category(UnitTest.class)
public class ClusterEntityUnitTests extends EntityTestsBase {

    private static final String NAME = "h2prod";
    private static final String USER = "tgianos";
    private static final String CONFIG = "s3://netflix/clusters/configs/config1";
    private static final String VERSION = "1.2.3";

    private ClusterEntity c;
    private Set<String> configs;

    /**
     * Setup the tests.
     */
    @Before
    public void setup() {
        this.c = new ClusterEntity();
        this.configs = Sets.newHashSet(CONFIG);
        this.c.setName(NAME);
        this.c.setUser(USER);
        this.c.setVersion(VERSION);
        this.c.setStatus(ClusterStatus.UP);
    }

    /**
     * Test the default Constructor.
     */
    @Test
    public void testDefaultConstructor() {
        final ClusterEntity entity = new ClusterEntity();
        Assert.assertNull(entity.getName());
        Assert.assertNull(entity.getStatus());
        Assert.assertNull(entity.getUser());
        Assert.assertNull(entity.getVersion());
        Assert.assertNotNull(entity.getConfigs());
        Assert.assertTrue(entity.getConfigs().isEmpty());
        Assert.assertNotNull(entity.getDependencies());
        Assert.assertTrue(entity.getDependencies().isEmpty());
        Assert.assertNotNull(entity.getTags());
        Assert.assertTrue(entity.getTags().isEmpty());
        Assert.assertNotNull(entity.getCommands());
        Assert.assertTrue(entity.getCommands().isEmpty());
    }

    /**
     * Test to make sure validation works.
     *
     * @throws GenieException If any precondition isn't met.
     */
    @Test
    public void testOnCreateOrUpdateCluster() throws GenieException {
        Assert.assertNotNull(this.c.getTags());
        Assert.assertTrue(this.c.getTags().isEmpty());
        this.c.onCreateOrUpdateCluster();
        Assert.assertEquals(2, this.c.getTags().size());
    }

    /**
     * Make sure validation works on valid apps.
     */
    @Test
    public void testValidate() {
        this.validate(this.c);
    }

    /**
     * Make sure validation works on with failure from super class.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateNoName() {
        this.c.setName(null);
        this.validate(this.c);
    }

    /**
     * Make sure validation works on with failure from super class.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateNoUser() {
        this.c.setUser(" ");
        this.validate(this.c);
    }

    /**
     * Make sure validation works on with failure from super class.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateNoVersion() {
        this.c.setVersion("");
        this.validate(this.c);
    }

    /**
     * Make sure validation works on with failure from cluster.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateNoStatus() {
        this.c.setStatus(null);
        this.validate(this.c);
    }

    /**
     * Test setting the status.
     */
    @Test
    public void testSetStatus() {
        this.c.setStatus(ClusterStatus.TERMINATED);
        Assert.assertEquals(ClusterStatus.TERMINATED, this.c.getStatus());
    }

    /**
     * Test setting the tags.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testSetTags() throws GeniePreconditionException {
        Assert.assertNotNull(this.c.getTags());
        final Set<String> tags = Sets.newHashSet("prod", "sla");
        this.c.setTags(tags);
        Assert.assertEquals(tags, this.c.getTags());

        this.c.setTags(null);
        Assert.assertThat(this.c.getTags(), Matchers.empty());
    }

    /**
     * Test setting the configs.
     */
    @Test
    public void testSetConfigs() {
        Assert.assertNotNull(this.c.getConfigs());
        this.c.setConfigs(this.configs);
        Assert.assertEquals(this.configs, this.c.getConfigs());

        this.c.setConfigs(null);
        Assert.assertThat(c.getConfigs(), Matchers.empty());
    }

    /**
     * Test setting the dependencies.
     */
    @Test
    public void testSetDependencies() {
        Assert.assertNotNull(this.c.getDependencies());
        final Set<String> dependencies = Sets.newHashSet("s3://netflix/jars/myJar.jar");
        this.c.setDependencies(dependencies);
        Assert.assertEquals(dependencies, this.c.getDependencies());

        this.c.setDependencies(null);
        Assert.assertThat(this.c.getDependencies(), Matchers.empty());
    }

    /**
     * Test setting the commands.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testSetCommands() throws GeniePreconditionException {
        Assert.assertNotNull(this.c.getCommands());
        Assert.assertTrue(this.c.getCommands().isEmpty());
        final CommandEntity one = new CommandEntity();
        one.setId("one");
        final CommandEntity two = new CommandEntity();
        two.setId("two");
        final List<CommandEntity> commands = new ArrayList<>();
        commands.add(one);
        commands.add(two);
        this.c.setCommands(commands);
        Assert.assertEquals(commands, this.c.getCommands());
        Assert.assertTrue(one.getClusters().contains(this.c));
        Assert.assertTrue(two.getClusters().contains(this.c));
        this.c.setCommands(null);
        Assert.assertThat(this.c.getCommands(), Matchers.empty());
        Assert.assertFalse(one.getClusters().contains(this.c));
        Assert.assertFalse(two.getClusters().contains(this.c));
    }

    /**
     * Make sure we can't set commands with duplicates.
     *
     * @throws GeniePreconditionException on duplicates
     */
    @Test(expected = GeniePreconditionException.class)
    public void cantSetCommandsWithDuplicates() throws GeniePreconditionException {
        final CommandEntity one = new CommandEntity();
        one.setId(UUID.randomUUID().toString());
        final CommandEntity two = new CommandEntity();
        two.setId(UUID.randomUUID().toString());

        this.c.setCommands(Lists.newArrayList(one, two, one));
    }

    /**
     * Test adding a command.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testAddCommand() throws GeniePreconditionException {
        final CommandEntity commandEntity = new CommandEntity();
        commandEntity.setId("commandId");
        Assert.assertNotNull(this.c.getCommands());
        Assert.assertTrue(this.c.getCommands().isEmpty());
        this.c.addCommand(commandEntity);
        Assert.assertTrue(this.c.getCommands().contains(commandEntity));
        Assert.assertTrue(commandEntity.getClusters().contains(this.c));
    }

    /**
     * Make sure we can't add a duplicate command.
     *
     * @throws GeniePreconditionException If the command was already in the list
     */
    @Test(expected = GeniePreconditionException.class)
    public void cantAddDuplicateCommand() throws GeniePreconditionException {
        final CommandEntity entity = new CommandEntity();
        entity.setId(UUID.randomUUID().toString());
        this.c.addCommand(entity);

        // Should throw exception here
        this.c.addCommand(entity);
    }

    /**
     * Test removing a command.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testRemoveCommand() throws GeniePreconditionException {
        final CommandEntity one = new CommandEntity();
        one.setId("one");
        final CommandEntity two = new CommandEntity();
        two.setId("two");
        Assert.assertNotNull(this.c.getCommands());
        Assert.assertTrue(this.c.getCommands().isEmpty());
        this.c.addCommand(one);
        Assert.assertTrue(this.c.getCommands().contains(one));
        Assert.assertFalse(this.c.getCommands().contains(two));
        Assert.assertTrue(one.getClusters().contains(this.c));
        Assert.assertNotNull(two.getClusters());
        Assert.assertTrue(two.getClusters().isEmpty());
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
     * Test removing all the commands.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testRemoveAllCommands() throws GeniePreconditionException {
        Assert.assertNotNull(this.c.getCommands());
        Assert.assertTrue(this.c.getCommands().isEmpty());
        final CommandEntity one = new CommandEntity();
        one.setId("one");
        final CommandEntity two = new CommandEntity();
        two.setId("two");
        final List<CommandEntity> commands = new ArrayList<>();
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

    /**
     * Test the setClusterTags method.
     */
    @Test
    public void canSetClusterTags() {
        final Set<String> tags = Sets.newHashSet("one", "two", "Pre");
        this.c.setTags(tags);
        Assert.assertThat(this.c.getTags(), Matchers.is(tags));
        Assert.assertThat(this.c.getTags().size(), Matchers.is(tags.size()));
        this.c.getTags().forEach(tag -> Assert.assertTrue(tags.contains(tag)));

        this.c.setTags(Sets.newHashSet());
        Assert.assertThat(this.c.getTags(), Matchers.empty());

        this.c.setTags(null);
        Assert.assertThat(this.c.getTags(), Matchers.empty());
    }

    /**
     * Test to make sure the entity can return a valid DTO.
     *
     * @throws GenieException on error
     */
    @Test
    public void canGetDTO() throws GenieException {
        final ClusterEntity entity = new ClusterEntity();
        final String name = UUID.randomUUID().toString();
        entity.setName(name);
        final String user = UUID.randomUUID().toString();
        entity.setUser(user);
        final String version = UUID.randomUUID().toString();
        entity.setVersion(version);
        entity.setStatus(ClusterStatus.TERMINATED);
        final String id = UUID.randomUUID().toString();
        entity.setId(id);
        final Date created = entity.getCreated();
        final Date updated = entity.getUpdated();
        final String description = UUID.randomUUID().toString();
        entity.setDescription(description);
        final Set<String> tags = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        entity.setTags(tags);
        final Set<String> confs = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        entity.setConfigs(confs);
        final Set<String> dependencies = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        entity.setDependencies(dependencies);

        final Cluster cluster = entity.getDTO();
        Assert.assertThat(cluster.getId().orElseGet(RandomSuppliers.STRING), Matchers.is(id));
        Assert.assertThat(cluster.getName(), Matchers.is(name));
        Assert.assertThat(cluster.getUser(), Matchers.is(user));
        Assert.assertThat(cluster.getVersion(), Matchers.is(version));
        Assert.assertThat(cluster.getDescription().orElseGet(RandomSuppliers.STRING), Matchers.is(description));
        Assert.assertThat(cluster.getStatus(), Matchers.is(ClusterStatus.TERMINATED));
        Assert.assertThat(cluster.getCreated().orElseGet(RandomSuppliers.DATE), Matchers.is(created));
        Assert.assertThat(cluster.getUpdated().orElseGet(RandomSuppliers.DATE), Matchers.is(updated));
        Assert.assertThat(cluster.getTags(), Matchers.is(tags));
        Assert.assertThat(cluster.getConfigs(), Matchers.is(confs));
        Assert.assertThat(cluster.getDependencies(), Matchers.is(dependencies));
    }
}
