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
package com.netflix.genie.web.jpa.entities;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.validation.ConstraintViolationException;
import java.util.ArrayList;
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
    private Set<FileEntity> configs;

    /**
     * Setup the tests.
     */
    @Before
    public void setup() {
        this.c = new ClusterEntity();
        final FileEntity config = new FileEntity();
        config.setFile(CONFIG);
        this.configs = Sets.newHashSet(config);
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
        this.c.setName("");
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
        this.c.setVersion("\t");
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
     */
    @Test
    public void testSetTags() {
        Assert.assertNotNull(this.c.getTags());
        final TagEntity prodTag = new TagEntity();
        prodTag.setTag("prod");
        final TagEntity slaTag = new TagEntity();
        slaTag.setTag("sla");
        final Set<TagEntity> tags = Sets.newHashSet(prodTag, slaTag);
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
        final FileEntity dependency = new FileEntity();
        dependency.setFile("s3://netflix/jars/myJar.jar");
        final Set<FileEntity> dependencies = Sets.newHashSet(dependency);
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
        one.setUniqueId("one");
        final CommandEntity two = new CommandEntity();
        two.setUniqueId("two");
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
        one.setUniqueId(UUID.randomUUID().toString());
        final CommandEntity two = new CommandEntity();
        two.setUniqueId(UUID.randomUUID().toString());

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
        commandEntity.setUniqueId("commandId");
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
        entity.setUniqueId(UUID.randomUUID().toString());
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
        one.setUniqueId("one");
        final CommandEntity two = new CommandEntity();
        two.setUniqueId("two");
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
        one.setUniqueId("one");
        final CommandEntity two = new CommandEntity();
        two.setUniqueId("two");
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
        final TagEntity oneTag = new TagEntity();
        oneTag.setTag("one");
        final TagEntity twoTag = new TagEntity();
        twoTag.setTag("tow");
        final TagEntity preTag = new TagEntity();
        preTag.setTag("Pre");
        final Set<TagEntity> tags = Sets.newHashSet(oneTag, twoTag, preTag);
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
     * Test the toString method.
     */
    @Test
    public void testToString() {
        Assert.assertNotNull(this.c.toString());
    }
}
