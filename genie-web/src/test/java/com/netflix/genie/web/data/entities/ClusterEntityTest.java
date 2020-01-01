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
package com.netflix.genie.web.data.entities;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.external.dtos.v4.ClusterStatus;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
class ClusterEntityTest extends EntityTestBase {

    private static final String NAME = "h2prod";
    private static final String USER = "tgianos";
    private static final String CONFIG = "s3://netflix/clusters/configs/config1";
    private static final String VERSION = "1.2.3";

    private ClusterEntity c;
    private Set<FileEntity> configs;

    /**
     * Setup the tests.
     */
    @BeforeEach
    void setup() {
        this.c = new ClusterEntity();
        final FileEntity config = new FileEntity();
        config.setFile(CONFIG);
        this.configs = Sets.newHashSet(config);
        this.c.setName(NAME);
        this.c.setUser(USER);
        this.c.setVersion(VERSION);
        this.c.setStatus(ClusterStatus.UP.name());
    }

    /**
     * Test the default Constructor.
     */
    @Test
    void testDefaultConstructor() {
        final ClusterEntity entity = new ClusterEntity();
        Assertions.assertThat(entity.getName()).isNull();
        Assertions.assertThat(entity.getStatus()).isNull();
        Assertions.assertThat(entity.getUser()).isNull();
        Assertions.assertThat(entity.getVersion()).isNull();
        Assertions.assertThat(entity.getConfigs()).isEmpty();
        Assertions.assertThat(entity.getDependencies()).isEmpty();
        Assertions.assertThat(entity.getTags()).isEmpty();
        Assertions.assertThat(entity.getCommands()).isEmpty();
    }

    /**
     * Make sure validation works on valid apps.
     */
    @Test
    void testValidate() {
        this.validate(this.c);
    }

    /**
     * Make sure validation works on with failure from super class.
     */
    @Test
    void testValidateNoName() {
        this.c.setName("");
        Assertions
            .assertThatExceptionOfType(ConstraintViolationException.class)
            .isThrownBy(() -> this.validate(this.c));
    }

    /**
     * Make sure validation works on with failure from super class.
     */
    @Test
    void testValidateNoUser() {
        this.c.setUser(" ");
        Assertions
            .assertThatExceptionOfType(ConstraintViolationException.class)
            .isThrownBy(() -> this.validate(this.c));
    }

    /**
     * Make sure validation works on with failure from super class.
     */
    @Test
    void testValidateNoVersion() {
        this.c.setVersion("\t");
        Assertions
            .assertThatExceptionOfType(ConstraintViolationException.class)
            .isThrownBy(() -> this.validate(this.c));
    }

    /**
     * Test setting the status.
     */
    @Test
    void testSetStatus() {
        this.c.setStatus(ClusterStatus.TERMINATED.name());
        Assertions.assertThat(this.c.getStatus()).isEqualTo(ClusterStatus.TERMINATED.name());
    }

    /**
     * Test setting the tags.
     */
    @Test
    void testSetTags() {
        Assertions.assertThat(this.c.getTags()).isEmpty();
        final TagEntity prodTag = new TagEntity();
        prodTag.setTag("prod");
        final TagEntity slaTag = new TagEntity();
        slaTag.setTag("sla");
        final Set<TagEntity> tags = Sets.newHashSet(prodTag, slaTag);
        this.c.setTags(tags);
        Assertions.assertThat(this.c.getTags()).isEqualTo(tags);

        this.c.setTags(null);
        Assertions.assertThat(this.c.getTags()).isEmpty();
    }

    /**
     * Test setting the configs.
     */
    @Test
    void testSetConfigs() {
        Assertions.assertThat(this.c.getConfigs()).isEmpty();
        this.c.setConfigs(this.configs);
        Assertions.assertThat(this.c.getConfigs()).isEqualTo(this.configs);

        this.c.setConfigs(null);
        Assertions.assertThat(c.getConfigs()).isEmpty();
    }

    /**
     * Test setting the dependencies.
     */
    @Test
    void testSetDependencies() {
        Assertions.assertThat(this.c.getDependencies()).isEmpty();
        final FileEntity dependency = new FileEntity();
        dependency.setFile("s3://netflix/jars/myJar.jar");
        final Set<FileEntity> dependencies = Sets.newHashSet(dependency);
        this.c.setDependencies(dependencies);
        Assertions.assertThat(this.c.getDependencies()).isEqualTo(dependencies);

        this.c.setDependencies(null);
        Assertions.assertThat(this.c.getDependencies()).isEmpty();
    }

    /**
     * Test setting the commands.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    void testSetCommands() throws GeniePreconditionException {
        Assertions.assertThat(this.c.getCommands()).isEmpty();
        final CommandEntity one = new CommandEntity();
        one.setUniqueId("one");
        final CommandEntity two = new CommandEntity();
        two.setUniqueId("two");
        final List<CommandEntity> commands = new ArrayList<>();
        commands.add(one);
        commands.add(two);
        this.c.setCommands(commands);
        Assertions.assertThat(this.c.getCommands()).isEqualTo(commands);
        Assertions.assertThat(one.getClusters()).contains(this.c);
        Assertions.assertThat(two.getClusters()).contains(this.c);
        this.c.setCommands(null);
        Assertions.assertThat(this.c.getCommands()).isEmpty();
        Assertions.assertThat(one.getClusters()).doesNotContain(this.c);
        Assertions.assertThat(two.getClusters()).doesNotContain(this.c);
    }

    /**
     * Make sure we can't set commands with duplicates.
     */
    @Test
    void cantSetCommandsWithDuplicates() {
        final CommandEntity one = new CommandEntity();
        one.setUniqueId(UUID.randomUUID().toString());
        final CommandEntity two = new CommandEntity();
        two.setUniqueId(UUID.randomUUID().toString());

        Assertions
            .assertThatExceptionOfType(GeniePreconditionException.class)
            .isThrownBy(() -> this.c.setCommands(Lists.newArrayList(one, two, one)));
    }

    /**
     * Test adding a command.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    void testAddCommand() throws GeniePreconditionException {
        final CommandEntity commandEntity = new CommandEntity();
        commandEntity.setUniqueId("commandId");
        Assertions.assertThat(this.c.getCommands()).isEmpty();
        this.c.addCommand(commandEntity);
        Assertions.assertThat(this.c.getCommands()).contains(commandEntity);
        Assertions.assertThat(commandEntity.getClusters()).contains(this.c);
    }

    /**
     * Make sure we can't add a duplicate command.
     *
     * @throws GeniePreconditionException If the command was already in the list
     */
    @Test
    void cantAddDuplicateCommand() throws GeniePreconditionException {
        final CommandEntity entity = new CommandEntity();
        entity.setUniqueId(UUID.randomUUID().toString());
        this.c.addCommand(entity);

        // Should throw exception here
        Assertions
            .assertThatExceptionOfType(GeniePreconditionException.class)
            .isThrownBy(() -> this.c.addCommand(entity));
    }

    /**
     * Test removing a command.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    void testRemoveCommand() throws GeniePreconditionException {
        final CommandEntity one = new CommandEntity();
        one.setUniqueId("one");
        final CommandEntity two = new CommandEntity();
        two.setUniqueId("two");
        Assertions.assertThat(this.c.getCommands()).isEmpty();
        this.c.addCommand(one);
        Assertions.assertThat(this.c.getCommands()).contains(one);
        Assertions.assertThat(this.c.getCommands()).doesNotContain(two);
        Assertions.assertThat(one.getClusters()).contains(this.c);
        Assertions.assertThat(two.getClusters()).isNotNull();
        Assertions.assertThat(two.getClusters()).isEmpty();
        this.c.addCommand(two);
        Assertions.assertThat(this.c.getCommands()).contains(one);
        Assertions.assertThat(this.c.getCommands()).contains(two);
        Assertions.assertThat(one.getClusters()).contains(this.c);
        Assertions.assertThat(two.getClusters()).contains(this.c);

        this.c.removeCommand(one);
        Assertions.assertThat(this.c.getCommands()).doesNotContain(one);
        Assertions.assertThat(this.c.getCommands()).contains(two);
        Assertions.assertThat(one.getClusters()).doesNotContain(this.c);
        Assertions.assertThat(two.getClusters()).contains(this.c);
    }

    /**
     * Test removing all the commands.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    void testRemoveAllCommands() throws GeniePreconditionException {
        Assertions.assertThat(this.c.getCommands()).isEmpty();
        final CommandEntity one = new CommandEntity();
        one.setUniqueId("one");
        final CommandEntity two = new CommandEntity();
        two.setUniqueId("two");
        final List<CommandEntity> commands = new ArrayList<>();
        commands.add(one);
        commands.add(two);
        this.c.setCommands(commands);
        Assertions.assertThat(this.c.getCommands()).isEqualTo(commands);
        Assertions.assertThat(one.getClusters()).contains(this.c);
        Assertions.assertThat(two.getClusters()).contains(this.c);

        this.c.removeAllCommands();
        Assertions.assertThat(this.c.getCommands()).isEmpty();
        Assertions.assertThat(one.getClusters()).doesNotContain(this.c);
        Assertions.assertThat(two.getClusters()).doesNotContain(this.c);
    }

    /**
     * Test the setClusterTags method.
     */
    @Test
    void canSetClusterTags() {
        final TagEntity oneTag = new TagEntity();
        oneTag.setTag("one");
        final TagEntity twoTag = new TagEntity();
        twoTag.setTag("tow");
        final TagEntity preTag = new TagEntity();
        preTag.setTag("Pre");
        final Set<TagEntity> tags = Sets.newHashSet(oneTag, twoTag, preTag);
        this.c.setTags(tags);
        Assertions.assertThat(this.c.getTags()).isEqualTo(tags);

        this.c.setTags(Sets.newHashSet());
        Assertions.assertThat(this.c.getTags()).isEmpty();

        this.c.setTags(null);
        Assertions.assertThat(this.c.getTags()).isEmpty();
    }

    /**
     * Test the toString method.
     */
    @Test
    void testToString() {
        Assertions.assertThat(this.c.toString()).isNotBlank();
    }
}
