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
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import org.apache.commons.lang3.StringUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.validation.ConstraintViolationException;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Test the command class.
 *
 * @author tgianos
 */
class CommandEntityTest extends EntityTestBase {
    private static final String NAME = "pig13";
    private static final String USER = "tgianos";
    private static final List<String> EXECUTABLE = Lists.newArrayList("/bin/pig13", "-Dblah");
    private static final String VERSION = "1.0";
    private static final long CHECK_DELAY = 18083L;
    private static final int MEMORY = 10_240;

    private CommandEntity c;

    /**
     * Setup the tests.
     */
    @BeforeEach
    void setup() {
        this.c = new CommandEntity();
        this.c.setName(NAME);
        this.c.setUser(USER);
        this.c.setVersion(VERSION);
        this.c.setStatus(CommandStatus.ACTIVE.name());
        this.c.setExecutable(EXECUTABLE);
        this.c.setCheckDelay(CHECK_DELAY);
        this.c.setMemory(MEMORY);
    }

    /**
     * Test the default Constructor.
     */
    @Test
    void testDefaultConstructor() {
        final CommandEntity entity = new CommandEntity();
        Assertions.assertThat(entity.getSetupFile()).isNotPresent();
        Assertions.assertThat(entity.getExecutable()).isEmpty();
        Assertions.assertThat(entity.getCheckDelay()).isEqualTo(Command.DEFAULT_CHECK_DELAY);
        Assertions.assertThat(entity.getName()).isNull();
        Assertions.assertThat(entity.getStatus()).isNull();
        Assertions.assertThat(entity.getUser()).isNull();
        Assertions.assertThat(entity.getVersion()).isNull();
        Assertions.assertThat(entity.getConfigs()).isEmpty();
        Assertions.assertThat(entity.getDependencies()).isEmpty();
        Assertions.assertThat(entity.getTags()).isEmpty();
        Assertions.assertThat(entity.getClusters()).isEmpty();
        Assertions.assertThat(entity.getApplications()).isEmpty();
        Assertions.assertThat(entity.getMemory()).isNotPresent();
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
        this.c.setUser("   ");
        Assertions
            .assertThatExceptionOfType(ConstraintViolationException.class)
            .isThrownBy(() -> this.validate(this.c));
    }

    /**
     * Make sure validation works on with failure from super class.
     */
    @Test
    void testValidateNoVersion() {
        this.c.setVersion("");
        Assertions
            .assertThatExceptionOfType(ConstraintViolationException.class)
            .isThrownBy(() -> this.validate(this.c));
    }

    /**
     * Make sure validation works on with failure from command.
     */
    @Test
    void testValidateEmptyExecutable() {
        this.c.setExecutable(Lists.newArrayList());
        Assertions
            .assertThatExceptionOfType(ConstraintViolationException.class)
            .isThrownBy(() -> this.validate(this.c));
    }

    /**
     * Make sure validation works on with failure from command.
     */
    @Test
    void testValidateBlankExecutable() {
        this.c.setExecutable(Lists.newArrayList("    "));
        Assertions
            .assertThatExceptionOfType(ConstraintViolationException.class)
            .isThrownBy(() -> this.validate(this.c));
    }

    /**
     * Make sure validation works on with failure from command.
     */
    @Test
    void testValidateExecutableArgumentTooLong() {
        this.c.setExecutable(Lists.newArrayList(StringUtils.leftPad("", 1025, 'e')));
        Assertions
            .assertThatExceptionOfType(ConstraintViolationException.class)
            .isThrownBy(() -> this.validate(this.c));
    }

    /**
     * Make sure validation works on with failure from command.
     */
    @Test
    void testValidateBadCheckDelay() {
        this.c.setCheckDelay(0L);
        Assertions
            .assertThatExceptionOfType(ConstraintViolationException.class)
            .isThrownBy(() -> this.validate(this.c));
    }

    /**
     * Test setting the status.
     */
    @Test
    void testSetStatus() {
        this.c.setStatus(CommandStatus.ACTIVE.name());
        Assertions.assertThat(this.c.getStatus()).isEqualTo(CommandStatus.ACTIVE.name());
    }

    /**
     * Test setting the setup file.
     */
    @Test
    void testSetSetupFile() {
        Assertions.assertThat(this.c.getSetupFile()).isNotPresent();
        final String setupFile = "s3://netflix.propFile";
        final FileEntity setupFileEntity = new FileEntity(setupFile);
        this.c.setSetupFile(setupFileEntity);
        Assertions.assertThat(this.c.getSetupFile()).isPresent().contains(setupFileEntity);
    }

    /**
     * Test setting the executable.
     */
    @Test
    void testSetExecutable() {
        this.c.setExecutable(EXECUTABLE);
        Assertions.assertThat(this.c.getExecutable()).isEqualTo(EXECUTABLE);
    }

    /**
     * Make sure the check delay setter and getter works properly.
     */
    @Test
    void testSetCheckDelay() {
        final long newDelay = 108327L;
        Assertions.assertThat(this.c.getCheckDelay()).isEqualTo(CHECK_DELAY);
        this.c.setCheckDelay(newDelay);
        Assertions.assertThat(this.c.getCheckDelay()).isEqualTo(newDelay);
    }

    /**
     * Make sure can set the memory for the command if a user desires it.
     */
    @Test
    void testSetMemory() {
        Assertions.assertThat(this.c.getMemory()).isPresent().contains(MEMORY);
        final int newMemory = MEMORY + 1;
        this.c.setMemory(newMemory);
        Assertions.assertThat(this.c.getMemory()).isPresent().contains(newMemory);
    }

    /**
     * Test setting the configs.
     */
    @Test
    void testSetConfigs() {
        Assertions.assertThat(this.c.getConfigs()).isEmpty();
        final Set<FileEntity> configs = Sets.newHashSet(new FileEntity("s3://netflix.configFile"));
        this.c.setConfigs(configs);
        Assertions.assertThat(this.c.getConfigs()).isEqualTo(configs);

        this.c.setConfigs(null);
        Assertions.assertThat(this.c.getConfigs()).isEmpty();
    }

    /**
     * Test setting the dependencies.
     */
    @Test
    void testSetDependencies() {
        Assertions.assertThat(this.c.getDependencies()).isEmpty();
        final Set<FileEntity> dependencies = Sets.newHashSet(new FileEntity("dep1"));
        this.c.setDependencies(dependencies);
        Assertions.assertThat(this.c.getDependencies()).isEqualTo(dependencies);

        this.c.setDependencies(null);
        Assertions.assertThat(this.c.getDependencies()).isEmpty();
    }

    /**
     * Test setting the tags.
     */
    @Test
    void testSetTags() {
        Assertions.assertThat(this.c.getTags()).isEmpty();
        final TagEntity one = new TagEntity("tag1");
        final TagEntity two = new TagEntity("tag2");
        final Set<TagEntity> tags = Sets.newHashSet(one, two);
        this.c.setTags(tags);
        Assertions.assertThat(this.c.getTags()).isEqualTo(tags);

        this.c.setTags(null);
        Assertions.assertThat(this.c.getTags()).isEmpty();
    }

    /**
     * Test setting applications.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    void testSetApplications() throws GeniePreconditionException {
        Assertions.assertThat(this.c.getApplications()).isEmpty();
        final ApplicationEntity one = new ApplicationEntity();
        one.setUniqueId("one");
        final ApplicationEntity two = new ApplicationEntity();
        two.setUniqueId("two");
        final List<ApplicationEntity> applicationEntities = Lists.newArrayList(one, two);
        this.c.setApplications(applicationEntities);
        Assertions.assertThat(this.c.getApplications()).isEqualTo(applicationEntities);
        Assertions.assertThat(one.getCommands()).contains(this.c);
        Assertions.assertThat(two.getCommands()).contains(this.c);

        applicationEntities.clear();
        applicationEntities.add(two);
        this.c.setApplications(applicationEntities);
        Assertions.assertThat(this.c.getApplications()).isEqualTo(applicationEntities);
        Assertions.assertThat(one.getCommands()).doesNotContain(this.c);
        Assertions.assertThat(two.getCommands()).contains(this.c);
        this.c.setApplications(null);
        Assertions.assertThat(this.c.getApplications()).isEmpty();
        Assertions.assertThat(one.getCommands()).isEmpty();
        Assertions.assertThat(two.getCommands()).isEmpty();
    }

    /**
     * Make sure if a List with duplicate applications is sent in it fails.
     */
    @Test
    void cantSetApplicationsIfDuplicates() {
        final ApplicationEntity one = Mockito.mock(ApplicationEntity.class);
        Mockito.when(one.getUniqueId()).thenReturn(UUID.randomUUID().toString());
        final ApplicationEntity two = Mockito.mock(ApplicationEntity.class);
        Mockito.when(two.getUniqueId()).thenReturn(UUID.randomUUID().toString());

        Assertions
            .assertThatExceptionOfType(GeniePreconditionException.class)
            .isThrownBy(() -> this.c.setApplications(Lists.newArrayList(one, two, one)));
    }

    /**
     * Test to make sure we can add an application.
     *
     * @throws GeniePreconditionException On error
     */
    @Test
    void canAddApplication() throws GeniePreconditionException {
        final String id = UUID.randomUUID().toString();
        final ApplicationEntity app = new ApplicationEntity();
        app.setUniqueId(id);

        this.c.addApplication(app);
        Assertions.assertThat(this.c.getApplications()).contains(app);
        Assertions.assertThat(app.getCommands()).contains(this.c);
    }

    /**
     * Test to make sure we can't add an application to a command if it's already in the list.
     *
     * @throws GeniePreconditionException on duplicate
     */
    @Test
    void cantAddApplicationThatAlreadyIsInList() throws GeniePreconditionException {
        final String id = UUID.randomUUID().toString();
        final ApplicationEntity app = new ApplicationEntity();
        app.setUniqueId(id);

        this.c.addApplication(app);
        Assertions
            .assertThatExceptionOfType(GeniePreconditionException.class)
            .isThrownBy(() -> this.c.addApplication(app));
    }

    /**
     * Test removing an application.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    void canRemoveApplication() throws GeniePreconditionException {
        final ApplicationEntity one = new ApplicationEntity();
        one.setUniqueId("one");
        final ApplicationEntity two = new ApplicationEntity();
        two.setUniqueId("two");
        Assertions.assertThat(this.c.getApplications()).isEmpty();
        this.c.addApplication(one);
        Assertions.assertThat(this.c.getApplications()).contains(one);
        Assertions.assertThat(this.c.getApplications()).doesNotContain(two);
        Assertions.assertThat(one.getCommands()).contains(this.c);
        Assertions.assertThat(two.getCommands()).isEmpty();
        this.c.addApplication(two);
        Assertions.assertThat(this.c.getApplications()).contains(one);
        Assertions.assertThat(this.c.getApplications()).contains(two);
        Assertions.assertThat(one.getCommands()).contains(this.c);
        Assertions.assertThat(two.getCommands()).contains(this.c);

        this.c.removeApplication(one);
        Assertions.assertThat(this.c.getApplications()).doesNotContain(one);
        Assertions.assertThat(this.c.getApplications()).contains(two);
        Assertions.assertThat(one.getCommands()).doesNotContain(this.c);
        Assertions.assertThat(two.getCommands()).contains(this.c);
    }

    /**
     * Test setting the clusters.
     */
    @Test
    void testSetClusters() {
        Assertions.assertThat(this.c.getClusters()).isEmpty();
        final Set<ClusterEntity> clusterEntities = Sets.newHashSet(new ClusterEntity());
        this.c.setClusters(clusterEntities);
        Assertions.assertThat(this.c.getClusters()).isEqualTo(clusterEntities);

        this.c.setClusters(null);
        Assertions.assertThat(this.c.getClusters()).isEmpty();
    }

    /**
     * Test the toString method.
     */
    @Test
    void testToString() {
        Assertions.assertThat(this.c.toString()).isNotBlank();
    }
}
