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
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.genie.test.suppliers.RandomSuppliers;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import javax.validation.ConstraintViolationException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Test the command class.
 *
 * @author tgianos
 */
@Category(UnitTest.class)
public class CommandEntityUnitTests extends EntityTestsBase {
    private static final String NAME = "pig13";
    private static final String USER = "tgianos";
    private static final String EXECUTABLE = "/bin/pig13";
    private static final String VERSION = "1.0";
    private static final long CHECK_DELAY = 18083L;
    private static final int MEMORY = 10_240;

    private CommandEntity c;

    /**
     * Setup the tests.
     */
    @Before
    public void setup() {
        this.c = new CommandEntity();
        this.c.setName(NAME);
        this.c.setUser(USER);
        this.c.setVersion(VERSION);
        this.c.setStatus(CommandStatus.ACTIVE);
        this.c.setExecutable(EXECUTABLE);
        this.c.setCheckDelay(CHECK_DELAY);
        this.c.setMemory(MEMORY);
    }

    /**
     * Test the default Constructor.
     */
    @Test
    public void testDefaultConstructor() {
        final CommandEntity entity = new CommandEntity();
        Assert.assertFalse(entity.getSetupFile().isPresent());
        Assert.assertNull(entity.getExecutable());
        Assert.assertThat(entity.getCheckDelay(), Matchers.is(Command.DEFAULT_CHECK_DELAY));
        Assert.assertNull(entity.getName());
        Assert.assertNull(entity.getStatus());
        Assert.assertNull(entity.getUser());
        Assert.assertNull(entity.getVersion());
        Assert.assertNotNull(entity.getConfigs());
        Assert.assertTrue(entity.getConfigs().isEmpty());
        Assert.assertNotNull(entity.getTags());
        Assert.assertTrue(entity.getTags().isEmpty());
        Assert.assertNotNull(entity.getClusters());
        Assert.assertTrue(entity.getClusters().isEmpty());
        Assert.assertNotNull(entity.getApplications());
        Assert.assertTrue(entity.getApplications().isEmpty());
        Assert.assertFalse(entity.getMemory().isPresent());
    }

    /**
     * Test to make sure validation works.
     *
     * @throws GenieException If any precondition isn't met.
     */
    @Test
    public void testOnCreateOrUpdateCommand() throws GenieException {
        Assert.assertNotNull(this.c.getTags());
        Assert.assertTrue(this.c.getTags().isEmpty());
        this.c.onCreateOrUpdateCommand();
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
        this.c.setUser("   ");
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
     * Make sure validation works on with failure from command.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateNoStatus() {
        this.c.setStatus(null);
        this.validate(this.c);
    }

    /**
     * Make sure validation works on with failure from command.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateNoExecutable() {
        this.c.setExecutable("    ");
        this.validate(this.c);
    }

    /**
     * Make sure validation works on with failure from command.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateBadCheckDelay() {
        this.c.setCheckDelay(0L);
        this.validate(this.c);
    }

    /**
     * Test setting the status.
     */
    @Test
    public void testSetStatus() {
        this.c.setStatus(CommandStatus.ACTIVE);
        Assert.assertEquals(CommandStatus.ACTIVE, this.c.getStatus());
    }

    /**
     * Test setting the setup file.
     */
    @Test
    public void testSetSetupFile() {
        Assert.assertFalse(this.c.getSetupFile().isPresent());
        final String propFile = "s3://netflix.propFile";
        this.c.setSetupFile(propFile);
        Assert.assertEquals(propFile, this.c.getSetupFile().orElseThrow(IllegalArgumentException::new));
    }

    /**
     * Test setting the executable.
     */
    @Test
    public void testSetExecutable() {
        this.c.setExecutable(EXECUTABLE);
        Assert.assertEquals(EXECUTABLE, this.c.getExecutable());
    }

    /**
     * Make sure the check delay setter and getter works properly.
     */
    @Test
    public void testSetCheckDelay() {
        final long newDelay = 108327L;
        Assert.assertThat(this.c.getCheckDelay(), Matchers.is(CHECK_DELAY));
        this.c.setCheckDelay(newDelay);
        Assert.assertThat(this.c.getCheckDelay(), Matchers.is(newDelay));
    }

    /**
     * Make sure can set the memory for the command if a user desires it.
     */
    @Test
    public void testSetMemory() {
        Assert.assertThat(this.c.getMemory().orElseGet(RandomSuppliers.INT), Matchers.is(MEMORY));
        final int newMemory = MEMORY + 1;
        this.c.setMemory(newMemory);
        Assert.assertThat(this.c.getMemory().orElseGet(RandomSuppliers.INT), Matchers.is(newMemory));
    }

    /**
     * Test setting the configs.
     */
    @Test
    public void testSetConfigs() {
        Assert.assertNotNull(this.c.getConfigs());
        Assert.assertTrue(this.c.getConfigs().isEmpty());
        final Set<String> configs = Sets.newHashSet("s3://netflix.configFile");
        this.c.setConfigs(configs);
        Assert.assertEquals(configs, this.c.getConfigs());

        this.c.setConfigs(null);
        Assert.assertThat(this.c.getConfigs(), Matchers.empty());
    }

    /**
     * Test setting the tags.
     */
    @Test
    public void testSetTags() {
        Assert.assertNotNull(this.c.getTags());
        Assert.assertTrue(this.c.getTags().isEmpty());
        final Set<String> tags = Sets.newHashSet("tag1", "tag2");
        this.c.setTags(tags);
        Assert.assertEquals(tags, this.c.getTags());

        this.c.setTags(null);
        Assert.assertThat(this.c.getTags(), Matchers.empty());
    }

    /**
     * Test to make sure we can set the command tags.
     */
    @Test
    public void canSetCommandTags() {
        final Set<String> tags = Sets.newHashSet("Third", "first", "second");
        this.c.setTags(tags);
        Assert.assertThat(this.c.getTags(), Matchers.is(tags));

        this.c.setTags(null);
        Assert.assertThat(this.c.getTags(), Matchers.empty());
    }

    /**
     * Test setting applications.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testSetApplications() throws GeniePreconditionException {
        Assert.assertNotNull(this.c.getApplications());
        Assert.assertTrue(this.c.getApplications().isEmpty());
        final List<ApplicationEntity> applicationEntities = new ArrayList<>();
        final ApplicationEntity one = new ApplicationEntity();
        one.setId("one");
        final ApplicationEntity two = new ApplicationEntity();
        two.setId("two");
        applicationEntities.add(one);
        applicationEntities.add(two);
        this.c.setApplications(applicationEntities);
        Assert.assertEquals(2, this.c.getApplications().size());
        Assert.assertTrue(this.c.getApplications().get(0).equals(one));
        Assert.assertTrue(this.c.getApplications().get(1).equals(two));
        Assert.assertTrue(one.getCommands().contains(this.c));
        Assert.assertTrue(two.getCommands().contains(this.c));

        applicationEntities.clear();
        applicationEntities.add(two);
        this.c.setApplications(applicationEntities);
        Assert.assertEquals(1, this.c.getApplications().size());
        Assert.assertTrue(this.c.getApplications().get(0).equals(two));
        Assert.assertFalse(one.getCommands().contains(this.c));
        Assert.assertTrue(two.getCommands().contains(this.c));
        this.c.setApplications(null);
        Assert.assertTrue(this.c.getApplications().isEmpty());
        Assert.assertTrue(one.getCommands().isEmpty());
        Assert.assertTrue(two.getCommands().isEmpty());
    }

    /**
     * Make sure if a List with duplicate applications is sent in it fails.
     *
     * @throws GeniePreconditionException on duplicate applications
     */
    @Test(expected = GeniePreconditionException.class)
    public void cantSetApplicationsIfDuplicates() throws GeniePreconditionException {
        final ApplicationEntity one = Mockito.mock(ApplicationEntity.class);
        Mockito.when(one.getId()).thenReturn(UUID.randomUUID().toString());
        final ApplicationEntity two = Mockito.mock(ApplicationEntity.class);
        Mockito.when(two.getId()).thenReturn(UUID.randomUUID().toString());

        this.c.setApplications(Lists.newArrayList(one, two, one));
    }

    /**
     * Test to make sure we can add an application.
     *
     * @throws GeniePreconditionException On error
     */
    @Test
    public void canAddApplication() throws GeniePreconditionException {
        final String id = UUID.randomUUID().toString();
        final ApplicationEntity app = new ApplicationEntity();
        app.setId(id);

        this.c.addApplication(app);
        Assert.assertThat(this.c.getApplications(), Matchers.hasItem(app));
        Assert.assertThat(app.getCommands(), Matchers.hasItem(this.c));
    }

    /**
     * Test to make sure we can't add an application to a command if it's already in the list.
     *
     * @throws GeniePreconditionException on duplicate
     */
    @Test(expected = GeniePreconditionException.class)
    public void cantAddApplicationThatAlreadyIsInList() throws GeniePreconditionException {
        final String id = UUID.randomUUID().toString();
        final ApplicationEntity app = new ApplicationEntity();
        app.setId(id);

        this.c.addApplication(app);
        this.c.addApplication(app);
    }

    /**
     * Test removing an application.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void canRemoveApplication() throws GeniePreconditionException {
        final ApplicationEntity one = new ApplicationEntity();
        one.setId("one");
        final ApplicationEntity two = new ApplicationEntity();
        two.setId("two");
        Assert.assertNotNull(this.c.getApplications());
        Assert.assertTrue(this.c.getApplications().isEmpty());
        this.c.addApplication(one);
        Assert.assertTrue(this.c.getApplications().contains(one));
        Assert.assertFalse(this.c.getApplications().contains(two));
        Assert.assertTrue(one.getCommands().contains(this.c));
        Assert.assertNotNull(two.getCommands());
        Assert.assertTrue(two.getCommands().isEmpty());
        this.c.addApplication(two);
        Assert.assertTrue(this.c.getApplications().contains(one));
        Assert.assertTrue(this.c.getApplications().contains(two));
        Assert.assertTrue(one.getCommands().contains(this.c));
        Assert.assertTrue(two.getCommands().contains(this.c));

        this.c.removeApplication(one);
        Assert.assertFalse(this.c.getApplications().contains(one));
        Assert.assertTrue(this.c.getApplications().contains(two));
        Assert.assertFalse(one.getCommands().contains(this.c));
        Assert.assertTrue(two.getCommands().contains(this.c));
    }

    /**
     * Test setting the clusters.
     */
    @Test
    public void testSetClusters() {
        Assert.assertNotNull(this.c.getClusters());
        Assert.assertTrue(this.c.getClusters().isEmpty());
        final Set<ClusterEntity> clusterEntities = Sets.newHashSet(new ClusterEntity());
        this.c.setClusters(clusterEntities);
        Assert.assertEquals(clusterEntities, this.c.getClusters());

        this.c.setClusters(null);
        Assert.assertThat(this.c.getClusters(), Matchers.empty());
    }

    /**
     * Make sure we can get a valid Command DTO.
     *
     * @throws GenieException on error
     */
    @Test
    public void canGetDTO() throws GenieException {
        final CommandEntity entity = new CommandEntity();
        final String id = UUID.randomUUID().toString();
        entity.setId(id);
        final String name = UUID.randomUUID().toString();
        entity.setName(name);
        final String user = UUID.randomUUID().toString();
        entity.setUser(user);
        final String version = UUID.randomUUID().toString();
        entity.setVersion(version);
        final String description = UUID.randomUUID().toString();
        entity.setDescription(description);
        final Date created = entity.getCreated();
        final Date updated = entity.getUpdated();
        entity.setStatus(CommandStatus.DEPRECATED);
        final Set<String> tags = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        entity.setTags(tags);
        final Set<String> configs = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        entity.setConfigs(configs);
        final String setupFile = UUID.randomUUID().toString();
        entity.setSetupFile(setupFile);
        final String executable = UUID.randomUUID().toString();
        entity.setExecutable(executable);
        final long checkDelay = 2180234L;
        entity.setCheckDelay(checkDelay);
        final int memory = 10_241;
        entity.setMemory(memory);

        final Command command = entity.getDTO();
        Assert.assertThat(command.getId().orElseGet(RandomSuppliers.STRING), Matchers.is(id));
        Assert.assertThat(command.getName(), Matchers.is(name));
        Assert.assertThat(command.getUser(), Matchers.is(user));
        Assert.assertThat(command.getVersion(), Matchers.is(version));
        Assert.assertThat(command.getStatus(), Matchers.is(CommandStatus.DEPRECATED));
        Assert.assertThat(command.getDescription().orElseGet(RandomSuppliers.STRING), Matchers.is(description));
        Assert.assertThat(command.getCreated().orElseGet(RandomSuppliers.DATE), Matchers.is(created));
        Assert.assertThat(command.getUpdated().orElseGet(RandomSuppliers.DATE), Matchers.is(updated));
        Assert.assertThat(command.getExecutable(), Matchers.is(executable));
        Assert.assertThat(command.getCheckDelay(), Matchers.is(checkDelay));
        Assert.assertThat(command.getTags(), Matchers.is(tags));
        Assert.assertThat(command.getSetupFile().orElseGet(RandomSuppliers.STRING), Matchers.is(setupFile));
        Assert.assertThat(command.getConfigs(), Matchers.is(configs));
        Assert.assertThat(command.getMemory().orElseGet(RandomSuppliers.INT), Matchers.is(memory));
    }
}
