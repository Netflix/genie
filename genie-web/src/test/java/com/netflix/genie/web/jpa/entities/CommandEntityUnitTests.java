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
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.genie.test.suppliers.RandomSuppliers;
import org.apache.commons.lang3.StringUtils;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import javax.validation.ConstraintViolationException;
import java.util.ArrayList;
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
    private static final List<String> EXECUTABLE = Lists.newArrayList("/bin/pig13", "-Dblah");
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
        Assert.assertThat(entity.getExecutable(), Matchers.empty());
        Assert.assertThat(entity.getCheckDelay(), Matchers.is(Command.DEFAULT_CHECK_DELAY));
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
        Assert.assertNotNull(entity.getClusters());
        Assert.assertTrue(entity.getClusters().isEmpty());
        Assert.assertNotNull(entity.getApplications());
        Assert.assertTrue(entity.getApplications().isEmpty());
        Assert.assertFalse(entity.getMemory().isPresent());
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
    public void testValidateEmptyExecutable() {
        this.c.setExecutable(Lists.newArrayList());
        this.validate(this.c);
    }

    /**
     * Make sure validation works on with failure from command.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateBlankExecutable() {
        this.c.setExecutable(Lists.newArrayList("    "));
        this.validate(this.c);
    }

    /**
     * Make sure validation works on with failure from command.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateExecutableArgumentTooLong() {
        this.c.setExecutable(Lists.newArrayList(StringUtils.leftPad("", 1025, 'e')));
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
        final String setupFile = "s3://netflix.propFile";
        final FileEntity setupFileEntity = new FileEntity();
        setupFileEntity.setFile(setupFile);
        this.c.setSetupFile(setupFileEntity);
        Assert.assertEquals(setupFileEntity, this.c.getSetupFile().orElseThrow(IllegalArgumentException::new));
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
        final FileEntity config = new FileEntity();
        config.setFile("s3://netflix.configFile");
        final Set<FileEntity> configs = Sets.newHashSet(config);
        this.c.setConfigs(configs);
        Assert.assertEquals(configs, this.c.getConfigs());

        this.c.setConfigs(null);
        Assert.assertThat(this.c.getConfigs(), Matchers.empty());
    }

    /**
     * Test setting the dependencies.
     */
    @Test
    public void testSetDependencies() {
        Assert.assertNotNull(this.c.getDependencies());
        Assert.assertTrue(this.c.getDependencies().isEmpty());
        final FileEntity dependency = new FileEntity();
        dependency.setFile("dep1");
        final Set<FileEntity> dependencies = Sets.newHashSet(dependency);
        this.c.setDependencies(dependencies);
        Assert.assertEquals(dependencies, this.c.getDependencies());

        this.c.setDependencies(null);
        Assert.assertThat(this.c.getDependencies(), Matchers.empty());
    }

    /**
     * Test setting the tags.
     */
    @Test
    public void testSetTags() {
        Assert.assertNotNull(this.c.getTags());
        Assert.assertTrue(this.c.getTags().isEmpty());
        final TagEntity one = new TagEntity();
        one.setTag("tag1");
        final TagEntity two = new TagEntity();
        two.setTag("tag2");
        final Set<TagEntity> tags = Sets.newHashSet(one, two);
        this.c.setTags(tags);
        Assert.assertEquals(tags, this.c.getTags());

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
        one.setUniqueId("one");
        final ApplicationEntity two = new ApplicationEntity();
        two.setUniqueId("two");
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
        Mockito.when(one.getUniqueId()).thenReturn(UUID.randomUUID().toString());
        final ApplicationEntity two = Mockito.mock(ApplicationEntity.class);
        Mockito.when(two.getUniqueId()).thenReturn(UUID.randomUUID().toString());

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
        app.setUniqueId(id);

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
        app.setUniqueId(id);

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
        one.setUniqueId("one");
        final ApplicationEntity two = new ApplicationEntity();
        two.setUniqueId("two");
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
     * Test the toString method.
     */
    @Test
    public void testToString() {
        Assert.assertNotNull(this.c.toString());
    }
}
