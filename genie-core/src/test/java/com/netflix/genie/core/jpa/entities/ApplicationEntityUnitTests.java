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

import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.ApplicationStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.validation.ConstraintViolationException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * Test the Application class.
 *
 * @author tgianos
 */
@Category(UnitTest.class)
public class ApplicationEntityUnitTests extends EntityTestsBase {
    private static final String NAME = "pig";
    private static final String USER = "tgianos";
    private static final String VERSION = "1.0";

    private ApplicationEntity a;

    /**
     * Setup the tests.
     */
    @Before
    public void setup() {
        this.a = new ApplicationEntity();
    }

    /**
     * Test the default Constructor.
     */
    @Test
    public void testDefaultConstructor() {
        Assert.assertNull(this.a.getSetupFile());
        Assert.assertNull(this.a.getStatus());
        Assert.assertNull(this.a.getName());
        Assert.assertNull(this.a.getUser());
        Assert.assertNull(this.a.getVersion());
        Assert.assertNotNull(this.a.getDependencies());
        Assert.assertTrue(this.a.getDependencies().isEmpty());
        Assert.assertNotNull(this.a.getConfigs());
        Assert.assertTrue(this.a.getConfigs().isEmpty());
        Assert.assertNotNull(this.a.getTags());
        Assert.assertTrue(this.a.getTags().isEmpty());
        Assert.assertNotNull(this.a.getCommands());
        Assert.assertTrue(this.a.getCommands().isEmpty());
    }

    /**
     * Test the argument Constructor.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testConstructor() throws GeniePreconditionException {
        this.a = new ApplicationEntity(NAME, USER, VERSION, ApplicationStatus.ACTIVE);
        Assert.assertNull(this.a.getSetupFile());
        Assert.assertEquals(ApplicationStatus.ACTIVE, this.a.getStatus());
        Assert.assertEquals(NAME, this.a.getName());
        Assert.assertEquals(USER, this.a.getUser());
        Assert.assertEquals(VERSION, this.a.getVersion());
        Assert.assertNotNull(this.a.getDependencies());
        Assert.assertTrue(this.a.getDependencies().isEmpty());
        Assert.assertNotNull(this.a.getConfigs());
        Assert.assertTrue(this.a.getConfigs().isEmpty());
        Assert.assertNotNull(this.a.getTags());
        Assert.assertTrue(this.a.getTags().isEmpty());
        Assert.assertNotNull(this.a.getCommands());
        Assert.assertTrue(this.a.getCommands().isEmpty());
    }

    /**
     * Test to make sure validation works.
     *
     * @throws GenieException If any precondition isn't met.
     */
    @Test
    public void testOnCreateOrUpdateApplication() throws GenieException {
        this.a = new ApplicationEntity(NAME, USER, VERSION, ApplicationStatus.ACTIVE);
        Assert.assertNotNull(this.a.getTags());
        this.a.onCreateOrUpdateApplication();
        Assert.assertEquals(2, this.a.getTags().size());
    }

    /**
     * Make sure validation works on valid apps.
     */
    @Test
    public void testValidate() {
        this.a = new ApplicationEntity(NAME, USER, VERSION, ApplicationStatus.ACTIVE);
        this.validate(this.a);
    }

    /**
     * Make sure validation works on with failure from super class.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateNoName() {
        this.a = new ApplicationEntity(null, USER, VERSION, ApplicationStatus.ACTIVE);
        this.validate(this.a);
    }

    /**
     * Make sure validation works on with failure from super class.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateNoUser() {
        this.a = new ApplicationEntity(NAME, "", VERSION, ApplicationStatus.ACTIVE);
        this.validate(this.a);
    }

    /**
     * Make sure validation works on with failure from super class.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateNoVersion() {
        this.a = new ApplicationEntity(NAME, USER, " ", ApplicationStatus.ACTIVE);
        this.validate(this.a);
    }

    /**
     * Make sure validation works on with failure from application.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateNoStatus() {
        this.a = new ApplicationEntity(NAME, USER, VERSION, null);
        this.validate(this.a);
    }

    /**
     * Test setting the status.
     */
    @Test
    public void testSetStatus() {
        Assert.assertNull(this.a.getStatus());
        this.a.setStatus(ApplicationStatus.ACTIVE);
        Assert.assertEquals(ApplicationStatus.ACTIVE, this.a.getStatus());
    }

    /**
     * Test setting the setup file.
     */
    @Test
    public void testSetSetupFile() {
        Assert.assertNull(this.a.getSetupFile());
        final String propFile = "s3://netflix.propFile";
        this.a.setSetupFile(propFile);
        Assert.assertEquals(propFile, this.a.getSetupFile());
    }

    /**
     * Test setting the configs.
     */
    @Test
    public void testSetConfigs() {
        Assert.assertNotNull(this.a.getConfigs());
        final Set<String> configs = new HashSet<>();
        configs.add("s3://netflix.configFile");
        this.a.setConfigs(configs);
        Assert.assertEquals(configs, this.a.getConfigs());

        this.a.setConfigs(null);
        Assert.assertThat(this.a.getConfigs(), Matchers.empty());
    }

    /**
     * Test setting the jars.
     */
    @Test
    public void testSetDependencies() {
        Assert.assertNotNull(this.a.getDependencies());
        final Set<String> dependencies = new HashSet<>();
        dependencies.add("s3://netflix/jars/myJar.jar");
        this.a.setDependencies(dependencies);
        Assert.assertEquals(dependencies, this.a.getDependencies());

        this.a.setDependencies(null);
        Assert.assertThat(this.a.getDependencies(), Matchers.empty());
    }

    /**
     * Test setting the tags.
     */
    @Test
    public void testSetTags() {
        Assert.assertNotNull(this.a.getTags());
        final Set<String> tags = new HashSet<>();
        tags.add("tag1");
        tags.add("tag2");
        this.a.setTags(tags);
        Assert.assertEquals(tags, this.a.getTags());

        this.a.setTags(null);
        Assert.assertThat(this.a.getTags(), Matchers.empty());
    }

    /**
     * Test setting the commands.
     */
    @Test
    public void testSetCommands() {
        Assert.assertNotNull(this.a.getCommands());
        final Set<CommandEntity> commandEntities = new HashSet<>();
        commandEntities.add(new CommandEntity());
        this.a.setCommands(commandEntities);
        Assert.assertEquals(commandEntities, this.a.getCommands());

        this.a.setCommands(null);
        Assert.assertThat(this.a.getCommands(), Matchers.empty());
    }

    /**
     * Make sure can properly set the application tags using public api.
     */
    @Test
    public void canSetApplicationTags() {
        final Set<String> tags = Sets.newHashSet("Third", "first", "second");
        this.a.setApplicationTags(tags);
        Assert.assertThat(this.a.getApplicationTags(), Matchers.is(tags));
        Assert.assertThat(this.a.getTags(), Matchers.is(tags));

        this.a.setApplicationTags(null);
        Assert.assertThat(this.a.getApplicationTags(), Matchers.empty());
        Assert.assertThat(this.a.getTags(), Matchers.empty());
    }

    /**
     * Test to make sure can properly generate a DTO from the entity.
     *
     * @throws GenieException on error
     */
    @Test
    public void canGetDTO() throws GenieException {
        final ApplicationEntity entity = new ApplicationEntity();
        final String name = UUID.randomUUID().toString();
        entity.setName(name);
        final String user = UUID.randomUUID().toString();
        entity.setUser(user);
        final String version = UUID.randomUUID().toString();
        entity.setVersion(version);
        final String id = UUID.randomUUID().toString();
        entity.setId(id);
        final Date created = entity.getCreated();
        final Date updated = entity.getUpdated();
        final String description = UUID.randomUUID().toString();
        entity.setDescription(description);
        final Set<String> tags = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        entity.setApplicationTags(tags);
        final Set<String> configs = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        entity.setConfigs(configs);
        final String setupFile = UUID.randomUUID().toString();
        entity.setSetupFile(setupFile);
        final Set<String> dependencies = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        entity.setDependencies(dependencies);
        entity.setStatus(ApplicationStatus.ACTIVE);

        final Application application = entity.getDTO();
        Assert.assertThat(application.getId(), Matchers.is(id));
        Assert.assertThat(application.getName(), Matchers.is(name));
        Assert.assertThat(application.getUser(), Matchers.is(user));
        Assert.assertThat(application.getVersion(), Matchers.is(version));
        Assert.assertThat(application.getCreated(), Matchers.is(created));
        Assert.assertThat(application.getUpdated(), Matchers.is(updated));
        Assert.assertThat(application.getDescription(), Matchers.is(description));
        Assert.assertThat(application.getTags(), Matchers.is(tags));
        Assert.assertThat(application.getConfigs(), Matchers.is(configs));
        Assert.assertThat(application.getSetupFile(), Matchers.is(setupFile));
        Assert.assertThat(application.getDependencies(), Matchers.is(dependencies));
        Assert.assertThat(application.getStatus(), Matchers.is(ApplicationStatus.ACTIVE));
    }
}
