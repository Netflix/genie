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

import com.netflix.genie.common.dto.ApplicationStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.validation.ConstraintViolationException;
import java.util.HashSet;
import java.util.Set;

/**
 * Test the Application class.
 *
 * @author tgianos
 */
public class ApplicationEntityTests extends EntityTestsBase {
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
    }

    /**
     * Test setting the jars.
     */
    @Test
    public void testSetDependencies() {
        Assert.assertNotNull(this.a.getDependencies());
        final Set<String> jars = new HashSet<>();
        jars.add("s3://netflix/jars/myJar.jar");
        this.a.setDependencies(jars);
        Assert.assertEquals(jars, this.a.getDependencies());
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
    }
}
