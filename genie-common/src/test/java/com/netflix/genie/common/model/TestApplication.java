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
package com.netflix.genie.common.model;

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
public class TestApplication extends TestEntityBase {
    private static final String NAME = "pig";
    private static final String USER = "tgianos";
    private static final String VERSION = "1.0";

    private Application a;

    /**
     * Setup the tests.
     */
    @Before
    public void setup() {
        this.a = new Application();
    }

    /**
     * Test the default Constructor.
     */
    @Test
    public void testDefaultConstructor() {
        Assert.assertNull(this.a.getCommands());
        Assert.assertNull(this.a.getConfigs());
        Assert.assertNull(this.a.getEnvPropFile());
        Assert.assertNull(this.a.getStatus());
        Assert.assertNull(this.a.getJars());
        Assert.assertNull(this.a.getName());
        Assert.assertNull(this.a.getUser());
        Assert.assertNull(this.a.getVersion());
    }

    /**
     * Test the argument Constructor.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testConstructor() throws GeniePreconditionException {
        this.a = new Application(NAME, USER, VERSION, ApplicationStatus.ACTIVE);
        Assert.assertNull(this.a.getCommands());
        Assert.assertNull(this.a.getConfigs());
        Assert.assertNull(this.a.getEnvPropFile());
        Assert.assertEquals(ApplicationStatus.ACTIVE, this.a.getStatus());
        Assert.assertNull(this.a.getJars());
        Assert.assertEquals(NAME, this.a.getName());
        Assert.assertEquals(USER, this.a.getUser());
        Assert.assertEquals(VERSION, this.a.getVersion());
    }

    /**
     * Test to make sure validation works.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testOnCreateOrUpdateApplication() throws GeniePreconditionException {
        this.a = new Application(NAME, USER, VERSION, ApplicationStatus.ACTIVE);
        Assert.assertNull(this.a.getTags());
        this.a.onCreateOrUpdateApplication();
        Assert.assertEquals(2, this.a.getTags().size());
    }

    /**
     * Make sure validation works on valid apps.
     */
    @Test
    public void testValidate() {
        this.a = new Application(NAME, USER, VERSION, ApplicationStatus.ACTIVE);
        this.validate(this.a);
    }

    /**
     * Make sure validation works on with failure from super class.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateNoName() {
        this.a = new Application(null, USER, VERSION, ApplicationStatus.ACTIVE);
        this.validate(this.a);
    }

    /**
     * Make sure validation works on with failure from super class.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateNoUser() {
        this.a = new Application(NAME, "", VERSION, ApplicationStatus.ACTIVE);
        this.validate(this.a);
    }

    /**
     * Make sure validation works on with failure from super class.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateNoVersion() {
        this.a = new Application(NAME, USER, " ", ApplicationStatus.ACTIVE);
        this.validate(this.a);
    }

    /**
     * Make sure validation works on with failure from application.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateNoStatus() {
        this.a = new Application(NAME, USER, VERSION, null);
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
     * Test setting the property file.
     */
    @Test
    public void testSetEnvPropFile() {
        Assert.assertNull(this.a.getEnvPropFile());
        final String propFile = "s3://netflix.propFile";
        this.a.setEnvPropFile(propFile);
        Assert.assertEquals(propFile, this.a.getEnvPropFile());
    }

    /**
     * Test setting the configs.
     */
    @Test
    public void testSetConfigs() {
        Assert.assertNull(this.a.getConfigs());
        final Set<String> configs = new HashSet<>();
        configs.add("s3://netflix.configFile");
        this.a.setConfigs(configs);
        Assert.assertEquals(configs, this.a.getConfigs());
    }

    /**
     * Test setting the jars.
     */
    @Test
    public void testSetJars() {
        Assert.assertNull(this.a.getJars());
        final Set<String> jars = new HashSet<>();
        jars.add("s3://netflix/jars/myJar.jar");
        this.a.setJars(jars);
        Assert.assertEquals(jars, this.a.getJars());
    }

    /**
     * Test setting the tags.
     */
    @Test
    public void testSetTags() {
        Assert.assertNull(this.a.getTags());
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
        Assert.assertNull(this.a.getCommands());
        final Set<Command> commands = new HashSet<>();
        commands.add(new Command());
        this.a.setCommands(commands);
        Assert.assertEquals(commands, this.a.getCommands());
    }
}
