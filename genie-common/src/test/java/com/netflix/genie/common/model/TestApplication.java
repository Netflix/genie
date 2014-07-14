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
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * Test the Application class.
 *
 * @author tgianos
 */
public class TestApplication {
    private static final String NAME = "pig";
    private static final String USER = "tgianos";

    /**
     * Test the default Constructor.
     */
    @Test
    public void testDefaultConstructor() {
        final Application a = new Application();
        Assert.assertNull(a.getCommands());
        Assert.assertNull(a.getConfigs());
        Assert.assertNull(a.getEnvPropFile());
        Assert.assertEquals(ApplicationStatus.INACTIVE, a.getStatus());
        Assert.assertNull(a.getJars());
        Assert.assertNull(a.getName());
        Assert.assertNull(a.getUser());
        Assert.assertNull(a.getVersion());
    }

    /**
     * Test the argument Constructor.
     */
    @Test
    public void testConstructor() {
        final Application a = new Application(NAME, USER, ApplicationStatus.ACTIVE);
        Assert.assertNull(a.getCommands());
        Assert.assertNull(a.getConfigs());
        Assert.assertNull(a.getEnvPropFile());
        Assert.assertEquals(ApplicationStatus.ACTIVE, a.getStatus());
        Assert.assertNull(a.getJars());
        Assert.assertEquals(NAME, a.getName());
        Assert.assertEquals(USER, a.getUser());
        Assert.assertNull(a.getVersion());
    }

    /**
     * Test to make sure validation works.
     *
     * @throws GenieException
     */
    @Test
    public void testOnCreateApplication() throws GenieException {
        final Application a = new Application(NAME, USER, ApplicationStatus.ACTIVE);
        a.onCreateApplication();
    }

    /**
     * Test to make sure validation works.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testOnCreateApplicationWithNothing() throws GenieException {
        final Application a = new Application();
        a.onCreateApplication();
    }

    /**
     * Test to make sure validation works when no name entered.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testOnCreateApplicationNoName() throws GenieException {
        final Application a = new Application(null, USER, ApplicationStatus.ACTIVE);
        a.onCreateApplication();
    }

    /**
     * Test to make sure validation works and throws exception when no User entered.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testOnCreateApplicationNoUser() throws GenieException {
        final Application a = new Application(NAME, null, ApplicationStatus.ACTIVE);
        a.onCreateApplication();
    }

    /**
     * Test to make sure validation works and throws exception when no status entered.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testOnCreateApplicationNoStatus() throws GenieException {
        final Application a = new Application(NAME, USER, null);
        a.onCreateApplication();
    }

    /**
     * Make sure validation works on valid apps.
     *
     * @throws GenieException
     */
    @Test
    public void testValidate() throws GenieException {
        final Application a = new Application(NAME, USER, ApplicationStatus.ACTIVE);
        Application.validate(a);
    }

    /**
     * Test to make sure null throws expected exception.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testValidateNull() throws GenieException {
        Application.validate(null);
    }

    /**
     * Test setting the name.
     */
    @Test
    public void testSetName() {
        final Application a = new Application();
        Assert.assertNull(a.getName());
        a.setName(NAME);
        Assert.assertEquals(NAME, a.getName());
    }

    /**
     * Test setting the user.
     */
    @Test
    public void testSetUser() {
        final Application a = new Application();
        Assert.assertNull(a.getUser());
        a.setUser(USER);
        Assert.assertEquals(USER, a.getUser());
    }

    /**
     * Test setting the status.
     */
    @Test
    public void testSetStatus() {
        final Application a = new Application();
        Assert.assertEquals(ApplicationStatus.INACTIVE, a.getStatus());
        a.setStatus(ApplicationStatus.ACTIVE);
        Assert.assertEquals(ApplicationStatus.ACTIVE, a.getStatus());
    }

    /**
     * Test setting the version.
     */
    @Test
    public void testSetVersion() {
        final Application a = new Application();
        Assert.assertNull(a.getVersion());
        final String version = "1.2.3";
        a.setVersion(version);
        Assert.assertEquals(version, a.getVersion());
    }

    /**
     * Test setting the property file.
     */
    @Test
    public void testSetEnvPropFile() {
        final Application a = new Application();
        Assert.assertNull(a.getEnvPropFile());
        final String propFile = "s3://netflix.propFile";
        a.setEnvPropFile(propFile);
        Assert.assertEquals(propFile, a.getEnvPropFile());
    }

    /**
     * Test setting the configs.
     */
    @Test
    public void testSetConfigs() {
        final Application a = new Application();
        Assert.assertNull(a.getConfigs());
        final Set<String> configs = new HashSet<String>();
        configs.add("s3://netflix.configFile");
        a.setConfigs(configs);
        Assert.assertEquals(configs, a.getConfigs());
    }

    /**
     * Test setting the jars.
     */
    @Test
    public void testSetJars() {
        final Application a = new Application();
        Assert.assertNull(a.getJars());
        final Set<String> jars = new HashSet<String>();
        jars.add("s3://netflix/jars/myJar.jar");
        a.setJars(jars);
        Assert.assertEquals(jars, a.getJars());
    }

    /**
     * Test setting the commands.
     */
    @Test
    public void testSetCommands() {
        final Application a = new Application();
        Assert.assertNull(a.getCommands());
        final Set<Command> commands = new HashSet<Command>();
        commands.add(new Command());
        a.setCommands(commands);
        Assert.assertEquals(commands, a.getCommands());
    }
}
