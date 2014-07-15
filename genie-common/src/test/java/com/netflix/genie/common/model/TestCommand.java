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
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * Test the command class.
 *
 * @author tgianos
 */
public class TestCommand {
    private static final String NAME = "pig13";
    private static final String USER = "tgianos";
    private static final String EXECUTABLE = "/bin/pig13";

    private Command c;

    /**
     * Setup the tests.
     */
    @Before
    public void setup() {
        this.c = new Command();
    }

    /**
     * Test the default Constructor.
     */
    @Test
    public void testDefaultConstructor() {
        Assert.assertNull(this.c.getApplication());
        Assert.assertNull(this.c.getClusters());
        Assert.assertNull(this.c.getConfigs());
        Assert.assertNull(this.c.getEnvPropFile());
        Assert.assertNull(this.c.getExecutable());
        Assert.assertNull(this.c.getJobType());
        Assert.assertNull(this.c.getName());
        Assert.assertEquals(CommandStatus.INACTIVE, this.c.getStatus());
        Assert.assertNull(this.c.getUser());
        Assert.assertNull(this.c.getVersion());
    }

    /**
     * Test the argument Constructor.
     */
    @Test
    public void testConstructor() {
        c = new Command(NAME, USER, CommandStatus.ACTIVE, EXECUTABLE);
        Assert.assertNull(this.c.getApplication());
        Assert.assertNull(this.c.getClusters());
        Assert.assertNull(this.c.getConfigs());
        Assert.assertNull(this.c.getEnvPropFile());
        Assert.assertEquals(EXECUTABLE, this.c.getExecutable());
        Assert.assertNull(this.c.getJobType());
        Assert.assertEquals(NAME, this.c.getName());
        Assert.assertEquals(CommandStatus.ACTIVE, this.c.getStatus());
        Assert.assertEquals(USER, this.c.getUser());
        Assert.assertNull(this.c.getVersion());
    }

    /**
     * Test to make sure validation works.
     *
     * @throws GenieException
     */
    @Test
    public void testOnCreateOrUpdate() throws GenieException {
        this.c = new Command(NAME, USER, CommandStatus.ACTIVE, EXECUTABLE);
        this.c.onCreateOrUpdate();
    }

    /**
     * Test to make sure validation works.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testOnCreateOrUpdateWithNothing() throws GenieException {
        this.c.onCreateOrUpdate();
    }

    /**
     * Test to make sure validation works when no name entered.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testOnCreateOrUpdateNoName() throws GenieException {
        this.c = new Command(null, USER, CommandStatus.ACTIVE, EXECUTABLE);
        this.c.onCreateOrUpdate();
    }

    /**
     * Test to make sure validation works and throws exception when no User entered.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testOnCreateOrUpdateNoUser() throws GenieException {
        this.c = new Command(NAME, null, CommandStatus.ACTIVE, EXECUTABLE);
        this.c.onCreateOrUpdate();
    }

    /**
     * Test to make sure validation works and throws exception when no status entered.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testOnCreateOrUpdateNoStatus() throws GenieException {
        this.c = new Command(NAME, USER, null, EXECUTABLE);
        this.c.onCreateOrUpdate();
    }

    /**
     * Test to make sure validation works and throws exception when no executable
     * entered.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testOnCreateOrUpdateNoExecutable() throws GenieException {
        this.c = new Command(NAME, USER, CommandStatus.ACTIVE, null);
        this.c.onCreateOrUpdate();
    }

    /**
     * Make sure validation works on valid commands.
     *
     * @throws GenieException
     */
    @Test
    public void testValidate() throws GenieException {
        this.c = new Command(NAME, USER, CommandStatus.ACTIVE, EXECUTABLE);
        Command.validate(this.c);
    }

    /**
     * Test to make sure null throws expected exception.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testValidateNull() throws GenieException {
        Command.validate(null);
    }

    /**
     * Test setting the name.
     */
    @Test
    public void testSetName() {
        Assert.assertNull(this.c.getName());
        this.c.setName(NAME);
        Assert.assertEquals(NAME, this.c.getName());
    }

    /**
     * Test setting the user.
     */
    @Test
    public void testSetUser() {
        Assert.assertNull(this.c.getUser());
        this.c.setUser(USER);
        Assert.assertEquals(USER, this.c.getUser());
    }

    /**
     * Test setting the status.
     */
    @Test
    public void testSetStatus() {
        Assert.assertEquals(CommandStatus.INACTIVE, this.c.getStatus());
        this.c.setStatus(CommandStatus.ACTIVE);
        Assert.assertEquals(CommandStatus.ACTIVE, this.c.getStatus());
    }

    /**
     * Test setting the version.
     */
    @Test
    public void testSetVersion() {
        Assert.assertNull(this.c.getVersion());
        final String version = "1.2.3";
        this.c.setVersion(version);
        Assert.assertEquals(version, this.c.getVersion());
    }

    /**
     * Test setting the property file.
     */
    @Test
    public void testSetEnvPropFile() {
        Assert.assertNull(this.c.getEnvPropFile());
        final String propFile = "s3://netflix.propFile";
        this.c.setEnvPropFile(propFile);
        Assert.assertEquals(propFile, this.c.getEnvPropFile());
    }

    /**
     * Test setting the job type.
     */
    @Test
    public void testSetJobType() {
        Assert.assertNull(this.c.getJobType());
        final String jobType = "pig";
        this.c.setJobType(jobType);
        Assert.assertEquals(jobType, this.c.getJobType());
    }

    /**
     * Test setting the executable.
     */
    @Test
    public void testSetExecutable() {
        Assert.assertNull(this.c.getExecutable());
        this.c.setExecutable(EXECUTABLE);
        Assert.assertEquals(EXECUTABLE, this.c.getExecutable());
    }

    /**
     * Test setting the configs.
     */
    @Test
    public void testSetConfigs() {
        Assert.assertNull(this.c.getConfigs());
        final Set<String> configs = new HashSet<String>();
        configs.add("s3://netflix.configFile");
        this.c.setConfigs(configs);
        Assert.assertEquals(configs, this.c.getConfigs());
    }

    /**
     * Test setting an application.
     *
     * @throws GenieException
     */
    @Test
    public void testSetApplication() throws GenieException {
        Assert.assertNull(this.c.getApplication());
        final Application one = new Application();
        one.setId("one");
        final Application two = new Application();
        two.setId("two");
        this.c.setApplication(one);
        Assert.assertEquals(one, this.c.getApplication());
        Assert.assertTrue(one.getCommands().contains(this.c));
        this.c.setApplication(two);
        Assert.assertEquals(two, this.c.getApplication());
        Assert.assertFalse(one.getCommands().contains(this.c));
        Assert.assertTrue(two.getCommands().contains(this.c));
        this.c.setApplication(null);
        Assert.assertNull(this.c.getApplication());
        Assert.assertTrue(one.getCommands().isEmpty());
        Assert.assertTrue(two.getCommands().isEmpty());
    }

    /**
     * Test setting the clusters.
     */
    @Test
    public void testSetClusters() {
        Assert.assertNull(this.c.getClusters());
        final Set<Cluster> clusters = new HashSet<Cluster>();
        clusters.add(new Cluster());
        this.c.setClusters(clusters);
        Assert.assertEquals(clusters, this.c.getClusters());
    }
}
