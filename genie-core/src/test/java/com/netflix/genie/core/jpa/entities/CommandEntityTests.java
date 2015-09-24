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

import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.validation.ConstraintViolationException;
import java.util.HashSet;
import java.util.Set;

/**
 * Test the command class.
 *
 * @author tgianos
 */
public class CommandEntityTests extends EntityTestsBase {
    private static final String NAME = "pig13";
    private static final String USER = "tgianos";
    private static final String EXECUTABLE = "/bin/pig13";
    private static final String VERSION = "1.0";

    private CommandEntity c;

    /**
     * Setup the tests.
     */
    @Before
    public void setup() {
        this.c = new CommandEntity();
    }

    /**
     * Test the default Constructor.
     */
    @Test
    public void testDefaultConstructor() {
        Assert.assertNull(this.c.getSetupFile());
        Assert.assertNull(this.c.getExecutable());
        Assert.assertNull(this.c.getJobType());
        Assert.assertNull(this.c.getName());
        Assert.assertNull(this.c.getStatus());
        Assert.assertNull(this.c.getUser());
        Assert.assertNull(this.c.getVersion());
        Assert.assertNotNull(this.c.getConfigs());
        Assert.assertTrue(this.c.getConfigs().isEmpty());
        Assert.assertNotNull(this.c.getTags());
        Assert.assertTrue(this.c.getTags().isEmpty());
        Assert.assertNotNull(this.c.getClusters());
        Assert.assertTrue(this.c.getClusters().isEmpty());
        Assert.assertNotNull(this.c.getApplications());
        Assert.assertTrue(this.c.getApplications().isEmpty());
    }

    /**
     * Test the argument Constructor.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testConstructor() throws GeniePreconditionException {
        c = new CommandEntity(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE);
        Assert.assertNull(this.c.getSetupFile());
        Assert.assertEquals(EXECUTABLE, this.c.getExecutable());
        Assert.assertNull(this.c.getJobType());
        Assert.assertEquals(NAME, this.c.getName());
        Assert.assertEquals(CommandStatus.ACTIVE, this.c.getStatus());
        Assert.assertEquals(USER, this.c.getUser());
        Assert.assertEquals(VERSION, this.c.getVersion());
        Assert.assertNotNull(this.c.getConfigs());
        Assert.assertTrue(this.c.getConfigs().isEmpty());
        Assert.assertNotNull(this.c.getTags());
        Assert.assertTrue(this.c.getTags().isEmpty());
        Assert.assertNotNull(this.c.getClusters());
        Assert.assertTrue(this.c.getClusters().isEmpty());
        Assert.assertNotNull(this.c.getApplications());
        Assert.assertTrue(this.c.getApplications().isEmpty());
    }

    /**
     * Test to make sure validation works.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testOnCreateOrUpdateCommand() throws GeniePreconditionException {
        this.c = new CommandEntity(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE);
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
        this.c = new CommandEntity(NAME, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE);
        this.validate(this.c);
    }

    /**
     * Make sure validation works on with failure from super class.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateNoName() {
        this.c = new CommandEntity(null, USER, VERSION, CommandStatus.ACTIVE, EXECUTABLE);
        this.validate(this.c);
    }

    /**
     * Make sure validation works on with failure from super class.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateNoUser() {
        this.c = new CommandEntity(NAME, "   ", VERSION, CommandStatus.ACTIVE, EXECUTABLE);
        this.validate(this.c);
    }

    /**
     * Make sure validation works on with failure from super class.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateNoVersion() {
        this.c = new CommandEntity(NAME, USER, "", CommandStatus.ACTIVE, EXECUTABLE);
        this.validate(this.c);
    }

    /**
     * Make sure validation works on with failure from command.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateNoStatus() {
        this.c = new CommandEntity(NAME, USER, VERSION, null, EXECUTABLE);
        this.validate(this.c);
    }

    /**
     * Make sure validation works on with failure from command.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateNoExecutable() {
        this.c = new CommandEntity(NAME, USER, VERSION, CommandStatus.ACTIVE, "    ");
        this.validate(this.c);
    }

    /**
     * Test setting the status.
     */
    @Test
    public void testSetStatus() {
        Assert.assertNull(this.c.getStatus());
        this.c.setStatus(CommandStatus.ACTIVE);
        Assert.assertEquals(CommandStatus.ACTIVE, this.c.getStatus());
    }

    /**
     * Test setting the property file.
     */
    @Test
    public void testSetEnvPropFile() {
        Assert.assertNull(this.c.getSetupFile());
        final String propFile = "s3://netflix.propFile";
        this.c.setSetupFile(propFile);
        Assert.assertEquals(propFile, this.c.getSetupFile());
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
        Assert.assertNotNull(this.c.getConfigs());
        Assert.assertTrue(this.c.getConfigs().isEmpty());
        final Set<String> configs = new HashSet<>();
        configs.add("s3://netflix.configFile");
        this.c.setConfigs(configs);
        Assert.assertEquals(configs, this.c.getConfigs());
    }

    /**
     * Test setting the tags.
     */
    @Test
    public void testSetTags() {
        Assert.assertNotNull(this.c.getTags());
        Assert.assertTrue(this.c.getTags().isEmpty());
        final Set<String> tags = new HashSet<>();
        tags.add("tag1");
        tags.add("tag2");
        this.c.setTags(tags);
        Assert.assertEquals(tags, this.c.getTags());
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
        final Set<ApplicationEntity> applicationEntities = new HashSet<>();
        final ApplicationEntity one = new ApplicationEntity();
        one.setId("one");
        final ApplicationEntity two = new ApplicationEntity();
        two.setId("two");
        applicationEntities.add(one);
        applicationEntities.add(two);
        this.c.setApplications(applicationEntities);
        Assert.assertEquals(2, this.c.getApplications().size());
        Assert.assertTrue(this.c.getApplications().contains(one));
        Assert.assertTrue(this.c.getApplications().contains(two));
        Assert.assertTrue(one.getCommands().contains(this.c));
        Assert.assertTrue(two.getCommands().contains(this.c));

        applicationEntities.clear();
        applicationEntities.add(two);
        this.c.setApplications(applicationEntities);
        Assert.assertEquals(1, this.c.getApplications().size());
        Assert.assertTrue(this.c.getApplications().contains(two));
        Assert.assertFalse(one.getCommands().contains(this.c));
        Assert.assertTrue(two.getCommands().contains(this.c));
        this.c.setApplications(null);
        Assert.assertTrue(this.c.getApplications().isEmpty());
        Assert.assertTrue(one.getCommands().isEmpty());
        Assert.assertTrue(two.getCommands().isEmpty());
    }

    /**
     * Test setting the clusters.
     */
    @Test
    public void testSetClusters() {
        Assert.assertNotNull(this.c.getClusters());
        Assert.assertTrue(this.c.getClusters().isEmpty());
        final Set<ClusterEntity> clusterEntities = new HashSet<>();
        clusterEntities.add(new ClusterEntity());
        this.c.setClusters(clusterEntities);
        Assert.assertEquals(clusterEntities, this.c.getClusters());
    }
}
