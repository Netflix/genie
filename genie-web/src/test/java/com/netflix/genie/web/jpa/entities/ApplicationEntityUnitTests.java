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

import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.ApplicationStatus;
import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.validation.ConstraintViolationException;
import java.util.Set;

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
        this.a.setName(NAME);
        this.a.setUser(USER);
        this.a.setVersion(VERSION);
        this.a.setStatus(ApplicationStatus.ACTIVE);
    }

    /**
     * Test the default Constructor.
     */
    @Test
    public void testDefaultConstructor() {
        final ApplicationEntity entity = new ApplicationEntity();
        Assert.assertFalse(entity.getSetupFile().isPresent());
        Assert.assertNull(entity.getStatus());
        Assert.assertNull(entity.getName());
        Assert.assertNull(entity.getUser());
        Assert.assertNull(entity.getVersion());
        Assert.assertNotNull(entity.getDependencies());
        Assert.assertTrue(entity.getDependencies().isEmpty());
        Assert.assertNotNull(entity.getConfigs());
        Assert.assertTrue(entity.getConfigs().isEmpty());
        Assert.assertNotNull(entity.getTags());
        Assert.assertTrue(entity.getTags().isEmpty());
        Assert.assertNotNull(entity.getCommands());
        Assert.assertTrue(entity.getCommands().isEmpty());
    }

    /**
     * Make sure validation works on valid apps.
     */
    @Test
    public void testValidate() {
        this.a.setName(NAME);
        this.a.setUser(USER);
        this.a.setVersion(VERSION);
        this.a.setStatus(ApplicationStatus.ACTIVE);
        this.validate(this.a);
    }

    /**
     * Make sure validation works on with failure from super class.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateNoName() {
        this.a.setName("");
        this.validate(this.a);
    }

    /**
     * Make sure validation works on with failure from super class.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateNoUser() {
        this.a.setUser("");
        this.validate(this.a);
    }

    /**
     * Make sure validation works on with failure from super class.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateNoVersion() {
        this.a.setVersion("");
        this.validate(this.a);
    }

    /**
     * Test setting the status.
     */
    @Test
    public void testSetStatus() {
        this.a.setStatus(ApplicationStatus.ACTIVE);
        Assert.assertEquals(ApplicationStatus.ACTIVE, this.a.getStatus());
    }

    /**
     * Test setting the setup file.
     */
    @Test
    public void testSetSetupFile() {
        Assert.assertFalse(this.a.getSetupFile().isPresent());
        final String setupFile = "s3://netflix.propFile";
        final FileEntity setupFileEntity = new FileEntity();
        setupFileEntity.setFile(setupFile);
        this.a.setSetupFile(setupFileEntity);
        Assert.assertEquals(setupFile, this.a.getSetupFile().orElseThrow(IllegalArgumentException::new).getFile());
    }

    /**
     * Test setting the configs.
     */
    @Test
    public void testSetConfigs() {
        Assert.assertNotNull(this.a.getConfigs());
        final Set<FileEntity> configs = Sets.newHashSet();
        final FileEntity config = new FileEntity();
        config.setFile("s3://netflix.configFile");
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
        final Set<FileEntity> dependencies = Sets.newHashSet();
        final FileEntity dependency = new FileEntity();
        dependency.setFile("s3://netflix/jars/myJar.jar");
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
        final Set<TagEntity> tags = Sets.newHashSet();
        final TagEntity tag1 = new TagEntity();
        tag1.setTag("tag1");
        final TagEntity tag2 = new TagEntity();
        tag2.setTag("tag2");

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
        final Set<CommandEntity> commandEntities = Sets.newHashSet(new CommandEntity());
        this.a.setCommands(commandEntities);
        Assert.assertEquals(commandEntities, this.a.getCommands());

        this.a.setCommands(null);
        Assert.assertThat(this.a.getCommands(), Matchers.empty());
    }

    /**
     * Test the toString method.
     */
    @Test
    public void testToString() {
        Assert.assertNotNull(this.a.toString());
    }
}
