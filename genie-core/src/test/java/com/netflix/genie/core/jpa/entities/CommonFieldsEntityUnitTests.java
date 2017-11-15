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

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.validation.ConstraintViolationException;
import java.util.UUID;

/**
 * Test the CommonFieldsEntity class and methods.
 *
 * @author tgianos
 */
@Category(UnitTest.class)
public class CommonFieldsEntityUnitTests extends EntityTestsBase {
    private static final String UNIQUE_ID = UUID.randomUUID().toString();
    private static final String NAME = "pig13";
    private static final String USER = "tgianos";
    private static final String VERSION = "1.0";

    private CommonFieldsEntity c;

    /**
     * Setup the tests.
     */
    @Before
    public void setup() {
        this.c = new CommonFieldsEntity();
        this.c.setUniqueId(UNIQUE_ID);
        this.c.setName(NAME);
        this.c.setUser(USER);
        this.c.setVersion(VERSION);
    }

    /**
     * Test the default Constructor.
     */
    @Test
    public void testDefaultConstructor() {
        final CommonFieldsEntity local = new CommonFieldsEntity();
        Assert.assertNotNull(local.getUniqueId());
        Assert.assertNull(local.getName());
        Assert.assertNull(local.getUser());
        Assert.assertNull(local.getVersion());
        Assert.assertFalse(local.getDescription().isPresent());
        Assert.assertFalse(local.getSetupFile().isPresent());
    }

    /**
     * Test to make sure validation works.
     */
    @Test
    public void testValidate() {
        this.validate(this.c);
    }

    /**
     * Test to make sure validation works.
     *
     * @throws GenieException For any issue
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateWithNothing() throws GenieException {
        this.validate(new CommonFieldsEntity());
    }

    /**
     * Test to make sure validation works and throws exception when no name entered.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateNoName() {
        this.c.setName(null);
        this.validate(this.c);
    }

    /**
     * Test to make sure validation works and throws exception when no name entered.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateNoUser() {
        this.c.setUser("     ");
        this.validate(this.c);
    }

    /**
     * Test to make sure validation works and throws exception when no name entered.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateNoVersion() {
        this.c.setVersion("");
        this.validate(this.c);
    }

    /**
     * Test the getting and setting of the unique id.
     */
    @Test
    public void testSetUniqueId() {
        final CommonFieldsEntity local = new CommonFieldsEntity();
        Assert.assertNotNull(local.getUniqueId());
        Assert.assertNotEquals(local.getUniqueId(), UNIQUE_ID);
        local.setUniqueId(UNIQUE_ID);
        Assert.assertEquals(UNIQUE_ID, local.getUniqueId());
    }

    /**
     * Test to make sure the name is being set properly.
     */
    @Test
    public void testSetName() {
        final CommonFieldsEntity local = new CommonFieldsEntity();
        Assert.assertNull(local.getName());
        local.setName(NAME);
        Assert.assertEquals(NAME, local.getName());
    }

    /**
     * Test to make sure the user is being set properly.
     */
    @Test
    public void testSetUser() {
        final CommonFieldsEntity local = new CommonFieldsEntity();
        Assert.assertNull(local.getUser());
        local.setUser(USER);
        Assert.assertEquals(USER, local.getUser());
    }

    /**
     * Test to make sure the version is being set properly.
     */
    @Test
    public void testSetVersion() {
        final CommonFieldsEntity local = new CommonFieldsEntity();
        Assert.assertNull(local.getVersion());
        local.setVersion(VERSION);
        Assert.assertEquals(VERSION, local.getVersion());
    }

    /**
     * Test the description get/set.
     */
    @Test
    public void testSetDescription() {
        Assert.assertFalse(this.c.getDescription().isPresent());
        final String description = "Test description";
        this.c.setDescription(description);
        Assert.assertEquals(description, this.c.getDescription().orElseThrow(IllegalArgumentException::new));
    }

    /**
     * Test the setup file get/set.
     */
    @Test
    public void testSetSetupFile() {
        Assert.assertFalse(this.c.getSetupFile().isPresent());
        final FileEntity setupFile = new FileEntity();
        setupFile.setFile(UUID.randomUUID().toString());
        this.c.setSetupFile(setupFile);
        Assert.assertThat(this.c.getSetupFile().orElseThrow(IllegalArgumentException::new), Matchers.is(setupFile));
        this.c.setSetupFile(null);
        Assert.assertFalse(this.c.getSetupFile().isPresent());
    }

    /**
     * Test to make sure equals and hash code only care about the unique id.
     */
    @Test
    public void testEqualsAndHashCode() {
        final String id = UUID.randomUUID().toString();
        final String name = UUID.randomUUID().toString();
        final CommonFieldsEntity one = new CommonFieldsEntity();
        one.setUniqueId(id);
        one.setName(UUID.randomUUID().toString());
        final CommonFieldsEntity two = new CommonFieldsEntity();
        two.setUniqueId(id);
        two.setName(name);
        final CommonFieldsEntity three = new CommonFieldsEntity();
        three.setUniqueId(UUID.randomUUID().toString());
        three.setName(name);

        Assert.assertTrue(one.equals(two));
        Assert.assertFalse(one.equals(three));
        Assert.assertFalse(two.equals(three));

        Assert.assertEquals(one.hashCode(), two.hashCode());
        Assert.assertNotEquals(one.hashCode(), three.hashCode());
        Assert.assertNotEquals(two.hashCode(), three.hashCode());
    }
}
