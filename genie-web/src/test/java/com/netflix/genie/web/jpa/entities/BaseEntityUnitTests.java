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

import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.validation.ConstraintViolationException;
import java.util.UUID;

/**
 * Test the BaseEntity class and methods.
 *
 * @author tgianos
 */
@Category(UnitTest.class)
public class BaseEntityUnitTests extends EntityTestsBase {
    private static final String UNIQUE_ID = UUID.randomUUID().toString();
    private static final String NAME = "pig13";
    private static final String USER = "tgianos";
    private static final String VERSION = "1.0";
    private static final String METADATA = "{\"key\": \"value\"}";

    private BaseEntity b;

    /**
     * Setup the tests.
     */
    @Before
    public void setup() {
        this.b = new BaseEntity();
        this.b.setUniqueId(UNIQUE_ID);
        this.b.setName(NAME);
        this.b.setUser(USER);
        this.b.setVersion(VERSION);
    }

    /**
     * Test the default Constructor.
     */
    @Test
    public void testDefaultConstructor() {
        final BaseEntity local = new BaseEntity();
        Assert.assertNotNull(local.getUniqueId());
        Assert.assertNull(local.getName());
        Assert.assertNull(local.getUser());
        Assert.assertNull(local.getVersion());
        Assert.assertFalse(local.getDescription().isPresent());
        Assert.assertFalse(local.getSetupFile().isPresent());
        Assert.assertFalse(local.isRequestedId());
    }

    /**
     * Test to make sure validation works.
     */
    @Test
    public void testValidate() {
        this.validate(this.b);
    }

    /**
     * Test to make sure validation works.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateWithNothing() {
        this.validate(new BaseEntity());
    }

    /**
     * Test to make sure validation works and throws exception when no name entered.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateNoName() {
        this.b.setName("");
        this.validate(this.b);
    }

    /**
     * Test to make sure validation works and throws exception when no name entered.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateNoUser() {
        this.b.setUser("     ");
        this.validate(this.b);
    }

    /**
     * Test to make sure validation works and throws exception when no name entered.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateNoVersion() {
        this.b.setVersion("");
        this.validate(this.b);
    }

    /**
     * Test the getting and setting of the unique id.
     */
    @Test
    public void testSetUniqueId() {
        final BaseEntity local = new BaseEntity();
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
        final BaseEntity local = new BaseEntity();
        Assert.assertNull(local.getName());
        local.setName(NAME);
        Assert.assertEquals(NAME, local.getName());
    }

    /**
     * Test to make sure the user is being set properly.
     */
    @Test
    public void testSetUser() {
        final BaseEntity local = new BaseEntity();
        Assert.assertNull(local.getUser());
        local.setUser(USER);
        Assert.assertEquals(USER, local.getUser());
    }

    /**
     * Test to make sure the version is being set properly.
     */
    @Test
    public void testSetVersion() {
        final BaseEntity local = new BaseEntity();
        Assert.assertNull(local.getVersion());
        local.setVersion(VERSION);
        Assert.assertThat(local.getVersion(), Matchers.is(VERSION));
    }

    /**
     * Test the description get/set.
     */
    @Test
    public void testSetDescription() {
        Assert.assertFalse(this.b.getDescription().isPresent());
        final String description = "Test description";
        this.b.setDescription(description);
        Assert.assertEquals(description, this.b.getDescription().orElseThrow(IllegalArgumentException::new));
    }

    /**
     * Test the setup file get/set.
     */
    @Test
    public void testSetSetupFile() {
        Assert.assertFalse(this.b.getSetupFile().isPresent());
        final FileEntity setupFile = new FileEntity();
        setupFile.setFile(UUID.randomUUID().toString());
        this.b.setSetupFile(setupFile);
        Assert.assertThat(this.b.getSetupFile().orElseThrow(IllegalArgumentException::new), Matchers.is(setupFile));
        this.b.setSetupFile(null);
        Assert.assertFalse(this.b.getSetupFile().isPresent());
    }

    /**
     * Test the metadata setter and getter.
     */
    @Test
    public void testSetMetadata() {
        Assert.assertFalse(this.b.getMetadata().isPresent());
        this.b.setMetadata(METADATA);
        Assert.assertThat(this.b.getMetadata().orElseThrow(IllegalArgumentException::new), Matchers.is(METADATA));
        this.b.setMetadata(null);
        Assert.assertFalse(this.b.getMetadata().isPresent());
    }

    /**
     * Test the is requested id fields.
     */
    @Test
    public void testSetRequestedId() {
        Assert.assertFalse(this.b.isRequestedId());
        this.b.setRequestedId(true);
        Assert.assertTrue(this.b.isRequestedId());
    }

    /**
     * Test to make sure equals and hash code only care about the unique id.
     */
    @Test
    public void testEqualsAndHashCode() {
        final String id = UUID.randomUUID().toString();
        final String name = UUID.randomUUID().toString();
        final BaseEntity one = new BaseEntity();
        one.setUniqueId(id);
        one.setName(UUID.randomUUID().toString());
        final BaseEntity two = new BaseEntity();
        two.setUniqueId(id);
        two.setName(name);
        final BaseEntity three = new BaseEntity();
        three.setUniqueId(UUID.randomUUID().toString());
        three.setName(name);

        Assert.assertTrue(one.equals(two));
        Assert.assertFalse(one.equals(three));
        Assert.assertFalse(two.equals(three));

        Assert.assertEquals(one.hashCode(), two.hashCode());
        Assert.assertNotEquals(one.hashCode(), three.hashCode());
        Assert.assertNotEquals(two.hashCode(), three.hashCode());
    }

    /**
     * Test the toString method.
     */
    @Test
    public void testToString() {
        Assert.assertNotNull(this.b.toString());
    }
}
