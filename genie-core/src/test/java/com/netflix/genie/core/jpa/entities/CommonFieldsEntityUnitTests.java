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
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.validation.ConstraintViolationException;
import java.util.Set;
import java.util.UUID;

/**
 * Test the CommonFieldsEntity class and methods.
 *
 * @author tgianos
 */
@Category(UnitTest.class)
public class CommonFieldsEntityUnitTests extends EntityTestsBase {
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
        Assert.assertNull(local.getName());
        Assert.assertNull(local.getUser());
        Assert.assertNull(local.getVersion());
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
        Assert.assertNull(this.c.getDescription());
        final String description = "Test description";
        this.c.setDescription(description);
        Assert.assertEquals(description, this.c.getDescription());
    }

    /**
     * Make sure can set the sorted tags from an unsorted set.
     */
    @Test
    public void canSetTags() {
        final Set<String> tags = Sets.newHashSet("second", "Third", "Fourth", "first");
        this.c.setTags(tags);
        Assert.assertThat(this.c.getTags(), Matchers.is(tags));

        this.c.setTags(null);
        Assert.assertThat(this.c.getTags(), Matchers.empty());

        this.c.setTags(Sets.newHashSet());
        Assert.assertThat(this.c.getTags(), Matchers.empty());
    }

    /**
     * Make sure we generate the proper final tags.
     *
     * @throws GenieException on error
     */
    @Test
    public void canGetFinalTags() throws GenieException {
        final String id = UUID.randomUUID().toString();
        this.c.setId(id);

        Assert.assertThat(this.c.getFinalTags(), Matchers.is(Sets.newHashSet("genie.id:" + id, "genie.name:" + NAME)));

        final String sortedTags = "big,genie.id:id,genie.name:name,tom";
        this.c.setTags(Sets.newHashSet(sortedTags.split(",")));
        final Set<String> finalTags = this.c.getFinalTags();
        Assert.assertThat(
            finalTags,
            Matchers.is(Sets.newHashSet("genie.id:" + id, "genie.name:" + NAME, "big", "tom"))
        );
        Assert.assertThat(finalTags.size(), Matchers.is(4));

        this.c = new CommonFieldsEntity();
        this.c.setName(NAME);
        Assert.assertThat(this.c.getFinalTags().size(), Matchers.is(2));
    }
}
