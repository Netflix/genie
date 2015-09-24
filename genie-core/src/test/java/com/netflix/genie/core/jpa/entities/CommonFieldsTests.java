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
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.validation.ConstraintViolationException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * Test the CommonFields class and methods.
 *
 * @author tgianos
 */
public class CommonFieldsTests extends EntityTestsBase {
    private static final String NAME = "pig13";
    private static final String USER = "tgianos";
    private static final String VERSION = "1.0";

    private CommonFields c;

    /**
     * Setup the tests.
     */
    @Before
    public void setup() {
        this.c = new CommonFields(NAME, USER, VERSION);
    }

    /**
     * Test the default Constructor.
     */
    @Test
    public void testDefaultConstructor() {
        final CommonFields local = new CommonFields();
        Assert.assertNull(local.getName());
        Assert.assertNull(local.getUser());
        Assert.assertNull(local.getVersion());
    }

    /**
     * Test the argument Constructor.
     */
    @Test
    public void testConstructor() {
        Assert.assertEquals(NAME, this.c.getName());
        Assert.assertEquals(USER, this.c.getUser());
        Assert.assertEquals(VERSION, this.c.getVersion());
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
        this.validate(new CommonFields());
    }

    /**
     * Test to make sure validation works and throws exception when no name entered.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateNoName() {
        this.validate(new CommonFields(null, USER, VERSION));
    }

    /**
     * Test to make sure validation works and throws exception when no name entered.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateNoUser() {
        this.validate(new CommonFields(NAME, "     ", VERSION));
    }

    /**
     * Test to make sure validation works and throws exception when no name entered.
     */
    @Test(expected = ConstraintViolationException.class)
    public void testValidateNoVersion() {
        this.validate(new CommonFields(NAME, USER, ""));
    }

    /**
     * Test to make sure the name is being set properly.
     */
    @Test
    public void testSetName() {
        final CommonFields local = new CommonFields();
        Assert.assertNull(local.getName());
        local.setName(NAME);
        Assert.assertEquals(NAME, local.getName());
    }

    /**
     * Test to make sure the user is being set properly.
     */
    @Test
    public void testSetUser() {
        final CommonFields local = new CommonFields();
        Assert.assertNull(local.getUser());
        local.setUser(USER);
        Assert.assertEquals(USER, local.getUser());
    }

    /**
     * Test to make sure the version is being set properly.
     */
    @Test
    public void testSetVersion() {
        final CommonFields local = new CommonFields();
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
     * Test the method which adds system tags.
     *
     * @throws GeniePreconditionException If a precondition isn't met
     */
    @Test
    public void testAddAndValidateSystemTags() throws GeniePreconditionException {
        final Set<String> tags = new HashSet<>();
        final String tag1 = UUID.randomUUID().toString();
        final String tag2 = UUID.randomUUID().toString();
        final String tag3 = UUID.randomUUID().toString();
        tags.add(tag1);
        tags.add(tag2);
        tags.add(tag3);

        final String id = UUID.randomUUID().toString();
        this.c.setId(id);

        this.c.addAndValidateSystemTags(tags);
        Assert.assertEquals(5, tags.size());
        Assert.assertTrue(tags.contains(tag1));
        Assert.assertTrue(tags.contains(tag2));
        Assert.assertTrue(tags.contains(tag3));
        Assert.assertTrue(tags.contains(CommonFields.GENIE_ID_TAG_NAMESPACE + id));
        Assert.assertTrue(tags.contains(CommonFields.GENIE_NAME_TAG_NAMESPACE + NAME));
    }

    /**
     * Test the method which adds system tags.
     *
     * @throws GeniePreconditionException If a precondition isn't met
     */
    @Test
    public void testAddAndValidateSystemTagsWithChangedName() throws GeniePreconditionException {
        final Set<String> tags = new HashSet<>();
        final String tag1 = UUID.randomUUID().toString();
        final String tag2 = UUID.randomUUID().toString();
        final String tag3 = UUID.randomUUID().toString();
        tags.add(tag1);
        tags.add(tag2);
        tags.add(tag3);

        final String id = UUID.randomUUID().toString();
        this.c.setId(id);

        this.c.addAndValidateSystemTags(tags);
        Assert.assertEquals(5, tags.size());
        Assert.assertTrue(tags.contains(tag1));
        Assert.assertTrue(tags.contains(tag2));
        Assert.assertTrue(tags.contains(tag3));
        Assert.assertTrue(tags.contains(CommonFields.GENIE_ID_TAG_NAMESPACE + id));
        Assert.assertTrue(tags.contains(CommonFields.GENIE_NAME_TAG_NAMESPACE + NAME));

        final String newName = UUID.randomUUID().toString();
        this.c.setName(newName);

        this.c.addAndValidateSystemTags(tags);
        Assert.assertEquals(5, tags.size());
        Assert.assertTrue(tags.contains(tag1));
        Assert.assertTrue(tags.contains(tag2));
        Assert.assertTrue(tags.contains(tag3));
        Assert.assertTrue(tags.contains(CommonFields.GENIE_ID_TAG_NAMESPACE + id));
        Assert.assertTrue(tags.contains(CommonFields.GENIE_NAME_TAG_NAMESPACE + newName));
    }

    /**
     * Test the method which adds system tags.
     *
     * @throws GeniePreconditionException If a precondition isn't met
     */
    @Test(expected = GeniePreconditionException.class)
    public void testAddAndValidateSystemTagsNullTags() throws GeniePreconditionException {
        this.c.addAndValidateSystemTags(null);
    }

    /**
     * Test the method which adds system tags.
     *
     * @throws GeniePreconditionException If a precondition isn't met
     */
    @Test(expected = GeniePreconditionException.class)
    public void testAddAndValidateSystemTagsWithTooManyIdTags() throws GeniePreconditionException {
        final Set<String> tags = new HashSet<>();
        final String tag1 = UUID.randomUUID().toString();
        final String tag2 = UUID.randomUUID().toString();
        final String tag3 = UUID.randomUUID().toString();
        tags.add(tag1);
        tags.add(tag2);
        tags.add(tag3);

        final String id = UUID.randomUUID().toString();
        this.c.setId(id);
        tags.add(CommonFields.GENIE_ID_TAG_NAMESPACE + UUID.randomUUID().toString());

        this.c.addAndValidateSystemTags(tags);
    }

    /**
     * Test the method which adds system tags.
     *
     * @throws GeniePreconditionException If a precondition isn't met
     */
    @Test(expected = GeniePreconditionException.class)
    public void testAddAndValidateSystemTagsWithTooManyNameTags() throws GeniePreconditionException {
        final Set<String> tags = new HashSet<>();
        final String tag1 = UUID.randomUUID().toString();
        final String tag2 = UUID.randomUUID().toString();
        final String tag3 = UUID.randomUUID().toString();
        tags.add(tag1);
        tags.add(tag2);
        tags.add(tag3);
        tags.add(CommonFields.GENIE_NAME_TAG_NAMESPACE + UUID.randomUUID().toString());
        tags.add(CommonFields.GENIE_NAME_TAG_NAMESPACE + UUID.randomUUID().toString());

        final String id = UUID.randomUUID().toString();
        this.c.setId(id);

        this.c.addAndValidateSystemTags(tags);
    }

    /**
     * Test the method which adds system tags.
     *
     * @throws GeniePreconditionException If a precondition isn't met
     */
    @Test(expected = GeniePreconditionException.class)
    public void testAddAndValidateSystemTagsWithTooManyGenieNamespaceTags() throws GeniePreconditionException {
        final Set<String> tags = new HashSet<>();
        final String tag1 = UUID.randomUUID().toString();
        final String tag2 = UUID.randomUUID().toString();
        final String tag3 = UUID.randomUUID().toString();
        tags.add(tag1);
        tags.add(tag2);
        tags.add(tag3);
        tags.add(CommonFields.GENIE_TAG_NAMESPACE + UUID.randomUUID().toString());

        final String id = UUID.randomUUID().toString();
        this.c.setId(id);

        this.c.addAndValidateSystemTags(tags);
    }
}
