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

import com.netflix.genie.common.exceptions.GeniePreconditionException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the CommonEntityFields class and methods.
 *
 * @author tgianos
 */
public class TestCommonEntityFields {
    private static final String NAME = "pig13";
    private static final String USER = "tgianos";
    private static final String VERSION = "1.0";

    private CommonEntityFields c;

    /**
     * Setup the tests.
     */
    @Before
    public void setup() {
        this.c = new CommonEntityFields();
    }

    /**
     * Test the default Constructor.
     */
    @Test
    public void testDefaultConstructor() {
        Assert.assertNull(this.c.getName());
        Assert.assertNull(this.c.getUser());
        Assert.assertNull(this.c.getVersion());
    }

    /**
     * Test the argument Constructor.
     */
    @Test
    public void testConstructor() {
        c = new CommonEntityFields(NAME, USER, VERSION);
        Assert.assertEquals(NAME, this.c.getName());
        Assert.assertEquals(USER, this.c.getUser());
        Assert.assertEquals(VERSION, this.c.getVersion());
    }

    /**
     * Test to make sure validation works.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testOnCreateOrUpdateCommonEntityFields() throws GeniePreconditionException {
        this.c = new CommonEntityFields(NAME, USER, VERSION);
        this.c.onCreateOrUpdateCommonEntityFields();
    }

    /**
     * Test to make sure validation works.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test(expected = GeniePreconditionException.class)
    public void testOnCreateOrUpdateCommonEntityFieldsWithNothing() throws GeniePreconditionException {
        this.c.onCreateOrUpdateCommonEntityFields();
    }

    /**
     * Test to make sure validation works and throws exception when no name entered.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test(expected = GeniePreconditionException.class)
    public void testOnCreateOrUpdateCommonEntityFieldsNoName() throws GeniePreconditionException {
        this.c = new CommonEntityFields(null, USER, VERSION);
        this.c.onCreateOrUpdateCommonEntityFields();
    }

    /**
     * Test to make sure validation works and throws exception when no user entered.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test(expected = GeniePreconditionException.class)
    public void testOnCreateOrUpdateCommonEntityFieldsNoUser() throws GeniePreconditionException {
        this.c = new CommonEntityFields(null, USER, VERSION);
        this.c.onCreateOrUpdateCommonEntityFields();
    }

    /**
     * Test to make sure validation works and throws exception when no user entered.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test(expected = GeniePreconditionException.class)
    public void testOnCreateOrUpdateCommonEntityFieldsNoVersion() throws GeniePreconditionException {
        this.c = new CommonEntityFields(NAME, USER, null);
        this.c.onCreateOrUpdateCommonEntityFields();
    }

    /**
     * Make sure validation works on valid commands.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testValidate() throws GeniePreconditionException {
        this.c = new CommonEntityFields(NAME, USER, VERSION);
        this.c.validate();
    }

    /**
     * Test to make sure the name is being set properly.
     */
    @Test
    public void testSetName() {
        Assert.assertNull(this.c.getName());
        this.c.setName(NAME);
        Assert.assertEquals(NAME, this.c.getName());
    }

    /**
     * Test to make sure the user is being set properly.
     */
    @Test
    public void testSetUser() {
        Assert.assertNull(this.c.getUser());
        this.c.setUser(USER);
        Assert.assertEquals(USER, this.c.getUser());
    }

    /**
     * Test to make sure the version is being set properly.
     */
    @Test
    public void testSetVersion() {
        Assert.assertNull(this.c.getVersion());
        this.c.setVersion(VERSION);
        Assert.assertEquals(VERSION, this.c.getVersion());
    }
}
