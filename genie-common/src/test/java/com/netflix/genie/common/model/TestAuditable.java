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
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;
import java.util.UUID;

/**
 * Tests for Auditable.
 *
 * @author tgianos
 */
public class TestAuditable {
    /**
     * Test to make sure objects are constructed properly.
     */
    @Test
    public void testConstructor() {
        final Auditable a = new Auditable();
        Assert.assertNull(a.getId());
        Assert.assertNotNull(a.getCreated());
        Assert.assertTrue(0 < a.getCreated().getTime());
        Assert.assertNotNull(a.getUpdated());
        Assert.assertTrue(0 < a.getUpdated().getTime());
    }

    /**
     * Test the setter for id.
     *
     * @throws GenieException
     */
    @Test
    public void testSetId() throws GenieException {
        final Auditable a = new Auditable();
        Assert.assertNull(a.getId());
        final String id = UUID.randomUUID().toString();
        a.setId(id);
        Assert.assertEquals(id, a.getId());
    }

    /**
     * Test the setter for id.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testSetIdWithNull() throws GenieException {
        final Auditable a = new Auditable();
        Assert.assertNull(a.getId());
        a.setId(null);
    }

    /**
     * Test the setter for id.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testSetIdWithEmpty() throws GenieException {
        final Auditable a = new Auditable();
        Assert.assertNull(a.getId());
        a.setId("");
    }

    /**
     * Test the setter for id.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testSetIdTwice() throws GenieException {
        final Auditable a = new Auditable();
        Assert.assertNull(a.getId());
        final String id = UUID.randomUUID().toString();
        a.setId(id);
        Assert.assertEquals(id, a.getId());
        //Should throw exception here
        a.setId(UUID.randomUUID().toString());
    }

    /**
     * Test to make sure @PrePersist annotation will do what we want before persistence.
     *
     * @throws InterruptedException
     * @throws GenieException
     */
    @Test
    public void testOnCreateAuditable() throws InterruptedException, GenieException {
        final Auditable a = new Auditable();
        Assert.assertNull(a.getId());
        final Date originalCreate = a.getCreated();
        final Date originalUpdate = a.getUpdated();
        Thread.sleep(1);
        a.onCreateAuditable();
        Assert.assertNotNull(a.getId());
        Assert.assertTrue(originalCreate.getTime() < a.getCreated().getTime());
        Assert.assertTrue(originalUpdate.getTime() < a.getUpdated().getTime());
        Assert.assertEquals(a.getCreated(), a.getUpdated());

        //Test to make sure if an ID already was set we don't change it
        final Auditable auditable = new Auditable();
        final String id = UUID.randomUUID().toString();
        auditable.setId(id);
        auditable.onCreateAuditable();
        Assert.assertEquals(id, auditable.getId());
    }

    /**
     * Test to make sure the update timestamp is updated by this method.
     *
     * @throws java.lang.InterruptedException
     */
    @Test
    public void testOnUpdateAuditable() throws InterruptedException {
        final Auditable a = new Auditable();
        Assert.assertNull(a.getId());
        final Date originalCreate = a.getCreated();
        final Date originalUpdate = a.getUpdated();
        Thread.sleep(1);
        a.onUpdateAuditable();
        Assert.assertEquals(originalCreate, a.getCreated());
        Assert.assertNotEquals(originalUpdate, a.getUpdated());
    }

    /**
     * Test to make sure created can't just be set arbitrarily.
     */
    @Test
    public void testSetCreated() {
        final Auditable a = new Auditable();
        final Date oc = a.getCreated();
        final Date newer = new Date(oc.getTime() + 1);
        final Date older = new Date(oc.getTime() - 1);
        a.setCreated(newer);
        Assert.assertEquals(oc, a.getCreated());
        a.setCreated(older);
        Assert.assertEquals(older, a.getCreated());
    }

    /**
     * Test to make sure updated is always changed by set.
     */
    @Test
    public void testSetUpdated() {
        final Auditable a = new Auditable();
        final Date ou = a.getUpdated();
        final Date newer = new Date(ou.getTime() + 1);
        a.setUpdated(newer);
        Assert.assertEquals(newer, a.getUpdated());
    }

    /**
     * Test the entity version code.
     */
    @Test
    public void testEntityVersion() {
        final Auditable a = new Auditable();
        Assert.assertNull(a.getEntityVersion());
        final Long version = 4L;
        a.setEntityVersion(version);
        Assert.assertEquals(version, a.getEntityVersion());
    }

    /**
     * Make sure we generate valid JSON.
     *
     * @throws com.netflix.genie.common.exceptions.GenieException
     * @throws IOException
     */
    @Test
    public void testToString() throws GenieException, IOException {
        final Auditable a = new Auditable();
        final String id = UUID.randomUUID().toString();
        a.setId(id);
        final Date created = a.getCreated();
        final Date updated = a.getUpdated();

        final String json = a.toString();

        final ObjectMapper mapper = new ObjectMapper();
        final Auditable b = mapper.readValue(json, Auditable.class);
        Assert.assertEquals(id, b.getId());
        Assert.assertEquals(created, b.getCreated());
        Assert.assertEquals(updated, b.getUpdated());
    }
}
