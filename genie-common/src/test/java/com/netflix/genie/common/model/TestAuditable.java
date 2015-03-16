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
package com.netflix.genie.common.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
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
        Assert.assertNull(a.getCreated());
        Assert.assertNull(a.getUpdated());
    }

    /**
     * Test the setter for id.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testSetId() throws GeniePreconditionException {
        final Auditable a = new Auditable();
        Assert.assertNull(a.getId());
        final String id = UUID.randomUUID().toString();
        a.setId(id);
        Assert.assertEquals(id, a.getId());
    }

    /**
     * Test the setter for id.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test(expected = GeniePreconditionException.class)
    public void testSetIdTwice() throws GeniePreconditionException {
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
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testOnCreateAuditable() throws InterruptedException, GeniePreconditionException {
        final Auditable a = new Auditable();
        Assert.assertNull(a.getId());
        Assert.assertNull(a.getCreated());
        Assert.assertNull(a.getUpdated());
        a.onCreateAuditable();
        Assert.assertNotNull(a.getId());
        Assert.assertNotNull(a.getCreated());
        Assert.assertNotNull(a.getUpdated());
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
     * @throws InterruptedException
     */
    @Test
    public void testOnUpdateAuditable() throws InterruptedException {
        final Auditable a = new Auditable();
        Assert.assertNull(a.getId());
        Assert.assertNull(a.getCreated());
        Assert.assertNull(a.getUpdated());
        a.onCreateAuditable();
        final Date originalCreate = a.getCreated();
        final Date originalUpdate = a.getUpdated();
        Thread.sleep(1);
        a.onUpdateAuditable();
        Assert.assertEquals(originalCreate, a.getCreated());
        Assert.assertNotEquals(originalUpdate, a.getUpdated());
    }

    /**
     * Test to make sure the setter of created does nothing.
     */
    @Test
    public void testSetCreated() {
        final Auditable a = new Auditable();
        Assert.assertNull(a.getCreated());
        a.setCreated(new Date());
        Assert.assertNull(a.getCreated());
        a.onCreateAuditable();
        Assert.assertNotNull(a.getCreated());
    }

    /**
     * Test to make sure updated is never changed by set.
     */
    @Test
    public void testSetUpdated() {
        final Auditable a = new Auditable();
        Assert.assertNull(a.getUpdated());
        final Date newer = new Date();
        a.setUpdated(newer);
        Assert.assertNull(a.getUpdated());
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
     * @throws GeniePreconditionException
     * @throws IOException
     */
    @Test
    public void testToString() throws GeniePreconditionException, IOException {
        final Auditable a = new Auditable();
        a.onCreateAuditable();
        final String id = a.getId();
        final Date created = a.getCreated();
        final Date updated = a.getUpdated();

        final String json = a.toString();

        final ObjectMapper mapper = new ObjectMapper();
        final Auditable b = mapper.readValue(json, Auditable.class);
        Assert.assertEquals(id, b.getId());
        Assert.assertNotEquals(created, b.getCreated());
        Assert.assertNotEquals(updated, b.getUpdated());
        Assert.assertNull(b.getCreated());
        Assert.assertNull(b.getUpdated());
    }
}
