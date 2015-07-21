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
        Assert.assertNotNull(a.getCreated());
        Assert.assertNotNull(a.getUpdated());
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
     * @throws InterruptedException       If the process is interrupted
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @Test
    public void testOnCreateAuditable() throws InterruptedException, GeniePreconditionException {
        final Auditable a = new Auditable();
        Assert.assertNull(a.getId());
        Assert.assertNotNull(a.getCreated());
        Assert.assertNotNull(a.getUpdated());
        final Date originalCreated = a.getCreated();
        final Date originalUpdated = a.getUpdated();
        Thread.sleep(1);
        a.onCreateAuditable();
        Assert.assertNotNull(a.getId());
        Assert.assertNotNull(a.getCreated());
        Assert.assertNotNull(a.getUpdated());
        Assert.assertNotEquals(originalCreated, a.getCreated());
        Assert.assertNotEquals(originalUpdated, a.getUpdated());
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
     * @throws InterruptedException If the process is interrupted
     */
    @Test
    public void testOnUpdateAuditable() throws InterruptedException {
        final Auditable a = new Auditable();
        Assert.assertNull(a.getId());
        Assert.assertNotNull(a.getCreated());
        Assert.assertNotNull(a.getUpdated());
        a.onCreateAuditable();
        final Date originalCreate = a.getCreated();
        final Date originalUpdate = a.getUpdated();
        Thread.sleep(1);
        a.onUpdateAuditable();
        Assert.assertEquals(originalCreate, a.getCreated());
        Assert.assertNotEquals(originalUpdate, a.getUpdated());
    }

    /**
     * Test to make sure the setter of created does nothing relative to persistence.
     */
    @Test
    public void testSetCreated() {
        final Auditable a = new Auditable();
        Assert.assertNotNull(a.getCreated());
        final Date date = new Date(0);
        a.setCreated(date);
        Assert.assertNotNull(a.getCreated());
        Assert.assertEquals(date, a.getCreated());
        a.onCreateAuditable();
        Assert.assertNotNull(a.getCreated());
        Assert.assertNotEquals(date, a.getCreated());
    }

    /**
     * Test to make sure updated is set but really is overwritten by onUpdate.
     *
     * @throws InterruptedException If processing is interrupted
     */
    @Test
    public void testSetUpdated() throws InterruptedException {
        final Auditable a = new Auditable();
        Assert.assertNotNull(a.getUpdated());
        final Date date = new Date(0);
        a.setUpdated(date);
        Assert.assertNotNull(a.getUpdated());
        Assert.assertEquals(date, a.getUpdated());
        a.onCreateAuditable();
        Assert.assertNotEquals(date, a.getUpdated());
        final Date oldUpdated = a.getUpdated();
        Thread.sleep(1);
        a.onUpdateAuditable();
        Assert.assertNotEquals(oldUpdated, a.getUpdated());
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
     * @throws GeniePreconditionException If a precondition isn't met
     * @throws IOException                If an exception happens during serialization
     */
    @Test
    public void testToString() throws GeniePreconditionException, IOException {
        final Auditable a = new Auditable();
        a.onCreateAuditable();

        final String json = a.toString();

        final ObjectMapper mapper = new ObjectMapper();
        final Auditable b = mapper.readValue(json, Auditable.class);
        Assert.assertEquals(a.getId(), b.getId());

        // Need to take off the milliseconds because they are lost anyway in the serialization/deserialization process
        final long expectedCreated = a.getCreated().getTime() - a.getCreated().getTime() % 1000L;
        Assert.assertEquals(expectedCreated, b.getCreated().getTime());
        final long expectedUpdated = a.getUpdated().getTime() - a.getUpdated().getTime() % 1000L;
        Assert.assertEquals(expectedUpdated, b.getUpdated().getTime());
    }
}
