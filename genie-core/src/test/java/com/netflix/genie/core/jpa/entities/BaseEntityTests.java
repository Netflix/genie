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

import com.netflix.genie.common.exceptions.GeniePreconditionException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Date;
import java.util.UUID;

/**
 * Tests for the base entity.
 *
 * @author tgianos
 */
public class BaseEntityTests {

    /**
     * Test to make sure objects are constructed properly.
     */
    @Test
    public void testConstructor() {
        final BaseEntity a = new BaseEntity();
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
        final BaseEntity a = new BaseEntity();
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
        final BaseEntity a = new BaseEntity();
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
        final BaseEntity a = new BaseEntity();
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
        final BaseEntity baseEntity = new BaseEntity();
        final String id = UUID.randomUUID().toString();
        baseEntity.setId(id);
        baseEntity.onCreateAuditable();
        Assert.assertEquals(id, baseEntity.getId());
    }

    /**
     * Test to make sure the update timestamp is updated by this method.
     *
     * @throws InterruptedException If the process is interrupted
     */
    @Test
    public void testOnUpdateAuditable() throws InterruptedException {
        final BaseEntity a = new BaseEntity();
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
        final BaseEntity a = new BaseEntity();
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
        final BaseEntity a = new BaseEntity();
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
        final BaseEntity a = new BaseEntity();
        Assert.assertNull(a.getEntityVersion());
        final Long version = 4L;
        a.setEntityVersion(version);
        Assert.assertEquals(version, a.getEntityVersion());
    }
}
