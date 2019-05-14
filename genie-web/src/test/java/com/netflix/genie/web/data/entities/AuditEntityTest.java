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
package com.netflix.genie.web.data.entities;

import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;

/**
 * Tests for the audit entity.
 *
 * @author tgianos
 */
public class AuditEntityTest {

    /**
     * Test to make sure objects are constructed properly.
     */
    @Test
    public void testConstructor() {
        final AuditEntity a = new AuditEntity();
        Assert.assertNotNull(a.getCreated());
        Assert.assertNotNull(a.getUpdated());
    }

    /**
     * Test to make sure @PrePersist annotation will do what we want before persistence.
     *
     * @throws InterruptedException If the process is interrupted
     */
    @Test
    public void testOnCreateAuditEntity() throws InterruptedException {
        final AuditEntity a = new AuditEntity();
        Assert.assertNotNull(a.getCreated());
        Assert.assertNotNull(a.getUpdated());
        final Instant originalCreated = a.getCreated();
        final Instant originalUpdated = a.getUpdated();
        Thread.sleep(1);
        a.onCreateBaseEntity();
        Assert.assertNotNull(a.getCreated());
        Assert.assertNotNull(a.getUpdated());
        Assert.assertNotEquals(originalCreated, a.getCreated());
        Assert.assertNotEquals(originalUpdated, a.getUpdated());
        Assert.assertEquals(a.getCreated(), a.getUpdated());
    }

    /**
     * Test to make sure the update timestamp is updated by this method.
     *
     * @throws InterruptedException If the process is interrupted
     */
    @Test
    public void testOnUpdateAuditEntity() throws InterruptedException {
        final AuditEntity a = new AuditEntity();
        Assert.assertNotNull(a.getCreated());
        Assert.assertNotNull(a.getUpdated());
        a.onCreateBaseEntity();
        final Instant originalCreate = a.getCreated();
        final Instant originalUpdate = a.getUpdated();
        Thread.sleep(1);
        a.onUpdateBaseEntity();
        Assert.assertEquals(originalCreate, a.getCreated());
        Assert.assertNotEquals(originalUpdate, a.getUpdated());
    }

    /**
     * Test the toString method.
     */
    @Test
    public void testToString() {
        Assert.assertNotNull(new AuditEntity().toString());
    }
}
