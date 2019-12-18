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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;

/**
 * Tests for the audit entity.
 *
 * @author tgianos
 */
class AuditEntityTest {

    /**
     * Test to make sure objects are constructed properly.
     */
    @Test
    void testConstructor() {
        final AuditEntity a = new AuditEntity();
        Assertions.assertThat(a.getCreated()).isNotNull();
        Assertions.assertThat(a.getUpdated()).isNotNull();
    }

    /**
     * Test to make sure @PrePersist annotation will do what we want before persistence.
     *
     * @throws InterruptedException If the process is interrupted
     */
    @Test
    void testOnCreateAuditEntity() throws InterruptedException {
        final AuditEntity a = new AuditEntity();
        Assertions.assertThat(a.getCreated()).isNotNull();
        Assertions.assertThat(a.getUpdated()).isNotNull();
        final Instant originalCreated = a.getCreated();
        final Instant originalUpdated = a.getUpdated();
        Thread.sleep(1);
        a.onCreateBaseEntity();
        Assertions.assertThat(a.getCreated()).isNotNull();
        Assertions.assertThat(a.getUpdated()).isNotNull();
        Assertions.assertThat(a.getCreated()).isNotEqualTo(originalCreated);
        Assertions.assertThat(a.getUpdated()).isNotEqualTo(originalUpdated);
        Assertions.assertThat(a.getCreated()).isEqualTo(a.getUpdated());
    }

    /**
     * Test to make sure the update timestamp is updated by this method.
     *
     * @throws InterruptedException If the process is interrupted
     */
    @Test
    void testOnUpdateAuditEntity() throws InterruptedException {
        final AuditEntity a = new AuditEntity();
        Assertions.assertThat(a.getCreated()).isNotNull();
        Assertions.assertThat(a.getUpdated()).isNotNull();
        a.onCreateBaseEntity();
        final Instant originalCreated = a.getCreated();
        final Instant originalUpdated = a.getUpdated();
        Thread.sleep(1);
        a.onUpdateBaseEntity();
        Assertions.assertThat(a.getCreated()).isEqualTo(originalCreated);
        Assertions.assertThat(a.getUpdated()).isNotEqualTo(originalUpdated);
    }

    /**
     * Test the toString method.
     */
    @Test
    void testToString() {
        Assertions.assertThat(new AuditEntity().toString()).isNotBlank();
    }
}
