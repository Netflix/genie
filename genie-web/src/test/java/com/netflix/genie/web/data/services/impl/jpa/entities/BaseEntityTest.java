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
package com.netflix.genie.web.data.services.impl.jpa.entities;

import com.fasterxml.jackson.databind.JsonNode;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import jakarta.validation.ConstraintViolationException;
import java.util.UUID;

/**
 * Test the BaseEntity class and methods.
 *
 * @author tgianos
 */
class BaseEntityTest extends EntityTestBase {
    private static final String UNIQUE_ID = UUID.randomUUID().toString();
    private static final String NAME = "pig13";
    private static final String USER = "tgianos";
    private static final String VERSION = "1.0";
    private static final JsonNode METADATA = Mockito.mock(JsonNode.class);

    private BaseEntity b;

    /**
     * Setup the tests.
     */
    @BeforeEach
    void setup() {
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
    void defaultConstructor() {
        final BaseEntity local = new BaseEntity();
        Assertions.assertThat(local.getUniqueId()).isNotBlank();
        Assertions.assertThat(local.getName()).isNull();
        Assertions.assertThat(local.getUser()).isNull();
        Assertions.assertThat(local.getVersion()).isNull();
        Assertions.assertThat(local.getDescription()).isNotPresent();
        Assertions.assertThat(local.getSetupFile()).isNotPresent();
        Assertions.assertThat(local.isRequestedId()).isFalse();
    }

    /**
     * Test to make sure validation works.
     */
    @Test
    void validate() {
        this.validate(this.b);
    }

    /**
     * Test to make sure validation works.
     */
    @Test
    void validateWithNothing() {
        Assertions
            .assertThatExceptionOfType(ConstraintViolationException.class)
            .isThrownBy(() -> this.validate(new BaseEntity()));
    }

    /**
     * Test to make sure validation works and throws exception when no name entered.
     */
    @Test
    void validateNoName() {
        this.b.setName("");
        Assertions
            .assertThatExceptionOfType(ConstraintViolationException.class)
            .isThrownBy(() -> this.validate(this.b));
    }

    /**
     * Test to make sure validation works and throws exception when no name entered.
     */
    @Test
    void validateNoUser() {
        this.b.setUser("     ");
        Assertions
            .assertThatExceptionOfType(ConstraintViolationException.class)
            .isThrownBy(() -> this.validate(this.b));
    }

    /**
     * Test to make sure validation works and throws exception when no name entered.
     */
    @Test
    void validateNoVersion() {
        this.b.setVersion("");
        Assertions
            .assertThatExceptionOfType(ConstraintViolationException.class)
            .isThrownBy(() -> this.validate(this.b));
    }

    /**
     * Test the getting and setting of the unique id.
     */
    @Test
    void setUniqueId() {
        final BaseEntity local = new BaseEntity();
        Assertions.assertThat(local.getUniqueId()).isNotBlank();
        Assertions.assertThat(local.getUniqueId()).isNotEqualTo(UNIQUE_ID);
        local.setUniqueId(UNIQUE_ID);
        Assertions.assertThat(local.getUniqueId()).isEqualTo(UNIQUE_ID);
    }

    /**
     * Test to make sure the name is being set properly.
     */
    @Test
    void setName() {
        final BaseEntity local = new BaseEntity();
        Assertions.assertThat(local.getName()).isNull();
        local.setName(NAME);
        Assertions.assertThat(local.getName()).isEqualTo(NAME);
    }

    /**
     * Test to make sure the user is being set properly.
     */
    @Test
    void setUser() {
        final BaseEntity local = new BaseEntity();
        Assertions.assertThat(local.getUser()).isNull();
        local.setUser(USER);
        Assertions.assertThat(local.getUser()).isEqualTo(USER);
    }

    /**
     * Test to make sure the version is being set properly.
     */
    @Test
    void setVersion() {
        final BaseEntity local = new BaseEntity();
        Assertions.assertThat(local.getVersion()).isNull();
        local.setVersion(VERSION);
        Assertions.assertThat(local.getVersion()).isEqualTo(VERSION);
    }

    /**
     * Test the description get/set.
     */
    @Test
    void setDescription() {
        Assertions.assertThat(this.b.getDescription()).isNotPresent();
        final String description = "Test description";
        this.b.setDescription(description);
        Assertions.assertThat(this.b.getDescription()).isPresent().contains(description);
    }

    /**
     * Test the setup file get/set.
     */
    @Test
    void setSetupFile() {
        Assertions.assertThat(this.b.getSetupFile()).isNotPresent();
        final FileEntity setupFile = new FileEntity(UUID.randomUUID().toString());
        this.b.setSetupFile(setupFile);
        Assertions.assertThat(this.b.getSetupFile()).isPresent().contains(setupFile);
        this.b.setSetupFile(null);
        Assertions.assertThat(this.b.getSetupFile()).isNotPresent();
    }

    /**
     * Test the metadata setter and getter.
     */
    @Test
    void setMetadata() {
        Assertions.assertThat(this.b.getMetadata()).isNotPresent();
        this.b.setMetadata(METADATA);
        Assertions.assertThat(this.b.getMetadata()).isPresent().contains(METADATA);
        this.b.setMetadata(null);
        Assertions.assertThat(this.b.getMetadata()).isNotPresent();
    }

    /**
     * Test the is requested id fields.
     */
    @Test
    void setRequestedId() {
        Assertions.assertThat(this.b.isRequestedId()).isFalse();
        this.b.setRequestedId(true);
        Assertions.assertThat(this.b.isRequestedId()).isTrue();
    }

    /**
     * Test to make sure equals and hash code only care about the unique id.
     */
    @Test
    void equalsAndHashCode() {
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

        Assertions.assertThat(one).isEqualTo(two);
        Assertions.assertThat(one).isNotEqualTo(three);
        Assertions.assertThat(two).isNotEqualTo(three);

        Assertions.assertThat(one.hashCode()).isEqualTo(two.hashCode());
        Assertions.assertThat(one.hashCode()).isNotEqualTo(three.hashCode());
        Assertions.assertThat(two.hashCode()).isNotEqualTo(three.hashCode());
    }

    /**
     * Test the toString method.
     */
    @Test
    void testToString() {
        Assertions.assertThat(this.b.toString()).isNotBlank();
    }
}
