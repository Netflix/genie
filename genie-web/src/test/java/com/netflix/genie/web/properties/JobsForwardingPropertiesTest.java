/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.genie.web.properties;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

/**
 * Unit tests for JobForwardingProperties.
 *
 * @author tgianos
 * @since 3.0.0
 */
class JobsForwardingPropertiesTest {

    private JobsForwardingProperties properties;

    /**
     * Setup for the tests.
     */
    @BeforeEach
    void setup() {
        this.properties = new JobsForwardingProperties();
    }

    /**
     * Test to make sure default constructor sets default values.
     */
    @Test
    void hasDefaultValues() {
        Assertions.assertThat(this.properties.isEnabled()).isFalse();
        Assertions.assertThat(this.properties.getScheme()).isEqualTo("http");
        Assertions.assertThat(this.properties.getPort()).isEqualTo(8080);
    }

    /**
     * Make sure setting the enabled property is persisted.
     */
    @Test
    void canEnable() {
        this.properties.setEnabled(true);
        Assertions.assertThat(this.properties.isEnabled()).isTrue();
    }

    /**
     * Make sure setting the scheme property is persisted.
     */
    @Test
    void canSetScheme() {
        final String scheme = UUID.randomUUID().toString();
        this.properties.setScheme(scheme);
        Assertions.assertThat(this.properties.getScheme()).isEqualTo(scheme);
    }

    /**
     * Make sure setting the port property is persisted.
     */
    @Test
    void canSetPort() {
        final int port = 443;
        this.properties.setPort(port);
        Assertions.assertThat(this.properties.getPort()).isEqualTo(port);
    }
}
