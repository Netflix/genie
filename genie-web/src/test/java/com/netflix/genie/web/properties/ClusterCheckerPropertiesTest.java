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
 * Tests for ClusterCheckerProperties.
 *
 * @author tgianos
 * @since 3.0.0
 */
class ClusterCheckerPropertiesTest {

    private ClusterCheckerProperties properties;

    /**
     * Setup for the tests.
     */
    @BeforeEach
    void setup() {
        this.properties = new ClusterCheckerProperties();
    }

    /**
     * Make sure we get reasonable default properties.
     */
    @Test
    void canConstructWithValidProperties() {
        Assertions.assertThat(this.properties.getPort()).isEqualTo(8080);
        Assertions.assertThat(this.properties.getScheme()).isEqualTo("http");
        Assertions.assertThat(this.properties.getLostThreshold()).isEqualTo(3);
        Assertions.assertThat(this.properties.getRate()).isEqualTo(300000L);
    }

    /**
     * Make sure we can set the port.
     */
    @Test
    void canSetPort() {
        final int port = 7001;
        this.properties.setPort(port);
        Assertions.assertThat(this.properties.getPort()).isEqualTo(port);
    }

    /**
     * Make sure we can set the scheme.
     */
    @Test
    void canSetScheme() {
        final String scheme = UUID.randomUUID().toString();
        this.properties.setScheme(scheme);
        Assertions.assertThat(this.properties.getScheme()).isEqualTo(scheme);
    }

    /**
     * Make sure we can set the lost threshold.
     */
    @Test
    void canSetLostThreshold() {
        final int lostThreshold = 89;
        this.properties.setLostThreshold(lostThreshold);
        Assertions.assertThat(this.properties.getLostThreshold()).isEqualTo(lostThreshold);
    }

    /**
     * Make sure we can set the check rate.
     */
    @Test
    void canSetRate() {
        final long rate = 808283L;
        this.properties.setRate(rate);
        Assertions.assertThat(this.properties.getRate()).isEqualTo(rate);
    }
}
