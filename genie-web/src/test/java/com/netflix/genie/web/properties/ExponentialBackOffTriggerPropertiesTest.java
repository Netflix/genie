/*
 *
 *  Copyright 2018 Netflix, Inc.
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
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit test for ExponentialBackOffTriggerProperties.
 *
 * @author mprimi
 * @since 3.3.9
 */
class ExponentialBackOffTriggerPropertiesTest {

    private ExponentialBackOffTriggerProperties properties;

    /**
     * Setup for the tests.
     */
    @BeforeEach
    void setUp() {
        properties = new ExponentialBackOffTriggerProperties();
    }

    /**
     * Make sure the constructor sets defaults.
     */
    @Test
    void canConstruct() {
        Assertions.assertThat(this.properties.getMinInterval()).isEqualTo(100L);
        Assertions.assertThat(this.properties.getMaxInterval()).isEqualTo(10_000L);
        Assertions.assertThat(this.properties.getFactor()).isCloseTo(1.2f, Offset.offset(0.001f));
    }

    /**
     * Make sure we can set the minInterval field.
     */
    @Test
    void canSetMinInterval() {
        this.properties.setMinInterval(1_234L);
        Assertions.assertThat(this.properties.getMinInterval()).isEqualTo(1_234L);
    }

    /**
     * Make sure we can set the maxInterval field.
     */
    @Test
    void canSetMaxInterval() {
        this.properties.setMaxInterval(1_234L);
        Assertions.assertThat(this.properties.getMaxInterval()).isEqualTo(1_234L);
    }

    /**
     * Make sure we can set the factor field.
     */
    @Test
    void canSetFactor() {
        this.properties.setFactor(2.4f);
        Assertions.assertThat(this.properties.getFactor()).isCloseTo(2.4f, Offset.offset(0.001f));
    }
}
