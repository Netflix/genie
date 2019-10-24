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

/**
 * Unit tests for the properties holder class.
 *
 * @author tgianos
 * @since 3.0.0
 */
class JobsMemoryPropertiesTest {

    private JobsMemoryProperties properties;

    /**
     * Setup for tests.
     */
    @BeforeEach
    void setup() {
        this.properties = new JobsMemoryProperties();
    }

    /**
     * Make sure we have the default properties.
     */
    @Test
    void hasDefaultProperties() {
        Assertions.assertThat(this.properties.getDefaultJobMemory()).isEqualTo(1_024);
        Assertions.assertThat(this.properties.getMaxJobMemory()).isEqualTo(10_240);
        Assertions.assertThat(this.properties.getMaxSystemMemory()).isEqualTo(30_720);
    }

    /**
     * Make sure can set the default job memory.
     */
    @Test
    void canSetDefaultJobMemory() {
        final int memory = 1_512;
        this.properties.setDefaultJobMemory(memory);
        Assertions.assertThat(this.properties.getDefaultJobMemory()).isEqualTo(memory);
    }

    /**
     * Make sure can set the max job memory.
     */
    @Test
    void canSetMaxJobMemory() {
        final int memory = 1_512;
        this.properties.setMaxJobMemory(memory);
        Assertions.assertThat(this.properties.getMaxJobMemory()).isEqualTo(memory);
    }

    /**
     * Make sure can set the max system memory.
     */
    @Test
    void canSetMaxSystemMemory() {
        final int memory = 1_512;
        this.properties.setMaxSystemMemory(memory);
        Assertions.assertThat(this.properties.getMaxSystemMemory()).isEqualTo(memory);
    }
}
