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
package com.netflix.genie.core.properties;

import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Unit tests for the properties holder class.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class JobsMemoryPropertiesUnitTests {

    private JobsMemoryProperties properties;

    /**
     * Setup for tests.
     */
    @Before
    public void setup() {
        this.properties = new JobsMemoryProperties();
    }

    /**
     * Make sure we have the default properties.
     */
    @Test
    public void hasDefaultProperties() {
        Assert.assertThat(this.properties.getDefaultJobMemory(), Matchers.is(1_024));
        Assert.assertThat(this.properties.getMaxJobMemory(), Matchers.is(10_240));
        Assert.assertThat(this.properties.getMaxSystemMemory(), Matchers.is(30_720));
    }

    /**
     * Make sure can set the default job memory.
     */
    @Test
    public void canSetDefaultJobMemory() {
        final int memory = 1_512;
        this.properties.setDefaultJobMemory(memory);
        Assert.assertThat(this.properties.getDefaultJobMemory(), Matchers.is(memory));
    }

    /**
     * Make sure can set the max job memory.
     */
    @Test
    public void canSetMaxJobMemory() {
        final int memory = 1_512;
        this.properties.setMaxJobMemory(memory);
        Assert.assertThat(this.properties.getMaxJobMemory(), Matchers.is(memory));
    }

    /**
     * Make sure can set the max system memory.
     */
    @Test
    public void canSetMaxSystemMemory() {
        final int memory = 1_512;
        this.properties.setMaxSystemMemory(memory);
        Assert.assertThat(this.properties.getMaxSystemMemory(), Matchers.is(memory));
    }
}
