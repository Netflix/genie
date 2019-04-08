/*
 *
 *  Copyright 2019 Netflix, Inc.
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

import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Unit tests for UserMetricsProperties.
 *
 * @author mprimi
 * @since 3.3.19
 */
@Category(UnitTest.class)
public class UserMetricsPropertiesUnitTest {

    private UserMetricsProperties properties;

    /**
     * Setup for tests.
     */
    @Before
    public void setup() {
        this.properties = new UserMetricsProperties();
    }

    /**
     * Test default values.
     */
    @Test
    public void testDefaults() {
        Assert.assertThat(properties.isEnabled(), Matchers.is(true));
        Assert.assertThat(properties.getRate(), Matchers.is(5000L));
    }

    /**
     * Test setting and getting values.
     */
    @Test
    public void testSetGet() {
        properties.setEnabled(false);
        properties.setRate(10000);
        Assert.assertThat(properties.isEnabled(), Matchers.is(false));
        Assert.assertThat(properties.getRate(), Matchers.is(10000L));
    }

}
