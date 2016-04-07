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

import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.UUID;

/**
 * Unit tests for JobForwardingProperties.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class JobForwardingPropertiesUnitTests {

    private JobForwardingProperties properties;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.properties = new JobForwardingProperties();
    }

    /**
     * Test to make sure default constructor sets default values.
     */
    @Test
    public void hasDefaultValues() {
        Assert.assertFalse(this.properties.isEnabled());
        Assert.assertThat(this.properties.getScheme(), Matchers.is("http"));
        Assert.assertThat(this.properties.getPort(), Matchers.is(8080));
    }

    /**
     * Make sure setting the enabled property is persisted.
     */
    @Test
    public void canEnable() {
        this.properties.setEnabled(true);
        Assert.assertTrue(this.properties.isEnabled());
    }

    /**
     * Make sure setting the scheme property is persisted.
     */
    @Test
    public void canSetScheme() {
        final String scheme = UUID.randomUUID().toString();
        this.properties.setScheme(scheme);
        Assert.assertThat(this.properties.getScheme(), Matchers.is(scheme));
    }

    /**
     * Make sure setting the port property is persisted.
     */
    @Test
    public void canSetPort() {
        final int port = 443;
        this.properties.setPort(port);
        Assert.assertThat(this.properties.getPort(), Matchers.is(port));
    }
}
