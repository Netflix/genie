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
 * Unit tests for DatabaseCleanupProperties.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class DatabaseCleanupPropertiesUnitTests {

    private DatabaseCleanupProperties properties;

    /**
     * Setup for tests.
     */
    @Before
    public void setup() {
        this.properties = new DatabaseCleanupProperties();
    }

    /**
     * Make sure constructor sets reasonable defaults.
     */
    @Test
    public void canGetDefaultValues() {
        Assert.assertFalse(this.properties.isEnabled());
        Assert.assertThat(this.properties.getExpression(), Matchers.is("0 0 0 * * *"));
        Assert.assertThat(this.properties.getRetention(), Matchers.is(90));
    }

    /**
     * Make sure can enable.
     */
    @Test
    public void canEnable() {
        this.properties.setEnabled(true);
        Assert.assertTrue(this.properties.isEnabled());
    }

    /**
     * Make sure can set a new cron expression.
     */
    @Test
    public void canSetExpression() {
        final String expression = UUID.randomUUID().toString();
        this.properties.setExpression(expression);
        Assert.assertThat(this.properties.getExpression(), Matchers.is(expression));
    }

    /**
     * Make sure can set a new retention time.
     */
    @Test
    public void canSetRetention() {
        final int retention = 2318;
        this.properties.setRetention(retention);
        Assert.assertThat(this.properties.getRetention(), Matchers.is(retention));
    }
}
