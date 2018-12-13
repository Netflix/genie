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

import com.netflix.genie.test.categories.UnitTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Unit test for ExponentialBackOffTriggerProperties.
 *
 * @author mprimi
 * @since 3.3.9
 */
@Category(UnitTest.class)
public class ExponentialBackOffTriggerPropertiesUnitTest {

    private ExponentialBackOffTriggerProperties properties;

    /**
     * Setup for the tests.
     */
    @Before
    public void setUp() {
        properties = new ExponentialBackOffTriggerProperties();
    }

    /**
     * Make sure the constructor sets defaults.
     */
    @Test
    public void canConstruct() {
        Assert.assertEquals(100, properties.getMinInterval());
        Assert.assertEquals(10_000, properties.getMaxInterval());
        Assert.assertEquals(1.2, properties.getFactor(), 0.001);
    }

    /**
     * Make sure we can set the minInterval field.
     */
    @Test
    public void canSetMinInterval() {
        this.properties.setMinInterval(1234);
        Assert.assertEquals(1234, this.properties.getMinInterval());
    }

    /**
     * Make sure we can set the maxInterval field.
     */
    @Test
    public void canSetMaxInterval() {
        this.properties.setMaxInterval(1234);
        Assert.assertEquals(1234, this.properties.getMaxInterval());
    }

    /**
     * Make sure we can set the factor field.
     */
    @Test
    public void canSetFactor() {
        this.properties.setFactor(2.4f);
        Assert.assertEquals(2.4f, this.properties.getFactor(), 0.001);
    }
}
