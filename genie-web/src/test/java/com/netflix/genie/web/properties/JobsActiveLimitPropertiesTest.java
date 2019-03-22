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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.core.env.Environment;

/**
 * Unit tests for JobsActiveLimitProperties.
 *
 * @author mprimi
 * @since 3.1.0
 */
public class JobsActiveLimitPropertiesTest {
    private JobsActiveLimitProperties properties;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.properties = new JobsActiveLimitProperties();
    }

    /**
     * Make sure the constructor sets defaults.
     */
    @Test
    public void canConstruct() {
        Assert.assertEquals(JobsActiveLimitProperties.DEFAULT_ENABLED, this.properties.isEnabled());
        Assert.assertEquals(JobsActiveLimitProperties.DEFAULT_COUNT, this.properties.getCount());
        Assert.assertEquals(JobsActiveLimitProperties.DEFAULT_COUNT, this.properties.getUserLimit("SomeUser"));
    }

    /**
     * Make sure we can set the enabled field.
     */
    @Test
    public void canSetEnabled() {
        final boolean newEnabledValue = !this.properties.isEnabled();
        this.properties.setEnabled(newEnabledValue);
        Assert.assertEquals(newEnabledValue, this.properties.isEnabled());
    }

    /**
     * Make sure we can set the count field.
     */
    @Test
    public void canSetRunAsEnabled() {
        final int newCountValue = 2 * this.properties.getCount();
        this.properties.setCount(newCountValue);
        Assert.assertEquals(newCountValue, this.properties.getCount());
    }

    /**
     * Make sure environment is used when looking for a user-specific limit override.
     */
    @Test
    public void testUserOverrides() {
        final String userName = "SomeUser";
        final int userLimit = 999;
        final Environment environment = Mockito.mock(Environment.class);
        Mockito
            .when(environment.getProperty(
                JobsActiveLimitProperties.USER_LIMIT_OVERRIDE_PROPERTY_PREFIX + userName,
                Integer.class,
                this.properties.getCount())
            )
            .thenReturn(userLimit);
        this.properties.setEnvironment(environment);

        Assert.assertEquals(userLimit, this.properties.getUserLimit(userName));
    }
}
