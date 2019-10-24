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
import org.mockito.Mockito;
import org.springframework.core.env.Environment;

/**
 * Unit tests for JobsActiveLimitProperties.
 *
 * @author mprimi
 * @since 3.1.0
 */
class JobsActiveLimitPropertiesTest {
    private JobsActiveLimitProperties properties;

    /**
     * Setup for the tests.
     */
    @BeforeEach
    void setup() {
        this.properties = new JobsActiveLimitProperties();
    }

    /**
     * Make sure the constructor sets defaults.
     */
    @Test
    void canConstruct() {
        Assertions.assertThat(this.properties.isEnabled()).isEqualTo(JobsActiveLimitProperties.DEFAULT_ENABLED);
        Assertions.assertThat(this.properties.getCount()).isEqualTo(JobsActiveLimitProperties.DEFAULT_COUNT);
        Assertions
            .assertThat(this.properties.getUserLimit("SomeUser"))
            .isEqualTo(JobsActiveLimitProperties.DEFAULT_COUNT);
    }

    /**
     * Make sure we can set the enabled field.
     */
    @Test
    void canSetEnabled() {
        final boolean newEnabledValue = !this.properties.isEnabled();
        this.properties.setEnabled(newEnabledValue);
        Assertions.assertThat(this.properties.isEnabled()).isEqualTo(newEnabledValue);
    }

    /**
     * Make sure we can set the count field.
     */
    @Test
    void canSetRunAsEnabled() {
        final int newCountValue = 2 * this.properties.getCount();
        this.properties.setCount(newCountValue);
        Assertions.assertThat(this.properties.getCount()).isEqualTo(newCountValue);
    }

    /**
     * Make sure environment is used when looking for a user-specific limit override.
     */
    @Test
    void testUserOverrides() {
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

        Assertions.assertThat(this.properties.getUserLimit(userName)).isEqualTo(userLimit);
    }
}
