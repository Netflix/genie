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
 * Unit tests for JobsUsersProperties.
 *
 * @author tgianos
 * @since 3.0.0
 */
class JobsUsersPropertiesTest {
    private JobsUsersProperties properties;

    /**
     * Setup for the tests.
     */
    @BeforeEach
    void setup() {
        this.properties = new JobsUsersProperties();
    }

    /**
     * Make sure the constructor sets defaults.
     */
    @Test
    void canConstruct() {
        Assertions.assertThat(this.properties.isCreationEnabled()).isFalse();
        Assertions.assertThat(this.properties.isRunAsUserEnabled()).isFalse();
    }

    /**
     * Make sure we can set the creationEnabled field.
     */
    @Test
    void canSetCreationEnabled() {
        this.properties.setCreationEnabled(true);
        Assertions.assertThat(this.properties.isCreationEnabled()).isTrue();
    }

    /**
     * Make sure we can set the run as user field.
     */
    @Test
    void canSetRunAsEnabled() {
        this.properties.setRunAsUserEnabled(true);
        Assertions.assertThat(this.properties.isRunAsUserEnabled()).isTrue();
    }
}
