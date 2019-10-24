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
 * Unit tests for the cleanup properties.
 *
 * @author tgianos
 * @since 3.0.0
 */
class JobsCleanupPropertiesTest {

    private JobsCleanupProperties properties;

    /**
     * Setup for tests.
     */
    @BeforeEach
    void setup() {
        this.properties = new JobsCleanupProperties();
    }

    /**
     * Make sure properties are true by default.
     */
    @Test
    void canConstruct() {
        Assertions.assertThat(this.properties.isDeleteDependencies()).isTrue();
    }

    /**
     * Make sure can set whether to delete the delete the dependencies.
     */
    @Test
    void canSetDeleteDependencies() {
        this.properties.setDeleteDependencies(false);
        Assertions.assertThat(this.properties.isDeleteDependencies()).isFalse();
    }
}
