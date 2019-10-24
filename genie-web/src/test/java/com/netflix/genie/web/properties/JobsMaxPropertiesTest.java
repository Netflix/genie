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
 * Unit tests for JobMaxProperties.
 *
 * @author tgianos
 * @since 3.0.0
 */
class JobsMaxPropertiesTest {

    private JobsMaxProperties properties;

    /**
     * Setup for the tests.
     */
    @BeforeEach
    void setup() {
        this.properties = new JobsMaxProperties();
    }

    /**
     * Make sure the default properties are set.
     */
    @Test
    void canConstruct() {
        Assertions.assertThat(this.properties.getStdOutSize()).isEqualTo(8_589_934_592L);
        Assertions.assertThat(this.properties.getStdErrSize()).isEqualTo(8_589_934_592L);
    }

    /**
     * Make sure can set and get the std out variable.
     */
    @Test
    void canSetStdOut() {
        final long newStdOut = 180_234L;
        this.properties.setStdOutSize(newStdOut);
        Assertions.assertThat(this.properties.getStdOutSize()).isEqualTo(newStdOut);
    }

    /**
     * Make sure can set and get the std err variable.
     */
    @Test
    void canSetStdErr() {
        final long newStdErr = 180234L;
        this.properties.setStdErrSize(newStdErr);
        Assertions.assertThat(this.properties.getStdErrSize()).isEqualTo(newStdErr);
    }
}
