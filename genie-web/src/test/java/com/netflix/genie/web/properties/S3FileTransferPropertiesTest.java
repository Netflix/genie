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
 * Unit tests for S3FileTransferProperties.
 *
 * @author mprimi
 * @since 3.1.0
 */
class S3FileTransferPropertiesTest {

    private S3FileTransferProperties properties;

    /**
     * Setup for tests.
     */
    @BeforeEach
    void setup() {
        this.properties = new S3FileTransferProperties();
    }

    /**
     * Make sure constructor sets reasonable defaults.
     */
    @Test
    void canGetDefaultValues() {
        Assertions.assertThat(this.properties.isStrictUrlCheckEnabled()).isFalse();
    }

    /**
     * Make sure can enable strict URL checking.
     */
    @Test
    void canEnableStrictUrlChecking() {
        this.properties.setStrictUrlCheckEnabled(true);
        Assertions.assertThat(this.properties.isStrictUrlCheckEnabled()).isTrue();
    }
}
