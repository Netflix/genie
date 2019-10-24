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

import java.util.UUID;

/**
 * Unit tests for DatabaseCleanupProperties.
 *
 * @author tgianos
 * @since 3.0.0
 */
class DatabaseCleanupPropertiesTest {

    private DatabaseCleanupProperties properties;

    /**
     * Setup for tests.
     */
    @BeforeEach
    void setup() {
        this.properties = new DatabaseCleanupProperties();
    }

    /**
     * Make sure constructor sets reasonable defaults.
     */
    @Test
    void canGetDefaultValues() {
        Assertions.assertThat(this.properties.isEnabled()).isFalse();
        Assertions.assertThat(this.properties.getExpression()).isEqualTo("0 0 0 * * *");
        Assertions.assertThat(this.properties.getRetention()).isEqualTo(90);
        Assertions.assertThat(this.properties.getMaxDeletedPerTransaction()).isEqualTo(1000);
        Assertions.assertThat(this.properties.getPageSize()).isEqualTo(1000);
        Assertions.assertThat(this.properties.isSkipJobsCleanup()).isFalse();
        Assertions.assertThat(this.properties.isSkipClustersCleanup()).isFalse();
        Assertions.assertThat(this.properties.isSkipTagsCleanup()).isFalse();
        Assertions.assertThat(this.properties.isSkipFilesCleanup()).isFalse();
    }

    /**
     * Make sure can enable.
     */
    @Test
    void canEnable() {
        this.properties.setEnabled(true);
        Assertions.assertThat(this.properties.isEnabled()).isTrue();
    }

    /**
     * Make sure can set a new cron expression.
     */
    @Test
    void canSetExpression() {
        final String expression = UUID.randomUUID().toString();
        this.properties.setExpression(expression);
        Assertions.assertThat(this.properties.getExpression()).isEqualTo(expression);
    }

    /**
     * Make sure can set a new retention time.
     */
    @Test
    void canSetRetention() {
        final int retention = 2318;
        this.properties.setRetention(retention);
        Assertions.assertThat(this.properties.getRetention()).isEqualTo(retention);
    }

    /**
     * Make sure can set a max deletion batch size.
     */
    @Test
    void canSetMaxDeletedPerTransaction() {
        final int max = 2318;
        this.properties.setMaxDeletedPerTransaction(max);
        Assertions.assertThat(this.properties.getMaxDeletedPerTransaction()).isEqualTo(max);
    }

    /**
     * Make sure can set a new page size.
     */
    @Test
    void canPageSize() {
        final int size = 2318;
        this.properties.setPageSize(size);
        Assertions.assertThat(this.properties.getPageSize()).isEqualTo(size);
    }

    /**
     * Make sure can enable Jobs entities cleanup.
     */
    @Test
    void canEnableJobsCleanup() {
        this.properties.setSkipJobsCleanup(true);
        Assertions.assertThat(this.properties.isSkipJobsCleanup()).isTrue();
    }

    /**
     * Make sure can enable Clusters entities cleanup.
     */
    @Test
    void canEnableClustersCleanup() {
        this.properties.setSkipClustersCleanup(true);
        Assertions.assertThat(this.properties.isSkipClustersCleanup()).isTrue();
    }

    /**
     * Make sure can enable Tags entities cleanup.
     */
    @Test
    void canEnableTagsCleanup() {
        this.properties.setSkipTagsCleanup(true);
        Assertions.assertThat(this.properties.isSkipTagsCleanup()).isTrue();
    }

    /**
     * Make sure can enable Files entities cleanup.
     */
    @Test
    void canEnableFilesCleanup() {
        this.properties.setSkipFilesCleanup(true);
        Assertions.assertThat(this.properties.isSkipFilesCleanup()).isTrue();
    }
}
