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
 * Unit tests for {@link DatabaseCleanupProperties}.
 *
 * @author tgianos
 * @since 3.0.0
 */
class DatabaseCleanupPropertiesTest {

    private DatabaseCleanupProperties properties;

    @BeforeEach
    void setup() {
        this.properties = new DatabaseCleanupProperties();
    }

    @Test
    void canGetDefaultValues() {
        Assertions.assertThat(this.properties.isEnabled()).isFalse();
        Assertions.assertThat(this.properties.getExpression()).isEqualTo("0 0 0 * * *");
        Assertions.assertThat(this.properties.getBatchSize()).isEqualTo(10_000);
        Assertions.assertThat(this.properties.getApplicationCleanup().isSkip()).isFalse();
        Assertions.assertThat(this.properties.getCommandCleanup().isSkip()).isFalse();
        Assertions.assertThat(this.properties.getCommandDeactivation().isSkip()).isFalse();
        Assertions.assertThat(this.properties.getCommandDeactivation().getCommandCreationThreshold()).isEqualTo(60);
        Assertions.assertThat(this.properties.getJobCleanup().isSkip()).isFalse();
        Assertions.assertThat(this.properties.getJobCleanup().getRetention()).isEqualTo(90);
        Assertions.assertThat(this.properties.getJobCleanup().getMaxDeletedPerTransaction()).isEqualTo(1000);
        Assertions.assertThat(this.properties.getJobCleanup().getPageSize()).isEqualTo(1000);
        Assertions.assertThat(this.properties.getClusterCleanup().isSkip()).isFalse();
        Assertions.assertThat(this.properties.getTagCleanup().isSkip()).isFalse();
        Assertions.assertThat(this.properties.getFileCleanup().isSkip()).isFalse();
        Assertions.assertThat(this.properties.getFileCleanup().getBatchDaysWithin()).isEqualTo(30);
        Assertions.assertThat(this.properties.getFileCleanup().getRollingWindowHours()).isEqualTo(12);
    }

    @Test
    void canEnable() {
        this.properties.setEnabled(true);
        Assertions.assertThat(this.properties.isEnabled()).isTrue();
    }

    @Test
    void canSetExpression() {
        final String expression = UUID.randomUUID().toString();
        this.properties.setExpression(expression);
        Assertions.assertThat(this.properties.getExpression()).isEqualTo(expression);
    }


    @Test
    void canSetBatchSize() {
        final int batchSize = 123;
        this.properties.setBatchSize(batchSize);
        Assertions.assertThat(this.properties.getBatchSize()).isEqualTo(batchSize);
    }

    @Test
    void canSetJobCleanupRetention() {
        final int retention = 2318;
        this.properties.getJobCleanup().setRetention(retention);
        Assertions.assertThat(this.properties.getJobCleanup().getRetention()).isEqualTo(retention);
    }

    @Test
    void canSetJobCleanupMaxDeletedPerTransaction() {
        final int max = 2318;
        this.properties.getJobCleanup().setMaxDeletedPerTransaction(max);
        Assertions.assertThat(this.properties.getJobCleanup().getMaxDeletedPerTransaction()).isEqualTo(max);
    }

    @Test
    void canSetJobCleanupPageSize() {
        final int size = 2318;
        this.properties.getJobCleanup().setPageSize(size);
        Assertions.assertThat(this.properties.getJobCleanup().getPageSize()).isEqualTo(size);
    }

    @Test
    void canSetSkipJobCleanup() {
        this.properties.getJobCleanup().setSkip(true);
        Assertions.assertThat(this.properties.getJobCleanup().isSkip()).isTrue();
    }

    @Test
    void canSetSkipClusterCleanup() {
        this.properties.getClusterCleanup().setSkip(true);
        Assertions.assertThat(this.properties.getClusterCleanup().isSkip()).isTrue();
    }

    @Test
    void canSetSkipTagsCleanup() {
        this.properties.getTagCleanup().setSkip(true);
        Assertions.assertThat(this.properties.getTagCleanup().isSkip()).isTrue();
    }

    @Test
    void caSetSkipFilesCleanup() {
        this.properties.getFileCleanup().setSkip(true);
        Assertions.assertThat(this.properties.getFileCleanup().isSkip()).isTrue();
    }

    @Test
    void canSetSkipApplicationsCleanup() {
        this.properties.getApplicationCleanup().setSkip(true);
        Assertions.assertThat(this.properties.getApplicationCleanup().isSkip()).isTrue();
    }

    @Test
    void canEnableSkipCommandsCleanup() {
        this.properties.getCommandCleanup().setSkip(true);
        Assertions.assertThat(this.properties.getCommandCleanup().isSkip()).isTrue();
    }

    @Test
    void canEnableSkipCommandDeactivation() {
        this.properties.getCommandDeactivation().setSkip(true);
        Assertions.assertThat(this.properties.getCommandDeactivation().isSkip()).isTrue();
    }

    @Test
    void canSetCommandDeactivationCommandCreationThreshold() {
        final int newThreshold = this.properties.getCommandDeactivation().getCommandCreationThreshold() + 1;
        this.properties.getCommandDeactivation().setCommandCreationThreshold(newThreshold);
        Assertions
            .assertThat(this.properties.getCommandDeactivation().getCommandCreationThreshold())
            .isEqualTo(newThreshold);
    }
}
