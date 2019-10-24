/*
 *
 *  Copyright 2015 Netflix, Inc.
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
package com.netflix.genie.common.dto;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

/**
 * Unit tests for the JobExecution class.
 *
 * @author tgianos
 * @since 3.0.0
 */
class JobExecutionTest {

    private static final String HOST_NAME = UUID.randomUUID().toString();
    private static final long CHECK_DELAY = 280843L;
    private static final int PROCESS_ID = 134234;
    private static final Instant TIMEOUT = Instant.now();
    private static final int MEMORY = 1_024;

    /**
     * Test to make sure can build a valid JobExecution using the builder.
     */
    @Test
    void canBuildJob() {
        final JobExecution execution = new JobExecution.Builder(HOST_NAME).build();
        Assertions.assertThat(execution.getHostName()).isEqualTo(HOST_NAME);
        Assertions.assertThat(execution.getProcessId().isPresent()).isFalse();
        Assertions.assertThat(execution.getCheckDelay().isPresent()).isFalse();
        Assertions.assertThat(execution.getTimeout().isPresent()).isFalse();
        Assertions.assertThat(execution.getExitCode().isPresent()).isFalse();
        Assertions.assertThat(execution.getCreated().isPresent()).isFalse();
        Assertions.assertThat(execution.getId().isPresent()).isFalse();
        Assertions.assertThat(execution.getUpdated().isPresent()).isFalse();
        Assertions.assertThat(execution.getMemory().isPresent()).isFalse();
    }

    /**
     * Test to make sure can build a valid JobExecution with optional parameters.
     */
    @Test
    void canBuildJobWithOptionals() {
        final JobExecution.Builder builder = new JobExecution.Builder(HOST_NAME);

        builder.withCheckDelay(CHECK_DELAY);
        builder.withProcessId(PROCESS_ID);
        builder.withTimeout(TIMEOUT);
        builder.withMemory(MEMORY);

        final int exitCode = 0;
        builder.withExitCode(exitCode);

        final Instant created = Instant.now();
        builder.withCreated(created);

        final String id = UUID.randomUUID().toString();
        builder.withId(id);

        final Instant updated = Instant.now();
        builder.withUpdated(updated);

        final JobExecution execution = builder.build();
        Assertions.assertThat(execution.getHostName()).isEqualTo(HOST_NAME);
        Assertions
            .assertThat(execution.getProcessId().orElseThrow(IllegalArgumentException::new))
            .isEqualTo(PROCESS_ID);
        Assertions
            .assertThat(execution.getCheckDelay().orElseThrow(IllegalArgumentException::new))
            .isEqualTo(CHECK_DELAY);
        Assertions.assertThat(execution.getTimeout().orElseThrow(IllegalArgumentException::new)).isEqualTo(TIMEOUT);
        Assertions.assertThat(execution.getExitCode().orElseThrow(IllegalArgumentException::new)).isEqualTo(exitCode);
        Assertions.assertThat(execution.getCreated().orElseThrow(IllegalArgumentException::new)).isEqualTo(created);
        Assertions.assertThat(execution.getId().orElseThrow(IllegalArgumentException::new)).isEqualTo(id);
        Assertions.assertThat(execution.getUpdated().orElseThrow(IllegalArgumentException::new)).isEqualTo(updated);
        Assertions.assertThat(execution.getMemory().orElseThrow(IllegalArgumentException::new)).isEqualTo(MEMORY);
    }

    /**
     * Test to make sure a JobExecution can be successfully built when nulls are inputted.
     */
    @Test
    void canBuildJobWithNulls() {
        final JobExecution.Builder builder = new JobExecution.Builder(HOST_NAME);
        builder.withExitCode(null);
        builder.withProcessId(null);
        builder.withCheckDelay(null);
        builder.withTimeout(null);
        builder.withMemory(null);
        builder.withCreated(null);
        builder.withId(null);
        builder.withUpdated(null);

        final JobExecution execution = builder.build();
        Assertions.assertThat(execution.getHostName()).isEqualTo(HOST_NAME);
        Assertions.assertThat(execution.getProcessId().isPresent()).isFalse();
        Assertions.assertThat(execution.getCheckDelay().isPresent()).isFalse();
        Assertions.assertThat(execution.getTimeout().isPresent()).isFalse();
        Assertions.assertThat(execution.getExitCode().isPresent()).isFalse();
        Assertions.assertThat(execution.getCreated().isPresent()).isFalse();
        Assertions.assertThat(execution.getId().isPresent()).isFalse();
        Assertions.assertThat(execution.getUpdated().isPresent()).isFalse();
        Assertions.assertThat(execution.getMemory().isPresent()).isFalse();
    }

    /**
     * Test equals.
     */
    @Test
    void canFindEquality() {
        final JobExecution.Builder builder = new JobExecution.Builder(HOST_NAME);
        builder.withCreated(null);
        builder.withId(UUID.randomUUID().toString());
        builder.withUpdated(null);

        final JobExecution jobExecution1 = builder.build();
        final JobExecution jobExecution2 = builder.build();
        builder.withId(UUID.randomUUID().toString());
        final JobExecution jobExecution3 = builder.build();

        Assertions.assertThat(jobExecution1).isEqualTo(jobExecution2);
        Assertions.assertThat(jobExecution1).isNotEqualTo(jobExecution3);
    }

    /**
     * Test hash code.
     */
    @Test
    void canUseHashCode() {
        final JobExecution.Builder builder = new JobExecution.Builder(HOST_NAME);
        builder.withCreated(null);
        builder.withId(UUID.randomUUID().toString());
        builder.withUpdated(null);

        final JobExecution jobExecution1 = builder.build();
        final JobExecution jobExecution2 = builder.build();
        builder.withId(UUID.randomUUID().toString());
        final JobExecution jobExecution3 = builder.build();

        Assertions.assertThat(jobExecution1.hashCode()).isEqualTo(jobExecution2.hashCode());
        Assertions.assertThat(jobExecution1.hashCode()).isNotEqualTo(jobExecution3.hashCode());
    }
}
