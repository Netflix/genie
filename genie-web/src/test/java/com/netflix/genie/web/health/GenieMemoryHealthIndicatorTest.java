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
package com.netflix.genie.web.health;

import com.netflix.genie.web.properties.JobsProperties;
import com.netflix.genie.web.services.JobMetricsService;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.actuate.health.Status;

import java.util.Map;

/**
 * Unit tests for GenieHealthIndicator.
 *
 * @author tgianos
 * @since 3.0.0
 */
class GenieMemoryHealthIndicatorTest {

    private static final int MAX_SYSTEM_MEMORY = 10_240;
    private static final int DEFAULT_JOB_MEMORY = 1_024;
    private static final int MAX_JOB_MEMORY = 5_120;

    private GenieMemoryHealthIndicator genieMemoryHealthIndicator;
    private JobMetricsService jobMetricsService;
    private JobsProperties jobsProperties;

    /**
     * Setup for the tests.
     */
    @BeforeEach
    void setup() {
        this.jobMetricsService = Mockito.mock(JobMetricsService.class);
        this.jobsProperties = JobsProperties.getJobsPropertiesDefaults();
        this.jobsProperties.getMemory().setDefaultJobMemory(DEFAULT_JOB_MEMORY);
        this.jobsProperties.getMemory().setMaxSystemMemory(MAX_SYSTEM_MEMORY);
        this.jobsProperties.getMemory().setMaxJobMemory(MAX_JOB_MEMORY);

        this.genieMemoryHealthIndicator = new GenieMemoryHealthIndicator(this.jobMetricsService, jobsProperties);
    }

    /**
     * Test to make sure the various health conditions are met.
     */
    @Test
    void canGetHealth() {
        Mockito
            .when(this.jobMetricsService.getNumActiveJobs())
            .thenReturn(1, 2, 3);
        Mockito
            .when(this.jobMetricsService.getUsedMemory())
            .thenReturn(1024, 2048, MAX_SYSTEM_MEMORY - MAX_JOB_MEMORY + 1);
        Assertions.assertThat(this.genieMemoryHealthIndicator.health().getStatus()).isEqualTo(Status.UP);
        Assertions.assertThat(this.genieMemoryHealthIndicator.health().getStatus()).isEqualTo(Status.UP);
        Assertions.assertThat(this.genieMemoryHealthIndicator.health().getStatus()).isEqualTo(Status.DOWN);
    }

    /**
     * Test to make sure invalid property values for job memory default/max don't result in exceptions.
     */
    @Test
    void canGetHealthWithInvalidJobSettings() {
        this.jobsProperties.getMemory().setDefaultJobMemory(0);
        this.jobsProperties.getMemory().setMaxJobMemory(0);
        final Map<String, Object> healthDetails = this.genieMemoryHealthIndicator.health().getDetails();
        Assertions.assertThat(healthDetails.get("availableDefaultJobCapacity")).isEqualTo(0);
        Assertions.assertThat(healthDetails.get("availableMaxJobCapacity")).isEqualTo(0);
    }

    /**
     * Test to make at full capacity we don't show a negative capacity.
     */
    @Test
    void nonNegativeJobsCapacity() {
        Mockito.when(this.jobMetricsService.getUsedMemory()).thenReturn(MAX_SYSTEM_MEMORY + 100);
        final Map<String, Object> healthDetails = this.genieMemoryHealthIndicator.health().getDetails();
        Assertions.assertThat(healthDetails.get("availableDefaultJobCapacity")).isEqualTo(0);
        Assertions.assertThat(healthDetails.get("availableMaxJobCapacity")).isEqualTo(0);
    }

}
