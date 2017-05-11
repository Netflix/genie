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

import com.netflix.genie.core.properties.JobsMemoryProperties;
import com.netflix.genie.core.properties.JobsProperties;
import com.netflix.genie.core.services.JobMetricsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import javax.validation.constraints.NotNull;

/**
 * A health indicator based around metrics from the Genie system.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Component
public class GenieMemoryHealthIndicator implements HealthIndicator {

    private static final String NUMBER_RUNNING_JOBS_KEY = "numRunningJobs";
    private static final String USED_MEMORY_KEY = "usedMemory";
    private static final String AVAILABLE_MEMORY = "availableMemory";
    private static final String AVAILABLE_DEFAULT_JOB_CAPACITY = "availableDefaultJobCapacity";
    private static final String AVAILABLE_MAX_JOB_CAPACITY = "availableMaxJobCapacity";

    private final JobMetricsService jobMetricsService;
    private final JobsProperties jobsProperties;

    /**
     * Constructor.
     *
     * @param jobMetricsService The metrics service to get status info from
     * @param jobsProperties    The various properties related to running jobs
     */
    @Autowired
    public GenieMemoryHealthIndicator(
        @NotNull final JobMetricsService jobMetricsService,
        @NotNull final JobsProperties jobsProperties
    ) {
        this.jobMetricsService = jobMetricsService;
        this.jobsProperties = jobsProperties;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Health health() {
        final int usedMemory = this.jobMetricsService.getUsedMemory();
        final JobsMemoryProperties memoryProperties = this.jobsProperties.getMemory();
        final int availableMemory = memoryProperties.getMaxSystemMemory() - usedMemory;
        final int maxJobMemory = memoryProperties.getMaxJobMemory();
        final int defaultJobMemory = memoryProperties.getDefaultJobMemory();

        final Health.Builder builder;

        // If we can fit one more max job in we're still healthy
        if (availableMemory >= maxJobMemory) {
            builder = Health.up();
        } else {
            builder = Health.outOfService();
        }

        return builder
            .withDetail(NUMBER_RUNNING_JOBS_KEY, this.jobMetricsService.getNumActiveJobs())
            .withDetail(USED_MEMORY_KEY, usedMemory)
            .withDetail(AVAILABLE_MEMORY, availableMemory)
            .withDetail(
                AVAILABLE_DEFAULT_JOB_CAPACITY,
                (availableMemory >= 0 && defaultJobMemory > 0) ? (availableMemory / defaultJobMemory) : 0)
            .withDetail(
                AVAILABLE_MAX_JOB_CAPACITY,
                (availableMemory >= 0 && maxJobMemory > 0) ? (availableMemory / maxJobMemory) : 0)
            .build();
    }
}
