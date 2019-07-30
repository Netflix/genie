/*
 *
 *  Copyright 2019 Netflix, Inc.
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

import com.google.common.annotations.VisibleForTesting;
import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.web.data.services.JobSearchService;
import com.netflix.genie.web.properties.LocalAgentLauncherProperties;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

/**
 * Health indicator based on data related to the health of the systems ability to launch more agent processes locally.
 * health.
 *
 * @author tgianos
 * @since 4.0.0
 */
public class LocalAgentLauncherHealthIndicator implements HealthIndicator {

    @VisibleForTesting
    static final String NUMBER_RUNNING_JOBS_KEY = "numRunningJobs";
    @VisibleForTesting
    static final String ALLOCATED_MEMORY_KEY = "allocatedMemory";
    @VisibleForTesting
    static final String USED_MEMORY_KEY = "usedMemory";
    @VisibleForTesting
    static final String AVAILABLE_MEMORY = "availableMemory";
    @VisibleForTesting
    static final String AVAILABLE_MAX_JOB_CAPACITY = "availableMaxJobCapacity";

    private final JobSearchService jobSearchService;
    private final LocalAgentLauncherProperties launcherProperties;
    private final String hostname;

    /**
     * Constructor.
     *
     * @param jobSearchService   The {@link JobSearchService} implementation to use
     * @param launcherProperties The properties related to local agent launch
     * @param genieHostInfo      The {@link GenieHostInfo} object containing metadata about the current Genie host
     */
    public LocalAgentLauncherHealthIndicator(
        final JobSearchService jobSearchService,
        final LocalAgentLauncherProperties launcherProperties,
        final GenieHostInfo genieHostInfo
    ) {
        this.jobSearchService = jobSearchService;
        this.launcherProperties = launcherProperties;
        this.hostname = genieHostInfo.getHostname();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Health health() {
        final long allocatedMemoryOnHost = this.jobSearchService.getAllocatedMemoryOnHost(this.hostname);
        final long usedMemoryOnHost = this.jobSearchService.getUsedMemoryOnHost(this.hostname);

        // Use allocated memory to make the host go OOS early enough that we don't throw as many exceptions on
        // accepted jobs during launch
        final long availableMemory = this.launcherProperties.getMaxTotalJobMemory() - allocatedMemoryOnHost;
        final int maxJobMemory = this.launcherProperties.getMaxJobMemory();

        final Health.Builder builder;

        // If we can fit one more max job in we're still healthy
        if (availableMemory >= maxJobMemory) {
            builder = Health.up();
        } else {
            builder = Health.down();
        }

        return builder
            .withDetail(NUMBER_RUNNING_JOBS_KEY, this.jobSearchService.getActiveJobCountOnHost(this.hostname))
            .withDetail(ALLOCATED_MEMORY_KEY, allocatedMemoryOnHost)
            .withDetail(AVAILABLE_MEMORY, availableMemory)
            .withDetail(USED_MEMORY_KEY, usedMemoryOnHost)
            .withDetail(
                AVAILABLE_MAX_JOB_CAPACITY,
                (availableMemory >= 0 && maxJobMemory > 0) ? (availableMemory / maxJobMemory) : 0)
            .build();
    }
}
