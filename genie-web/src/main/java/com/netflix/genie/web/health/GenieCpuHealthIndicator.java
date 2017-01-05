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

import com.netflix.genie.core.properties.HealthProperties;
import com.netflix.spectator.api.DistributionSummary;
import com.netflix.spectator.api.Registry;
import com.sun.management.OperatingSystemMXBean;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

import javax.validation.constraints.NotNull;
import java.lang.management.ManagementFactory;

/**
 * Health indicator for system cpu usage. Mark the service out-of-service if it crosses the threshold.
 * @author amajumdar
 * @since 3.0.0
 */
@Component
@Slf4j
public class GenieCpuHealthIndicator implements HealthIndicator {
    private static final String CPU_LOAD = "cpuLoad";
    private final double maxCpuLoadPercent;
    private final int maxCpuLoadConsecutiveOccurrences;
    private final DistributionSummary summaryCpuMetric;
    private int cpuLoadConsecutiveOccurrences;
    /**
     * Constructor.
     *
     * @param healthProperties The maximum physical memory threshold
     * @param registry         Registry
     * @param taskScheduler    task scheduler
     */
    @Autowired
    public GenieCpuHealthIndicator(
        @NotNull final HealthProperties healthProperties,
        @NotNull final Registry registry,
        @NotNull final TaskScheduler taskScheduler
    ) {
        this(healthProperties.getMaxCpuLoadPercent(),
            healthProperties.getMaxCpuLoadConsecutiveOccurrences(),
            (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean(),
            registry.distributionSummary("genie.cpuLoad"),
            taskScheduler);
    }

    GenieCpuHealthIndicator(final double maxCpuLoadPercent,
        final int maxCpuLoadConsecutiveOccurrences,
        final OperatingSystemMXBean operatingSystemMXBean,
        final DistributionSummary summaryCpuMetric,
        final TaskScheduler taskScheduler) {
        this.maxCpuLoadPercent = maxCpuLoadPercent;
        this.maxCpuLoadConsecutiveOccurrences = maxCpuLoadConsecutiveOccurrences;
        this.summaryCpuMetric = summaryCpuMetric;
        this.summaryCpuMetric.record((long) (operatingSystemMXBean.getSystemCpuLoad() * 100));
        taskScheduler.scheduleAtFixedRate(() -> this.summaryCpuMetric
            .record((long) (operatingSystemMXBean.getSystemCpuLoad() * 100)), 5000);
    }

    @Override
    public Health health() {
        // Use the distribution summary to get an average of the cpu metrics.
        final long cpuCount = summaryCpuMetric.count();
        final long cpuTotal = summaryCpuMetric.totalAmount();
        final double currentCpuLoadPercent = cpuCount == 0 ? 0 : (cpuTotal / (double) cpuCount);
        if (currentCpuLoadPercent > maxCpuLoadPercent) {
            cpuLoadConsecutiveOccurrences++;
        }
        // Mark the service down only after a consecutive number of cpu load occurrences.
        if (cpuLoadConsecutiveOccurrences >= maxCpuLoadConsecutiveOccurrences) {
            log.warn("CPU usage {} crossed the threshold of {}", currentCpuLoadPercent, maxCpuLoadPercent);
            return Health
                .outOfService()
                .withDetail(CPU_LOAD, currentCpuLoadPercent)
                .build();
        } else {
            cpuLoadConsecutiveOccurrences = 0;
            return Health
                .up()
                .withDetail(CPU_LOAD, currentCpuLoadPercent)
                .build();
        }
    }
}
