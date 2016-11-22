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

import com.sun.management.OperatingSystemMXBean;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

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
    private final OperatingSystemMXBean operatingSystemMXBean;
    /**
     * Constructor.
     *
     * @param maxCpuLoadPercent The maximum physical memory threshold
     */
    @Autowired
    public GenieCpuHealthIndicator(
        @Value("${genie.health.cpu.maxLoadPercent:80}")
        final double maxCpuLoadPercent
    ) {
        this(maxCpuLoadPercent, (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean());
    }

    GenieCpuHealthIndicator(final double maxCpuLoadPercent,
                            final OperatingSystemMXBean operatingSystemMXBean) {
        this.maxCpuLoadPercent = maxCpuLoadPercent;
        this.operatingSystemMXBean = operatingSystemMXBean;
    }

    @Override
    public Health health() {
        final double currentCpuLoadPercent = operatingSystemMXBean.getSystemCpuLoad() * 100;
        if (currentCpuLoadPercent > maxCpuLoadPercent) {
            log.warn("CPU usage {} crossed the threshold of {}", currentCpuLoadPercent, maxCpuLoadPercent);
            return Health
                .outOfService()
                .withDetail(CPU_LOAD, currentCpuLoadPercent)
                .build();
        } else {
            return Health
                .up()
                .withDetail(CPU_LOAD, currentCpuLoadPercent)
                .build();
        }
    }
}
