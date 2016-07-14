package com.netflix.genie.web.health;

import com.sun.management.OperatingSystemMXBean;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Health indicator for system memory usage. Also mark the service out-of-service if it crosses the threshold.
 * @author amajumdar
 * @since 3.0.0
 */
@Component
@Slf4j
public class MemoryHealthIndicator implements HealthIndicator {
    private static final String FREE_PHYSICAL_MEMORY_SIZE = "freePhysicalMemorySize";
    private static final String TOTAL_PHYSICAL_MEMORY_SIZE = "totalPhysicalMemorySize";

    private final double maxUsedPhysicalMemoryPercentage;
    private final OperatingSystemMXBean operatingSystemMXBean;

    /**
     * Constructor.
     *
     * @param maxUsedPhysicalMemoryPercentage The maximum physical memory threshold
     * @param operatingSystemMXBean MX bean for operating system
     */
    @Autowired
    public MemoryHealthIndicator(
            @Value("${genie.threshold.used-physical-memory-percent:90}")
            final double maxUsedPhysicalMemoryPercentage,
            final OperatingSystemMXBean operatingSystemMXBean
    ) {
        this.maxUsedPhysicalMemoryPercentage = maxUsedPhysicalMemoryPercentage;
        this.operatingSystemMXBean = operatingSystemMXBean;
    }

    @Override
    public Health health() {
        final double freePhysicalMemorySize = (double) operatingSystemMXBean.getFreePhysicalMemorySize();
        final double totalPhysicalMemorySize = (double) operatingSystemMXBean.getTotalPhysicalMemorySize();
        final double usedPhysicalMemoryPercentage = ((totalPhysicalMemorySize - freePhysicalMemorySize)
                / totalPhysicalMemorySize) * 100;

        if (usedPhysicalMemoryPercentage > maxUsedPhysicalMemoryPercentage) {
            log.warn("Physical memory {} crossed the threshold of {}", usedPhysicalMemoryPercentage,
                    maxUsedPhysicalMemoryPercentage);
            return Health
                    .outOfService()
                    .withDetail(FREE_PHYSICAL_MEMORY_SIZE, freePhysicalMemorySize)
                    .withDetail(TOTAL_PHYSICAL_MEMORY_SIZE, totalPhysicalMemorySize)
                    .build();
        } else {
            return Health
                    .up()
                    .withDetail(FREE_PHYSICAL_MEMORY_SIZE, freePhysicalMemorySize)
                    .withDetail(TOTAL_PHYSICAL_MEMORY_SIZE, totalPhysicalMemorySize)
                    .build();
        }
    }
}
