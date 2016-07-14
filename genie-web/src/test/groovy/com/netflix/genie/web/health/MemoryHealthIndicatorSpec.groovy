package com.netflix.genie.web.health

import com.sun.management.OperatingSystemMXBean
import org.springframework.boot.actuate.health.Status
import spock.lang.Specification

/**
 * Unit tests for MemoryHealthIndicator.
 *
 * @author amajumdar
 * @since 3.0.0
 */
class MemoryHealthIndicatorSpec extends Specification{
    OperatingSystemMXBean operatingSystemMXBean
    MemoryHealthIndicator memoryHealthIndicator;

    def setup(){
        operatingSystemMXBean = Mock(OperatingSystemMXBean)
        memoryHealthIndicator = new MemoryHealthIndicator(80, operatingSystemMXBean)
    }

    def testHealth(){
        given:
        operatingSystemMXBean.getFreePhysicalMemorySize() >> freeMemory
        operatingSystemMXBean.getTotalPhysicalMemorySize() >> totalMemory
        expect:
        memoryHealthIndicator.health().getStatus() == status
        where:
        freeMemory | totalMemory | status
        10.0       | 100.0       | Status.OUT_OF_SERVICE
        20.0       | 100.0       | Status.UP
        30.0       | 100.0       | Status.UP
        30.0       | 50.0        | Status.UP
    }
}
